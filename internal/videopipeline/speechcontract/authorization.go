// Package speechcontract defines the provider-neutral, immutable authorization
// envelope used to finish an approved ordered set of speech cues.
package speechcontract

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const SchemaVersion = "flo104.speech-batch-authorization.v1"

type CueAuthorization struct {
	CueID             string `json:"cueId"`
	JobID             string `json:"jobId"`
	InputHash         string `json:"inputHash"`
	UnicodeCharacters int    `json:"unicodeCharacters"`
	EstimatedAFPMilli int64  `json:"estimatedAfpMilli"`
	MaximumAFPMilli   int64  `json:"maximumAfpMilli"`
	MaxAttempts       int    `json:"maxAttempts"`
}

func (c CueAuthorization) Validate() error {
	switch {
	case strings.TrimSpace(c.CueID) == "":
		return errors.New("speech cue authorization requires cueId")
	case !strings.HasPrefix(c.JobID, "speech-v2-"):
		return errors.New("speech cue authorization requires a speech-v2 jobId")
	case !lowercaseSHA256(c.InputHash) || c.JobID != "speech-v2-"+c.InputHash[:32]:
		return errors.New("speech cue authorization jobId and inputHash do not match")
	case c.UnicodeCharacters <= 0:
		return errors.New("speech cue authorization character count must be positive")
	case c.EstimatedAFPMilli != int64(c.UnicodeCharacters)*135:
		return errors.New("speech cue authorization estimate must use 135 milli-AFP per Unicode character")
	case c.MaximumAFPMilli < c.EstimatedAFPMilli:
		return errors.New("speech cue authorization AFP ceiling is below its estimate")
	case c.MaxAttempts != 1:
		return errors.New("speech cue authorization requires MaxAttempts=1")
	}
	return nil
}

type BatchAuthorization struct {
	SchemaVersion                    string             `json:"schemaVersion"`
	ParentExecutionPackageHash       string             `json:"parentExecutionPackageHash"`
	ApprovalCommentID                string             `json:"approvalCommentId"`
	ApprovalActorID                  string             `json:"approvalActorId"`
	ValidUntil                       string             `json:"validUntil"`
	Provider                         string             `json:"provider"`
	ModelID                          string             `json:"modelId"`
	RouteVersion                     string             `json:"routeVersion"`
	ResourceID                       string             `json:"resourceId"`
	Speaker                          string             `json:"speaker"`
	VoiceAssetID                     string             `json:"voiceAssetId"`
	ParentVoiceAssetVersionID        string             `json:"parentVoiceAssetVersionId"`
	VoiceAssetVersionID              string             `json:"voiceAssetVersionId"`
	VoiceAssetVersionHash            string             `json:"voiceAssetVersionHash"`
	LicenseSnapshotID                string             `json:"licenseSnapshotId"`
	LicenseSnapshotHash              string             `json:"licenseSnapshotHash"`
	MaximumSubmits                   int                `json:"maximumSubmits"`
	EstimatedAFPMilli                int64              `json:"estimatedAfpMilli"`
	MaximumAFPMilli                  int64              `json:"maximumAfpMilli"`
	MaximumNonSubscriptionCashMicros int64              `json:"maximumNonSubscriptionCashMicros"`
	Cues                             []CueAuthorization `json:"cues"`
}

func (b BatchAuthorization) Validate() error {
	if b.SchemaVersion != SchemaVersion {
		return errors.New("speech batch authorization schemaVersion is invalid")
	}
	if !lowercaseSHA256(b.ParentExecutionPackageHash) {
		return errors.New("speech batch authorization parent package hash is invalid")
	}
	for name, value := range map[string]string{
		"approvalCommentId":  b.ApprovalCommentID,
		"approvalActorId":    b.ApprovalActorID,
		"voiceAssetId":       b.VoiceAssetID,
		"parentVoiceVersion": b.ParentVoiceAssetVersionID,
		"voiceVersion":       b.VoiceAssetVersionID,
		"licenseSnapshotId":  b.LicenseSnapshotID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("speech batch authorization %s must be a UUID", name)
		}
	}
	validUntil, err := time.Parse(time.RFC3339, b.ValidUntil)
	if err != nil || validUntil.UTC().Format(time.RFC3339) != b.ValidUntil {
		return errors.New("speech batch authorization validUntil must be canonical RFC3339")
	}
	if strings.TrimSpace(b.Provider) == "" || strings.TrimSpace(b.ModelID) == "" ||
		strings.TrimSpace(b.RouteVersion) == "" || strings.TrimSpace(b.ResourceID) == "" ||
		strings.TrimSpace(b.Speaker) == "" {
		return errors.New("speech batch authorization provider route and speaker are required")
	}
	if !lowercaseSHA256(b.VoiceAssetVersionHash) || !lowercaseSHA256(b.LicenseSnapshotHash) {
		return errors.New("speech batch authorization voice and license hashes are invalid")
	}
	if b.MaximumNonSubscriptionCashMicros != 0 {
		return errors.New("speech batch authorization requires a zero non-subscription cash ceiling")
	}
	if len(b.Cues) == 0 || b.MaximumSubmits != len(b.Cues) {
		return errors.New("speech batch maximum submits must equal the authorized cue count")
	}
	seenCues := make(map[string]struct{}, len(b.Cues))
	seenJobs := make(map[string]struct{}, len(b.Cues))
	seenInputs := make(map[string]struct{}, len(b.Cues))
	var estimated, maximum int64
	for index, cue := range b.Cues {
		if err := cue.Validate(); err != nil {
			return fmt.Errorf("speech batch cue %d: %w", index, err)
		}
		if _, duplicate := seenCues[cue.CueID]; duplicate {
			return fmt.Errorf("duplicate speech batch cue %q", cue.CueID)
		}
		if _, duplicate := seenJobs[cue.JobID]; duplicate {
			return fmt.Errorf("duplicate speech batch job %q", cue.JobID)
		}
		if _, duplicate := seenInputs[cue.InputHash]; duplicate {
			return fmt.Errorf("duplicate speech batch input %q", cue.InputHash)
		}
		seenCues[cue.CueID] = struct{}{}
		seenJobs[cue.JobID] = struct{}{}
		seenInputs[cue.InputHash] = struct{}{}
		estimated += cue.EstimatedAFPMilli
		maximum += cue.MaximumAFPMilli
	}
	if estimated != b.EstimatedAFPMilli || maximum != b.MaximumAFPMilli {
		return errors.New("speech batch aggregate AFP values do not equal the ordered cue totals")
	}
	return nil
}

func (b BatchAuthorization) Cue(cueID string) (CueAuthorization, int, bool) {
	for index, cue := range b.Cues {
		if cue.CueID == cueID {
			return cue, index, true
		}
	}
	return CueAuthorization{}, -1, false
}

func lowercaseSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
