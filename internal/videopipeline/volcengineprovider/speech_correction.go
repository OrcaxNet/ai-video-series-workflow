package volcengineprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const SpeechDurationCorrectionSchema = "flo104-speech-duration-correction-v1"

// SpeechDurationCorrection is an append-only audit record. It corrects the
// interpretation of historical evidence without changing the provider job
// registry, Stage 1 ledger, or immutable audio object that it binds.
type SpeechDurationCorrection struct {
	SchemaVersion string `json:"schemaVersion"`
	IssueID       string `json:"issueId"`
	CreatedAt     string `json:"createdAt"`
	FixedGitSHA   string `json:"fixedGitSha"`

	Binding struct {
		ProviderRegistrySHA256 string `json:"providerRegistrySha256"`
		Stage1LedgerSHA256     string `json:"stage1LedgerSha256"`
		AudioCASURI            string `json:"audioCasUri"`
		AudioSHA256            string `json:"audioSha256"`
		RuntimeSBOMSHA256      string `json:"runtimeSbomSha256"`
	} `json:"binding"`

	Provider struct {
		JobID         string `json:"jobId"`
		RequestID     string `json:"requestId"`
		ConnectID     string `json:"connectId"`
		LogID         string `json:"logId"`
		ProviderCalls int    `json:"providerCallsAddedByCorrection"`
	} `json:"provider"`

	Duration struct {
		RequestedMillis        int64  `json:"requestedMillis"`
		PreviouslyRecordedMS   int64  `json:"previouslyRecordedMillis"`
		MeasuredMillis         int64  `json:"measuredMillis"`
		AbsoluteDeltaMillis    int64  `json:"absoluteDeltaMillis"`
		ToleranceMillis        int64  `json:"toleranceMillis"`
		QCState                string `json:"qcState"`
		DownstreamAuthority    string `json:"downstreamAuthority"`
		HistoricalRecordsState string `json:"historicalRecordsState"`
	} `json:"duration"`

	RuntimeProvenance struct {
		ListedImageSHA256 map[string]string `json:"listedImageSha256"`
		InstanceBinding   string            `json:"requestInstanceBinding"`
		Classification    string            `json:"evidenceClassification"`
		Reason            string            `json:"reason"`
	} `json:"runtimeProvenance"`
}

type SpeechDurationCorrectionOptions struct {
	IssueID string

	ProviderRegistryPath   string
	ProviderRegistrySHA256 string
	Stage1LedgerPath       string
	Stage1LedgerSHA256     string
	AudioPath              string
	AudioSHA256            string
	RuntimeSBOMPath        string
	RuntimeSBOMSHA256      string

	FixedGitSHA string
	CreatedAt   time.Time
	OutputPath  string
	Inspector   MediaInspector
}

// AppendSpeechDurationCorrection verifies every immutable input, measures the
// audio itself, and creates one correction with O_EXCL semantics. An exact
// replay is idempotent; conflicting bytes are never overwritten.
func AppendSpeechDurationCorrection(
	ctx context.Context,
	options SpeechDurationCorrectionOptions,
) (SpeechDurationCorrection, error) {
	var correction SpeechDurationCorrection
	if strings.TrimSpace(options.IssueID) == "" || !validGitSHA(options.FixedGitSHA) ||
		options.CreatedAt.IsZero() || strings.TrimSpace(options.OutputPath) == "" {
		return correction, errors.New("correction identity, fixed Git SHA, creation time, and output path are required")
	}
	registryBytes, err := readAndVerifyFile(options.ProviderRegistryPath, options.ProviderRegistrySHA256)
	if err != nil {
		return correction, fmt.Errorf("provider registry: %w", err)
	}
	if _, err := readAndVerifyFile(options.Stage1LedgerPath, options.Stage1LedgerSHA256); err != nil {
		return correction, fmt.Errorf("Stage 1 ledger: %w", err)
	}
	if _, err := readAndVerifyFile(options.AudioPath, options.AudioSHA256); err != nil {
		return correction, fmt.Errorf("audio CAS object: %w", err)
	}
	runtimeSBOMBytes, err := readAndVerifyFile(options.RuntimeSBOMPath, options.RuntimeSBOMSHA256)
	if err != nil {
		return correction, fmt.Errorf("runtime SBOM: %w", err)
	}

	var record jobRecord
	decoder := json.NewDecoder(bytes.NewReader(registryBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return correction, fmt.Errorf("decode strict provider registry: %w", err)
	}
	if err := ensureCorrectionJSONEOF(decoder); err != nil {
		return correction, err
	}
	if record.Response.State != providercontract.StatusSucceeded ||
		len(record.Response.Artifacts) != 1 ||
		record.Response.Artifacts[0].Kind != providercontract.ModalityAudio {
		return correction, errors.New("provider registry is not one successful speech artifact")
	}
	artifact := record.Response.Artifacts[0]
	if artifact.SHA256 != options.AudioSHA256 || artifact.URI != "cas://sha256/"+options.AudioSHA256 ||
		record.Expected.DurationMillis <= 0 || artifact.DurationMillis <= 0 ||
		strings.TrimSpace(record.Response.JobID) == "" || strings.TrimSpace(record.Response.RequestID) == "" ||
		strings.TrimSpace(record.Response.ConnectID) == "" || strings.TrimSpace(record.Response.LogID) == "" {
		return correction, errors.New("provider registry identity or historical duration is incomplete")
	}
	inspector := options.Inspector
	if inspector == nil {
		inspector = FFprobeInspector{}
	}
	measured, err := inspector.Inspect(ctx, options.AudioPath)
	if err != nil || measured.DurationMillis <= 0 {
		return correction, errors.New("audio CAS object could not be measured")
	}
	images, err := runtimeImageHashes(runtimeSBOMBytes)
	if err != nil {
		return correction, err
	}

	delta := measured.DurationMillis - int64(record.Expected.DurationMillis)
	if delta < 0 {
		delta = -delta
	}
	qcState := "passed"
	if delta > speechDurationToleranceMillis {
		qcState = "requires_adjustment"
	}
	correction.SchemaVersion = SpeechDurationCorrectionSchema
	correction.IssueID = options.IssueID
	correction.CreatedAt = options.CreatedAt.UTC().Format(time.RFC3339Nano)
	correction.FixedGitSHA = strings.ToLower(options.FixedGitSHA)
	correction.Binding.ProviderRegistrySHA256 = options.ProviderRegistrySHA256
	correction.Binding.Stage1LedgerSHA256 = options.Stage1LedgerSHA256
	correction.Binding.AudioCASURI = artifact.URI
	correction.Binding.AudioSHA256 = options.AudioSHA256
	correction.Binding.RuntimeSBOMSHA256 = options.RuntimeSBOMSHA256
	correction.Provider.JobID = record.Response.JobID
	correction.Provider.RequestID = record.Response.RequestID
	correction.Provider.ConnectID = record.Response.ConnectID
	correction.Provider.LogID = record.Response.LogID
	correction.Provider.ProviderCalls = 0
	correction.Duration.RequestedMillis = int64(record.Expected.DurationMillis)
	correction.Duration.PreviouslyRecordedMS = artifact.DurationMillis
	correction.Duration.MeasuredMillis = measured.DurationMillis
	correction.Duration.AbsoluteDeltaMillis = delta
	correction.Duration.ToleranceMillis = speechDurationToleranceMillis
	correction.Duration.QCState = qcState
	correction.Duration.DownstreamAuthority = "measuredMillis"
	correction.Duration.HistoricalRecordsState = "immutable_not_rewritten"
	correction.RuntimeProvenance.ListedImageSHA256 = images
	correction.RuntimeProvenance.InstanceBinding = "unverifiable"
	correction.RuntimeProvenance.Classification = "model_availability_only"
	correction.RuntimeProvenance.Reason = "the historical SBOM lists candidate images but does not bind this request/job to the Adapter and Worker instances that executed it"

	encoded, err := json.MarshalIndent(correction, "", "  ")
	if err != nil {
		return SpeechDurationCorrection{}, fmt.Errorf("encode correction: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := appendImmutableFile(options.OutputPath, encoded); err != nil {
		return SpeechDurationCorrection{}, err
	}
	return correction, nil
}

func readAndVerifyFile(path, expectedSHA256 string) ([]byte, error) {
	if strings.TrimSpace(path) == "" || !validLowerSHA256(expectedSHA256) {
		return nil, errors.New("path and lowercase SHA-256 are required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != expectedSHA256 {
		return nil, errors.New("SHA-256 does not match immutable bytes")
	}
	return data, nil
}

func runtimeImageHashes(data []byte) (map[string]string, error) {
	var sbom struct {
		Components []struct {
			Name   string `json:"name"`
			Hashes []struct {
				Algorithm string `json:"alg"`
				Content   string `json:"content"`
			} `json:"hashes"`
		} `json:"components"`
	}
	if err := json.Unmarshal(data, &sbom); err != nil {
		return nil, errors.New("runtime SBOM is invalid JSON")
	}
	wanted := map[string]string{
		"ai-video-series-volcengine-provider": "adapter",
		"ai-video-series-orchestrator-worker": "worker",
	}
	images := make(map[string]string, len(wanted))
	for _, component := range sbom.Components {
		role, ok := wanted[component.Name]
		if !ok {
			continue
		}
		for _, hash := range component.Hashes {
			if strings.EqualFold(hash.Algorithm, "SHA-256") && validLowerSHA256(hash.Content) {
				images[role] = hash.Content
				break
			}
		}
	}
	if len(images) != len(wanted) {
		return nil, errors.New("runtime SBOM lacks Adapter or Worker image SHA-256")
	}
	return images, nil
}

func appendImmutableFile(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create correction directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o440)
	if errors.Is(err, os.ErrExist) {
		existing, readErr := os.ReadFile(path)
		if readErr == nil && bytes.Equal(existing, content) {
			return nil
		}
		return errors.New("conflicting correction already exists")
	}
	if err != nil {
		return fmt.Errorf("create immutable correction: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write immutable correction: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync immutable correction: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close immutable correction: %w", err)
	}
	return nil
}

func ensureCorrectionJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("provider registry contains trailing JSON")
	}
	return nil
}

func validLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validGitSHA(value string) bool {
	if len(value) != 40 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
