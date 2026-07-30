package postproduction

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type Cue struct {
	ID          string `json:"id"`
	Speaker     string `json:"speaker,omitempty"`
	Text        string `json:"text"`
	VoiceRef    string `json:"voiceRef,omitempty"`
	StartMillis int64  `json:"startMillis"`
	EndMillis   int64  `json:"endMillis"`
}

func (c Cue) Validate() error {
	switch {
	case strings.TrimSpace(c.ID) == "":
		return errors.New("cue id is required")
	case !utf8.ValidString(c.Text) || strings.TrimSpace(c.Text) == "":
		return errors.New("cue text must be non-empty UTF-8")
	case strings.ContainsRune(c.Text, '\x00'):
		return errors.New("cue text cannot contain NUL")
	case c.StartMillis < 0 || c.EndMillis <= c.StartMillis:
		return errors.New("cue timestamps are invalid")
	}
	return nil
}

type SubtitleRevision struct {
	ID                string   `json:"id"`
	ParentRevisionID  string   `json:"parentRevisionId,omitempty"`
	Revision          int      `json:"revision"`
	Language          string   `json:"language"`
	SourceRevisionIDs []string `json:"sourceRevisionIds"`
	Cues              []Cue    `json:"cues"`
	ContentHash       string   `json:"contentHash"`
}

type subtitleDigestInput struct {
	ParentRevisionID  string   `json:"parentRevisionId,omitempty"`
	Revision          int      `json:"revision"`
	Language          string   `json:"language"`
	SourceRevisionIDs []string `json:"sourceRevisionIds"`
	Cues              []Cue    `json:"cues"`
}

func NewSubtitleRevision(
	id string,
	parentID string,
	revision int,
	language string,
	sourceRevisionIDs []string,
	cues []Cue,
) (SubtitleRevision, error) {
	result := SubtitleRevision{
		ID:                id,
		ParentRevisionID:  parentID,
		Revision:          revision,
		Language:          language,
		SourceRevisionIDs: append([]string(nil), sourceRevisionIDs...),
		Cues:              append([]Cue(nil), cues...),
	}
	hash, err := digestJSON(result.digestInput())
	if err != nil {
		return SubtitleRevision{}, fmt.Errorf("hash subtitle revision: %w", err)
	}
	result.ContentHash = hash
	if err := result.Validate(0); err != nil {
		return SubtitleRevision{}, err
	}
	return result, nil
}

// ReviseSubtitle creates a child revision and leaves the approved parent
// untouched. Callers must supply a new stable revision identifier.
func ReviseSubtitle(parent SubtitleRevision, id string, cues []Cue) (SubtitleRevision, error) {
	if err := parent.Validate(0); err != nil {
		return SubtitleRevision{}, fmt.Errorf("parent subtitle revision: %w", err)
	}
	if id == parent.ID {
		return SubtitleRevision{}, errors.New("a subtitle edit requires a new revision identifier")
	}
	return NewSubtitleRevision(
		id,
		parent.ID,
		parent.Revision+1,
		parent.Language,
		parent.SourceRevisionIDs,
		cues,
	)
}

func (s SubtitleRevision) Validate(episodeDurationMillis int64) error {
	switch {
	case strings.TrimSpace(s.ID) == "":
		return errors.New("subtitle revision id is required")
	case s.Revision < 1:
		return errors.New("subtitle revision must be positive")
	case s.Revision > 1 && strings.TrimSpace(s.ParentRevisionID) == "":
		return errors.New("edited subtitle revisions require a parent")
	case strings.TrimSpace(s.Language) == "":
		return errors.New("subtitle language is required")
	case len(s.SourceRevisionIDs) == 0:
		return errors.New("subtitle source revisions are required")
	case !validDigest(s.ContentHash):
		return errors.New("subtitle content hash is invalid")
	}
	expected, err := digestJSON(s.digestInput())
	if err != nil {
		return err
	}
	if expected != s.ContentHash {
		return errors.New("subtitle content hash does not match its immutable content")
	}
	var priorEnd int64
	seen := make(map[string]struct{}, len(s.Cues))
	for index, cue := range s.Cues {
		if err := cue.Validate(); err != nil {
			return fmt.Errorf("cue %d: %w", index, err)
		}
		if _, duplicate := seen[cue.ID]; duplicate {
			return fmt.Errorf("duplicate cue id %q", cue.ID)
		}
		seen[cue.ID] = struct{}{}
		if index > 0 && cue.StartMillis < priorEnd {
			return fmt.Errorf("cue %q overlaps the preceding cue", cue.ID)
		}
		if episodeDurationMillis > 0 && cue.EndMillis > episodeDurationMillis {
			return fmt.Errorf("cue %q exceeds episode duration", cue.ID)
		}
		priorEnd = cue.EndMillis
	}
	return nil
}

func (s SubtitleRevision) digestInput() subtitleDigestInput {
	return subtitleDigestInput{
		ParentRevisionID:  s.ParentRevisionID,
		Revision:          s.Revision,
		Language:          s.Language,
		SourceRevisionIDs: append([]string(nil), s.SourceRevisionIDs...),
		Cues:              append([]Cue(nil), s.Cues...),
	}
}

// RenderSRT emits canonical UTF-8 with LF line endings. No BOM is written.
func RenderSRT(subtitle SubtitleRevision, episodeDurationMillis int64) ([]byte, error) {
	if err := subtitle.Validate(episodeDurationMillis); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	for index, cue := range subtitle.Cues {
		output.WriteString(strconv.Itoa(index + 1))
		output.WriteByte('\n')
		output.WriteString(formatSRTTimestamp(cue.StartMillis))
		output.WriteString(" --> ")
		output.WriteString(formatSRTTimestamp(cue.EndMillis))
		output.WriteByte('\n')
		text := strings.ReplaceAll(cue.Text, "\r\n", "\n")
		text = strings.ReplaceAll(text, "\r", "\n")
		output.WriteString(strings.TrimSpace(text))
		output.WriteString("\n\n")
	}
	return output.Bytes(), nil
}

func formatSRTTimestamp(milliseconds int64) string {
	hours := milliseconds / 3_600_000
	milliseconds %= 3_600_000
	minutes := milliseconds / 60_000
	milliseconds %= 60_000
	seconds := milliseconds / 1_000
	milliseconds %= 1_000
	return fmt.Sprintf("%02d:%02d:%02d,%03d", hours, minutes, seconds, milliseconds)
}
