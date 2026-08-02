// Package cerevaluation implements the frozen FLO-104 standard and Mandarin
// tone-aware character error rate protocol. The tone-aware metric is a product
// release gate; the standard metric remains an unmodified diagnostic.
package cerevaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/longbridgeapp/opencc"
	"golang.org/x/text/unicode/norm"
)

const (
	InputSchemaVersion    = "flo104.cer-evaluation-input.v1"
	EvidenceSchemaVersion = "flo104.cer-evidence.v1"
	EvaluatorRevision     = "mandarin-tone-aware-cer-v1"
	GateThresholdPercent  = 2.0

	ASRModelID               = "mobiuslabsgmbh/faster-whisper-large-v3-turbo"
	ASRModelRevision         = "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf"
	FasterWhisperVersion     = "1.2.1"
	ASRDevice                = "cpu"
	ASRComputeType           = "int8"
	ASRLanguage              = "zh"
	ASRBeamSize              = 5
	OpenCCVersion            = "v0.3.13"
	OpenCCConfiguration      = "t2s"
	UnicodeNormalizationForm = "NFKC"
)

type SourceDialogue struct {
	SHA256         string `json:"sha256"`
	DurationMillis int64  `json:"durationMillis"`
	SampleRate     int    `json:"sampleRate"`
	Channels       int    `json:"channels"`
}

type ASRConfig struct {
	ModelID           string  `json:"modelId"`
	ModelRevision     string  `json:"modelRevision"`
	FasterWhisper     string  `json:"fasterWhisperVersion"`
	Device            string  `json:"device"`
	ComputeType       string  `json:"computeType"`
	Language          string  `json:"language"`
	BeamSize          int     `json:"beamSize"`
	VADFilter         bool    `json:"vadFilter"`
	ReferencePrompt   *string `json:"referencePrompt"`
	ProjectDictionary bool    `json:"projectDictionary"`
	PostHocAllowlist  bool    `json:"postHocAllowlist"`
}

func FrozenASRConfig() ASRConfig {
	return ASRConfig{
		ModelID: ASRModelID, ModelRevision: ASRModelRevision,
		FasterWhisper: FasterWhisperVersion, Device: ASRDevice,
		ComputeType: ASRComputeType, Language: ASRLanguage,
		BeamSize: ASRBeamSize, VADFilter: true,
	}
}

type Input struct {
	SchemaVersion       string         `json:"schemaVersion"`
	EvaluationID        string         `json:"evaluationId"`
	SourceDialogue      SourceDialogue `json:"sourceDialogue"`
	ASR                 ASRConfig      `json:"asr"`
	ReferenceText       string         `json:"referenceText"`
	HypothesisText      string         `json:"hypothesisText"`
	ProviderSubmitCount int            `json:"providerSubmitCount"`
}

func (i Input) Validate() error {
	switch {
	case i.SchemaVersion != InputSchemaVersion:
		return fmt.Errorf("CER input schemaVersion must be %s", InputSchemaVersion)
	case strings.TrimSpace(i.EvaluationID) == "":
		return errors.New("CER evaluationId is required")
	case !validDigest(i.SourceDialogue.SHA256):
		return errors.New("source Dialogue SHA-256 must be lowercase hexadecimal")
	case i.SourceDialogue.DurationMillis <= 0 || i.SourceDialogue.SampleRate <= 0 || i.SourceDialogue.Channels <= 0:
		return errors.New("source Dialogue media specification must be positive")
	case strings.TrimSpace(i.ReferenceText) == "":
		return errors.New("CER reference text is required")
	case strings.TrimSpace(i.HypothesisText) == "":
		return errors.New("CER hypothesis text is required")
	case i.ProviderSubmitCount != 0:
		return errors.New("FLO-104 CER evaluation permits zero Provider submits")
	}
	return i.ASR.validateFrozen()
}

func (c ASRConfig) validateFrozen() error {
	expected := FrozenASRConfig()
	if c.ModelID != expected.ModelID || c.ModelRevision != expected.ModelRevision ||
		c.FasterWhisper != expected.FasterWhisper || c.Device != expected.Device ||
		c.ComputeType != expected.ComputeType || c.Language != expected.Language ||
		c.BeamSize != expected.BeamSize || c.VADFilter != expected.VADFilter {
		return errors.New("ASR configuration does not match the frozen FLO-104 protocol")
	}
	if c.ReferencePrompt != nil {
		return errors.New("referencePrompt must be null")
	}
	if c.ProjectDictionary || c.PostHocAllowlist {
		return errors.New("project dictionaries and post-hoc allowlists are forbidden")
	}
	return nil
}

type NormalizationEvidence struct {
	UnicodeForm             string `json:"unicodeForm"`
	SimplifiedChineseEngine string `json:"simplifiedChineseEngine"`
	SimplifiedChineseConfig string `json:"simplifiedChineseConfig"`
	PunctuationRemoved      bool   `json:"punctuationRemoved"`
	WhitespaceRemoved       bool   `json:"whitespaceRemoved"`
	Reference               string `json:"reference"`
	Hypothesis              string `json:"hypothesis"`
	ReferenceCharacters     int    `json:"referenceCharacters"`
	HypothesisCharacters    int    `json:"hypothesisCharacters"`
}

type DataEvidence struct {
	UnicodeVersion      string `json:"unicodeVersion"`
	Property            string `json:"property"`
	SourceURL           string `json:"sourceUrl"`
	SourceArchiveSHA256 string `json:"sourceArchiveSha256"`
	SubsetFile          string `json:"subsetFile"`
	SubsetFileSHA256    string `json:"subsetFileSha256"`
	EntryCount          int    `json:"entryCount"`
}

type EditOperation struct {
	Index              int      `json:"index"`
	Type               string   `json:"type"`
	ReferenceIndex     *int     `json:"referenceIndex"`
	HypothesisIndex    *int     `json:"hypothesisIndex"`
	Reference          string   `json:"reference"`
	Hypothesis         string   `json:"hypothesis"`
	StandardCost       int      `json:"standardCost"`
	ToneAwareCost      int      `json:"toneAwareCost"`
	ReferenceReadings  []string `json:"referenceReadings,omitempty"`
	HypothesisReadings []string `json:"hypothesisReadings,omitempty"`
	SharedReadings     []string `json:"sharedToneBearingReadings,omitempty"`
	Reclassified       bool     `json:"reclassified"`
}

type Metric struct {
	Errors    int     `json:"errors"`
	Reference int     `json:"referenceCharacters"`
	Percent   float64 `json:"percent"`
	Threshold float64 `json:"thresholdPercent"`
	Status    string  `json:"status"`
}

type Evidence struct {
	SchemaVersion        string                `json:"schemaVersion"`
	EvaluatorRevision    string                `json:"evaluatorRevision"`
	EvaluationID         string                `json:"evaluationId"`
	SourceDialogue       SourceDialogue        `json:"sourceDialogue"`
	ASR                  ASRConfig             `json:"asr"`
	Normalization        NormalizationEvidence `json:"normalization"`
	Unihan               DataEvidence          `json:"unihan"`
	TieBreak             []string              `json:"levenshteinTieBreak"`
	ReferenceText        string                `json:"referenceText"`
	HypothesisText       string                `json:"hypothesisText"`
	Operations           []EditOperation       `json:"editOperations"`
	StandardCER          Metric                `json:"standardCer"`
	MandarinToneAwareCER Metric                `json:"mandarinToneAwareCer"`
	ProviderSubmitCount  int                   `json:"providerSubmitCount"`
	ContentHash          string                `json:"contentHash,omitempty"`
}

func EvaluateFile(input Input, dialoguePath string) (Evidence, error) {
	if err := input.Validate(); err != nil {
		return Evidence{}, err
	}
	digest, err := fileDigest(dialoguePath)
	if err != nil {
		return Evidence{}, fmt.Errorf("hash source Dialogue: %w", err)
	}
	if digest != input.SourceDialogue.SHA256 {
		return Evidence{}, fmt.Errorf("source Dialogue SHA-256 is %s, expected %s", digest, input.SourceDialogue.SHA256)
	}
	return Evaluate(input)
}

func Evaluate(input Input) (Evidence, error) {
	if err := input.Validate(); err != nil {
		return Evidence{}, err
	}
	reference, err := normalizeText(input.ReferenceText)
	if err != nil {
		return Evidence{}, fmt.Errorf("normalize reference: %w", err)
	}
	hypothesis, err := normalizeText(input.HypothesisText)
	if err != nil {
		return Evidence{}, fmt.Errorf("normalize hypothesis: %w", err)
	}
	if reference == "" || hypothesis == "" {
		return Evidence{}, errors.New("normalized CER texts must be non-empty")
	}
	readings, dataEvidence, err := loadMandarinReadings()
	if err != nil {
		return Evidence{}, err
	}
	operations := levenshteinOperations([]rune(reference), []rune(hypothesis))
	standardErrors := 0
	toneAwareErrors := 0
	for index := range operations {
		operation := &operations[index]
		standardErrors += operation.StandardCost
		operation.ToneAwareCost = operation.StandardCost
		if operation.Type == "substitution" {
			refRune := []rune(operation.Reference)[0]
			hypRune := []rune(operation.Hypothesis)[0]
			if unicode.Is(unicode.Han, refRune) && unicode.Is(unicode.Han, hypRune) {
				operation.ReferenceReadings = append([]string(nil), readings[refRune]...)
				operation.HypothesisReadings = append([]string(nil), readings[hypRune]...)
				operation.SharedReadings = sharedReadings(operation.ReferenceReadings, operation.HypothesisReadings)
				if len(operation.SharedReadings) > 0 {
					operation.ToneAwareCost = 0
					operation.Reclassified = true
				}
			}
		}
		toneAwareErrors += operation.ToneAwareCost
	}
	referenceCharacters := len([]rune(reference))
	evidence := Evidence{
		SchemaVersion: EvidenceSchemaVersion, EvaluatorRevision: EvaluatorRevision,
		EvaluationID: input.EvaluationID, SourceDialogue: input.SourceDialogue, ASR: input.ASR,
		Normalization: NormalizationEvidence{
			UnicodeForm:             UnicodeNormalizationForm,
			SimplifiedChineseEngine: "github.com/longbridgeapp/opencc@" + OpenCCVersion,
			SimplifiedChineseConfig: OpenCCConfiguration,
			PunctuationRemoved:      true, WhitespaceRemoved: true,
			Reference: reference, Hypothesis: hypothesis,
			ReferenceCharacters:  referenceCharacters,
			HypothesisCharacters: len([]rune(hypothesis)),
		},
		Unihan:        dataEvidence,
		TieBreak:      []string{"match", "substitution", "deletion", "insertion"},
		ReferenceText: input.ReferenceText, HypothesisText: input.HypothesisText,
		Operations:           operations,
		StandardCER:          metric(standardErrors, referenceCharacters, false),
		MandarinToneAwareCER: metric(toneAwareErrors, referenceCharacters, true),
		ProviderSubmitCount:  input.ProviderSubmitCount,
	}
	payload, err := json.Marshal(evidence)
	if err != nil {
		return Evidence{}, fmt.Errorf("encode CER evidence for hashing: %w", err)
	}
	sum := sha256.Sum256(append(payload, '\n'))
	evidence.ContentHash = hex.EncodeToString(sum[:])
	return evidence, nil
}

func normalizeText(value string) (string, error) {
	converter, err := opencc.New(OpenCCConfiguration)
	if err != nil {
		return "", err
	}
	simplified, err := converter.Convert(norm.NFKC.String(value))
	if err != nil {
		return "", err
	}
	var normalized strings.Builder
	for _, char := range simplified {
		if unicode.IsPunct(char) || unicode.IsSpace(char) {
			continue
		}
		normalized.WriteRune(char)
	}
	return normalized.String(), nil
}

func levenshteinOperations(reference, hypothesis []rune) []EditOperation {
	distance := make([][]int, len(reference)+1)
	for i := range distance {
		distance[i] = make([]int, len(hypothesis)+1)
		distance[i][0] = i
	}
	for j := range distance[0] {
		distance[0][j] = j
	}
	for i := 1; i <= len(reference); i++ {
		for j := 1; j <= len(hypothesis); j++ {
			cost := 1
			if reference[i-1] == hypothesis[j-1] {
				cost = 0
			}
			distance[i][j] = min3(
				distance[i-1][j-1]+cost,
				distance[i-1][j]+1,
				distance[i][j-1]+1,
			)
		}
	}

	i, j := len(reference), len(hypothesis)
	reversed := make([]EditOperation, 0, maxInt(i, j))
	for i > 0 || j > 0 {
		operation := EditOperation{}
		switch {
		case i > 0 && j > 0 && reference[i-1] == hypothesis[j-1] && distance[i][j] == distance[i-1][j-1]:
			i--
			j--
			operation.Type = "match"
			operation.Reference, operation.Hypothesis = string(reference[i]), string(hypothesis[j])
			operation.ReferenceIndex, operation.HypothesisIndex = intPointer(i), intPointer(j)
		case i > 0 && j > 0 && distance[i][j] == distance[i-1][j-1]+1:
			i--
			j--
			operation.Type = "substitution"
			operation.Reference, operation.Hypothesis = string(reference[i]), string(hypothesis[j])
			operation.ReferenceIndex, operation.HypothesisIndex = intPointer(i), intPointer(j)
			operation.StandardCost = 1
		case i > 0 && distance[i][j] == distance[i-1][j]+1:
			i--
			operation.Type = "deletion"
			operation.Reference = string(reference[i])
			operation.ReferenceIndex = intPointer(i)
			operation.StandardCost = 1
		default:
			j--
			operation.Type = "insertion"
			operation.Hypothesis = string(hypothesis[j])
			operation.HypothesisIndex = intPointer(j)
			operation.StandardCost = 1
		}
		reversed = append(reversed, operation)
	}
	operations := make([]EditOperation, len(reversed))
	for index := range reversed {
		operations[index] = reversed[len(reversed)-1-index]
		operations[index].Index = index
	}
	return operations
}

func metric(errors, reference int, releaseGate bool) Metric {
	percent := math.Round((float64(errors)/float64(reference))*100*10_000) / 10_000
	status := "diagnostic_passed"
	if errors*100 > reference*2 {
		status = "diagnostic_failed"
	}
	if releaseGate {
		status = "passed"
		if errors*100 > reference*2 {
			status = "failed"
		}
	}
	return Metric{Errors: errors, Reference: reference, Percent: percent, Threshold: GateThresholdPercent, Status: status}
}

func sharedReadings(reference, hypothesis []string) []string {
	seen := make(map[string]struct{}, len(reference))
	for _, reading := range reference {
		seen[reading] = struct{}{}
	}
	shared := make([]string, 0)
	for _, reading := range hypothesis {
		if _, ok := seen[reading]; ok {
			shared = append(shared, reading)
		}
	}
	sort.Strings(shared)
	return shared
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func intPointer(value int) *int { return &value }

func min3(first, second, third int) int {
	if first <= second && first <= third {
		return first
	}
	if second <= third {
		return second
	}
	return third
}

func maxInt(first, second int) int {
	if first > second {
		return first
	}
	return second
}
