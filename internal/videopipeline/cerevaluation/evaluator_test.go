package cerevaluation

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	sampleReference  = "雨落了七夜，归航塔也暗了七夜。只要最后一盏灯还亮着，河就记得回家的方向。风会吹灭火，却吹不散守灯人的约定。小萤，醒来吧。一盏灯，接住另一盏灯。今夜，雨会停。"
	sampleHypothesis = "雨落了七夜归航塔也暗了七夜只要最后一盏灯还亮着何就记得回家的方向风会吹灭火却吹不散守灯人的约定小英醒来吧一盏灯接住另一盏灯今夜雨会停"
)

func TestEvaluate_FLO104FrozenSample(t *testing.T) {
	t.Parallel()
	input := validInput(sampleReference, sampleHypothesis)
	evidence, err := Evaluate(input)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Normalization.ReferenceCharacters != 66 {
		t.Fatalf("reference characters = %d, want 66", evidence.Normalization.ReferenceCharacters)
	}
	if evidence.StandardCER.Errors != 2 || evidence.StandardCER.Percent != 3.0303 || evidence.StandardCER.Status != "diagnostic_failed" {
		t.Fatalf("unexpected standard CER: %#v", evidence.StandardCER)
	}
	if evidence.MandarinToneAwareCER.Errors != 1 || evidence.MandarinToneAwareCER.Percent != 1.5152 || evidence.MandarinToneAwareCER.Status != "passed" {
		t.Fatalf("unexpected tone-aware CER: %#v", evidence.MandarinToneAwareCER)
	}
	var river, firefly *EditOperation
	for index := range evidence.Operations {
		operation := &evidence.Operations[index]
		if operation.Reference == "河" {
			river = operation
		}
		if operation.Reference == "萤" {
			firefly = operation
		}
	}
	if river == nil || !river.Reclassified || river.ToneAwareCost != 0 || strings.Join(river.SharedReadings, ",") != "hé" {
		t.Fatalf("河→何 was not reclassified from Unihan: %#v", river)
	}
	if firefly == nil || firefly.Reclassified || firefly.ToneAwareCost != 1 || len(firefly.SharedReadings) != 0 {
		t.Fatalf("萤→英 must retain its tone-aware error: %#v", firefly)
	}
	if evidence.ContentHash == "" || !validDigest(evidence.ContentHash) {
		t.Fatalf("evidence content hash is invalid: %q", evidence.ContentHash)
	}
}

func TestEvaluate_ReclassifiesOnlyAlignedHanSubstitutions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		reference  string
		hypothesis string
		standard   int
		toneAware  int
	}{
		{name: "same tone homophone", reference: "河", hypothesis: "何", standard: 1, toneAware: 0},
		{name: "different tone homophone", reference: "萤", hypothesis: "英", standard: 1, toneAware: 1},
		{name: "insertion is retained", reference: "河", hypothesis: "河何", standard: 1, toneAware: 1},
		{name: "deletion is retained", reference: "河何", hypothesis: "河", standard: 1, toneAware: 1},
		{name: "non Han substitution is retained", reference: "A", hypothesis: "a", standard: 1, toneAware: 1},
		{name: "missing reading is retained", reference: "河", hypothesis: "𰻞", standard: 1, toneAware: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			evidence, err := Evaluate(validInput(test.reference, test.hypothesis))
			if err != nil {
				t.Fatal(err)
			}
			if evidence.StandardCER.Errors != test.standard || evidence.MandarinToneAwareCER.Errors != test.toneAware {
				t.Fatalf("CER = %d/%d, want %d/%d", evidence.StandardCER.Errors, evidence.MandarinToneAwareCER.Errors, test.standard, test.toneAware)
			}
		})
	}
}

func TestEvaluate_UsesFrozenNormalizationAndTieBreak(t *testing.T) {
	t.Parallel()
	evidence, err := Evaluate(validInput("Ａ，風 河", "A风何"))
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Normalization.Reference != "A风河" || evidence.Normalization.Hypothesis != "A风何" {
		t.Fatalf("unexpected normalization: %#v", evidence.Normalization)
	}
	if evidence.StandardCER.Errors != 1 || evidence.MandarinToneAwareCER.Errors != 0 {
		t.Fatalf("unexpected normalized CER: %#v %#v", evidence.StandardCER, evidence.MandarinToneAwareCER)
	}

	operations := levenshteinOperations([]rune("ab"), []rune("ba"))
	if len(operations) != 2 || operations[0].Type != "substitution" || operations[1].Type != "substitution" {
		t.Fatalf("tie-break did not prefer substitutions: %#v", operations)
	}
}

func TestInputValidateRejectsProtocolDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "model revision", mutate: func(input *Input) { input.ASR.ModelRevision = strings.Repeat("0", 40) }},
		{name: "reference prompt", mutate: func(input *Input) { value := "河"; input.ASR.ReferencePrompt = &value }},
		{name: "project dictionary", mutate: func(input *Input) { input.ASR.ProjectDictionary = true }},
		{name: "post hoc allowlist", mutate: func(input *Input) { input.ASR.PostHocAllowlist = true }},
		{name: "provider submit", mutate: func(input *Input) { input.ProviderSubmitCount = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := validInput("河", "何")
			test.mutate(&input)
			if err := input.Validate(); err == nil {
				t.Fatal("expected protocol drift to fail closed")
			}
		})
	}
}

func TestEvaluateFileBindsDialogueBytes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dialogue.wav")
	data := []byte("immutable dialogue fixture")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	input := validInput("河", "何")
	input.SourceDialogue.SHA256 = hex.EncodeToString(sum[:])
	if _, err := EvaluateFile(input, path); err != nil {
		t.Fatal(err)
	}
	input.SourceDialogue.SHA256 = strings.Repeat("0", 64)
	if _, err := EvaluateFile(input, path); err == nil {
		t.Fatal("expected Dialogue hash drift to fail closed")
	}
}

func TestUnihanDataIsFrozenAndGeneral(t *testing.T) {
	t.Parallel()
	readings, evidence, err := loadMandarinReadings()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.UnicodeVersion != "17.0.0" || evidence.SubsetFileSHA256 != UnihanSubsetFileSHA256 || evidence.EntryCount != 44_348 {
		t.Fatalf("unexpected Unihan evidence: %#v", evidence)
	}
	tests := []struct {
		name string
		char rune
		want string
	}{
		{name: "river", char: '河', want: "hé"},
		{name: "what", char: '何', want: "hé"},
		{name: "firefly", char: '萤', want: "yíng"},
		{name: "lustrous", char: '莹', want: "yíng"},
		{name: "hero", char: '英', want: "yīng"},
		{name: "polyphonic", char: '万', want: "wàn mò"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := strings.Join(readings[test.char], " "); got != test.want {
				t.Fatalf("readings[%q] = %q, want %q", test.char, got, test.want)
			}
		})
	}
}

func validInput(reference, hypothesis string) Input {
	return Input{
		SchemaVersion: InputSchemaVersion, EvaluationID: "flo104-sample1-audio-timing-v4",
		SourceDialogue: SourceDialogue{SHA256: strings.Repeat("a", 64), DurationMillis: 50_000, SampleRate: 48_000, Channels: 2},
		ASR:            FrozenASRConfig(), ReferenceText: reference, HypothesisText: hypothesis,
	}
}
