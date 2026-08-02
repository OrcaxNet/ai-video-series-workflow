package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
)

func TestRunCreatesExclusiveEvidence(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	dialoguePath := filepath.Join(directory, "dialogue.wav")
	dialogue := []byte("frozen dialogue")
	if err := os.WriteFile(dialoguePath, dialogue, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(dialogue)
	input := cerevaluation.Input{
		SchemaVersion: cerevaluation.InputSchemaVersion,
		EvaluationID:  "fixture",
		SourceDialogue: cerevaluation.SourceDialogue{
			SHA256: hex.EncodeToString(sum[:]), DurationMillis: 50_000, SampleRate: 48_000, Channels: 2,
		},
		ASR: cerevaluation.FrozenASRConfig(), ReferenceText: "河", HypothesisText: "何",
	}
	inputPath := filepath.Join(directory, "input.json")
	inputBytes, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, inputBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(directory, "evidence.json")
	if err := run([]string{inputPath, dialoguePath, outputPath}); err != nil {
		t.Fatal(err)
	}
	var evidence cerevaluation.Evidence
	evidenceBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(evidenceBytes, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.StandardCER.Errors != 1 || evidence.MandarinToneAwareCER.Errors != 0 {
		t.Fatalf("unexpected evidence: %#v", evidence)
	}
	if err := run([]string{inputPath, dialoguePath, outputPath}); err == nil || !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("existing evidence must not be overwritten: %v", err)
	}
}
