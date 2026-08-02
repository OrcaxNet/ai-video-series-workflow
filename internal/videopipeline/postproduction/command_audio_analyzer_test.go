package postproduction

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

func TestCommandAudioAnalyzerBindsStrictOutputWithoutLeakingReferenceText(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runner := &analyzerFixtureRunner{}
	analyzer := &CommandAudioAnalyzer{Program: "fixture-analyzer", Store: store, Runner: runner}
	analysis, err := analyzer.Analyze(t.Context(), AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if analysis.ContentHash == "" || analysis.Transcript != "第一句第二句" {
		t.Fatalf("analysis = %#v", analysis)
	}
	if strings.Contains(string(runner.input), "第一句") || strings.Contains(string(runner.input), "第二句") {
		t.Fatal("analyzer command input leaked the reference subtitle text")
	}
	if !strings.Contains(string(runner.input), "contextSnapshotHash") ||
		!strings.Contains(string(runner.input), "faster-whisper-large-v3-turbo") ||
		!strings.Contains(string(runner.input), "lipSyncRunId") {
		t.Fatal("analyzer command input omitted frozen context or ASR provenance")
	}
}

type analyzerFixtureRunner struct {
	input []byte
}

func (r *analyzerFixtureRunner) Run(
	_ context.Context,
	_ string,
	_ string,
	args ...string,
) ([]byte, error) {
	input, err := os.ReadFile(args[0])
	if err != nil {
		return nil, err
	}
	r.input = input
	var payload analyzerCommandInput
	if err := json.Unmarshal(input, &payload); err != nil {
		return nil, err
	}
	sources := []string{payload.FinalMix.SHA256, payload.FinalVideo.SHA256}
	for _, mix := range payload.NativeMixes {
		sources = append(sources, mix.SHA256)
	}
	analysis := AudioAnalysis{
		SchemaVersion: AudioAnalysisSchemaVersion,
		AnalysisID:    "command-analysis-v1", Analyzer: "fixture", AnalyzerVersion: "v1",
		Evidence: payload.Evidence, ASR: payload.ASR, Transcript: "第一句第二句",
		SourceHashes: sources, AudioVideoStartMillis: make([]int64, len(payload.RunWindows)),
		IntegratedLUFS: -16, TruePeakDBTP: -1.2,
	}
	for _, cue := range payload.CueWindows {
		analysis.CueTimings = append(analysis.CueTimings, CueTimingMeasurement{
			CueID: cue.CueID, SpeechStartMillis: cue.StartMillis, SpeechEndMillis: cue.EndMillis,
		})
		analysis.LipSync = append(analysis.LipSync, LipSyncMeasurement{
			RunID: cue.LipSyncRunID, CueID: cue.CueID, Required: cue.LipSyncRequired,
			AudioStartMillis: cue.LipSyncStartMillis, AudioEndMillis: cue.LipSyncEndMillis,
			MouthStartMillis: cue.LipSyncStartMillis, MouthEndMillis: cue.LipSyncEndMillis,
		})
	}
	encoded, err := json.Marshal(analysis)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(args[1], encoded, 0o600); err != nil {
		return nil, err
	}
	return nil, nil
}
