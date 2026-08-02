package postproduction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/analyzerseal"
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

func TestSealedCommandAudioAnalyzerRejectsRuntimeDriftBeforeLaunch(t *testing.T) {
	tests := []struct {
		name   string
		target string
	}{
		{name: "executable", target: "executable"},
		{name: "config", target: "config"},
		{name: "ASR model", target: "asr_model"},
		{name: "license snapshot", target: "license_snapshot"},
		{name: "environment inventory", target: "python_environment"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeCommandAnalyzerSealFixture(t)
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			request := nativeAudioRequest(t)
			rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			analyzer, err := NewSealedCommandAudioAnalyzer(
				fixture.program, fixture.root, fixture.seal, store,
			)
			if err != nil {
				t.Fatal(err)
			}
			runner := &analyzerFixtureRunner{}
			analyzer.Runner = runner
			if analyzer.AnalyzerSealSHA256() != fixture.sealSHA256 {
				t.Fatal("fresh analyzer did not report its verified seal")
			}
			if err := os.WriteFile(fixture.targets[test.target], []byte("runtime drift\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = analyzer.Analyze(t.Context(), AudioAnalysisRequest{
				Request: request, NativeMixes: rendered.NativeMixes,
				FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
			})
			if providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
				t.Fatalf("Analyze() error = %v, want unavailable integrity error", err)
			}
			if runner.calls != 0 {
				t.Fatalf("drifted analyzer command launched %d times", runner.calls)
			}
			if analyzer.AnalyzerSealSHA256() != "" {
				t.Fatal("drifted analyzer continued reporting its cached seal")
			}
		})
	}
}

func TestSealedCommandAudioAnalyzerRejectsDriftDuringCommandBeforeQCEvidence(t *testing.T) {
	fixture := writeCommandAnalyzerSealFixture(t)
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	analyzer, err := NewSealedCommandAudioAnalyzer(fixture.program, fixture.root, fixture.seal, store)
	if err != nil {
		t.Fatal(err)
	}
	runner := &analyzerFixtureRunner{beforeOutput: func() error {
		return os.WriteFile(fixture.targets["config"], []byte("drift during command\n"), 0o600)
	}}
	analyzer.Runner = runner
	_, err = analyzer.Analyze(t.Context(), AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	})
	if providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
		t.Fatalf("Analyze() error = %v, want unavailable integrity error", err)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
}

func TestSealedCommandAudioAnalyzerAllowsUnchangedFixture(t *testing.T) {
	fixture := writeCommandAnalyzerSealFixture(t)
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	analyzer, err := NewSealedCommandAudioAnalyzer(fixture.program, fixture.root, fixture.seal, store)
	if err != nil {
		t.Fatal(err)
	}
	runner := &analyzerFixtureRunner{}
	analyzer.Runner = runner
	analysis, err := analyzer.Analyze(t.Context(), AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if runner.calls != 1 || analysis.ContentHash == "" {
		t.Fatalf("runner calls=%d analysis=%#v", runner.calls, analysis)
	}
}

func TestServiceRejectsRuntimeAnalyzerDriftWithoutQCEvidence(t *testing.T) {
	fixture := writeCommandAnalyzerSealFixture(t)
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	analyzer, err := NewSealedCommandAudioAnalyzer(fixture.program, fixture.root, fixture.seal, store)
	if err != nil {
		t.Fatal(err)
	}
	runner := &analyzerFixtureRunner{}
	analyzer.Runner = runner
	service, err := NewService(nil, &fakeMedia{store: store}, store, analyzer)
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	request.AnalyzerSealSHA256 = fixture.sealSHA256
	if err := os.WriteFile(fixture.targets["asr_model"], []byte("runtime drift\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := service.Finalize(t.Context(), request)
	if providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
		t.Fatalf("Finalize() error = %v, want unavailable integrity error", err)
	}
	if runner.calls != 0 || result.AudioQC.Kind != "" {
		t.Fatalf("runner calls=%d result=%#v", runner.calls, result)
	}
}

type analyzerFixtureRunner struct {
	input        []byte
	calls        int
	beforeOutput func() error
}

func (r *analyzerFixtureRunner) Run(
	_ context.Context,
	_ string,
	_ string,
	args ...string,
) ([]byte, error) {
	r.calls++
	input, err := os.ReadFile(args[0])
	if err != nil {
		return nil, err
	}
	r.input = input
	var payload analyzerCommandInput
	if err := json.Unmarshal(input, &payload); err != nil {
		return nil, err
	}
	if r.beforeOutput != nil {
		if err := r.beforeOutput(); err != nil {
			return nil, err
		}
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

type commandAnalyzerSealFixture struct {
	root, seal, program, sealSHA256 string
	targets                         map[string]string
}

func writeCommandAnalyzerSealFixture(t *testing.T) commandAnalyzerSealFixture {
	t.Helper()
	root := t.TempDir()
	targets := make(map[string]string)
	write := func(name, contents string, mode os.FileMode) analyzerseal.Artifact {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		data := []byte(contents)
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
		return analyzerseal.Artifact{
			Path: name, SHA256: commandAnalyzerDigest(data), Version: "v1",
		}
	}
	analyzer := write("bin/analyzer", "#!/bin/sh\nexit 0\n", 0o750)
	analyzer.Executable = true
	targets["executable"] = filepath.Join(root, analyzer.Path)
	config := write("config/analyzer.json", "{}\n", 0o640)
	targets["config"] = filepath.Join(root, config.Path)
	manifest := analyzerseal.Manifest{
		SchemaVersion: analyzerseal.SchemaVersion, Analyzer: analyzer, Config: config,
		Offline: analyzerseal.Offline{
			Network: "disabled", CommandSchema: audioAnalyzerCommandSchemaVersion,
		},
	}
	for _, kind := range []string{
		"asr_model", "tokenizer", "normalizer", "vad", "face_mouth",
		"av_sync", "ffmpeg", "ffprobe", "license_snapshot",
		"python_environment",
	} {
		artifact := write("components/"+kind+".bin", kind+" fixture\n", 0o640)
		manifest.Components = append(manifest.Components, analyzerseal.Component{
			Name: kind, Kind: kind, Path: artifact.Path, SHA256: artifact.SHA256,
			Version: "v1", SPDXLicense: "MIT", CommercialUse: true,
			Source: "https://example.invalid/" + kind,
		})
		targets[kind] = filepath.Join(root, artifact.Path)
	}
	seal := filepath.Join(root, "analyzer-seal.json")
	sealBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seal, sealBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	return commandAnalyzerSealFixture{
		root: root, seal: seal, program: targets["executable"],
		sealSHA256: commandAnalyzerDigest(sealBytes), targets: targets,
	}
}

func commandAnalyzerDigest(data []byte) string {
	value := sha256.Sum256(data)
	return hex.EncodeToString(value[:])
}
