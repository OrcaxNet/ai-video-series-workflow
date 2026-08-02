package postproduction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/analyzerseal"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
)

const audioAnalyzerCommandSchemaVersion = "flo154.audio-analyzer-command.v1"

// CommandAudioAnalyzer is the production boundary for a locally installed,
// frozen ASR/CV/audio-analysis executable. The executable receives no
// reference dialogue text, Provider credential, or network configuration. It
// writes one strict AudioAnalysis JSON object, which this adapter binds to the
// exact CAS inputs and hashes before the release gate evaluates it.
type CommandAudioAnalyzer struct {
	Program                     string
	Store                       *artifactstore.Store
	Runner                      CommandRunner
	sealSHA256, analyzerVersion string
}

// NewSealedCommandAudioAnalyzer accepts only the executable named by the
// verified local seal. The worker therefore cannot silently select another
// command after a native execution package has been approved.
func NewSealedCommandAudioAnalyzer(
	program, root, seal string,
	store *artifactstore.Store,
) (*CommandAudioAnalyzer, error) {
	manifest, evidence, err := analyzerseal.Verify(root, seal)
	if err != nil {
		return nil, fmt.Errorf("verify audio analyzer seal: %w", err)
	}
	wantProgram, err := filepath.Abs(filepath.Join(root, manifest.Analyzer.Path))
	if err != nil {
		return nil, err
	}
	gotProgram, err := filepath.Abs(program)
	if err != nil {
		return nil, err
	}
	if gotProgram != wantProgram {
		return nil, errors.New("audio analyzer command differs from the sealed executable")
	}
	analyzer, err := NewCommandAudioAnalyzer(wantProgram, store)
	if err != nil {
		return nil, err
	}
	analyzer.sealSHA256 = evidence.SealSHA256
	analyzer.analyzerVersion = manifest.Analyzer.Version
	return analyzer, nil
}

func (a *CommandAudioAnalyzer) AnalyzerSealSHA256() string {
	if a == nil {
		return ""
	}
	return a.sealSHA256
}

type analyzerMediaInput struct {
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	DurationMillis int64  `json:"durationMillis"`
}

type analyzerCueWindow struct {
	CueID              string `json:"cueId"`
	StartMillis        int64  `json:"startMillis"`
	EndMillis          int64  `json:"endMillis"`
	LipSyncRunID       string `json:"lipSyncRunId"`
	LipSyncStartMillis int64  `json:"lipSyncStartMillis"`
	LipSyncEndMillis   int64  `json:"lipSyncEndMillis"`
	LipSyncRequired    bool   `json:"lipSyncRequired"`
}

type analyzerRunWindow struct {
	RunID               string `json:"runId"`
	StartMillis         int64  `json:"startMillis"`
	EndMillis           int64  `json:"endMillis"`
	ContextSnapshotID   string `json:"contextSnapshotId"`
	ContextSnapshotHash string `json:"contextSnapshotHash"`
	AmbienceIdentity    string `json:"ambienceIdentity"`
	AmbienceVersion     string `json:"ambienceVersion"`
	ContinuityIntoNext  bool   `json:"continuityIntoNext"`
	LipSyncRequired     bool   `json:"lipSyncRequired"`
}

type analyzerCommandInput struct {
	SchemaVersion string                  `json:"schemaVersion"`
	Evidence      string                  `json:"evidence"`
	ASR           cerevaluation.ASRConfig `json:"asr"`
	FinalMix      analyzerMediaInput      `json:"finalMix"`
	FinalVideo    analyzerMediaInput      `json:"finalVideo"`
	NativeMixes   []analyzerMediaInput    `json:"nativeMixes"`
	Dialogue      *analyzerMediaInput     `json:"dialogue,omitempty"`
	CueWindows    []analyzerCueWindow     `json:"cueWindows"`
	RunWindows    []analyzerRunWindow     `json:"runWindows"`
}

func NewCommandAudioAnalyzer(
	program string,
	store *artifactstore.Store,
) (*CommandAudioAnalyzer, error) {
	if strings.TrimSpace(program) == "" || store == nil {
		return nil, errors.New("audio analyzer command and artifact store are required")
	}
	return &CommandAudioAnalyzer{Program: program, Store: store, Runner: ExecRunner{}}, nil
}

func (a *CommandAudioAnalyzer) Analyze(
	ctx context.Context,
	request AudioAnalysisRequest,
) (AudioAnalysis, error) {
	if a == nil || strings.TrimSpace(a.Program) == "" || a.Store == nil || a.Runner == nil {
		return AudioAnalysis{}, errors.New("audio analyzer command is not configured")
	}
	if err := request.Request.Validate(); err != nil {
		return AudioAnalysis{}, err
	}
	if err := validateAnalyzerArtifacts(request); err != nil {
		return AudioAnalysis{}, err
	}
	lipExpectations, err := deriveLipSyncExpectations(request.Request)
	if err != nil {
		return AudioAnalysis{}, fmt.Errorf("derive audio analyzer lip-sync bindings: %w", err)
	}
	workdir, err := os.MkdirTemp("", "video-audio-analysis-*")
	if err != nil {
		return AudioAnalysis{}, fmt.Errorf("create audio-analysis workspace: %w", err)
	}
	defer os.RemoveAll(workdir)

	payload := analyzerCommandInput{
		SchemaVersion: audioAnalyzerCommandSchemaVersion,
		Evidence:      request.Request.Evidence,
		ASR:           cerevaluation.FrozenASRConfig(),
		CueWindows:    make([]analyzerCueWindow, 0, len(request.Request.Subtitle.Cues)),
		RunWindows:    make([]analyzerRunWindow, 0, len(request.Request.Clips)),
	}
	if payload.FinalMix, err = a.materializeInput(request.FinalMix, workdir, "final-mix.wav"); err != nil {
		return AudioAnalysis{}, err
	}
	if payload.FinalVideo, err = a.materializeInput(request.FinalVideo, workdir, "final-video.mp4"); err != nil {
		return AudioAnalysis{}, err
	}
	for index, mix := range request.NativeMixes {
		materialized, materializeErr := a.materializeInput(
			mix, workdir, fmt.Sprintf("native-%03d.wav", index),
		)
		if materializeErr != nil {
			return AudioAnalysis{}, materializeErr
		}
		payload.NativeMixes = append(payload.NativeMixes, materialized)
	}
	if request.Dialogue != nil {
		materialized, materializeErr := a.materializeInput(*request.Dialogue, workdir, "dialogue.wav")
		if materializeErr != nil {
			return AudioAnalysis{}, materializeErr
		}
		payload.Dialogue = &materialized
	}
	for _, cue := range request.Request.Subtitle.Cues {
		expected := lipExpectations[cue.ID]
		payload.CueWindows = append(payload.CueWindows, analyzerCueWindow{
			CueID: cue.ID, StartMillis: cue.StartMillis, EndMillis: cue.EndMillis,
			LipSyncRunID: expected.RunID, LipSyncStartMillis: expected.StartMillis,
			LipSyncEndMillis: expected.EndMillis, LipSyncRequired: expected.Required,
		})
	}
	var offset int64
	for _, clip := range request.Request.Clips {
		ambience := clip.Ambience
		payload.RunWindows = append(payload.RunWindows, analyzerRunWindow{
			RunID: clip.RunID, StartMillis: offset, EndMillis: offset + clip.DurationMillis,
			ContextSnapshotID: clip.ContextSnapshotID, ContextSnapshotHash: clip.ContextSnapshotHash,
			AmbienceIdentity: ambience.Identity, AmbienceVersion: ambience.Version,
			ContinuityIntoNext: ambience.ContinuityIntoNext, LipSyncRequired: clip.LipSyncRequired,
		})
		offset += clip.DurationMillis
	}
	inputBytes, err := canonicalJSON(payload)
	if err != nil {
		return AudioAnalysis{}, fmt.Errorf("encode audio analyzer input: %w", err)
	}
	inputPath := filepath.Join(workdir, "input.json")
	outputPath := filepath.Join(workdir, "analysis.json")
	if err := os.WriteFile(inputPath, inputBytes, 0o600); err != nil {
		return AudioAnalysis{}, fmt.Errorf("write audio analyzer input: %w", err)
	}
	if _, err := a.Runner.Run(ctx, workdir, a.Program, inputPath, outputPath); err != nil {
		return AudioAnalysis{}, fmt.Errorf("run approved audio analyzer: %w", err)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		return AudioAnalysis{}, fmt.Errorf("read audio analyzer output: %w", err)
	}
	if len(output) == 0 || len(output) > 4<<20 {
		return AudioAnalysis{}, errors.New("audio analyzer output size is invalid")
	}
	var analysis AudioAnalysis
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&analysis); err != nil {
		return AudioAnalysis{}, fmt.Errorf("decode audio analyzer output: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return AudioAnalysis{}, errors.New("audio analyzer output must contain exactly one JSON value")
	}
	if a.analyzerVersion != "" && analysis.AnalyzerVersion != a.analyzerVersion {
		return AudioAnalysis{}, errors.New("audio analyzer output version differs from its seal")
	}
	// The adapter, not the external process, owns the immutable evidence hash.
	analysis.ContentHash = ""
	analysis.ContentHash, err = digestJSON(analysis.digestInput())
	if err != nil {
		return AudioAnalysis{}, err
	}
	if err := analysis.Validate(request); err != nil {
		return AudioAnalysis{}, fmt.Errorf("validate audio analyzer output: %w", err)
	}
	return analysis, nil
}

func validateAnalyzerArtifacts(request AudioAnalysisRequest) error {
	if err := request.FinalMix.Validate(); err != nil {
		return fmt.Errorf("final mix: %w", err)
	}
	if err := request.FinalVideo.Validate(); err != nil {
		return fmt.Errorf("final video: %w", err)
	}
	if len(request.NativeMixes) != len(request.Request.Clips) {
		return errors.New("audio analyzer requires one native mix per Provider clip")
	}
	for index, mix := range request.NativeMixes {
		if err := mix.Validate(); err != nil {
			return fmt.Errorf("native mix %d: %w", index, err)
		}
	}
	if request.Dialogue != nil {
		if err := request.Dialogue.Validate(); err != nil {
			return fmt.Errorf("dialogue: %w", err)
		}
	}
	return nil
}

func (a *CommandAudioAnalyzer) materializeInput(
	artifact Artifact,
	workdir string,
	name string,
) (analyzerMediaInput, error) {
	path := filepath.Join(workdir, name)
	source, err := a.Store.Open(artifact.Digest)
	if err != nil {
		return analyzerMediaInput{}, err
	}
	defer source.Close()
	target, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return analyzerMediaInput{}, fmt.Errorf("create analyzer media input: %w", err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return analyzerMediaInput{}, fmt.Errorf("materialize analyzer media input: %w", err)
	}
	if err := target.Close(); err != nil {
		return analyzerMediaInput{}, fmt.Errorf("close analyzer media input: %w", err)
	}
	return analyzerMediaInput{
		Path: path, SHA256: artifact.Digest, DurationMillis: artifact.DurationMillis,
	}, nil
}
