package postproduction

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
)

const (
	AudioAnalysisSchemaVersion = "flo154.audio-analysis.v1"
	AudioQCRevision            = "flo154.audio-qc.v1"
	lipSyncLimitMillis         = int64(80)
	subtitleBoundaryLimitMS    = int64(250)
	audioVideoStartLimitMS     = int64(120)
	ambienceSilenceLimitMS     = int64(250)
	ambienceLoudnessDeltaLUFS  = 1.0
	targetLoudnessLUFS         = -16.0
	targetLoudnessTolerance    = 1.0
	truePeakLimitDBTP          = -1.0
)

// AudioAnalyzer is implemented by the approved ASR, speaking-interval,
// mouth-motion, loudness, and ambience inspection pipeline. It is invoked
// after deterministic rendering and before any post-production manifest or G3
// database commit. Implementations must be read-only and must not call a video
// or TTS Provider.
type AudioAnalyzer interface {
	Analyze(context.Context, AudioAnalysisRequest) (AudioAnalysis, error)
}

// AudioQualityError keeps the failed metrics and their immutable CAS evidence
// available to Temporal/error handling while preserving the provider-neutral
// error code through Unwrap.
type AudioQualityError struct {
	Report   QCReport
	Evidence Artifact
	Cause    error
}

func (e *AudioQualityError) Error() string {
	return fmt.Sprintf(
		"%v (audio_qc=%s blocked_runs=%s)",
		e.Cause, e.Evidence.Digest, strings.Join(e.Report.BlockedRunIDs, ","),
	)
}

func (e *AudioQualityError) Unwrap() error { return e.Cause }

type AudioAnalysisRequest struct {
	Request     Request
	NativeMixes []Artifact
	Dialogue    *Artifact
	FinalMix    Artifact
	FinalVideo  Artifact
}

type CueTimingMeasurement struct {
	CueID             string `json:"cueId"`
	SpeechStartMillis int64  `json:"speechStartMillis"`
	SpeechEndMillis   int64  `json:"speechEndMillis"`
}

type LipSyncMeasurement struct {
	RunID            string `json:"runId"`
	CueID            string `json:"cueId"`
	Required         bool   `json:"required"`
	AudioStartMillis int64  `json:"audioStartMillis"`
	AudioEndMillis   int64  `json:"audioEndMillis"`
	MouthStartMillis int64  `json:"mouthStartMillis"`
	MouthEndMillis   int64  `json:"mouthEndMillis"`
}

type AmbienceTransitionMeasurement struct {
	FromRunID          string  `json:"fromRunId"`
	ToRunID            string  `json:"toRunId"`
	ContinuityRequired bool    `json:"continuityRequired"`
	FromIdentity       string  `json:"fromIdentity"`
	FromVersion        string  `json:"fromVersion"`
	ToIdentity         string  `json:"toIdentity"`
	ToVersion          string  `json:"toVersion"`
	HardSilenceMillis  int64   `json:"hardSilenceMillis"`
	LoudnessDeltaLUFS  float64 `json:"loudnessDeltaLufs"`
}

type AudioAnalysis struct {
	SchemaVersion         string                          `json:"schemaVersion"`
	AnalysisID            string                          `json:"analysisId"`
	Analyzer              string                          `json:"analyzer"`
	AnalyzerVersion       string                          `json:"analyzerVersion"`
	Evidence              string                          `json:"evidence"`
	ASR                   cerevaluation.ASRConfig         `json:"asr"`
	Transcript            string                          `json:"transcript"`
	SourceHashes          []string                        `json:"sourceHashes"`
	CueTimings            []CueTimingMeasurement          `json:"cueTimings"`
	LipSync               []LipSyncMeasurement            `json:"lipSync"`
	AmbienceTransitions   []AmbienceTransitionMeasurement `json:"ambienceTransitions"`
	AudioVideoStartMillis []int64                         `json:"audioVideoStartMillis"`
	IntegratedLUFS        float64                         `json:"integratedLufs"`
	TruePeakDBTP          float64                         `json:"truePeakDbtp"`
	ContentHash           string                          `json:"contentHash"`
}

type lipSyncExpectation struct {
	RunID       string
	StartMillis int64
	EndMillis   int64
	Required    bool
}

// deriveLipSyncExpectations freezes one authoritative cue/run pair from the
// immutable subtitle and clip timeline. A cue belongs to the run with the
// largest positive overlap; equal overlaps are ambiguous and fail closed.
func deriveLipSyncExpectations(request Request) (map[string]lipSyncExpectation, error) {
	type runWindow struct {
		runID       string
		startMillis int64
		endMillis   int64
		required    bool
	}
	windows := make([]runWindow, 0, len(request.Clips))
	var offset int64
	for index, clip := range request.Clips {
		if clip.DurationMillis <= 0 || offset > (1<<63-1)-clip.DurationMillis {
			return nil, fmt.Errorf("clip %d has an invalid lip-sync timeline", index)
		}
		end := offset + clip.DurationMillis
		windows = append(windows, runWindow{
			runID: clip.RunID, startMillis: offset, endMillis: end,
			required: clip.LipSyncRequired,
		})
		offset = end
	}
	expectations := make(map[string]lipSyncExpectation, len(request.Subtitle.Cues))
	for _, cue := range request.Subtitle.Cues {
		if strings.TrimSpace(cue.ID) == "" || cue.StartMillis < 0 ||
			cue.EndMillis <= cue.StartMillis || cue.EndMillis > offset {
			return nil, fmt.Errorf("subtitle cue %q has an invalid lip-sync timeline", cue.ID)
		}
		if _, duplicate := expectations[cue.ID]; duplicate {
			return nil, fmt.Errorf("duplicate subtitle cue %q in lip-sync timeline", cue.ID)
		}
		bestIndex := -1
		var bestOverlap int64
		ambiguous := false
		for index, window := range windows {
			start := max(cue.StartMillis, window.startMillis)
			end := min(cue.EndMillis, window.endMillis)
			overlap := end - start
			if overlap <= 0 {
				continue
			}
			switch {
			case overlap > bestOverlap:
				bestIndex, bestOverlap, ambiguous = index, overlap, false
			case overlap == bestOverlap:
				ambiguous = true
			}
		}
		if bestIndex < 0 || ambiguous {
			return nil, fmt.Errorf("subtitle cue %q has no unique authoritative lip-sync run", cue.ID)
		}
		window := windows[bestIndex]
		expectations[cue.ID] = lipSyncExpectation{
			RunID: window.runID, StartMillis: max(cue.StartMillis, window.startMillis),
			EndMillis: min(cue.EndMillis, window.endMillis), Required: window.required,
		}
	}
	return expectations, nil
}

func (a AudioAnalysis) digestInput() AudioAnalysis {
	a.ContentHash = ""
	return a
}

func (a AudioAnalysis) Validate(request AudioAnalysisRequest) error {
	if a.SchemaVersion != AudioAnalysisSchemaVersion || strings.TrimSpace(a.AnalysisID) == "" ||
		strings.TrimSpace(a.Analyzer) == "" || strings.TrimSpace(a.AnalyzerVersion) == "" ||
		strings.TrimSpace(a.Transcript) == "" || a.Evidence != request.Request.Evidence {
		return errors.New("audio analysis identity, transcript, or evidence is incomplete")
	}
	wantSources := []string{request.FinalMix.Digest, request.FinalVideo.Digest}
	for _, mix := range request.NativeMixes {
		wantSources = append(wantSources, mix.Digest)
	}
	if request.Dialogue != nil {
		wantSources = append(wantSources, request.Dialogue.Digest)
	}
	sort.Strings(wantSources)
	gotSources := append([]string(nil), a.SourceHashes...)
	sort.Strings(gotSources)
	if !slices.Equal(gotSources, wantSources) {
		return errors.New("audio analysis is bound to different source artifacts")
	}
	for _, digest := range a.SourceHashes {
		if !validDigest(digest) {
			return errors.New("audio analysis contains an invalid source hash")
		}
	}
	if len(a.AudioVideoStartMillis) != len(request.Request.Clips) {
		return errors.New("audio analysis must measure audio/video start for every Provider clip")
	}
	for _, offset := range a.AudioVideoStartMillis {
		if offset == -1<<63 {
			return errors.New("audio analysis contains an invalid audio/video start offset")
		}
	}
	if math.IsNaN(a.IntegratedLUFS) || math.IsInf(a.IntegratedLUFS, 0) ||
		math.IsNaN(a.TruePeakDBTP) || math.IsInf(a.TruePeakDBTP, 0) {
		return errors.New("audio analysis loudness measurements are invalid")
	}
	expectedHash, err := digestJSON(a.digestInput())
	if err != nil {
		return err
	}
	if a.ContentHash != expectedHash {
		return errors.New("audio analysis content hash drifted")
	}
	return nil
}

func EvaluateAudioQuality(
	request Request,
	rendered RenderResult,
	analysis AudioAnalysis,
) (QCReport, error) {
	analysisRequest := AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	}
	if rendered.Dialogue.Kind != "" {
		analysisRequest.Dialogue = &rendered.Dialogue
	}
	if err := analysis.Validate(analysisRequest); err != nil {
		return QCReport{}, fmt.Errorf("validate audio analysis: %w", err)
	}
	reference := make([]string, 0, len(request.Subtitle.Cues))
	for _, cue := range request.Subtitle.Cues {
		reference = append(reference, cue.Text)
	}
	cer, err := cerevaluation.Evaluate(cerevaluation.Input{
		SchemaVersion: cerevaluation.InputSchemaVersion,
		EvaluationID:  analysis.AnalysisID,
		SourceDialogue: cerevaluation.SourceDialogue{
			SHA256: rendered.FinalMix.Digest, DurationMillis: rendered.FinalMix.DurationMillis,
			SampleRate: defaultAudioRate, Channels: defaultAudioChannel,
		},
		ASR: analysis.ASR, ReferenceText: strings.Join(reference, ""),
		HypothesisText: analysis.Transcript, ProviderSubmitCount: 0,
	})
	if err != nil {
		return QCReport{}, fmt.Errorf("evaluate native audio CER: %w", err)
	}

	knownCues := make(map[string]Cue, len(request.Subtitle.Cues))
	for _, cue := range request.Subtitle.Cues {
		knownCues[cue.ID] = cue
	}
	boundaryOffsets := make([]int64, 0, len(analysis.CueTimings)*2)
	seenTimings := make(map[string]struct{}, len(analysis.CueTimings))
	for _, timing := range analysis.CueTimings {
		cue, ok := knownCues[timing.CueID]
		if !ok || timing.SpeechStartMillis < 0 || timing.SpeechEndMillis <= timing.SpeechStartMillis {
			return QCReport{}, errors.New("audio analysis contains an invalid cue timing")
		}
		if _, duplicate := seenTimings[timing.CueID]; duplicate {
			return QCReport{}, errors.New("audio analysis contains a duplicate cue timing")
		}
		seenTimings[timing.CueID] = struct{}{}
		boundaryOffsets = append(boundaryOffsets,
			absolute(timing.SpeechStartMillis-cue.StartMillis),
			absolute(timing.SpeechEndMillis-cue.EndMillis),
		)
	}
	if len(seenTimings) != len(knownCues) {
		return QCReport{}, errors.New("audio analysis must measure every subtitle cue")
	}

	lipExpectations, err := deriveLipSyncExpectations(request)
	if err != nil {
		return QCReport{}, fmt.Errorf("derive authoritative lip-sync cue/run bindings: %w", err)
	}
	lipOffsets := make([]int64, 0, len(analysis.LipSync)*2)
	blockedRuns := make([]string, 0)
	seenLipMeasurements := make(map[string]struct{}, len(analysis.LipSync))
	for _, measurement := range analysis.LipSync {
		expected, knownCue := lipExpectations[measurement.CueID]
		if !knownCue {
			return QCReport{}, errors.New("audio analysis contains an invalid lip-sync measurement")
		}
		if measurement.RunID != expected.RunID {
			return QCReport{}, errors.New("audio analysis lip-sync cue/run binding drifted from the frozen timeline")
		}
		if measurement.AudioStartMillis < expected.StartMillis ||
			measurement.AudioEndMillis <= measurement.AudioStartMillis ||
			measurement.AudioEndMillis > expected.EndMillis ||
			measurement.MouthStartMillis < expected.StartMillis ||
			measurement.MouthEndMillis <= measurement.MouthStartMillis ||
			measurement.MouthEndMillis > expected.EndMillis {
			return QCReport{}, errors.New("audio analysis contains an invalid lip-sync measurement")
		}
		if _, duplicate := seenLipMeasurements[measurement.CueID]; duplicate {
			return QCReport{}, errors.New("audio analysis contains a duplicate lip-sync measurement")
		}
		seenLipMeasurements[measurement.CueID] = struct{}{}
		if measurement.Required != expected.Required {
			return QCReport{}, errors.New("audio analysis lip-sync requirement drifted from shot cinematography")
		}
		if !measurement.Required {
			continue
		}
		offset := max(
			absolute(measurement.AudioStartMillis-measurement.MouthStartMillis),
			absolute(measurement.AudioEndMillis-measurement.MouthEndMillis),
		)
		lipOffsets = append(lipOffsets, offset)
		if offset > lipSyncLimitMillis {
			blockedRuns = append(blockedRuns, measurement.RunID)
		}
	}
	for cueID, expected := range lipExpectations {
		if expected.Required {
			if _, measured := seenLipMeasurements[cueID]; !measured {
				return QCReport{}, fmt.Errorf(
					"audio analysis omitted required lip-sync cue/run %q/%q",
					cueID, expected.RunID,
				)
			}
		}
	}
	sort.Strings(blockedRuns)
	blockedRuns = slices.Compact(blockedRuns)

	maxSilence := int64(0)
	ambienceFailed := false
	type expectedAmbienceTransition struct {
		from *AmbienceBinding
		to   *AmbienceBinding
	}
	expectedTransitions := make(map[string]expectedAmbienceTransition, max(len(request.Clips)-1, 0))
	for index := 0; index+1 < len(request.Clips); index++ {
		expectedTransitions[request.Clips[index].RunID+"\x00"+request.Clips[index+1].RunID] =
			expectedAmbienceTransition{
				from: request.Clips[index].Ambience,
				to:   request.Clips[index+1].Ambience,
			}
	}
	seenTransitions := make(map[string]struct{}, len(analysis.AmbienceTransitions))
	for _, transition := range analysis.AmbienceTransitions {
		if transition.FromRunID == "" || transition.ToRunID == "" ||
			transition.FromIdentity == "" || transition.FromVersion == "" ||
			transition.ToIdentity == "" || transition.ToVersion == "" ||
			transition.HardSilenceMillis < 0 || math.IsNaN(transition.LoudnessDeltaLUFS) {
			return QCReport{}, errors.New("audio analysis contains an invalid ambience transition")
		}
		key := transition.FromRunID + "\x00" + transition.ToRunID
		expected, ok := expectedTransitions[key]
		if !ok || expected.from == nil || expected.to == nil {
			return QCReport{}, errors.New("audio analysis ambience transition is outside adjacent Scene Context")
		}
		if _, duplicate := seenTransitions[key]; duplicate {
			return QCReport{}, errors.New("audio analysis contains a duplicate ambience transition")
		}
		seenTransitions[key] = struct{}{}
		if transition.ContinuityRequired != expected.from.ContinuityIntoNext ||
			transition.FromIdentity != expected.from.Identity ||
			transition.FromVersion != expected.from.Version ||
			transition.ToIdentity != expected.to.Identity ||
			transition.ToVersion != expected.to.Version {
			return QCReport{}, errors.New("audio analysis ambience identity/version drifted from Scene Context")
		}
		if !transition.ContinuityRequired {
			continue
		}
		maxSilence = max(maxSilence, transition.HardSilenceMillis)
		ambienceFailed = ambienceFailed || transition.HardSilenceMillis > ambienceSilenceLimitMS ||
			math.Abs(transition.LoudnessDeltaLUFS) > ambienceLoudnessDeltaLUFS
	}
	if len(seenTransitions) != len(expectedTransitions) {
		return QCReport{}, errors.New("audio analysis must measure every adjacent ambience transition")
	}

	boundaryP95 := percentileLinearMillis(boundaryOffsets, 0.95)
	lipP95 := percentileLinearMillis(lipOffsets, 0.95)
	avOffsets := make([]int64, 0, len(analysis.AudioVideoStartMillis))
	for _, offset := range analysis.AudioVideoStartMillis {
		avOffsets = append(avOffsets, absolute(offset))
	}
	avP95 := percentileLinearMillis(avOffsets, 0.95)
	cerPercent := cer.StandardCER.Percent
	qc := QCReport{
		State: "AUDIO_QC_PASSED", SubtitleCERPercent: &cerPercent,
		SubtitleBoundaryP95Millis: &boundaryP95, AudioVideoStartP95Millis: &avP95,
		LipSyncP95Millis: &lipP95, AmbienceHardSilenceMaxMillis: &maxSilence,
		IntegratedLoudnessLUFS: &analysis.IntegratedLUFS, TruePeakDBTP: &analysis.TruePeakDBTP,
		AnalysisRevision: AudioQCRevision, AnalysisHash: analysis.ContentHash,
		BlockedRunIDs: blockedRuns, ActualDurationMillis: rendered.FinalVideo.DurationMillis,
		MeasurementEvidence: request.Evidence,
	}
	failed := cerPercent > cerevaluation.GateThresholdPercent ||
		boundaryP95 > subtitleBoundaryLimitMS || avP95 > audioVideoStartLimitMS ||
		lipP95 > lipSyncLimitMillis || ambienceFailed ||
		math.Abs(analysis.IntegratedLUFS-targetLoudnessLUFS) > targetLoudnessTolerance ||
		analysis.TruePeakDBTP > truePeakLimitDBTP
	if request.Evidence == EvidenceMockOnly {
		qc.State = "AUDIO_CONTRACT_PASSED"
		qc.ManualTimingRequired = true
	}
	if !failed {
		return qc, nil
	}
	qc.State = "AUDIO_QC_FAILED"
	qc.ManualTimingRequired = true
	action := "revise the subtitle/ambience mix and rerun audio QC"
	if len(blockedRuns) > 0 {
		action = "regenerate only the blocked close-up shot or use approved audio-driven video"
	}
	return qc, &providercontract.Error{
		Code: providercontract.CodeConflict, Retryable: false, RequiresAction: true,
		SafeMessage:     "native audio quality failed the frozen FLO-154 release thresholds",
		SuggestedAction: action,
	}
}
