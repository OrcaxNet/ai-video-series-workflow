package postproduction

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
)

func TestSubtitleRevisionIsImmutableAndRendersCanonicalUTF8(t *testing.T) {
	t.Parallel()
	parent, err := NewSubtitleRevision(
		"subtitle-v1",
		"",
		1,
		"zh-CN",
		[]string{"script-v1"},
		[]Cue{
			{ID: "cue-1", Speaker: "阿澈", Text: "你好，世界。", StartMillis: 0, EndMillis: 1_250},
			{ID: "cue-2", Speaker: "小岚", Text: "出发吧！", StartMillis: 1_500, EndMillis: 2_900},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	edited := append([]Cue(nil), parent.Cues...)
	edited[1].Text = "现在出发吧！"
	child, err := ReviseSubtitle(parent, "subtitle-v2", edited)
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentRevisionID != parent.ID || child.ContentHash == parent.ContentHash {
		t.Fatal("child revision did not preserve parent identity and independent content")
	}
	if parent.Cues[1].Text != "出发吧！" {
		t.Fatal("parent subtitle revision was mutated")
	}
	rendered, err := RenderSRT(child, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	want := "1\n00:00:00,000 --> 00:00:01,250\n你好，世界。\n\n" +
		"2\n00:00:01,500 --> 00:00:02,900\n现在出发吧！\n\n"
	if string(rendered) != want {
		t.Fatalf("unexpected SRT:\n%s", rendered)
	}
	if bytes.HasPrefix(rendered, []byte{0xef, 0xbb, 0xbf}) {
		t.Fatal("canonical UTF-8 SRT must not contain a BOM")
	}
}

func TestSubtitleRevisionRejectsOverlapsAndHashDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*SubtitleRevision)
	}{
		{
			name: "overlap",
			mutate: func(revision *SubtitleRevision) {
				revision.Cues[1].StartMillis = 900
				revision.ContentHash, _ = digestJSON(revision.digestInput())
			},
		},
		{
			name: "hash drift",
			mutate: func(revision *SubtitleRevision) {
				revision.Cues[0].Text = "changed without a new hash"
			},
		},
		{
			name: "past episode",
			mutate: func(revision *SubtitleRevision) {
				revision.Cues[1].EndMillis = 4_000
				revision.ContentHash, _ = digestJSON(revision.digestInput())
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			revision := mustSubtitle(t)
			test.mutate(&revision)
			if err := revision.Validate(3_000); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestBuildCommandPlanIsDeterministicAndDoesNotInterpolateDialogue(t *testing.T) {
	t.Parallel()
	request := mustRequest(t, EvidenceMockOnly)
	attempts := []ProviderAttempt{
		{CueID: "cue-1", Artifact: testArtifact("dialogue_segment", "audio/wav", "d")},
		{CueID: "cue-2", Artifact: testArtifact("dialogue_segment", "audio/wav", "e")},
	}
	first, err := BuildCommandPlan(request, attempts, "ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildCommandPlan(request, attempts, "ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	firstHash, _ := digestJSON(first)
	secondHash, _ := digestJSON(second)
	if firstHash != secondHash {
		t.Fatal("same immutable inputs produced different command plans")
	}
	encoded, _ := canonicalJSON(first)
	for _, cue := range request.Subtitle.Cues {
		if strings.Contains(string(encoded), cue.Text) {
			t.Fatal("dialogue text must not be interpolated into FFmpeg arguments")
		}
	}
	if !strings.Contains(string(encoded), "AI-generated content") ||
		!strings.Contains(string(encoded), "Dialogue") {
		t.Fatal("final command does not preserve AI marker and independent dialogue track")
	}
	if !strings.Contains(string(encoded), "apad=whole_len=144000") ||
		!strings.Contains(string(encoded), "atrim=end_sample=144000") ||
		!strings.Contains(string(encoded), "sample_rates=48000:channel_layouts=stereo") ||
		strings.Contains(string(encoded), "apad=whole_dur=") {
		t.Fatal("dialogue command must produce the exact requested sample count and media specification")
	}
	if !strings.Contains(string(encoded), "color=c=0x") ||
		!strings.Contains(string(encoded), "sine=frequency=") {
		t.Fatal("mock_only command plan must use deterministic playable lavfi sources")
	}
	if strings.Contains(string(encoded), "clip-000.mp4") ||
		strings.Contains(string(encoded), "cue-000.wav") {
		t.Fatal("mock_only processing must not trust fixture bytes mislabeled as live media")
	}
}

func TestServiceFinalizationIsReplaySafeAndKeepsMockEvidenceSeparate(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{store: store}
	media := &fakeMedia{store: store}
	service, err := NewService(speech, media, store)
	if err != nil {
		t.Fatal(err)
	}
	request := mustRequest(t, EvidenceMockOnly)
	first, err := service.Finalize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Finalize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestHash != second.ManifestHash ||
		first.ServiceBOMHash != second.ServiceBOMHash ||
		first.FinalVideo.Digest != second.FinalVideo.Digest {
		t.Fatal("activity replay produced different immutable outputs")
	}
	if first.Evidence != EvidenceMockOnly || first.QC.MeasurementEvidence != EvidenceMockOnly {
		t.Fatal("mock evidence was promoted to live evidence")
	}
	if !first.QC.ManualTimingRequired {
		t.Fatal("structural mock QC must retain the manual timing gate")
	}
	if speech.calls != len(request.Subtitle.Cues)*2 {
		t.Fatalf("speech calls = %d, want %d", speech.calls, len(request.Subtitle.Cues)*2)
	}
}

func TestServiceBlocksPendingKeyBeforePaidSubmission(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{store: store}
	service, err := NewService(speech, &fakeMedia{store: store}, store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Finalize(context.Background(), mustRequest(t, EvidencePendingKey))
	if !errors.Is(err, ErrPendingKey) {
		t.Fatalf("error = %v, want ErrPendingKey", err)
	}
	if speech.calls != 0 {
		t.Fatal("pending_key request reached the paid provider boundary")
	}
}

func TestServiceReauthorizesBeforeEveryPaidSpeechSubmission(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{store: store}
	service, err := NewService(speech, &fakeMedia{store: store}, store)
	if err != nil {
		t.Fatal(err)
	}
	request := mustRequest(t, EvidenceMockOnly)
	revoked := errors.New("consent revoked after prior cue")
	authorizeCalls := 0
	request.AuthorizePaidSubmit = func(_ context.Context, _ Cue) error {
		authorizeCalls++
		if authorizeCalls == 2 {
			return revoked
		}
		return nil
	}
	_, err = service.Finalize(context.Background(), request)
	if !errors.Is(err, revoked) {
		t.Fatalf("Finalize() error = %v, want revoked authorization", err)
	}
	if authorizeCalls != 2 || speech.calls != 1 {
		t.Fatalf(
			"paid boundary calls = authorize:%d speech:%d, want authorize:2 speech:1",
			authorizeCalls, speech.calls,
		)
	}
}

func TestNativeAudioFinalizationMakesZeroSpeechCallsAndNoFakeDialogueStem(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{store: store}
	analyzer := &fakeAudioAnalyzer{}
	service, err := NewService(speech, &fakeMedia{store: store}, store, analyzer)
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	result, err := service.Finalize(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if speech.calls != 0 || len(result.SpeechAttempts) != 0 {
		t.Fatalf("native pass made speech calls=%d attempts=%d", speech.calls, len(result.SpeechAttempts))
	}
	if result.Dialogue.Kind != "" {
		t.Fatalf("native mix was mislabeled as dialogue stem: %#v", result.Dialogue)
	}
	if len(result.NativeMixes) != len(request.Clips) || result.FinalMix.Kind != "final_mix" ||
		result.AudioQC.Kind != "audio_qc_report" ||
		result.AudioStrategy != providercontract.AudioStrategyNativePreferred {
		t.Fatalf("native artifacts/result incomplete: %#v", result)
	}
	if analyzer.calls != 1 || result.QC.State != "AUDIO_CONTRACT_PASSED" {
		t.Fatalf("analyzer calls=%d qc=%#v", analyzer.calls, result.QC)
	}
}

func TestNativeAudioFinalizationRequiresFrozenAnalyzerSeal(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wanted := strings.Repeat("a", 64)
	request := nativeAudioRequest(t)
	request.AnalyzerSealSHA256 = wanted
	for _, test := range []struct {
		name    string
		seal    string
		wantErr bool
	}{
		{name: "exact seal", seal: wanted},
		{name: "drifted seal", seal: strings.Repeat("b", 64), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			analyzer := &fakeAudioAnalyzer{sealSHA256: test.seal}
			service, serviceErr := NewService(nil, &fakeMedia{store: store}, store, analyzer)
			if serviceErr != nil {
				t.Fatal(serviceErr)
			}
			_, finalizeErr := service.Finalize(t.Context(), request)
			if test.wantErr {
				if providercontract.ErrorCodeOf(finalizeErr) != providercontract.CodeUnavailable || analyzer.calls != 0 {
					t.Fatalf("Finalize() error=%v analyzer calls=%d", finalizeErr, analyzer.calls)
				}
				return
			}
			if finalizeErr != nil || analyzer.calls != 1 {
				t.Fatalf("Finalize() error=%v analyzer calls=%d", finalizeErr, analyzer.calls)
			}
		})
	}
}

func TestHybridAudioSynthesizesOnlyFrozenFallbackAndReusesCompletedAttempt(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &speechV2CanaryFake{store: store}
	service, err := NewService(speech, &fakeMedia{store: store}, store, &fakeAudioAnalyzer{})
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	v2 := speechV2Request(t)
	request.EpisodeRevisionID = v2.EpisodeRevisionID
	request.Subtitle = v2.SubtitleRevision
	request.AudioStrategy = providercontract.AudioStrategyHybrid
	request.Speech = v2.Config
	request.Speech.BatchAuthorization = batchForSpeechRequest(t, request, request.Subtitle.Cues[:1])
	request.Speech.AuthorizedCueID = ""
	request.Speech.MaximumAFPMilli = 0
	request.Speech.MaximumNonSubscriptionCashMicros = 0
	request.Speech.MaxAttempts = 0
	originalMix, err := store.Put(t.Context(), strings.NewReader("native-mix:"+request.RunIDs[0]))
	if err != nil {
		t.Fatal(err)
	}
	request.CueFallbacks = []CueFallback{{
		CueID: request.Subtitle.Cues[0].ID, Reason: "native cue failed CER gate",
		OriginalNativeMixSHA256: originalMix.Digest,
		ReplacementRevisionID:   "fallback-revision-v1",
	}}
	first, err := service.Finalize(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if speech.calls != 1 || len(first.SpeechAttempts) != 1 || first.SpeechAttempts[0].CueID != request.Subtitle.Cues[0].ID {
		t.Fatalf("fallback calls=%d attempts=%#v", speech.calls, first.SpeechAttempts)
	}
	if first.Dialogue.Kind != "dialogue_audio" {
		t.Fatal("real fallback dialogue was not retained as an independent stem")
	}
	request.Speech.CompletedAttempts = append([]ProviderAttempt(nil), first.SpeechAttempts...)
	request.Speech.BatchAuthorization = nil
	second, err := service.Finalize(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if speech.calls != 1 || second.FinalVideo.Digest != first.FinalVideo.Digest ||
		second.FinalMix.Digest != first.FinalMix.Digest {
		t.Fatalf("replay resubmitted speech or drifted media: calls=%d first=%#v second=%#v", speech.calls, first.FinalVideo, second.FinalVideo)
	}
}

func TestHybridAudioMissingNativeSourceFailsBeforeSpeechSubmit(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{store: store}
	service, err := NewService(speech, &fakeMedia{store: store}, store, &fakeAudioAnalyzer{})
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	request.AudioStrategy = providercontract.AudioStrategyHybrid
	request.Speech = mustRequest(t, EvidenceMockOnly).Speech
	request.CueFallbacks = []CueFallback{{
		CueID: request.Subtitle.Cues[0].ID, Reason: "failed native QC",
		OriginalNativeMixSHA256: strings.Repeat("f", 64),
		ReplacementRevisionID:   "fallback-revision-v1",
	}}
	_, err = service.Finalize(t.Context(), request)
	if providercontract.ErrorCodeOf(err) != providercontract.CodeConflict || speech.calls != 0 {
		t.Fatalf("Finalize() error=%v speech calls=%d", err, speech.calls)
	}
}

func TestNativeAudioLipSyncFailureBlocksOnlyMeasuredRun(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	analyzer := &fakeAudioAnalyzer{lipOffsetMillis: lipSyncLimitMillis + 1}
	service, err := NewService(nil, &fakeMedia{store: store}, store, analyzer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Finalize(t.Context(), nativeAudioRequest(t))
	if providercontract.ErrorCodeOf(err) != providercontract.CodeConflict ||
		!strings.Contains(err.Error(), "native audio quality failed") {
		t.Fatalf("Finalize() error = %v", err)
	}
	var quality *AudioQualityError
	if !errors.As(err, &quality) || quality.Evidence.Kind != "audio_qc_report" ||
		len(quality.Report.BlockedRunIDs) != 2 {
		t.Fatalf("quality failure lost exact CAS/run evidence: %#v", quality)
	}
}

func TestNativeAudioAmbienceHardSilenceBlocksG3(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	analyzer := &fakeAudioAnalyzer{ambienceHardSilenceMillis: ambienceSilenceLimitMS + 1}
	service, err := NewService(nil, &fakeMedia{store: store}, store, analyzer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Finalize(t.Context(), nativeAudioRequest(t))
	var quality *AudioQualityError
	if !errors.As(err, &quality) || quality.Report.AmbienceHardSilenceMaxMillis == nil ||
		*quality.Report.AmbienceHardSilenceMaxMillis != ambienceSilenceLimitMS+1 {
		t.Fatalf("ambience failure evidence = %#v error=%v", quality, err)
	}
}

func TestServiceRejectsProviderEvidenceDrift(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &fakeSpeech{
		store: store,
		mutate: func(attempt *ProviderAttempt) {
			attempt.Model.ModelID = "unapproved-model"
		},
	}
	service, err := NewService(speech, &fakeMedia{store: store}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Finalize(
		context.Background(), mustRequest(t, EvidenceMockOnly),
	); err == nil || !strings.Contains(err.Error(), "frozen route") {
		t.Fatalf("provider evidence drift error = %v", err)
	}
}

type fakeSpeech struct {
	store  *artifactstore.Store
	calls  int
	mutate func(*ProviderAttempt)
}

func (f *fakeSpeech) Synthesize(ctx context.Context, request SpeechRequest) (ProviderAttempt, error) {
	f.calls++
	committed, err := f.store.Put(ctx, strings.NewReader("audio:"+request.Cue.ID))
	if err != nil {
		return ProviderAttempt{}, err
	}
	actual := int64(7)
	attempt := ProviderAttempt{
		CueID:          request.Cue.ID,
		JobID:          "job-" + request.Cue.ID,
		RequestID:      "request-" + request.Cue.ID,
		UpstreamTaskID: "task-" + request.Cue.ID,
		Model:          request.Config.Route,
		Usage:          providercontract.Usage{GeneratedChars: int64(len([]rune(request.Cue.Text)))},
		Cost: providercontract.Cost{
			EstimatedMicros: request.BudgetMicros,
			ActualMicros:    &actual,
			Currency:        request.Config.BudgetCurrency,
			PricingVersion:  "fake-v1",
			Verified:        true,
		},
		Artifact: Artifact{
			Kind: "dialogue_segment", Digest: committed.Digest, URI: committed.URI,
			MediaType: "audio/wav", SizeBytes: committed.Size,
			DurationMillis: request.Cue.EndMillis - request.Cue.StartMillis,
		},
		Evidence: request.Evidence,
	}
	if f.mutate != nil {
		f.mutate(&attempt)
	}
	return attempt, nil
}

type fakeMedia struct {
	store *artifactstore.Store
	calls int
}

func (f *fakeMedia) Render(
	ctx context.Context,
	request Request,
	_ []byte,
	attempts []ProviderAttempt,
) (RenderResult, error) {
	f.calls++
	var dialogueArtifact Artifact
	if len(attempts) > 0 {
		dialogue, err := f.store.Put(ctx, strings.NewReader("dialogue:"+request.Subtitle.ContentHash))
		if err != nil {
			return RenderResult{}, err
		}
		dialogueArtifact = Artifact{
			Kind: "dialogue_audio", Digest: dialogue.Digest, URI: dialogue.URI,
			MediaType: "audio/wav", SizeBytes: dialogue.Size, DurationMillis: request.DurationMillis(),
		}
	}
	video, err := f.store.Put(ctx, strings.NewReader("video:"+request.EpisodeRevisionHash))
	if err != nil {
		return RenderResult{}, err
	}
	planHash, _ := digestJSON(map[string]any{"request": request.EpisodeRevisionHash})
	finalMix, err := f.store.Put(ctx, strings.NewReader("final-mix:"+request.EpisodeRevisionHash))
	if err != nil {
		return RenderResult{}, err
	}
	nativeMixes := make([]Artifact, 0, len(request.Clips))
	if request.ResolvedAudioStrategy().RequiresNativeAudio() {
		for _, clip := range request.Clips {
			mix, err := f.store.Put(ctx, strings.NewReader("native-mix:"+clip.RunID))
			if err != nil {
				return RenderResult{}, err
			}
			nativeMixes = append(nativeMixes, Artifact{
				Kind: "native_mix", Digest: mix.Digest, URI: mix.URI,
				MediaType: "audio/wav", SizeBytes: mix.Size, DurationMillis: clip.DurationMillis,
			})
		}
	}
	return RenderResult{
		Dialogue:    dialogueArtifact,
		NativeMixes: nativeMixes,
		FinalVideo: Artifact{
			Kind: "final_video", Digest: video.Digest, URI: video.URI,
			MediaType: "video/mp4", SizeBytes: video.Size, DurationMillis: request.DurationMillis(),
			Width: 1280, Height: 720, FPS: 24,
		},
		FinalMix: Artifact{
			Kind: "final_mix", Digest: finalMix.Digest, URI: finalMix.URI,
			MediaType: "audio/wav", SizeBytes: finalMix.Size, DurationMillis: request.DurationMillis(),
		},
		CommandPlanHash: planHash,
		QC: QCReport{
			State: "STRUCTURAL_PASSED", ActualDurationMillis: request.DurationMillis(),
			ManualTimingRequired: true, MeasurementEvidence: request.Evidence,
		},
		FFmpegVersion:  "ffmpeg fixture-v1",
		FFprobeVersion: "ffprobe fixture-v1",
	}, nil
}

func mustRequest(t *testing.T, evidence string) Request {
	t.Helper()
	subtitle := mustSubtitle(t)
	digestA := strings.Repeat("a", 64)
	digestB := strings.Repeat("b", 64)
	return Request{
		SchemaVersion:       SchemaVersion,
		Evidence:            evidence,
		EpisodeRevisionID:   "episode-revision-v1",
		EpisodeRevisionHash: strings.Repeat("9", 64),
		RunIDs:              []string{"run-1", "run-2"},
		Clips: []Clip{
			{
				RunID: "run-1", ShotSpecRevisionID: "shot-1", ShotSpecHash: digestA,
				PromptSnapshotID: "prompt-1", PromptSnapshotHash: digestA,
				ContextSnapshotID: "context-1", ContextSnapshotHash: digestA,
				Artifact:       testArtifact("shot_video", "video/mp4", "1"),
				DurationMillis: 1_500, LicenseReference: "license-1",
			},
			{
				RunID: "run-2", ShotSpecRevisionID: "shot-2", ShotSpecHash: digestB,
				PromptSnapshotID: "prompt-2", PromptSnapshotHash: digestB,
				ContextSnapshotID: "context-2", ContextSnapshotHash: digestB,
				Artifact:       testArtifact("shot_video", "video/mp4", "2"),
				DurationMillis: 1_500, LicenseReference: "license-2",
			},
		},
		Subtitle: subtitle,
		Speech: SpeechConfig{
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "fake", ModelID: "fixture-tts-v1", RouteVersion: "route-v1",
				CapabilityHash: strings.Repeat("c", 64), Verification: "mock_only",
			},
			ProviderProfileID: "speech-profile-v1", BudgetApprovalID: "budget-v1",
			BudgetMaximumMicros: 100, BudgetCurrency: "CNY",
		},
		Output: OutputPolicy{BurnSubtitles: true},
		Gates: []GateBinding{
			{Gate: "G1", DecisionID: "g1", Decision: "APPROVED", ContentHash: digestA},
			{Gate: "G2", DecisionID: "g2", Decision: "APPROVED", ContentHash: digestB},
		},
		TraceID: "trace-v1",
	}
}

func mustSubtitle(t *testing.T) SubtitleRevision {
	t.Helper()
	revision, err := NewSubtitleRevision(
		"subtitle-v1", "", 1, "zh-CN", []string{"script-v1"},
		[]Cue{
			{ID: "cue-1", Speaker: "A", Text: "第一句", StartMillis: 0, EndMillis: 1_000},
			{ID: "cue-2", Speaker: "B", Text: "第二句", StartMillis: 1_250, EndMillis: 2_500},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func testArtifact(kind, mediaType, seed string) Artifact {
	digest := strings.Repeat(seed, 64)
	return Artifact{
		Kind: kind, Digest: digest, URI: "cas://sha256/" + digest,
		MediaType: mediaType, SizeBytes: 10, DurationMillis: 1_500,
		Width: 1280, Height: 720, FPS: 24,
	}
}

type fakeAudioAnalyzer struct {
	calls                     int
	lipOffsetMillis           int64
	ambienceHardSilenceMillis int64
	sealSHA256                string
}

func (f *fakeAudioAnalyzer) AnalyzerSealSHA256() string { return f.sealSHA256 }

func (f *fakeAudioAnalyzer) Analyze(_ context.Context, input AudioAnalysisRequest) (AudioAnalysis, error) {
	f.calls++
	lipExpectations, err := deriveLipSyncExpectations(input.Request)
	if err != nil {
		return AudioAnalysis{}, err
	}
	sources := []string{input.FinalMix.Digest, input.FinalVideo.Digest}
	for _, mix := range input.NativeMixes {
		sources = append(sources, mix.Digest)
	}
	if input.Dialogue != nil {
		sources = append(sources, input.Dialogue.Digest)
	}
	timings := make([]CueTimingMeasurement, 0, len(input.Request.Subtitle.Cues))
	lip := make([]LipSyncMeasurement, 0, len(input.Request.Subtitle.Cues))
	transcript := ""
	for _, cue := range input.Request.Subtitle.Cues {
		expected := lipExpectations[cue.ID]
		if f.lipOffsetMillis < 0 || f.lipOffsetMillis >= expected.EndMillis-expected.StartMillis {
			return AudioAnalysis{}, errors.New("fixture lip-sync offset is outside the authoritative cue/run window")
		}
		timings = append(timings, CueTimingMeasurement{
			CueID: cue.ID, SpeechStartMillis: cue.StartMillis, SpeechEndMillis: cue.EndMillis,
		})
		lip = append(lip, LipSyncMeasurement{
			RunID: expected.RunID, CueID: cue.ID, Required: expected.Required,
			AudioStartMillis: expected.StartMillis,
			AudioEndMillis:   expected.EndMillis - f.lipOffsetMillis,
			MouthStartMillis: expected.StartMillis + f.lipOffsetMillis,
			MouthEndMillis:   expected.EndMillis,
		})
		transcript += cue.Text
	}
	transitions := make([]AmbienceTransitionMeasurement, 0, max(len(input.Request.RunIDs)-1, 0))
	for index := 1; index < len(input.Request.RunIDs); index++ {
		from := input.Request.Clips[index-1].Ambience
		to := input.Request.Clips[index].Ambience
		hardSilence := f.ambienceHardSilenceMillis
		if hardSilence == 0 {
			hardSilence = 20
		}
		transitions = append(transitions, AmbienceTransitionMeasurement{
			FromRunID: input.Request.RunIDs[index-1], ToRunID: input.Request.RunIDs[index],
			ContinuityRequired: from.ContinuityIntoNext,
			FromIdentity:       from.Identity, FromVersion: from.Version,
			ToIdentity: to.Identity, ToVersion: to.Version,
			HardSilenceMillis: hardSilence, LoudnessDeltaLUFS: 0.2,
		})
	}
	analysis := AudioAnalysis{
		SchemaVersion: AudioAnalysisSchemaVersion, AnalysisID: "analysis-v1",
		Analyzer: "fixture-audio-analyzer", AnalyzerVersion: "fixture-v1",
		Evidence: input.Request.Evidence, ASR: cerevaluation.FrozenASRConfig(),
		Transcript: transcript, SourceHashes: sources, CueTimings: timings,
		LipSync: lip, AmbienceTransitions: transitions,
		AudioVideoStartMillis: make([]int64, len(input.Request.Clips)),
		IntegratedLUFS:        -16, TruePeakDBTP: -1.2,
	}
	analysis.ContentHash, _ = digestJSON(analysis.digestInput())
	return analysis, nil
}

func nativeAudioRequest(t *testing.T) Request {
	t.Helper()
	request := mustRequest(t, EvidenceMockOnly)
	request.AudioStrategy = providercontract.AudioStrategyNativePreferred
	request.Speech = SpeechConfig{}
	for index := range request.Clips {
		request.Clips[index].ProviderVideo = &ProviderVideoEvidence{
			ProviderJobID:     "provider-job-" + request.Clips[index].RunID,
			ProviderRequestID: "provider-request-" + request.Clips[index].RunID,
			Provider:          "fixture-video", Model: "fixture-native-audio-v1", Version: "route-v1",
			GenerateAudio: true, AudioDelivery: providercontract.NativeAudioMix,
		}
		request.Clips[index].Ambience = &AmbienceBinding{
			Identity: "room-tone-main", Version: "ambience-v1",
			ContinuityIntoNext: index+1 < len(request.Clips),
		}
		request.Clips[index].LipSyncRequired = true
	}
	return request
}
