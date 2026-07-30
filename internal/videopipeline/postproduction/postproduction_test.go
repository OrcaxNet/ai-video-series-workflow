package postproduction

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
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
		{Artifact: testArtifact("dialogue_segment", "audio/wav", "d")},
		{Artifact: testArtifact("dialogue_segment", "audio/wav", "e")},
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
}

func (f *fakeMedia) Render(
	ctx context.Context,
	request Request,
	_ []byte,
	_ []ProviderAttempt,
) (RenderResult, error) {
	dialogue, err := f.store.Put(ctx, strings.NewReader("dialogue:"+request.Subtitle.ContentHash))
	if err != nil {
		return RenderResult{}, err
	}
	video, err := f.store.Put(ctx, strings.NewReader("video:"+request.EpisodeRevisionHash))
	if err != nil {
		return RenderResult{}, err
	}
	planHash, _ := digestJSON(map[string]any{"request": request.EpisodeRevisionHash})
	return RenderResult{
		Dialogue: Artifact{
			Kind: "dialogue_audio", Digest: dialogue.Digest, URI: dialogue.URI,
			MediaType: "audio/wav", SizeBytes: dialogue.Size, DurationMillis: request.DurationMillis(),
		},
		FinalVideo: Artifact{
			Kind: "final_video", Digest: video.Digest, URI: video.URI,
			MediaType: "video/mp4", SizeBytes: video.Size, DurationMillis: request.DurationMillis(),
			Width: 1280, Height: 720, FPS: 24,
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
