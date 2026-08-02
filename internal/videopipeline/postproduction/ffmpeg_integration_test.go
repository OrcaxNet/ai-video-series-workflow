//go:build integration

package postproduction

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

func TestFFmpegProcessor_RendersDeterministicDelivery(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	ctx := context.Background()
	fixtures := t.TempDir()
	makeFixture(t, fixtures, "clip-1.mp4", []string{
		"-f", "lavfi", "-i", "color=c=blue:s=320x180:r=24:d=1",
		"-an", "-c:v", "libx264", "-pix_fmt", "yuv420p",
	})
	makeFixture(t, fixtures, "clip-2.mp4", []string{
		"-f", "lavfi", "-i", "color=c=green:s=320x180:r=24:d=1",
		"-an", "-c:v", "libx264", "-pix_fmt", "yuv420p",
	})
	makeFixture(t, fixtures, "cue-1.wav", []string{
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=0.6",
		"-c:a", "pcm_s16le",
	})
	makeFixture(t, fixtures, "cue-2.wav", []string{
		"-f", "lavfi", "-i", "sine=frequency=660:sample_rate=48000:duration=0.6",
		"-c:a", "pcm_s16le",
	})
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clip1 := commitFixture(t, ctx, store, filepath.Join(fixtures, "clip-1.mp4"), "shot_video", "video/mp4")
	clip2 := commitFixture(t, ctx, store, filepath.Join(fixtures, "clip-2.mp4"), "shot_video", "video/mp4")
	cue1 := commitFixture(t, ctx, store, filepath.Join(fixtures, "cue-1.wav"), "dialogue_segment", "audio/wav")
	cue2 := commitFixture(t, ctx, store, filepath.Join(fixtures, "cue-2.wav"), "dialogue_segment", "audio/wav")
	subtitle, err := NewSubtitleRevision(
		"subtitle-fixture-v1", "", 1, "zh-CN", []string{"shot-1", "shot-2"},
		[]Cue{
			{ID: "cue-1", Text: "第一句", StartMillis: 100, EndMillis: 700},
			{ID: "cue-2", Text: "第二句", StartMillis: 1_100, EndMillis: 1_700},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		SchemaVersion:       SchemaVersion,
		Evidence:            EvidenceLive,
		EpisodeRevisionID:   "episode-fixture",
		EpisodeRevisionHash: strings.Repeat("a", 64),
		RunIDs:              []string{"run-1", "run-2"},
		Clips: []Clip{
			integrationClip("run-1", "shot-1", clip1),
			integrationClip("run-2", "shot-2", clip2),
		},
		Subtitle: subtitle,
		Speech: SpeechConfig{
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "fake", ModelID: "fixture-tts", RouteVersion: "v1",
				CapabilityHash: strings.Repeat("b", 64), Verification: "live_provider_call",
			},
			ProviderProfileID: "profile", BudgetApprovalID: "budget",
			BudgetMaximumMicros: 100, BudgetCurrency: "CNY",
		},
		Output: OutputPolicy{
			Width: 320, Height: 180, FPS: 24, Format: "mp4",
			// The external SRT is always produced. Keep burn-in disabled here
			// because some host FFmpeg distributions omit the optional libass
			// subtitles filter; the production image verifies that separately.
			BurnSubtitles: false, AudioSampleRate: 48_000, AudioChannels: 2,
		},
		Gates: []GateBinding{
			{Gate: "G1", DecisionID: "g1", Decision: "APPROVED", ContentHash: strings.Repeat("c", 64)},
			{Gate: "G2", DecisionID: "g2", Decision: "APPROVED", ContentHash: strings.Repeat("d", 64)},
		},
		TraceID: "integration",
	}
	attempts := []ProviderAttempt{
		{CueID: "cue-1", Artifact: cue1},
		{CueID: "cue-2", Artifact: cue2},
	}
	subtitleBytes, err := RenderSRT(subtitle, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewFFmpegProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := processor.Render(ctx, request, subtitleBytes, attempts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := processor.Render(ctx, request, subtitleBytes, attempts)
	if err != nil {
		t.Fatal(err)
	}
	if first.FinalVideo.Digest != second.FinalVideo.Digest ||
		first.Dialogue.Digest != second.Dialogue.Digest ||
		first.CommandPlanHash != second.CommandPlanHash {
		t.Fatalf("FFmpeg output is not deterministic:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.FinalVideo.Width != 320 || first.FinalVideo.Height != 180 ||
		first.FinalVideo.FPS != 24 || first.FinalVideo.DurationMillis != 2_000 {
		t.Fatalf("unexpected final media spec: %#v", first.FinalVideo)
	}
	assertCommittedAudioSpec(t, store, first.Dialogue, 2_000, 48_000, 2)
	if len(first.AudioTimingCorrections) != 2 ||
		first.QC.AudioVideoStartP95Millis == nil ||
		first.QC.SubtitleBoundaryP95Millis == nil ||
		first.QC.ManualTimingRequired || first.QC.State != "TIMING_PASSED_CER_PENDING" {
		t.Fatalf("live timing evidence was not measured: %#v %#v", first.QC, first.AudioTimingCorrections)
	}
	for _, correction := range first.AudioTimingCorrections {
		if correction.EndTrimApplied || correction.HardCutDetected {
			t.Fatalf("live dialogue was hard-cut: %#v", correction)
		}
	}

	// The contract mock deliberately stores JSON fixture bytes while labeling
	// their intended media type. mock_only finalization must turn those lineage
	// artifacts into playable deterministic lavfi sources, never present them
	// as decoded live Provider media.
	mockClip1 := commitBytesFixture(t, ctx, store, "mock video contract 1", "shot_video", "video/mp4")
	mockClip2 := commitBytesFixture(t, ctx, store, "mock video contract 2", "shot_video", "video/mp4")
	mockCue1 := commitBytesFixture(t, ctx, store, "mock audio contract 1", "dialogue_segment", "audio/wav")
	mockCue2 := commitBytesFixture(t, ctx, store, "mock audio contract 2", "dialogue_segment", "audio/wav")
	mockRequest := request
	mockRequest.Evidence = EvidenceMockOnly
	mockRequest.Speech.Route.Verification = "mock_only"
	mockRequest.Clips = append([]Clip(nil), request.Clips...)
	mockRequest.Clips[0].Artifact = mockClip1
	mockRequest.Clips[1].Artifact = mockClip2
	mockAttempts := []ProviderAttempt{
		{CueID: "cue-1", Artifact: mockCue1},
		{CueID: "cue-2", Artifact: mockCue2},
	}
	mockResult, err := processor.Render(ctx, mockRequest, subtitleBytes, mockAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if mockResult.FinalVideo.Width != 320 || mockResult.FinalVideo.Height != 180 ||
		mockResult.FinalVideo.FPS != 24 || mockResult.FinalVideo.DurationMillis != 2_000 {
		t.Fatalf("unexpected mock final media spec: %#v", mockResult.FinalVideo)
	}

	// Native audio is extracted from each immutable Provider MP4 and retained
	// as a mix. With no failed cue, no TTS input or fake Dialogue stem exists.
	nativeRequest := mockRequest
	nativeRequest.AudioStrategy = providercontract.AudioStrategyNativePreferred
	nativeRequest.Speech = SpeechConfig{}
	nativeRequest.Clips = append([]Clip(nil), mockRequest.Clips...)
	for index := range nativeRequest.Clips {
		nativeRequest.Clips[index].ProviderVideo = &ProviderVideoEvidence{
			ProviderJobID:     "job-" + nativeRequest.Clips[index].RunID,
			ProviderRequestID: "request-" + nativeRequest.Clips[index].RunID,
			Provider:          "mock", Model: "native-audio-fixture", Version: "v1",
			GenerateAudio: true, AudioDelivery: providercontract.NativeAudioMix,
		}
		nativeRequest.Clips[index].Ambience = &AmbienceBinding{
			Identity: "integration-room-tone", Version: "v1",
			ContinuityIntoNext: index+1 < len(nativeRequest.Clips),
		}
		nativeRequest.Clips[index].LipSyncRequired = true
	}
	nativeResult, err := processor.Render(ctx, nativeRequest, subtitleBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	nativeReplay, err := processor.Render(ctx, nativeRequest, subtitleBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if nativeResult.Dialogue.Kind != "" || len(nativeResult.NativeMixes) != 2 ||
		nativeResult.FinalMix.Kind != "final_mix" {
		t.Fatalf("unexpected native audio artifacts: %#v", nativeResult)
	}
	if nativeReplay.FinalVideo.Digest != nativeResult.FinalVideo.Digest ||
		nativeReplay.FinalMix.Digest != nativeResult.FinalMix.Digest ||
		nativeReplay.NativeMixes[0].Digest != nativeResult.NativeMixes[0].Digest ||
		nativeReplay.CommandPlanHash != nativeResult.CommandPlanHash {
		t.Fatalf("native FFmpeg output is not deterministic: first=%#v replay=%#v", nativeResult, nativeReplay)
	}
	for _, mix := range nativeResult.NativeMixes {
		assertCommittedAudioSpec(t, store, mix, 1_000, 48_000, 2)
	}
	assertCommittedAudioSpec(t, store, nativeResult.FinalMix, 2_000, 48_000, 2)
}

func assertCommittedAudioSpec(
	t *testing.T,
	store *artifactstore.Store,
	artifact Artifact,
	wantDurationMillis int64,
	wantSampleRate int,
	wantChannels int,
) {
	t.Helper()
	source, err := store.Open(artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	path := filepath.Join(t.TempDir(), "dialogue.wav")
	target, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(
		"ffprobe", "-v", "error",
		"-show_entries", "format=duration:stream=codec_type,duration,sample_rate,channels",
		"-of", "json", path,
	).Output()
	if err != nil {
		t.Fatal(err)
	}
	probe, err := parseAudioProbe(output)
	if err != nil {
		t.Fatal(err)
	}
	if probe.DurationMillis != wantDurationMillis ||
		probe.SampleRate != wantSampleRate || probe.Channels != wantChannels {
		t.Fatalf("dialogue probe = %#v, want %dms %dHz/%dch", probe, wantDurationMillis, wantSampleRate, wantChannels)
	}
	if artifact.DurationMillis != probe.DurationMillis {
		t.Fatalf("dialogue manifest duration = %dms, probed %dms", artifact.DurationMillis, probe.DurationMillis)
	}
}

func TestParseAudioProbe(t *testing.T) {
	t.Parallel()
	payload, err := json.Marshal(map[string]any{
		"streams": []map[string]any{{
			"codec_type": "audio", "duration": "50.000000",
			"sample_rate": "48000", "channels": 2,
		}},
		"format": map[string]any{"duration": "50.000000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := parseAudioProbe(payload)
	if err != nil {
		t.Fatal(err)
	}
	if probe.DurationMillis != 50_000 || probe.SampleRate != 48_000 || probe.Channels != 2 {
		t.Fatalf("unexpected audio probe: %#v", probe)
	}
}

func TestService_MockProviderToPlayableDelivery(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	ctx := context.Background()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	provider := mockprovider.New(runtimeconfig.MockProvider{
		ProviderID:   "postproduction-integration",
		Capabilities: []string{"speech.primary"},
	}, store)
	server := httptest.NewServer(provider.Handler())
	defer server.Close()
	speech, err := NewHTTPSpeechProvider(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	speech.PollInterval = time.Millisecond
	media, err := NewFFmpegProcessor(store)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(speech, media, store)
	if err != nil {
		t.Fatal(err)
	}
	request := mustRequest(t, EvidenceMockOnly)
	request.Clips = append([]Clip(nil), request.Clips...)
	request.Clips[0].Artifact = commitBytesFixture(
		t, ctx, store, `{"kind":"mock-video-contract","shot":1}`, "shot_video", "video/mp4",
	)
	request.Clips[1].Artifact = commitBytesFixture(
		t, ctx, store, `{"kind":"mock-video-contract","shot":2}`, "shot_video", "video/mp4",
	)
	request.Speech.BudgetMaximumMicros = 1_000
	request.Output = OutputPolicy{
		Width: 320, Height: 180, FPS: 24, Format: "mp4",
		AudioSampleRate: 48_000, AudioChannels: 2,
	}
	first, err := service.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestHash != second.ManifestHash ||
		first.FinalVideo.Digest != second.FinalVideo.Digest ||
		len(first.SpeechAttempts) != len(request.Subtitle.Cues) {
		t.Fatalf("mock Provider replay diverged:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.Evidence != EvidenceMockOnly ||
		first.QC.MeasurementEvidence != EvidenceMockOnly ||
		!first.QC.ManualTimingRequired {
		t.Fatalf("mock Provider result promoted its evidence: %#v", first.QC)
	}
}

func makeFixture(t *testing.T, directory, name string, args []string) {
	t.Helper()
	fullArgs := append([]string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}, args...)
	fullArgs = append(fullArgs, filepath.Join(directory, name))
	command := exec.Command("ffmpeg", fullArgs...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create %s: %v: %s", name, err, output)
	}
}

func commitFixture(
	t *testing.T,
	ctx context.Context,
	store *artifactstore.Store,
	path, kind, mediaType string,
) Artifact {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	committed, err := store.Put(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	return Artifact{
		Kind: kind, Digest: committed.Digest, URI: committed.URI,
		MediaType: mediaType, SizeBytes: committed.Size,
		DurationMillis: 1_000, Width: 320, Height: 180, FPS: 24,
	}
}

func commitBytesFixture(
	t *testing.T,
	ctx context.Context,
	store *artifactstore.Store,
	payload, kind, mediaType string,
) Artifact {
	t.Helper()
	committed, err := store.Put(ctx, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	return Artifact{
		Kind: kind, Digest: committed.Digest, URI: committed.URI,
		MediaType: mediaType, SizeBytes: committed.Size,
		DurationMillis: 1_000, Width: 320, Height: 180, FPS: 24,
	}
}

func integrationClip(runID, shotID string, artifact Artifact) Clip {
	return Clip{
		RunID: runID, ShotSpecRevisionID: shotID, ShotSpecHash: strings.Repeat("e", 64),
		PromptSnapshotID: "prompt-" + shotID, PromptSnapshotHash: strings.Repeat("f", 64),
		ContextSnapshotID: "context-" + shotID, ContextSnapshotHash: strings.Repeat("1", 64),
		Artifact: artifact, DurationMillis: 1_000, LicenseReference: "fixture-license",
	}
}
