package postproduction

import (
	"context"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

func TestDeriveSpeechV2JobIdentityBindsFrozenVoiceSemantics(t *testing.T) {
	t.Parallel()
	base := speechV2Request(t)
	first, err := DeriveSpeechJobIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := DeriveSpeechJobIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	if first != replayed || !strings.HasPrefix(first.JobID, "speech-v2-") ||
		len(first.JobID) != len("speech-v2-")+32 || len(first.InputHash) != 64 {
		t.Fatalf("speech-v2 identity = %#v, replay = %#v", first, replayed)
	}

	tests := []struct {
		name   string
		mutate func(*SpeechRequest)
	}{
		{name: "episode revision", mutate: func(value *SpeechRequest) { value.EpisodeRevisionID += "-next" }},
		{name: "subtitle content", mutate: func(value *SpeechRequest) { value.SubtitleRevision.ContentHash = strings.Repeat("8", 64) }},
		{name: "cue", mutate: func(value *SpeechRequest) { value.Cue.ID = "cue-002" }},
		{name: "voice revision", mutate: func(value *SpeechRequest) {
			voice := *value.Config.Voice
			voice.AssetVersionID = "10400000-0000-4000-8000-000000000099"
			value.Config.Voice = &voice
		}},
		{name: "route", mutate: func(value *SpeechRequest) { value.Config.Route.RouteVersion += "-next" }},
		{name: "resource", mutate: func(value *SpeechRequest) {
			voice := *value.Config.Voice
			voice.ResourceID += "-next"
			value.Config.Voice = &voice
		}},
		{name: "speaker", mutate: func(value *SpeechRequest) {
			voice := *value.Config.Voice
			voice.Speaker += "-next"
			value.Config.Voice = &voice
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := base
			tt.mutate(&changed)
			identity, err := DeriveSpeechJobIdentity(changed)
			if err != nil {
				t.Fatal(err)
			}
			if identity == first {
				t.Fatalf("%s drift reused %#v", tt.name, first)
			}
		})
	}
}

func TestSpeechV2CanaryStopsAfterOneAuthorizedCueBeforeMediaRender(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	speech := &speechV2CanaryFake{store: store}
	media := &fakeMedia{store: store}
	service, err := NewService(speech, media, store)
	if err != nil {
		t.Fatal(err)
	}
	request := mustRequest(t, EvidenceLive)
	v2 := speechV2Request(t)
	request.Speech = v2.Config
	request.EpisodeRevisionID = v2.EpisodeRevisionID
	request.Subtitle = v2.SubtitleRevision
	request.AuthorizePaidSubmit = func(_ context.Context, cue Cue) error {
		if cue.ID != request.Speech.AuthorizedCueID {
			t.Fatalf("authorized cue = %q", cue.ID)
		}
		speech.authorizations++
		return nil
	}

	_, err = service.Finalize(t.Context(), request)
	if providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("Finalize() error = %v", err)
	}
	if speech.calls != 1 || speech.authorizations != 1 || media.calls != 0 {
		t.Fatalf("canary boundaries = speech:%d authorize:%d media:%d", speech.calls, speech.authorizations, media.calls)
	}
}

func speechV2Request(t *testing.T) SpeechRequest {
	t.Helper()
	subtitle, err := NewSubtitleRevision(
		"subtitle-v2", "", 1, "zh-CN", []string{"script-v1"},
		[]Cue{
			{ID: "cue-001", Speaker: "旁白", Text: "镜头越过清晨的山谷。", VoiceRef: "10400000-0000-4000-8000-000000000011", StartMillis: 0, EndMillis: 1_000},
			{ID: "cue-002", Speaker: "旁白", Text: "风声渐近。", VoiceRef: "10400000-0000-4000-8000-000000000011", StartMillis: 1_250, EndMillis: 2_500},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return SpeechRequest{
		EpisodeRevisionID: "10400000-0000-4000-8000-000000000001",
		SubtitleRevision:  subtitle,
		Cue:               subtitle.Cues[0],
		Config: SpeechConfig{
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "VOLCENGINE", ModelID: "doubao-seed-tts-2.0",
				RouteVersion: "agent-plan-large-tts-v2", CapabilityHash: strings.Repeat("c", 64),
				Verification: providercontract.PendingKey,
			},
			ProviderProfileID:   "10400000-0000-4000-8000-000000000020",
			BudgetApprovalID:    "10400000-0000-4000-8000-000000000021",
			BudgetMaximumMicros: 100, BudgetCurrency: "CNY",
			IdentityVersion: SpeechIdentityV2,
			Voice: &SpeechVoiceBinding{
				AssetID:              "10400000-0000-4000-8000-00000000000f",
				ParentAssetVersionID: "10400000-0000-4000-8000-000000000010",
				AssetVersionID:       "10400000-0000-4000-8000-000000000011",
				AssetVersionHash:     strings.Repeat("a", 64),
				LicenseSnapshotID:    "10400000-0000-4000-8000-000000000012",
				LicenseSnapshotHash:  strings.Repeat("b", 64),
				Provider:             "volcengine_ark", ModelID: "doubao-seed-tts-2.0",
				ResourceID: "seed-tts-2.0", Speaker: "zh_female_tianmeitaozi_mars_bigtts",
			},
			AuthorizedCueID: "cue-001", MaximumAFPMilli: 2_228,
			MaximumNonSubscriptionCashMicros: 0, MaxAttempts: 1,
		},
		Evidence: EvidenceLive, TraceID: "trace-speech-v2", BudgetMicros: 50,
	}
}

type speechV2CanaryFake struct {
	store          *artifactstore.Store
	calls          int
	authorizations int
}

func (f *speechV2CanaryFake) Synthesize(ctx context.Context, request SpeechRequest) (ProviderAttempt, error) {
	f.calls++
	identity, err := DeriveSpeechJobIdentity(request)
	if err != nil {
		return ProviderAttempt{}, err
	}
	committed, err := f.store.Put(ctx, strings.NewReader("speech-v2-canary"))
	if err != nil {
		return ProviderAttempt{}, err
	}
	characters := int64(len([]rune(strings.TrimSpace(request.Cue.Text))))
	zero := int64(0)
	return ProviderAttempt{
		CueID: request.Cue.ID, JobID: identity.JobID,
		RequestID: "tts-request-id", UpstreamTaskID: "tts-connect-id",
		ConnectID: "tts-connect-id", LogID: "tts-log-id", Model: request.Config.Route,
		Usage: providercontract.Usage{
			GeneratedChars: characters, OutputUnits: characters * 135, Unit: "milli_afp",
		},
		Cost: providercontract.Cost{
			EstimatedMicros: request.BudgetMicros, ActualMicros: &zero,
			Currency: request.Config.BudgetCurrency, PricingVersion: "agent-plan-large-included-v1",
			Verified: true, BillingMode: "subscription_included",
		},
		Artifact: Artifact{
			Kind: "dialogue_segment", Digest: committed.Digest, URI: committed.URI,
			MediaType: "audio/mpeg", SizeBytes: committed.Size,
			DurationMillis: request.Cue.EndMillis - request.Cue.StartMillis,
		},
		Evidence: request.Evidence,
	}, nil
}

func TestValidateProviderAttemptRequiresAgentPlanTTSUsageAndTraceEvidence(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	input := SpeechRequest{
		Cue: Cue{ID: "cue-1"}, Evidence: EvidenceLive, BudgetMicros: 100,
		Config: SpeechConfig{
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "volcengine_ark", ModelID: "doubao-seed-tts-2.0",
				RouteVersion: "agent-plan-large-tts-v1", CapabilityHash: digest,
				Verification: providercontract.PendingKey,
			},
			BudgetCurrency: "CNY",
		},
	}
	zero := int64(0)
	valid := ProviderAttempt{
		CueID: "cue-1", JobID: "job-1", RequestID: "request-1", UpstreamTaskID: "connect-1",
		ConnectID: "connect-1", LogID: "log-1", Model: input.Config.Route,
		Usage: providercontract.Usage{GeneratedChars: 5, OutputUnits: 675, Unit: "milli_afp"},
		Cost: providercontract.Cost{
			EstimatedMicros: 0, ActualMicros: &zero, Currency: "CNY",
			PricingVersion: "agent-plan-large-included-v1",
		},
		Artifact: Artifact{
			Kind: "dialogue_segment", Digest: digest, URI: "cas://sha256/" + digest,
			MediaType: "audio/mpeg", SizeBytes: 10, DurationMillis: 1_000,
		},
		Evidence: EvidenceLive,
	}
	if err := validateProviderAttempt(input, valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ProviderAttempt)
	}{
		{name: "missing connect", mutate: func(value *ProviderAttempt) { value.ConnectID = "" }},
		{name: "missing log ID", mutate: func(value *ProviderAttempt) { value.LogID = "" }},
		{name: "missing provider usage", mutate: func(value *ProviderAttempt) { value.Usage.GeneratedChars = 0 }},
		{name: "wrong AFP attribution", mutate: func(value *ProviderAttempt) { value.Usage.OutputUnits++ }},
		{name: "wrong unit", mutate: func(value *ProviderAttempt) { value.Usage.Unit = "afp" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempt := valid
			tt.mutate(&attempt)
			if err := validateProviderAttempt(input, attempt); err == nil {
				t.Fatal("invalid Agent Plan TTS evidence unexpectedly passed")
			}
		})
	}
}
