package postproduction

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
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

func TestSpeechV2BatchReusesCompletedCueAndSubmitsRemainingCuesSerially(t *testing.T) {
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
	v2 := speechV2Request(t)
	completed, err := speech.Synthesize(t.Context(), v2)
	if err != nil {
		t.Fatal(err)
	}
	speech.calls = 0
	request := mustRequest(t, EvidenceLive)
	request.EpisodeRevisionID = v2.EpisodeRevisionID
	request.Subtitle = v2.SubtitleRevision
	request.Speech = v2.Config
	request.Speech.BatchAuthorization = batchForSpeechRequest(t, request, []Cue{request.Subtitle.Cues[1]})
	request.Speech.CompletedAttempts = []ProviderAttempt{completed}
	request.Speech.AuthorizedCueID = ""
	request.Speech.MaximumAFPMilli = 0
	request.Speech.MaximumNonSubscriptionCashMicros = 0
	request.Speech.MaxAttempts = 0
	authorized := make([]string, 0, 1)
	request.AuthorizePaidSubmit = func(_ context.Context, cue Cue) error {
		authorized = append(authorized, cue.ID)
		return nil
	}

	result, err := service.Finalize(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if speech.calls != 1 || media.calls != 1 || len(result.SpeechAttempts) != 2 ||
		len(authorized) != 1 || authorized[0] != "cue-002" ||
		result.SpeechAttempts[0].JobID != completed.JobID {
		t.Fatalf("batch result calls=%d media=%d authorized=%v attempts=%#v", speech.calls, media.calls, authorized, result.SpeechAttempts)
	}
}

func TestSpeechV2BatchStopsBeforeLaterCueAfterFailure(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := speechV2Request(t)
	third := Cue{
		ID: "cue-003", Speaker: "旁白", Text: "山门关闭。",
		VoiceRef: base.Config.Voice.AssetVersionID, StartMillis: 2_600, EndMillis: 2_900,
	}
	subtitle, err := NewSubtitleRevision(
		base.SubtitleRevision.ID, "", 1, "zh-CN", []string{"script-v1"},
		append(append([]Cue(nil), base.SubtitleRevision.Cues...), third),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := mustRequest(t, EvidenceLive)
	request.EpisodeRevisionID = base.EpisodeRevisionID
	request.Subtitle = subtitle
	request.Speech = base.Config
	request.Speech.BatchAuthorization = batchForSpeechRequest(t, request, request.Subtitle.Cues)
	request.Speech.AuthorizedCueID = ""
	request.Speech.MaximumAFPMilli = 0
	request.Speech.MaximumNonSubscriptionCashMicros = 0
	request.Speech.MaxAttempts = 0
	fake := &orderedFailingSpeech{
		delegate: &speechV2CanaryFake{store: store}, failCue: "cue-002",
	}
	media := &fakeMedia{store: store}
	service, err := NewService(fake, media, store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Finalize(t.Context(), request)
	if err == nil || fake.calls != 2 || strings.Join(fake.cues, ",") != "cue-001,cue-002" || media.calls != 0 {
		t.Fatalf("Finalize() error=%v calls=%d cues=%v media=%d", err, fake.calls, fake.cues, media.calls)
	}
}

func TestSpeechV2BatchAmbiguousJobUsesReadOnlyReconciliation(t *testing.T) {
	t.Parallel()
	base := speechV2Request(t)
	request := mustRequest(t, EvidenceLive)
	request.EpisodeRevisionID = base.EpisodeRevisionID
	request.Subtitle = base.SubtitleRevision
	request.Speech = base.Config
	request.Speech.BatchAuthorization = batchForSpeechRequest(t, request, []Cue{request.Subtitle.Cues[1]})
	request.Speech.AuthorizedCueID = ""
	request.Speech.MaximumAFPMilli = 0
	request.Speech.MaximumNonSubscriptionCashMicros = 0
	request.Speech.MaxAttempts = 0
	input := SpeechRequest{
		EpisodeRevisionID: request.EpisodeRevisionID,
		SubtitleRevision:  request.Subtitle, Cue: request.Subtitle.Cues[1],
		Config: request.Speech, Evidence: request.Evidence,
		TraceID: request.TraceID, BudgetMicros: 50,
	}
	identity, err := DeriveSpeechJobIdentity(input)
	if err != nil {
		t.Fatal(err)
	}
	gets, posts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, httpRequest *http.Request) {
		switch httpRequest.Method {
		case http.MethodGet:
			gets++
			_ = json.NewEncoder(writer).Encode(providercontract.JobResponse{
				JobID: identity.JobID, State: providercontract.StatusUnknown,
				Error: &providercontract.Error{Code: providercontract.CodeUnavailable, Retryable: false},
			})
		case http.MethodPost:
			posts++
			writer.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	provider, err := NewHTTPSpeechProvider(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Synthesize(t.Context(), input)
	var providerErr *providercontract.Error
	if !errors.As(err, &providerErr) || providerErr.Retryable || gets != 1 || posts != 0 {
		t.Fatalf("Synthesize() error=%v gets=%d posts=%d", err, gets, posts)
	}
}

func batchForSpeechRequest(t *testing.T, request Request, cues []Cue) *speechcontract.BatchAuthorization {
	t.Helper()
	batch := &speechcontract.BatchAuthorization{
		SchemaVersion:              speechcontract.SchemaVersion,
		ParentExecutionPackageHash: strings.Repeat("d", 64),
		ApprovalCommentID:          "10400000-0000-4000-8000-000000000030",
		ApprovalActorID:            "10400000-0000-4000-8000-000000000031",
		ValidUntil:                 "2026-08-31T15:59:59Z",
		Provider:                   request.Speech.Voice.Provider, ModelID: request.Speech.Voice.ModelID,
		RouteVersion: request.Speech.Route.RouteVersion,
		ResourceID:   request.Speech.Voice.ResourceID, Speaker: request.Speech.Voice.Speaker,
		VoiceAssetID:              request.Speech.Voice.AssetID,
		ParentVoiceAssetVersionID: request.Speech.Voice.ParentAssetVersionID,
		VoiceAssetVersionID:       request.Speech.Voice.AssetVersionID,
		VoiceAssetVersionHash:     request.Speech.Voice.AssetVersionHash,
		LicenseSnapshotID:         request.Speech.Voice.LicenseSnapshotID,
		LicenseSnapshotHash:       request.Speech.Voice.LicenseSnapshotHash,
		MaximumSubmits:            len(cues), Cues: make([]speechcontract.CueAuthorization, 0, len(cues)),
	}
	for _, cue := range cues {
		identity, err := DeriveSpeechJobIdentity(SpeechRequest{
			EpisodeRevisionID: request.EpisodeRevisionID,
			SubtitleRevision:  request.Subtitle, Cue: cue, Config: request.Speech,
		})
		if err != nil {
			t.Fatal(err)
		}
		characters := len([]rune(strings.TrimSpace(cue.Text)))
		estimated := int64(characters) * 135
		maximum := estimated + 100
		batch.Cues = append(batch.Cues, speechcontract.CueAuthorization{
			CueID: cue.ID, JobID: identity.JobID, InputHash: identity.InputHash,
			UnicodeCharacters: characters, EstimatedAFPMilli: estimated,
			MaximumAFPMilli: maximum, MaxAttempts: 1,
		})
		batch.EstimatedAFPMilli += estimated
		batch.MaximumAFPMilli += maximum
	}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
	return batch
}

type orderedFailingSpeech struct {
	delegate *speechV2CanaryFake
	failCue  string
	calls    int
	cues     []string
}

func (f *orderedFailingSpeech) Synthesize(ctx context.Context, request SpeechRequest) (ProviderAttempt, error) {
	f.calls++
	f.cues = append(f.cues, request.Cue.ID)
	if request.Cue.ID == f.failCue {
		return ProviderAttempt{}, errors.New("fixture speech failure")
	}
	return f.delegate.Synthesize(ctx, request)
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
				ResourceID: "seed-tts-2.0", Speaker: "zh_female_vv_uranus_bigtts",
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
