package volcengineprovider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
)

func TestSpeechV2BatchRejectsOutOfOrderThenRunsStrictlySerial(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, requests := testSpeechBatchFixture(t, store)
	speech := &fakeSpeechSynthesizer{}
	adapter, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech, Inspector: fakeSpeechInspector(),
		Now: func() time.Time { return time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()
	driftTests := []struct {
		name   string
		mutate func(*providercontract.JobRequest)
	}{
		{name: "non-approved cue", mutate: func(request *providercontract.JobRequest) {
			request.Request.PromptSnapshotID = "subtitle-v2:cue-999"
		}},
		{name: "old job", mutate: func(request *providercontract.JobRequest) {
			request.InputHash = strings.Repeat("f", 64)
			request.JobID = "speech-v2-" + request.InputHash[:32]
			request.Request.RequestID = request.JobID
			request.Request.IdempotencyKey = request.JobID
			rebindSpeechBatchBudget(t, request)
		}},
		{name: "retry enabled", mutate: func(request *providercontract.JobRequest) {
			request.Request.Budget.MaxAttempts = 2
			rebindSpeechBatchBudget(t, request)
		}},
		{name: "voice drift", mutate: func(request *providercontract.JobRequest) {
			request.Request.Assets = append([]providercontract.AssetRef(nil), request.Request.Assets...)
			request.Request.Assets[0].Revision = "10400000-0000-4000-8000-000000000099"
		}},
		{name: "character budget drift", mutate: func(request *providercontract.JobRequest) {
			request.Request.Prompt += "额外"
		}},
	}
	for _, tt := range driftTests {
		t.Run(tt.name, func(t *testing.T) {
			drifted := requests[0]
			tt.mutate(&drifted)
			status, _ := submitSpeechCanary(t, server.URL, drifted)
			if status == http.StatusCreated || status == http.StatusOK || speech.callCount() != 0 {
				t.Fatalf("drifted status=%d calls=%d", status, speech.callCount())
			}
		})
	}

	status, providerErr := submitSpeechCanary(t, server.URL, requests[1])
	if status != http.StatusConflict || providercontract.ErrorCodeOf(providerErr) != providercontract.CodeConflict ||
		speech.callCount() != 0 {
		t.Fatalf("out-of-order status=%d error=%v calls=%d", status, providerErr, speech.callCount())
	}
	for index, request := range requests {
		status, providerErr = submitSpeechCanary(t, server.URL, request)
		if status != http.StatusCreated || providerErr != nil || speech.callCount() != index+1 {
			t.Fatalf("cue %d status=%d error=%v calls=%d", index, status, providerErr, speech.callCount())
		}
	}
	if replayed := postJob(t, server.URL, requests[0]); replayed.State != providercontract.StatusSucceeded || speech.callCount() != len(requests) {
		t.Fatalf("replay=%#v calls=%d", replayed, speech.callCount())
	}
}

func TestSpeechV2BatchAcrossAdaptersConsumesFirstAuthorizationOnce(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, requests := testSpeechBatchFixture(t, store)
	speech := &fakeSpeechSynthesizer{}
	options := Options{
		Speech: speech, Inspector: fakeSpeechInspector(),
		Now: func() time.Time { return time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC) },
	}
	first, err := New(cfg, &fakeProvider{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg, &fakeProvider{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	servers := []*httptest.Server{httptest.NewServer(first.Handler()), httptest.NewServer(second.Handler())}
	defer servers[0].Close()
	defer servers[1].Close()
	body, err := json.Marshal(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		status int
		err    error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, server := range servers {
		wait.Add(1)
		go func(endpoint string) {
			defer wait.Done()
			request, err := http.NewRequest(http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(body))
			if err != nil {
				results <- result{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", requests[0].JobID)
			response, err := authenticatedTestClient(t).Do(request)
			if err != nil {
				results <- result{err: err}
				return
			}
			response.Body.Close()
			results <- result{status: response.StatusCode}
		}(server.URL)
	}
	wait.Wait()
	close(results)
	statuses := map[int]int{}
	for outcome := range results {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		statuses[outcome.status]++
	}
	if speech.callCount() != 1 || statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != 1 {
		t.Fatalf("calls=%d statuses=%v", speech.callCount(), statuses)
	}
}

func testSpeechBatchFixture(
	t *testing.T,
	store *artifactstore.Store,
) (runtimeconfig.VolcengineProvider, []providercontract.JobRequest) {
	t.Helper()
	cfg, first := testSpeechCanaryFixture(t)
	storeSpeechCanaryVoiceDescriptor(t, store, &cfg, &first)
	first.InputHash = strings.Repeat("a", 64)
	first.JobID = "speech-v2-" + first.InputHash[:32]
	first.Request.RequestID = first.JobID
	first.Request.IdempotencyKey = first.JobID
	first.Request.PromptSnapshotID = "subtitle-v2:cue-002"
	rebindSpeechBatchBudget(t, &first)
	second := first
	second.InputHash = strings.Repeat("b", 64)
	second.JobID = "speech-v2-" + second.InputHash[:32]
	second.Request.RequestID = second.JobID
	second.Request.IdempotencyKey = second.JobID
	second.Request.Prompt = "长街灯火逐盏亮起。"
	second.Request.PromptSnapshotID = "subtitle-v2:cue-004"
	rebindSpeechBatchBudget(t, &second)
	requests := []providercontract.JobRequest{first, second}
	batch := &speechcontract.BatchAuthorization{
		SchemaVersion:              speechcontract.SchemaVersion,
		ParentExecutionPackageHash: strings.Repeat("d", 64),
		ApprovalCommentID:          "10400000-0000-4000-8000-000000000030",
		ApprovalActorID:            "10400000-0000-4000-8000-000000000031",
		ValidUntil:                 "2026-08-31T15:59:59Z",
		Provider:                   "volcengine_ark", ModelID: cfg.SpeechModel,
		RouteVersion: AgentPlanTTSRouteVersion, ResourceID: AgentPlanTTSResourceID,
		Speaker: cfg.SpeechSpeaker, VoiceAssetID: cfg.SpeechCanaryVoiceAssetID,
		ParentVoiceAssetVersionID: cfg.SpeechCanaryParentVoiceVersion,
		VoiceAssetVersionID:       cfg.SpeechCanaryVoiceVersion,
		VoiceAssetVersionHash:     cfg.SpeechCanaryVoiceHash,
		LicenseSnapshotID:         cfg.SpeechCanaryLicenseSnapshotID,
		LicenseSnapshotHash:       cfg.SpeechCanaryLicenseHash,
		MaximumSubmits:            len(requests), Cues: make([]speechcontract.CueAuthorization, 0, len(requests)),
	}
	for index, request := range requests {
		characters := len([]rune(strings.TrimSpace(request.Request.Prompt)))
		estimated := int64(characters) * ttsAFPMilliPerChar
		maximum := estimated + 100
		batch.Cues = append(batch.Cues, speechcontract.CueAuthorization{
			CueID: []string{"cue-002", "cue-004"}[index], JobID: request.JobID,
			InputHash: request.InputHash, UnicodeCharacters: characters,
			EstimatedAFPMilli: estimated, MaximumAFPMilli: maximum, MaxAttempts: 1,
		})
		batch.EstimatedAFPMilli += estimated
		batch.MaximumAFPMilli += maximum
	}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.SpeechBatchAuthorization = batch
	cfg.SpeechCanaryJobID = ""
	cfg.SpeechCanaryInputHash = ""
	cfg.SpeechCanaryCueID = ""
	cfg.SpeechCanaryVoiceAssetID = ""
	cfg.SpeechCanaryParentVoiceVersion = ""
	cfg.SpeechCanaryVoiceVersion = ""
	cfg.SpeechCanaryVoiceHash = ""
	cfg.SpeechCanaryLicenseSnapshotID = ""
	cfg.SpeechCanaryLicenseHash = ""
	cfg.SpeechCanaryMaximumAFPMilli = 0
	cfg.SpeechCanaryMaximumCashMicros = 0
	return cfg, requests
}

func rebindSpeechBatchBudget(t *testing.T, request *providercontract.JobRequest) {
	t.Helper()
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID:  request.BudgetReservation.ReservationID,
		Currency:       request.BudgetReservation.Currency,
		AmountMicros:   request.Request.Budget.MaxCostMicros,
		PricingVersion: request.BudgetReservation.PricingVersion,
		ConfirmedBy:    request.BudgetReservation.ConfirmedBy,
	}, providercontract.BudgetBindingInput{
		RunID: request.RunID, InputHash: request.InputHash,
		Model: request.Model, Budget: request.Request.Budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.BudgetReservation = reservation
}
