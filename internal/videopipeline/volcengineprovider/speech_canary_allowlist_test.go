package volcengineprovider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

func TestSpeechV2RequiresConfiguredCanaryAllowlist(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, request := testSpeechCanaryFixture(t)
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

	speech := &fakeSpeechSynthesizer{}
	server, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	status, providerErr := submitSpeechCanary(t, httpServer.URL, request)
	if status != http.StatusUnprocessableEntity || speech.callCount() != 0 ||
		providerErr == nil || providerErr.Code != providercontract.CodeInvalidRequest ||
		providerErr.Retryable || !providerErr.RequiresAction {
		t.Fatalf("missing canary allowlist returned status=%d and made %d Provider call(s); want 422/0", status, speech.callCount())
	}
}

func TestSpeechV2CanaryAllowlistContract(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*runtimeconfig.VolcengineProvider)
		wantStatus int
		wantCalls  int
	}{
		{
			name:       "complete exact allowlist",
			mutate:     func(*runtimeconfig.VolcengineProvider) {},
			wantStatus: http.StatusCreated,
			wantCalls:  1,
		},
		{
			name: "partial allowlist",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryParentVoiceVersion = ""
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "job identity drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryJobID = "speech-v2-fedcba9876543210fedcba9876543210"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "input identity drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryInputHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "cue identity drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryCueID = "cue-002"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "voice asset drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryVoiceAssetID = "10400000-0000-4000-8000-000000000099"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "voice version drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryVoiceVersion = "10400000-0000-4000-8000-000000000099"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "voice digest drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryVoiceHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "license identity drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryLicenseSnapshotID = "10400000-0000-4000-8000-000000000099"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "license digest drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryLicenseHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "AFP ceiling drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryMaximumAFPMilli = 1
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "cash ceiling drift",
			mutate: func(config *runtimeconfig.VolcengineProvider) {
				config.SpeechCanaryMaximumCashMicros = 1
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			config, request := testSpeechCanaryFixture(t)
			tt.mutate(&config)
			speech := &fakeSpeechSynthesizer{}
			server, err := New(config, &fakeProvider{}, store, Options{Speech: speech})
			if err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(server.Handler())
			defer httpServer.Close()

			status, providerErr := submitSpeechCanary(t, httpServer.URL, request)
			if status != tt.wantStatus || speech.callCount() != tt.wantCalls {
				t.Fatalf("submit returned status=%d and calls=%d; want %d/%d", status, speech.callCount(), tt.wantStatus, tt.wantCalls)
			}
			if tt.wantStatus == http.StatusCreated {
				if providerErr != nil {
					t.Fatalf("successful submit returned error: %#v", providerErr)
				}
				return
			}
			if providerErr == nil || providerErr.Code != providercontract.CodeInvalidRequest ||
				providerErr.Retryable || !providerErr.RequiresAction ||
				providerErr.SafeMessage != "speech job is outside the frozen single-call canary" {
				t.Fatalf("fail-closed error = %#v", providerErr)
			}
		})
	}
}

func submitSpeechCanary(
	t *testing.T,
	endpoint string,
	request providercontract.JobRequest,
) (int, *providercontract.Error) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest, err := http.NewRequest(http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Idempotency-Key", request.JobID)
	response, err := authenticatedTestClient(t).Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusCreated {
		return response.StatusCode, nil
	}
	var envelope struct {
		Error *providercontract.Error `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode speech canary error: %v", err)
	}
	return response.StatusCode, envelope.Error
}
