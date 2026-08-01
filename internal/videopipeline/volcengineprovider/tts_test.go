package volcengineprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

func TestAgentPlanTTS_UsesFixedPlanContractAndReturnsAuditableUsage(t *testing.T) {
	t.Parallel()
	credential := strings.Join([]string{"runtime", "plan", "credential"}, "-")
	audio := []byte("fixture-mp3-audio")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/tts" {
			http.NotFound(w, r)
			return
		}
		for name, want := range map[string]string{
			"X-Api-Key":                             credential,
			"X-Api-Resource-Id":                     AgentPlanTTSResourceID,
			"X-Api-Request-Id":                      "request-uuid",
			"X-Api-Connect-Id":                      "connect-uuid",
			"X-Control-Require-Usage-Tokens-Return": "*",
		} {
			if got := r.Header.Get(name); got != want {
				t.Errorf("%s = %q, want %q", name, got, want)
			}
		}
		var payload struct {
			Request struct {
				Text    string `json:"text"`
				Speaker string `json:"speaker"`
				Audio   struct {
					Format     string `json:"format"`
					SampleRate int    `json:"sample_rate"`
				} `json:"audio_params"`
			} `json:"req_params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if payload.Request.Text != "你好，世界" || payload.Request.Speaker != "speaker-v2" ||
			payload.Request.Audio.Format != "mp3" || payload.Request.Audio.SampleRate != 24_000 {
			t.Errorf("payload = %#v", payload)
		}
		w.Header().Set("X-Tt-Logid", "tts-log-id")
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintf(w, "{\"code\":0,\"data\":%q}\n", base64.StdEncoding.EncodeToString(audio[:7]))
		fmt.Fprintf(w, "{\"code\":0,\"data\":%q}\n", base64.StdEncoding.EncodeToString(audio[7:]))
		fmt.Fprintln(w, `{"code":20000000,"usage":{"text_words":5}}`)
	}))
	defer server.Close()

	client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
		Endpoint: server.URL + "/tts", APIKey: credential, HTTPClient: server.Client(),
		NewRequestID: func() string { return "request-uuid" },
		NewConnectID: func() string { return "connect-uuid" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Synthesize(t.Context(), SpeechSynthesisRequest{Text: "你好，世界", Speaker: "speaker-v2"})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Audio) != string(audio) || result.MediaType != "audio/mpeg" ||
		result.RequestID != "request-uuid" || result.ConnectID != "connect-uuid" ||
		result.LogID != "tts-log-id" || result.UsageTokens != 5 {
		t.Fatalf("result = %#v", result)
	}
	usage, err := TTSUsageAttributes(result.UsageTokens)
	if err != nil {
		t.Fatal(err)
	}
	if usage.GeneratedChars != 5 || usage.OutputUnits != 675 || usage.Unit != "milli_afp" {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestAgentPlanTTS_FailsClosedOnMissingEvidenceAndClassifiesErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		status int
		body   string
		want   providercontract.ErrorCode
	}{
		{name: "missing usage", status: http.StatusOK, body: `{"code":0,"data":"YXVkaW8="}` + "\n" + `{"code":20000000}`, want: providercontract.CodeUnavailable},
		{name: "authentication", status: http.StatusUnauthorized, body: `{}`, want: providercontract.CodeUnauthenticated},
		{name: "quota", status: http.StatusTooManyRequests, body: `{}`, want: providercontract.CodeQuotaExceeded},
		{name: "bad HTTP request", status: http.StatusBadRequest, body: `{"code":45000001}`, want: providercontract.CodeInvalidRequest},
		{name: "invalid request", status: http.StatusOK, body: `{"code":45000001}`, want: providercontract.CodeInvalidRequest},
		{name: "resource mismatch", status: http.StatusOK, body: `{"code":55000000}`, want: providercontract.CodeModelUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Tt-Logid", "safe-log-id")
				w.WriteHeader(tt.status)
				_, _ = fmt.Fprint(w, tt.body)
			}))
			defer server.Close()
			client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
				Endpoint: server.URL, APIKey: "fixture-key", HTTPClient: server.Client(),
				NewRequestID: func() string { return "request-uuid" },
				NewConnectID: func() string { return "connect-uuid" },
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Synthesize(t.Context(), SpeechSynthesisRequest{Text: "测试", Speaker: "speaker-v2"})
			if providercontract.ErrorCodeOf(err) != tt.want {
				t.Fatalf("error = %v (%s), want %s", err, providercontract.ErrorCodeOf(err), tt.want)
			}
			if strings.Contains(fmt.Sprint(err), "fixture-key") || strings.Contains(fmt.Sprint(err), "测试") {
				t.Fatalf("error leaked protected input: %v", err)
			}
		})
	}
}

func TestFindUsageTokensAcceptsDocumentedNestedAdditionEncoding(t *testing.T) {
	t.Parallel()
	frame := map[string]any{
		"addition": `{"usage":{"text_words":17}}`,
	}
	if got := findUsageTokens(frame); got != 17 {
		t.Fatalf("usage tokens = %d, want 17", got)
	}
}

func TestAgentPlanTTS_RejectsDialogueOver600CharsBeforeHTTP(t *testing.T) {
	t.Parallel()
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
		Endpoint: server.URL, APIKey: "fixture-key", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Synthesize(t.Context(), SpeechSynthesisRequest{
		Text: strings.Repeat("字", AgentPlanTTSMaxChars+1), Speaker: "speaker-v2",
	})
	if providercontract.ErrorCodeOf(err) != providercontract.CodeInvalidRequest || calls != 0 {
		t.Fatalf("error = %v, HTTP calls = %d", err, calls)
	}
	usage, err := TTSUsageAttributes(AgentPlanTTSMaxChars)
	if err != nil {
		t.Fatal(err)
	}
	if usage.OutputUnits != 81_000 {
		t.Fatalf("600 chars attributed %d milli-AFP, want 81000", usage.OutputUnits)
	}
}

type fakeSpeechSynthesizer struct {
	mu    sync.Mutex
	calls int
}

func (s *fakeSpeechSynthesizer) Synthesize(context.Context, SpeechSynthesisRequest) (SpeechSynthesisResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return SpeechSynthesisResult{
		Audio: []byte("adapter-speech-fixture"), MediaType: "audio/mpeg",
		RequestID: "tts-request-id", ConnectID: "tts-connect-id", LogID: "tts-log-id",
		UsageTokens: 5,
	}, nil
}

func TestServer_SpeechSubmitCommitsCASAndReplaysWithoutSecondTTSCall(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{}
	speech := &fakeSpeechSynthesizer{}
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	adapter, err := New(cfg, provider, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()
	response, err := authenticatedTestClient(t).Get(server.URL + "/v1/capabilities")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var capabilities struct {
		Capabilities []providercontract.CapabilitySnapshot `json:"capabilities"`
	}
	if err := json.NewDecoder(response.Body).Decode(&capabilities); err != nil {
		t.Fatal(err)
	}
	if len(capabilities.Capabilities) != 2 ||
		capabilities.Capabilities[1].Alias != providercontract.CapabilitySpeech ||
		capabilities.Capabilities[1].Capability.Verification != providercontract.PendingKey ||
		capabilities.Capabilities[1].Capability.ModelFamily != AgentPlanTTSModelID ||
		capabilities.Capabilities[1].Limits["resourceId"] != AgentPlanTTSResourceID {
		t.Fatalf("speech capability = %#v", capabilities.Capabilities)
	}
	request := testSpeechJobRequest(t)
	created := postJob(t, server.URL, request)
	if created.State != providercontract.StatusSucceeded || created.RequestID != "tts-request-id" ||
		created.ConnectID != "tts-connect-id" || created.UpstreamTaskID != "tts-connect-id" ||
		created.LogID != "tts-log-id" || len(created.Artifacts) != 1 ||
		created.Usage.GeneratedChars != 5 || created.Usage.OutputUnits != 675 ||
		created.Usage.Unit != "milli_afp" {
		t.Fatalf("created = %#v", created)
	}
	artifact := created.Artifacts[0]
	if artifact.Kind != providercontract.ModalityAudio || artifact.MediaType != "audio/mpeg" ||
		artifact.URI != "cas://sha256/"+artifact.SHA256 || artifact.DurationMillis != 2_000 {
		t.Fatalf("artifact = %#v", artifact)
	}
	if exists, err := store.Exists(artifact.SHA256); err != nil || !exists {
		t.Fatalf("CAS exists = %v, err = %v", exists, err)
	}
	record, err := os.ReadFile(adapter.recordPath(request.JobID))
	if err != nil {
		t.Fatal(err)
	}
	for _, protected := range [][]byte{[]byte(request.Request.Prompt), []byte(request.Request.ModelHint), []byte("ARK_API_KEY")} {
		if bytes.Contains(record, protected) {
			t.Fatalf("persisted provider record leaked protected input %q", protected)
		}
	}
	replayed := postJob(t, server.URL, request)
	if replayed.LogID != created.LogID || speech.calls != 1 || provider.submitCount != 0 {
		t.Fatalf("replay = %#v, TTS calls = %d, video calls = %d", replayed, speech.calls, provider.submitCount)
	}
}

func testSpeechJobRequest(t *testing.T) providercontract.JobRequest {
	t.Helper()
	inputHash := sha256.Sum256([]byte("speech immutable input"))
	capabilityHash := sha256.Sum256([]byte("agent-plan-tts-v1"))
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: "speech-job-1", RunID: "episode-1",
		Capability: providercontract.CapabilitySpeech,
		InputHash:  hex.EncodeToString(inputHash[:]),
		Model: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilitySpeech), Provider: "volcengine_ark",
			ModelID: AgentPlanTTSModelID, RouteVersion: "agent-plan-large-tts-v1",
			CapabilityHash: hex.EncodeToString(capabilityHash[:]), Verification: providercontract.PendingKey,
		},
		Request: providercontract.GenerationRequest{
			RequestID: "speech-job-1", IdempotencyKey: "speech-job-1",
			Modality: providercontract.ModalityAudio, Prompt: "你好，世界",
			PromptSnapshotID: "subtitle-1:cue-1", ModelHint: "speaker-v2",
			Output: providercontract.OutputSpec{DurationMillis: 2_000, Format: "mp3"},
			Budget: providercontract.BudgetEnvelope{EstimatedCostMicros: 81, MaxCostMicros: 81, MaxAttempts: 1},
		},
		TraceID: "trace-speech-1",
	}
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID: "speech-budget-1", Currency: "CNY", AmountMicros: 81,
		PricingVersion: "agent-plan-large-included-v1", ConfirmedBy: "stage1-approval",
	}, providercontract.BudgetBindingInput{
		RunID: request.RunID, InputHash: request.InputHash, Model: request.Model, Budget: request.Request.Budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.BudgetReservation = reservation
	return request
}
