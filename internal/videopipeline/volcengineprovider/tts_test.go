package volcengineprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestAgentPlanTTS_DefaultEndpointUsesDocumentedV3ChunkedPath(t *testing.T) {
	t.Parallel()
	client, err := NewAgentPlanTTS(AgentPlanTTSConfig{APIKey: "fixture-key"})
	if err != nil {
		t.Fatal(err)
	}
	const want = "https://openspeech.bytedance.com/api/v3/plan/tts/unidirectional"
	if client.endpoint != want {
		t.Fatalf("default endpoint = %q, want %q", client.endpoint, want)
	}
}

func TestAgentPlanTTS_RejectsNonPlanEndpointBeforeHTTP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "standard OpenSpeech route", endpoint: "https://openspeech.bytedance.com/api/v3/tts/unidirectional"},
		{name: "plan route with query", endpoint: AgentPlanTTSEndpoint + "?billing=other"},
		{name: "plan route with trailing slash", endpoint: AgentPlanTTSEndpoint + "/"},
		{name: "plan route with whitespace", endpoint: " " + AgentPlanTTSEndpoint},
		{name: "different host", endpoint: "https://example.com/api/v3/plan/tts/unidirectional"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			_, err := NewAgentPlanTTS(AgentPlanTTSConfig{
				Endpoint: tt.endpoint,
				APIKey:   "fixture-key",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					return nil, fmt.Errorf("unexpected HTTP call")
				})},
			})
			if providercontract.ErrorCodeOf(err) != providercontract.CodeInvalidRequest || calls != 0 {
				t.Fatalf("error = %v, HTTP calls = %d", err, calls)
			}
		})
	}
}

func TestAgentPlanTTS_UsesFixedPlanContractAndReturnsAuditableUsage(t *testing.T) {
	t.Parallel()
	credential := strings.Join([]string{"runtime", "plan", "credential"}, "-")
	audio := []byte("fixture-mp3-audio")
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.String() != AgentPlanTTSEndpoint {
			t.Errorf("request = %s %s", r.Method, r.URL)
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
		body := fmt.Sprintf(
			"{\"code\":0,\"data\":%q}\n{\"code\":0,\"data\":%q}\n{\"code\":20000000,\"usage\":{\"text_words\":5}}\n",
			base64.StdEncoding.EncodeToString(audio[:7]),
			base64.StdEncoding.EncodeToString(audio[7:]),
		)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{"application/x-ndjson"},
				"X-Tt-Logid":   []string{"tts-log-id"},
			},
			Body: io.NopCloser(strings.NewReader(body)),
		}, nil
	})}

	provider, err := NewAgentPlanTTS(AgentPlanTTSConfig{
		APIKey: credential, HTTPClient: client,
		NewRequestID: func() string { return "request-uuid" },
		NewConnectID: func() string { return "connect-uuid" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Synthesize(t.Context(), SpeechSynthesisRequest{Text: "你好，世界", Speaker: "speaker-v2"})
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
			client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
				APIKey: "fixture-key",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.URL.String() != AgentPlanTTSEndpoint {
						t.Errorf("request URL = %q", request.URL)
					}
					return &http.Response{
						StatusCode: tt.status,
						Header:     http.Header{"X-Tt-Logid": []string{"safe-log-id"}},
						Body:       io.NopCloser(strings.NewReader(tt.body)),
					}, nil
				})},
				NewRequestID: func() string { return "request-uuid" },
				NewConnectID: func() string { return "connect-uuid" },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Synthesize(t.Context(), SpeechSynthesisRequest{Text: "测试", Speaker: "speaker-v2"})
			if providercontract.ErrorCodeOf(err) != tt.want {
				t.Fatalf("error = %v (%s), want %s", err, providercontract.ErrorCodeOf(err), tt.want)
			}
			if result.RequestID != "request-uuid" || result.ConnectID != "connect-uuid" || result.LogID != "safe-log-id" {
				t.Fatalf("failure evidence = %#v", result)
			}
			if strings.Contains(fmt.Sprint(err), "fixture-key") || strings.Contains(fmt.Sprint(err), "测试") {
				t.Fatalf("error leaked protected input: %v", err)
			}
		})
	}
}

func TestAgentPlanTTSErrorMappingRetainsExactSanitizedStatusWithoutInferringUnknown55(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       int
		providerCode string
		messageClass string
		want         providercontract.ErrorCode
	}{
		{name: "authentication wins", status: http.StatusUnauthorized, providerCode: "55000000", messageClass: "authentication", want: providercontract.CodeUnauthenticated},
		{name: "known unavailable voice", status: http.StatusOK, providerCode: "55000000", messageClass: "speaker", want: providercontract.CodeModelUnavailable},
		{name: "unknown 55 remains unclassified", status: http.StatusUnprocessableEntity, providerCode: "55999999", messageClass: "speaker", want: providercontract.CodeUnavailable},
		{name: "unknown 55 is not inferred from HTTP quota", status: http.StatusTooManyRequests, providerCode: "55999999", messageClass: "quota", want: providercontract.CodeUnavailable},
		{name: "provider contract", status: http.StatusOK, providerCode: "45000001", messageClass: "request_contract", want: providercontract.CodeInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapTTSError(tt.status, tt.providerCode, tt.messageClass)
			var providerErr *providercontract.Error
			if !errors.As(err, &providerErr) {
				t.Fatalf("mapTTSError() = %T %v", err, err)
			}
			if providerErr.Code != tt.want || providerErr.HTTPStatus != tt.status ||
				providerErr.ProviderCode != tt.providerCode ||
				providerErr.ProviderMessageClass != tt.messageClass {
				t.Fatalf("mapped error = %#v", providerErr)
			}
		})
	}
}

func TestAgentPlanTTSRetainsSafeFailureHeadersAndLogID(t *testing.T) {
	t.Parallel()
	client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
		APIKey: "fixture-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Header: http.Header{
					"X-Api-Status-Code": []string{"55999999"},
					"X-Api-Message":     []string{"speaker unavailable for private tenant"},
					"X-Tt-Logid":        []string{"tts-safe-log-id"},
				},
				Body: io.NopCloser(strings.NewReader(`{"code":55000000,"message":"raw body is ignored"}`)),
			}, nil
		})},
		NewRequestID: func() string { return "request-uuid" },
		NewConnectID: func() string { return "connect-uuid" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Synthesize(t.Context(), SpeechSynthesisRequest{Text: "测试", Speaker: "speaker-v2"})
	var providerErr *providercontract.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("Synthesize() error = %T %v", err, err)
	}
	if result.LogID != "tts-safe-log-id" || providerErr.HTTPStatus != http.StatusUnprocessableEntity ||
		providerErr.ProviderCode != "55999999" ||
		providerErr.ProviderMessageClass != "speaker" ||
		providerErr.Code != providercontract.CodeUnavailable {
		t.Fatalf("failure result = %#v, error = %#v", result, providerErr)
	}
	if strings.Contains(providerErr.SafeMessage, "private tenant") ||
		strings.Contains(providerErr.SafeMessage, "raw body") {
		t.Fatalf("failure leaked raw Provider text: %#v", providerErr)
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
	client, err := NewAgentPlanTTS(AgentPlanTTSConfig{
		APIKey: "fixture-key",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, fmt.Errorf("unexpected HTTP call")
		})},
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
	mu     sync.Mutex
	calls  int
	result SpeechSynthesisResult
	err    error
}

func (s *fakeSpeechSynthesizer) Synthesize(context.Context, SpeechSynthesisRequest) (SpeechSynthesisResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return s.result, s.err
	}
	if len(s.result.Audio) != 0 {
		return s.result, nil
	}
	return SpeechSynthesisResult{
		Audio: []byte("adapter-speech-fixture"), MediaType: "audio/mpeg",
		RequestID: "tts-request-id", ConnectID: "tts-connect-id", LogID: "tts-log-id",
		UsageTokens: 5,
	}, nil
}

func (s *fakeSpeechSynthesizer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
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
	if replayed.LogID != created.LogID || speech.callCount() != 1 || provider.submitCount != 0 {
		t.Fatalf("replay = %#v, TTS calls = %d, video calls = %d", replayed, speech.callCount(), provider.submitCount)
	}
}

func TestServer_SpeechRetryRequiresExactRecordAndPreservesFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		configure  func(*runtimeconfig.VolcengineProvider, string)
		wantCalls  int
		wantStatus providercontract.JobStatus
	}{
		{
			name: "exact one-shot authorization",
			configure: func(cfg *runtimeconfig.VolcengineProvider, digest string) {
				cfg.SpeechRetryJobID = "speech-job-1"
				cfg.SpeechRetryRecord = digest
			},
			wantCalls: 1, wantStatus: providercontract.StatusSucceeded,
		},
		{
			name: "record hash mismatch",
			configure: func(cfg *runtimeconfig.VolcengineProvider, _ string) {
				cfg.SpeechRetryJobID = "speech-job-1"
				cfg.SpeechRetryRecord = strings.Repeat("0", 64)
			},
			wantCalls: 0, wantStatus: providercontract.StatusRequiresAction,
		},
		{
			name: "job mismatch",
			configure: func(cfg *runtimeconfig.VolcengineProvider, digest string) {
				cfg.SpeechRetryJobID = "speech-other"
				cfg.SpeechRetryRecord = digest
			},
			wantCalls: 0, wantStatus: providercontract.StatusRequiresAction,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			request := testSpeechJobRequest(t)
			record := failedSpeechRecord(t, request)
			seed, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.createRecord(request.JobID, record); err != nil {
				t.Fatal(err)
			}
			digest, err := persistedRecordSHA256(record)
			if err != nil {
				t.Fatal(err)
			}
			cfg := testLiveConfig()
			cfg.SpeechModel = AgentPlanTTSModelID
			tt.configure(&cfg, digest)
			speech := &fakeSpeechSynthesizer{}
			adapter, err := New(cfg, &fakeProvider{}, store, Options{
				Speech: speech,
				Now:    func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
			})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(adapter.Handler())
			defer server.Close()

			response := postJob(t, server.URL, request)
			if response.State != tt.wantStatus || speech.callCount() != tt.wantCalls {
				t.Fatalf("response state = %s, calls = %d", response.State, speech.callCount())
			}
			persisted, ok, err := adapter.loadRecord(request.JobID)
			if err != nil || !ok {
				t.Fatalf("load record ok = %v, err = %v", ok, err)
			}
			if tt.wantCalls == 0 {
				if len(persisted.Reconciliations) != 0 {
					t.Fatalf("unauthorized retry changed record: %#v", persisted.Reconciliations)
				}
				return
			}
			if len(persisted.Reconciliations) != 1 ||
				persisted.Reconciliations[0].PreviousResponse.State != providercontract.StatusRequiresAction ||
				persisted.Reconciliations[0].AuthorizedRecordSHA256 != digest ||
				persisted.Reconciliations[0].StartedAt != "2027-01-15T08:00:00Z" {
				t.Fatalf("reconciliation = %#v", persisted.Reconciliations)
			}
			replayed := postJob(t, server.URL, request)
			if replayed.State != providercontract.StatusSucceeded || speech.callCount() != 1 {
				t.Fatalf("replay state = %s, calls = %d", replayed.State, speech.callCount())
			}
		})
	}
}

func TestServer_SpeechRetryConsumesAuthorizationBeforeRetryableFailure(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := testSpeechJobRequest(t)
	record := failedSpeechRecord(t, request)
	seed, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.createRecord(request.JobID, record); err != nil {
		t.Fatal(err)
	}
	digest, err := persistedRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	cfg.SpeechRetryJobID = request.JobID
	cfg.SpeechRetryRecord = digest
	speech := &fakeSpeechSynthesizer{err: &providercontract.Error{
		Code: providercontract.CodeUnavailable, Retryable: true, SafeMessage: "fixture unavailable",
	}}
	adapter, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	status := submitSpeechJobStatus(t, server.URL, request)
	if status != http.StatusServiceUnavailable || speech.callCount() != 1 {
		t.Fatalf("first status = %d, calls = %d", status, speech.callCount())
	}
	replayed := postJob(t, server.URL, request)
	if replayed.State != providercontract.StatusUnknown || speech.callCount() != 1 {
		t.Fatalf("replay state = %s, calls = %d", replayed.State, speech.callCount())
	}
	persisted, ok, err := adapter.loadRecord(request.JobID)
	if err != nil || !ok || len(persisted.Reconciliations) != 1 {
		t.Fatalf("persisted record = %#v, ok = %v, err = %v", persisted, ok, err)
	}
}

func TestServer_SpeechSecondReconciliationPersistsEvidenceAndBlocksThirdCall(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := testSpeechJobRequest(t)
	initial := failedSpeechRecord(t, request)
	seed, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.createRecord(request.JobID, initial); err != nil {
		t.Fatal(err)
	}

	firstDigest, err := persistedRecordSHA256(initial)
	if err != nil {
		t.Fatal(err)
	}
	firstConfig := testLiveConfig()
	firstConfig.SpeechModel = AgentPlanTTSModelID
	firstConfig.SpeechRetryJobID = request.JobID
	firstConfig.SpeechRetryRecord = firstDigest
	firstSpeech := &fakeSpeechSynthesizer{err: &providercontract.Error{
		Code:        providercontract.CodeUnauthenticated,
		SafeMessage: "first fixture authentication failure",
	}}
	firstAdapter, err := New(firstConfig, &fakeProvider{}, store, Options{
		Speech: firstSpeech,
		Now:    func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	firstServer := httptest.NewServer(firstAdapter.Handler())
	if status := submitSpeechJobStatus(t, firstServer.URL, request); status != http.StatusUnauthorized {
		firstServer.Close()
		t.Fatalf("first reconciliation status = %d", status)
	}
	firstServer.Close()
	if firstSpeech.callCount() != 1 {
		t.Fatalf("first reconciliation calls = %d", firstSpeech.callCount())
	}
	firstFailure, ok, err := firstAdapter.loadRecord(request.JobID)
	if err != nil || !ok || len(firstFailure.Reconciliations) != 1 {
		t.Fatalf("first failure = %#v, ok = %v, err = %v", firstFailure, ok, err)
	}
	secondAuthorizedDigest, err := persistedRecordSHA256(firstFailure)
	if err != nil {
		t.Fatal(err)
	}

	secondConfig := testLiveConfig()
	secondConfig.SpeechModel = AgentPlanTTSModelID
	secondConfig.SpeechRetryJobID = request.JobID
	secondConfig.SpeechRetryRecord = secondAuthorizedDigest
	secondSpeech := &fakeSpeechSynthesizer{
		result: SpeechSynthesisResult{
			MediaType: "audio/mpeg", RequestID: "failed-request-id",
			ConnectID: "failed-connect-id", LogID: "failed-log-id", UsageTokens: 2,
		},
		err: &providercontract.Error{
			Code:        providercontract.CodeUnauthenticated,
			SafeMessage: "second fixture authentication failure",
		},
	}
	secondAdapter, err := New(secondConfig, &fakeProvider{}, store, Options{
		Speech: secondSpeech,
		Now:    func() time.Time { return time.Unix(1_800_000_100, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	secondServer := httptest.NewServer(secondAdapter.Handler())
	if status := submitSpeechJobStatus(t, secondServer.URL, request); status != http.StatusUnauthorized {
		secondServer.Close()
		t.Fatalf("second reconciliation status = %d", status)
	}
	secondServer.Close()
	if secondSpeech.callCount() != 1 {
		t.Fatalf("second reconciliation calls = %d", secondSpeech.callCount())
	}
	secondFailure, ok, err := secondAdapter.loadRecord(request.JobID)
	if err != nil || !ok {
		t.Fatalf("second failure ok = %v, err = %v", ok, err)
	}
	if len(secondFailure.Reconciliations) != 2 ||
		secondFailure.Reconciliations[0].Attempt != 1 ||
		secondFailure.Reconciliations[0].AuthorizedRecordSHA256 != firstDigest ||
		secondFailure.Reconciliations[1].Attempt != 2 ||
		secondFailure.Reconciliations[1].AuthorizedRecordSHA256 != secondAuthorizedDigest ||
		secondFailure.Reconciliations[1].PreviousResponse.Error == nil ||
		secondFailure.Reconciliations[1].PreviousResponse.Error.Code != providercontract.CodeUnauthenticated {
		t.Fatalf("reconciliation history = %#v", secondFailure.Reconciliations)
	}
	if secondFailure.Response.State != providercontract.StatusRequiresAction ||
		secondFailure.Response.RequestID != "failed-request-id" ||
		secondFailure.Response.ConnectID != "failed-connect-id" ||
		secondFailure.Response.LogID != "failed-log-id" ||
		secondFailure.Response.Usage.GeneratedChars != 2 ||
		secondFailure.Response.Usage.OutputUnits != 270 {
		t.Fatalf("second failure response = %#v", secondFailure.Response)
	}

	thirdDigest, err := persistedRecordSHA256(secondFailure)
	if err != nil {
		t.Fatal(err)
	}
	thirdConfig := testLiveConfig()
	thirdConfig.SpeechModel = AgentPlanTTSModelID
	thirdConfig.SpeechRetryJobID = request.JobID
	thirdConfig.SpeechRetryRecord = thirdDigest
	thirdSpeech := &fakeSpeechSynthesizer{}
	thirdAdapter, err := New(thirdConfig, &fakeProvider{}, store, Options{Speech: thirdSpeech})
	if err != nil {
		t.Fatal(err)
	}
	thirdServer := httptest.NewServer(thirdAdapter.Handler())
	defer thirdServer.Close()
	replayed := postJob(t, thirdServer.URL, request)
	if replayed.State != providercontract.StatusRequiresAction || thirdSpeech.callCount() != 0 {
		t.Fatalf("third replay state = %s, calls = %d", replayed.State, thirdSpeech.callCount())
	}
}

func TestQASecondReconciliationAcrossAdapterInstancesIsSingleCall(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := testSpeechJobRequest(t)
	record := failedSpeechRecord(t, request)
	record.Reconciliations = []speechReconciliation{{
		Attempt:                1,
		StartedAt:              "2027-01-15T08:00:00Z",
		AuthorizedRecordSHA256: strings.Repeat("a", 64),
		PreviousResponse:       record.Response,
	}}
	seed, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.createRecord(request.JobID, record); err != nil {
		t.Fatal(err)
	}
	digest, err := persistedRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}

	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	cfg.SpeechRetryJobID = request.JobID
	cfg.SpeechRetryRecord = digest
	speech := &fakeSpeechSynthesizer{}
	first, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech,
		Now:    func() time.Time { return time.Unix(1_800_000_100, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech,
		Now:    func() time.Time { return time.Unix(1_800_000_101, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Model two long-lived processes that cached the same attempt-1 record
	// before the attempt-2 authorization was consumed.
	if _, ok, err := first.loadRecord(request.JobID); err != nil || !ok {
		t.Fatalf("first cache prime: ok=%v err=%v", ok, err)
	}
	if _, ok, err := second.loadRecord(request.JobID); err != nil || !ok {
		t.Fatalf("second cache prime: ok=%v err=%v", ok, err)
	}

	firstHTTP := httptest.NewServer(first.Handler())
	defer firstHTTP.Close()
	secondHTTP := httptest.NewServer(second.Handler())
	defer secondHTTP.Close()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	client := authenticatedTestClient(t)
	urls := []string{firstHTTP.URL, secondHTTP.URL}
	type result struct {
		status int
		err    error
	}
	results := make(chan result, len(urls))
	var wg sync.WaitGroup
	for _, endpoint := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			httpRequest, requestErr := http.NewRequest(http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(body))
			if requestErr != nil {
				results <- result{err: requestErr}
				return
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Idempotency-Key", request.JobID)
			response, requestErr := client.Do(httpRequest)
			if requestErr != nil {
				results <- result{err: requestErr}
				return
			}
			response.Body.Close()
			results <- result{status: response.StatusCode}
		}()
	}
	wg.Wait()
	close(results)
	statuses := make(map[int]int)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		statuses[result.status]++
	}
	if calls := speech.callCount(); calls != 1 {
		t.Fatalf("durable attempt-2 authorization produced %d Provider calls, want 1", calls)
	}
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != 1 {
		t.Fatalf("submit statuses = %#v, want one created and one replay", statuses)
	}
	persisted, ok, err := first.loadRecordFromDisk(request.JobID)
	if err != nil || !ok {
		t.Fatalf("persisted record: ok=%v err=%v", ok, err)
	}
	if len(persisted.Reconciliations) != 2 ||
		persisted.Reconciliations[1].Attempt != 2 ||
		persisted.Reconciliations[1].AuthorizedRecordSHA256 != digest ||
		persisted.Response.State != providercontract.StatusSucceeded {
		t.Fatalf("persisted attempt-2 record = %#v", persisted)
	}
	claimData, err := os.ReadFile(first.speechRetryClaimPath(request.JobID, digest))
	if err != nil {
		t.Fatal(err)
	}
	var claim speechRetryClaim
	if err := json.Unmarshal(claimData, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.SchemaVersion != "v1" || claim.JobID != request.JobID || claim.AuthorizedRecordSHA256 != digest {
		t.Fatalf("durable claim = %#v", claim)
	}
	restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	if replayed := postJob(t, restartedHTTP.URL, request); replayed.State != providercontract.StatusSucceeded || speech.callCount() != 1 {
		t.Fatalf("restart replay state = %s, calls = %d", replayed.State, speech.callCount())
	}
}

func TestSpeechV2CanaryAcrossAdapterInstancesSubmitsExactlyOnceAndReplays(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg, request := testSpeechCanaryFixture(t)
	speech := &fakeSpeechSynthesizer{}
	first, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}

	drifted := request
	drifted.Request.Assets = append([]providercontract.AssetRef(nil), request.Request.Assets...)
	drifted.Request.Assets[0].Revision = "10400000-0000-4000-8000-000000000099"
	driftHTTP := httptest.NewServer(first.Handler())
	if status := submitSpeechJobStatus(t, driftHTTP.URL, drifted); status != http.StatusUnprocessableEntity {
		driftHTTP.Close()
		t.Fatalf("drifted canary status = %d, want 422", status)
	}
	driftHTTP.Close()
	if speech.callCount() != 0 {
		t.Fatalf("drifted canary made %d TTS calls", speech.callCount())
	}

	firstHTTP := httptest.NewServer(first.Handler())
	defer firstHTTP.Close()
	secondHTTP := httptest.NewServer(second.Handler())
	defer secondHTTP.Close()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		status int
		err    error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, endpoint := range []string{firstHTTP.URL, secondHTTP.URL} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			httpRequest, requestErr := http.NewRequest(http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(body))
			if requestErr != nil {
				results <- result{err: requestErr}
				return
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Idempotency-Key", request.JobID)
			response, requestErr := authenticatedTestClient(t).Do(httpRequest)
			if requestErr != nil {
				results <- result{err: requestErr}
				return
			}
			response.Body.Close()
			results <- result{status: response.StatusCode}
		}()
	}
	wg.Wait()
	close(results)
	statuses := make(map[int]int)
	for outcome := range results {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		statuses[outcome.status]++
	}
	if speech.callCount() != 1 || statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != 1 {
		t.Fatalf("speech-v2 submit calls = %d, statuses = %#v", speech.callCount(), statuses)
	}

	restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	if replayed := postJob(t, restartedHTTP.URL, request); replayed.State != providercontract.StatusSucceeded || speech.callCount() != 1 {
		t.Fatalf("restart replay = %#v, calls = %d", replayed, speech.callCount())
	}
}

func TestServer_SpeechRetryClaimSurvivesCrashBeforeRecordTransition(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := testSpeechJobRequest(t)
	record := failedSpeechRecord(t, request)
	seed, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.createRecord(request.JobID, record); err != nil {
		t.Fatal(err)
	}
	digest, err := persistedRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	cfg.SpeechRetryJobID = request.JobID
	cfg.SpeechRetryRecord = digest
	speech := &fakeSpeechSynthesizer{}
	crashed, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := crashed.loadRecord(request.JobID); err != nil || !ok {
		t.Fatalf("cache prime: ok=%v err=%v", ok, err)
	}
	claimed, err := crashed.createSpeechRetryClaim(request.JobID, digest)
	if err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}

	restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	replayed := postJob(t, restartedHTTP.URL, request)
	if replayed.State != providercontract.StatusRequiresAction || speech.callCount() != 0 {
		t.Fatalf("crash replay state = %s, calls = %d", replayed.State, speech.callCount())
	}
	persisted, ok, err := restarted.loadRecordFromDisk(request.JobID)
	if err != nil || !ok || len(persisted.Reconciliations) != 0 {
		t.Fatalf("record after crash replay = %#v, ok=%v err=%v", persisted, ok, err)
	}
}

func TestServer_SpeechSecondReconciliationCommitFailurePreservesFirstHistory(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := testSpeechJobRequest(t)
	record := failedSpeechRecord(t, request)
	record.Reconciliations = []speechReconciliation{{
		Attempt:                1,
		StartedAt:              "2027-01-15T08:00:00Z",
		AuthorizedRecordSHA256: strings.Repeat("a", 64),
		PreviousResponse:       record.Response,
	}}
	adapter, err := New(testLiveConfig(), &fakeProvider{}, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(adapter.stateDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adapter.stateDir, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter.config.SpeechRetryRecord = strings.Repeat("b", 64)
	if _, err := adapter.beginSpeechRetry(request, record); err == nil {
		t.Fatal("beginSpeechRetry() error = nil")
	}
	if len(record.Reconciliations) != 1 ||
		record.Reconciliations[0].Attempt != 1 ||
		record.Reconciliations[0].AuthorizedRecordSHA256 != strings.Repeat("a", 64) ||
		record.Response.State != providercontract.StatusRequiresAction {
		t.Fatalf("record after failed commit = %#v", record)
	}
}

func failedSpeechRecord(t *testing.T, request providercontract.JobRequest) *jobRecord {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	return &jobRecord{
		RequestHash: hex.EncodeToString(digest[:]),
		Expected:    request.Request.Output,
		Response: providercontract.JobResponse{
			JobID: request.JobID, RunID: request.RunID,
			State: providercontract.StatusRequiresAction, Model: request.Model,
			Cost: providercontract.Cost{Currency: "CNY"},
			Error: &providercontract.Error{
				Code: providercontract.CodeUnavailable, SafeMessage: "operator reconciliation required",
				RequiresAction: true,
			},
		},
	}
}

func submitSpeechJobStatus(t *testing.T, endpoint string, request providercontract.JobRequest) int {
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
	return response.StatusCode
}

func testSpeechJobRequest(t *testing.T) providercontract.JobRequest {
	t.Helper()
	inputHash := sha256.Sum256([]byte("speech immutable input"))
	cfg := testLiveConfig()
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: "speech-job-1", RunID: "episode-1",
		Capability: providercontract.CapabilitySpeech,
		InputHash:  hex.EncodeToString(inputHash[:]),
		Model: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilitySpeech), Provider: "volcengine_ark",
			ModelID: AgentPlanTTSModelID, RouteVersion: AgentPlanTTSRouteVersion,
			CapabilityHash: AgentPlanTTSCapabilityHash(cfg), Verification: providercontract.PendingKey,
		},
		Request: providercontract.GenerationRequest{
			RequestID: "speech-job-1", IdempotencyKey: "speech-job-1",
			Modality: providercontract.ModalityAudio, Prompt: "你好，世界",
			PromptSnapshotID: "subtitle-1:cue-1", ModelHint: cfg.SpeechSpeaker,
			Output: providercontract.OutputSpec{DurationMillis: 2_000, Format: "mp3"},
			Budget: providercontract.BudgetEnvelope{EstimatedCostMicros: 81, MaxCostMicros: 81, MaxAttempts: 2},
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

func testSpeechCanaryFixture(t *testing.T) (runtimeconfig.VolcengineProvider, providercontract.JobRequest) {
	t.Helper()
	cfg := testLiveConfig()
	cfg.SpeechCanaryJobID = "speech-v2-0123456789abcdef0123456789abcdef"
	cfg.SpeechCanaryInputHash = strings.Repeat("4", 64)
	cfg.SpeechCanaryCueID = "cue-001"
	cfg.SpeechCanaryVoiceAssetID = "10400000-0000-4000-8000-00000000000f"
	cfg.SpeechCanaryParentVoiceVersion = "10400000-0000-4000-8000-000000000010"
	cfg.SpeechCanaryVoiceVersion = "10400000-0000-4000-8000-000000000011"
	cfg.SpeechCanaryVoiceHash = strings.Repeat("5", 64)
	cfg.SpeechCanaryLicenseSnapshotID = "10400000-0000-4000-8000-000000000012"
	cfg.SpeechCanaryLicenseHash = strings.Repeat("6", 64)
	cfg.SpeechCanaryMaximumAFPMilli = 2_228
	cfg.SpeechCanaryMaximumCashMicros = 0

	request := testSpeechJobRequest(t)
	request.JobID = cfg.SpeechCanaryJobID
	request.InputHash = cfg.SpeechCanaryInputHash
	request.Request.RequestID = request.JobID
	request.Request.IdempotencyKey = request.JobID
	request.Request.Prompt = "青石门在晨雾中缓缓开启。"
	request.Request.PromptSnapshotID = "subtitle-v2:" + cfg.SpeechCanaryCueID
	request.Request.ModelHint = cfg.SpeechSpeaker
	request.Request.Output.Format = "mp3"
	request.Request.Budget.MaxAttempts = 1
	request.Request.Assets = []providercontract.AssetRef{{
		ID: cfg.SpeechCanaryVoiceAssetID, Revision: cfg.SpeechCanaryVoiceVersion,
		Kind: providercontract.ModalityAudio, Role: providercontract.AssetRoleReferenceAudio,
		URI:              "cas://sha256/" + cfg.SpeechCanaryVoiceHash,
		SHA256:           cfg.SpeechCanaryVoiceHash,
		LicenseReference: cfg.SpeechCanaryLicenseSnapshotID + ":" + cfg.SpeechCanaryLicenseHash,
		MediaType:        "audio/x-voice-profile+json",
	}}
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
	return cfg, request
}
