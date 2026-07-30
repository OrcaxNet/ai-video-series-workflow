package providercontract

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestVolcengineProvider_SubmitTextMapping(t *testing.T) {
	t.Parallel()
	apiKey := strings.Join([]string{"test", "runtime", "credential"}, "-")
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/responses" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "text-request-1")
		fmt.Fprint(w, `{
			"id":"response-test-1",
			"model":"configured-text-model",
			"status":"completed",
			"usage":{"input_tokens":12,"output_tokens":2}
		}`)
	}))
	defer server.Close()

	provider, err := NewVolcengineProvider(VolcengineConfig{
		BaseURL: server.URL,
		APIKey:  apiKey,
		Models:  VolcengineModels{Text: "configured-text-model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testGenerationRequest()
	request.Modality = ModalityText
	request.ModelHint = "untrusted-request-hint"
	job, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "configured-text-model" || payload["store"] != false {
		t.Fatalf("text payload = %#v", payload)
	}
	if job.Status != StatusSucceeded || job.ProviderModel != "configured-text-model" ||
		job.ProviderRequestID != "text-request-1" || job.Output == nil ||
		job.Output.Usage.InputTokens != 12 || job.Output.Usage.OutputTokens != 2 {
		t.Fatalf("Submit() job = %#v", job)
	}
}

func TestVolcengineProvider_SubmitImageMapping(t *testing.T) {
	t.Parallel()
	apiKey := strings.Join([]string{"test", "runtime", "credential"}, "-")
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/images/generations" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data":[{"url":"https://example.invalid/generated.png"}],
			"usage":{"output_tokens":1000,"generated_images":1}
		}`)
	}))
	defer server.Close()

	provider, err := NewVolcengineProvider(VolcengineConfig{
		BaseURL: server.URL,
		APIKey:  apiKey,
		Models:  VolcengineModels{Image: "configured-image-model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testGenerationRequest()
	request.Modality = ModalityImage
	job, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	images, ok := payload["image"].([]any)
	if payload["model"] != "configured-image-model" || payload["size"] != "1280x720" ||
		payload["watermark"] != true || !ok || len(images) != 1 {
		t.Fatalf("image payload = %#v", payload)
	}
	if job.Status != StatusSucceeded || job.ProviderModel != "configured-image-model" ||
		job.Output == nil || len(job.Output.Assets) != 1 ||
		job.Output.Usage.GeneratedImages != 1 {
		t.Fatalf("Submit() job = %#v", job)
	}
}

func TestVolcengineProvider_SubmitVideoMapping(t *testing.T) {
	t.Parallel()
	apiKey := strings.Join([]string{"test", "runtime", "credential"}, "-")
	var (
		mu      sync.Mutex
		payload map[string]any
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/contents/generations/tasks" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			http.Error(w, "missing runtime auth", http.StatusUnauthorized)
			return
		}
		var got map[string]any
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		mu.Lock()
		payload = got
		mu.Unlock()
		w.Header().Set("X-Request-Id", "volc-request-1")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"cgt-test-1"}`)
	}))
	defer server.Close()

	provider, err := NewVolcengineProvider(VolcengineConfig{
		BaseURL: server.URL,
		APIKey:  apiKey,
		Models:  VolcengineModels{Video: "doubao-seedance-runtime-version"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testGenerationRequest()
	request.CallbackURL = "https://callback.example.invalid/provider/volcengine"
	job, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "cgt-test-1" || job.Status != StatusQueued ||
		job.ProviderRequestID != "volc-request-1" {
		t.Fatalf("Submit() job = %#v", job)
	}

	mu.Lock()
	gotPayload := payload
	mu.Unlock()
	if gotPayload["model"] != "doubao-seedance-runtime-version" {
		t.Fatalf("model = %#v", gotPayload["model"])
	}
	if gotPayload["return_last_frame"] != true {
		t.Fatalf("return_last_frame = %#v, want true", gotPayload["return_last_frame"])
	}
	content, ok := gotPayload["content"].([]any)
	if !ok || len(content) != 2 {
		t.Fatalf("content = %#v, want text and reference image", gotPayload["content"])
	}
	textItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("text item = %#v", content[0])
	}
	text, _ := textItem["text"].(string)
	for _, flag := range []string{"--ratio 16:9", "--resolution 720p", "--dur 5", "--generate_audio false"} {
		if !strings.Contains(text, flag) {
			t.Errorf("mapped prompt %q does not contain %q", text, flag)
		}
	}
	imageItem, ok := content[1].(map[string]any)
	if !ok || imageItem["role"] != string(AssetRoleReferenceImage) {
		t.Fatalf("reference image item = %#v", content[1])
	}
}

func TestVolcengineProvider_PollMapsManifestEvidence(t *testing.T) {
	t.Parallel()
	apiKey := strings.Join([]string{"test", "runtime", "credential"}, "-")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents/generations/tasks/cgt-test-1" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("X-Tt-Logid", "log-id-1")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"id":"cgt-test-1",
			"model":"doubao-seedance-runtime-version",
			"status":"succeeded",
			"created_at":1800000000,
			"updated_at":1800000030,
			"content":{
				"video_url":"https://example.invalid/result.mp4",
				"last_frame_url":"https://example.invalid/last.png"
			},
			"duration":"5",
			"usage":{"total_tokens":250000}
		}`)
	}))
	defer server.Close()
	provider, err := NewVolcengineProvider(VolcengineConfig{
		BaseURL: server.URL,
		APIKey:  apiKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	job, err := provider.Poll(t.Context(), "cgt-test-1")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusSucceeded || job.ProviderRequestID != "log-id-1" ||
		job.Output == nil || len(job.Output.Assets) != 2 {
		t.Fatalf("Poll() job = %#v", job)
	}
	if job.Output.Usage.VideoTokens != 250_000 || job.Output.Usage.GeneratedMillis != 5_000 {
		t.Fatalf("Poll() usage = %#v", job.Output.Usage)
	}
}

func TestVolcengineProvider_ErrorMappingDoesNotLeak(t *testing.T) {
	t.Parallel()
	apiKey := strings.Join([]string{"test", "runtime", "credential"}, "-")
	rawCredential := strings.Join([]string{"provider", "echoed", strings.Repeat("q", 20)}, "-")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "request-safe")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(w, `{"error":{"code":"QuotaExceeded","message":"upstream echoed %s"}}`, rawCredential)
	}))
	defer server.Close()
	provider, err := NewVolcengineProvider(VolcengineConfig{
		BaseURL: server.URL,
		APIKey:  apiKey,
		Models:  VolcengineModels{Image: "runtime-image-model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testGenerationRequest()
	request.Modality = ModalityImage

	_, err = provider.Submit(t.Context(), request)
	if ErrorCodeOf(err) != CodeQuotaExceeded {
		t.Fatalf("Submit() error = %v, want quota_exceeded", err)
	}
	if strings.Contains(err.Error(), rawCredential) {
		t.Fatalf("safe error leaked provider response: %q", err)
	}
}

func TestNewVolcengineProvider_RequiresRuntimeKey(t *testing.T) {
	t.Parallel()
	_, err := NewVolcengineProvider(VolcengineConfig{})
	if ErrorCodeOf(err) != CodeUnauthenticated {
		t.Fatalf("NewVolcengineProvider() error = %v, want unauthenticated", err)
	}
}

func TestVolcengineProvider_DiscoverMarksLivePending(t *testing.T) {
	t.Parallel()
	provider, err := NewVolcengineProvider(VolcengineConfig{
		APIKey: strings.Join([]string{"test", "runtime", "credential"}, "-"),
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := provider.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities) != 3 {
		t.Fatalf("Discover() returned %d capabilities, want 3", len(capabilities))
	}
	for _, capability := range capabilities {
		if capability.Verification != "official_docs_pending_key" {
			t.Fatalf("capability verification = %q", capability.Verification)
		}
	}
}
