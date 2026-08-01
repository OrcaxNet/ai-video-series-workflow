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
	"image"
	"image/color"
	"image/png"
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

type fakeProvider struct {
	mu          sync.Mutex
	submitCount int
	pollCount   int
	cancelCount int
	outputURL   string
	submitErr   error
	lastRequest providercontract.GenerationRequest
}

func (p *fakeProvider) counts() (submits, polls, cancels int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.submitCount, p.pollCount, p.cancelCount
}

func (p *fakeProvider) submittedRequest() providercontract.GenerationRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastRequest
}

type lockedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *lockedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *lockedClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

func (p *fakeProvider) Discover(context.Context) ([]providercontract.Capability, error) {
	return nil, nil
}

func (p *fakeProvider) Submit(_ context.Context, request providercontract.GenerationRequest) (providercontract.Job, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitCount++
	p.lastRequest = request
	if p.submitErr != nil {
		return providercontract.Job{}, p.submitErr
	}
	return providercontract.Job{
		ID: "cgt-live-1", RequestID: request.RequestID,
		Status:   providercontract.StatusQueued,
		Provider: "volcengine_ark", ProviderModel: "doubao-seedance-2.0",
		ProviderRegion: "cn-beijing", ProviderRequestID: "submit-request-1",
		CreatedAt: time.Unix(1_800_000_000, 0), UpdatedAt: time.Unix(1_800_000_000, 0),
	}, nil
}

func TestServer_ResolvesCASVisualInputAndOmitsVoiceDescriptorBeforeSubmit(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{}
	adapter, store := testAdapter(t, provider, fixedInspector{})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	imageBytes := testPNG(t)
	image, err := store.Put(t.Context(), bytes.NewReader(imageBytes))
	if err != nil {
		t.Fatal(err)
	}
	voice, err := store.Put(t.Context(), strings.NewReader(`{"speaker":"approved-voice"}`))
	if err != nil {
		t.Fatal(err)
	}
	request := testJobRequest(t)
	request.Request.Assets = []providercontract.AssetRef{
		{
			ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
			Role: providercontract.AssetRoleReferenceImage, URI: image.URI, SHA256: image.Digest,
			LicenseReference: "license:visual", MediaType: "image/png", SizeBytes: image.Size,
		},
		{
			ID: "voice-asset", Revision: "voice-version", Kind: providercontract.ModalityAudio,
			Role: providercontract.AssetRoleReferenceAudio, URI: voice.URI, SHA256: voice.Digest,
			LicenseReference: "license:voice", MediaType: "audio/x-voice-profile+json", SizeBytes: voice.Size,
		},
	}
	postJob(t, server.URL, request)

	submitted := provider.submittedRequest()
	if len(submitted.Assets) != 1 {
		t.Fatalf("upstream assets = %d, want one visual input", len(submitted.Assets))
	}
	asset := submitted.Assets[0]
	const prefix = "data:image/png;base64,"
	if !strings.HasPrefix(asset.URI, prefix) {
		t.Fatalf("upstream visual URI scheme = %q, want transient image data URL", strings.SplitN(asset.URI, ":", 2)[0])
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(asset.URI, prefix))
	if err != nil {
		t.Fatalf("decode upstream visual data URL: %v", err)
	}
	if !bytes.Equal(decoded, imageBytes) {
		t.Fatal("upstream visual bytes differ from the immutable CAS object")
	}
	if asset.SHA256 != image.Digest || asset.URI == image.URI {
		t.Fatal("upstream visual input did not retain its digest or remained an unreachable CAS URI")
	}
	if submits, _, _ := provider.counts(); submits != 1 {
		t.Fatalf("upstream submits = %d, want one", submits)
	}
}

func TestServer_RejectsInvalidProviderAssetsBeforeUpstreamSubmit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset func(*testing.T, *artifactstore.Store) providercontract.AssetRef
	}{
		{
			name: "non-CAS URI",
			asset: func(_ *testing.T, _ *artifactstore.Store) providercontract.AssetRef {
				return providercontract.AssetRef{
					ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
					Role: providercontract.AssetRoleReferenceImage, URI: "https://invalid.example/reference.png",
					SHA256: strings.Repeat("a", 64), LicenseReference: "license:visual",
					MediaType: "image/png", SizeBytes: 123,
				}
			},
		},
		{
			name: "declared size mismatch",
			asset: func(t *testing.T, store *artifactstore.Store) providercontract.AssetRef {
				object, err := store.Put(t.Context(), bytes.NewReader(testPNG(t)))
				if err != nil {
					t.Fatal(err)
				}
				return providercontract.AssetRef{
					ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
					Role: providercontract.AssetRoleReferenceImage, URI: object.URI, SHA256: object.Digest,
					LicenseReference: "license:visual", MediaType: "image/png", SizeBytes: object.Size + 1,
				}
			},
		},
		{
			name: "CAS URI digest mismatch",
			asset: func(t *testing.T, store *artifactstore.Store) providercontract.AssetRef {
				object, err := store.Put(t.Context(), bytes.NewReader(testPNG(t)))
				if err != nil {
					t.Fatal(err)
				}
				return providercontract.AssetRef{
					ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
					Role: providercontract.AssetRoleReferenceImage, URI: object.URI,
					SHA256: strings.Repeat("b", 64), LicenseReference: "license:visual",
					MediaType: "image/png", SizeBytes: object.Size,
				}
			},
		},
		{
			name: "CAS bytes changed after commit",
			asset: func(t *testing.T, store *artifactstore.Store) providercontract.AssetRef {
				object, err := store.Put(t.Context(), bytes.NewReader(testPNG(t)))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(object.Path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(object.Path, bytes.Repeat([]byte{0}, int(object.Size)), 0o440); err != nil {
					t.Fatal(err)
				}
				return providercontract.AssetRef{
					ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
					Role: providercontract.AssetRoleReferenceImage, URI: object.URI, SHA256: object.Digest,
					LicenseReference: "license:visual", MediaType: "image/png", SizeBytes: object.Size,
				}
			},
		},
		{
			name: "declared media does not match bytes",
			asset: func(t *testing.T, store *artifactstore.Store) providercontract.AssetRef {
				object, err := store.Put(t.Context(), strings.NewReader("this is not a PNG image"))
				if err != nil {
					t.Fatal(err)
				}
				return providercontract.AssetRef{
					ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
					Role: providercontract.AssetRoleReferenceImage, URI: object.URI, SHA256: object.Digest,
					LicenseReference: "license:visual", MediaType: "image/png", SizeBytes: object.Size,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{}
			adapter, store := testAdapter(t, provider, fixedInspector{})
			server := httptest.NewServer(adapter.Handler())
			defer server.Close()

			request := testJobRequest(t)
			request.Request.Assets = []providercontract.AssetRef{test.asset(t, store)}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			httpRequest, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewReader(body))
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
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				encoded, _ := io.ReadAll(response.Body)
				t.Fatalf("invalid asset accepted: HTTP %d: %s", response.StatusCode, encoded)
			}
			if submits, _, _ := provider.counts(); submits != 0 {
				t.Fatalf("invalid asset reached upstream: submits=%d", submits)
			}
			entries, err := os.ReadDir(adapter.stateDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid asset left %d adapter job record(s)", len(entries))
			}
		})
	}
}

func testPNG(t *testing.T) []byte {
	t.Helper()
	imageData := image.NewRGBA(image.Rect(0, 0, 2, 2))
	imageData.Set(0, 0, color.RGBA{R: 0xff, A: 0xff})
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, imageData); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestServer_MissingCASInputFailsBeforeUpstreamSubmit(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{}
	adapter, _ := testAdapter(t, provider, fixedInspector{})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	request := testJobRequest(t)
	missing := strings.Repeat("a", 64)
	request.Request.Assets = []providercontract.AssetRef{{
		ID: "visual-asset", Revision: "visual-version", Kind: providercontract.ModalityImage,
		Role: providercontract.AssetRoleReferenceImage, URI: "cas://sha256/" + missing, SHA256: missing,
		LicenseReference: "license:visual", MediaType: "image/png",
	}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewReader(body))
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
	if response.StatusCode != http.StatusServiceUnavailable {
		encoded, _ := io.ReadAll(response.Body)
		t.Fatalf("missing CAS response = HTTP %d: %s", response.StatusCode, encoded)
	}
	if submits, _, _ := provider.counts(); submits != 0 {
		t.Fatalf("upstream submits = %d, want zero", submits)
	}
}

func (p *fakeProvider) Poll(context.Context, string) (providercontract.Job, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pollCount++
	return providercontract.Job{
		ID: "cgt-live-1", Status: providercontract.StatusSucceeded,
		Provider: "volcengine_ark", ProviderModel: "doubao-seedance-2.0",
		ProviderRegion: "cn-beijing", ProviderRequestID: "poll-request-1",
		CreatedAt: time.Unix(1_800_000_000, 0), UpdatedAt: time.Unix(1_800_000_030, 0),
		Output: &providercontract.Output{
			Actual: providercontract.OutputSpec{
				Resolution: "720p", AspectRatio: "16:9", FPS: 24,
				DurationMillis: 5_000, Format: "mp4",
			},
			Usage: providercontract.Usage{
				InputTokens: 12, OutputTokens: 34, VideoTokens: 250_000,
				GeneratedMillis: 5_000,
			},
			Assets: []providercontract.AssetRef{{
				ID: "cgt-live-1-video", Revision: "provider-result",
				Kind: providercontract.ModalityVideo, Role: providercontract.AssetRoleOutput,
				URI: p.outputURL, SHA256: "pending_download",
				LicenseReference: "request-license-manifest",
			}},
		},
	}, nil
}

func (p *fakeProvider) Cancel(context.Context, string) (providercontract.Job, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelCount++
	return providercontract.Job{ID: "cgt-live-1", Status: providercontract.StatusCancelled}, nil
}

const testServiceAuthSecret = "test-service-auth-secret-32-bytes-long"

type fixedInspector struct {
	spec MediaSpec
	err  error
}

func (i fixedInspector) Inspect(context.Context, string) (MediaSpec, error) {
	return i.spec, i.err
}

func TestServer_SubmitPollDownloadAndCASWithoutTransportLeak(t *testing.T) {
	t.Parallel()
	video := []byte("synthetic-mp4-fixture")
	download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("signature") == "" {
			http.Error(w, "invalid", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(video)
	}))
	defer download.Close()
	provider := &fakeProvider{outputURL: download.URL + "/result.mp4?signature=temporary-secret"}
	adapter, store := testAdapter(t, provider, fixedInspector{spec: MediaSpec{
		Width: 1280, Height: 720, FPS: 24, DurationMillis: 5_062, Format: "mp4",
	}})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	request := testJobRequest(t)
	created := postJob(t, server.URL, request)
	if created.State != providercontract.StatusQueued || created.UpstreamTaskID != "cgt-live-1" ||
		created.RequestID != "submit-request-1" || created.Model.Verification != providercontract.PendingKey {
		t.Fatalf("created response = %#v", created)
	}
	replayed := postJob(t, server.URL, request)
	if replayed.UpstreamTaskID != created.UpstreamTaskID || provider.submitCount != 1 {
		t.Fatalf("idempotent replay = %#v, submit count = %d", replayed, provider.submitCount)
	}

	response, err := authenticatedTestClient(t).Get(server.URL + "/v1/jobs/" + request.JobID)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("poll HTTP %d: %s", response.StatusCode, body)
	}
	for _, forbidden := range []string{"signature=", "temporary-secret", provider.outputURL, "ARK_API_KEY"} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Fatalf("durable response leaked %q: %s", forbidden, body)
		}
	}
	var completed providercontract.JobResponse
	if err := json.Unmarshal(body, &completed); err != nil {
		t.Fatal(err)
	}
	if completed.State != providercontract.StatusSucceeded || len(completed.Artifacts) != 1 ||
		completed.UpstreamTaskID != "cgt-live-1" || completed.RequestID != "submit-request-1" ||
		completed.Usage.VideoTokens != 250_000 {
		t.Fatalf("completed response = %#v", completed)
	}
	artifact := completed.Artifacts[0]
	if artifact.URI != "cas://sha256/"+artifact.SHA256 || artifact.Width != 1280 ||
		artifact.Height != 720 || artifact.FPS != 24 || artifact.DurationMillis != 5_062 ||
		artifact.MediaType != "video/mp4" || artifact.SizeBytes != int64(len(video)) {
		t.Fatalf("committed artifact = %#v", artifact)
	}
	if exists, err := store.Exists(artifact.SHA256); err != nil || !exists {
		t.Fatalf("CAS artifact exists = %v, err = %v", exists, err)
	}
	if completed.Cost.ActualMicros == nil || *completed.Cost.ActualMicros != 0 ||
		!completed.Cost.Verified || completed.Cost.ProviderReported ||
		completed.Cost.BillingMode != "subscription_included" {
		t.Fatalf("subscription cost evidence = %#v", completed.Cost)
	}

	// A process restart reloads the sanitized registry from the shared CAS
	// volume and replays the same result without another paid submit.
	restarted, err := New(testLiveConfig(), provider, store, Options{
		DownloadClient: http.DefaultClient,
		Inspector: fixedInspector{spec: MediaSpec{
			Width: 1280, Height: 720, FPS: 24, DurationMillis: 5_062, Format: "mp4",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	restartedServer := httptest.NewServer(restarted.Handler())
	defer restartedServer.Close()
	afterRestart := postJob(t, restartedServer.URL, request)
	if afterRestart.State != providercontract.StatusSucceeded || provider.submitCount != 1 {
		t.Fatalf("restart replay = %#v, submit count = %d", afterRestart, provider.submitCount)
	}
}

func TestServer_RejectsAnonymousSubmitPollAndCancelBeforeUpstream(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{}
	adapter, _ := testAdapter(t, provider, fixedInspector{})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	request := testJobRequest(t)
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	unauthenticatedSubmit, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	unauthenticatedSubmit.Header.Set("Content-Type", "application/json")
	unauthenticatedSubmit.Header.Set("Idempotency-Key", request.JobID)
	assertUnauthenticated(t, unauthenticatedSubmit)
	if provider.submitCount != 0 {
		t.Fatalf("anonymous submit reached upstream %d times", provider.submitCount)
	}

	created := postJob(t, server.URL, request)
	if created.State != providercontract.StatusQueued || provider.submitCount != 1 {
		t.Fatalf("authenticated setup = %#v, submits = %d", created, provider.submitCount)
	}

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "poll", method: http.MethodGet, path: "/v1/jobs/" + request.JobID},
		{name: "cancel", method: http.MethodPost, path: "/v1/jobs/" + request.JobID + "/cancel"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpRequest, err := http.NewRequest(tt.method, server.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			assertUnauthenticated(t, httpRequest)
		})
	}
	if provider.pollCount != 0 || provider.cancelCount != 0 {
		t.Fatalf("anonymous actions reached upstream: polls=%d cancels=%d", provider.pollCount, provider.cancelCount)
	}
}

func TestServer_RejectsFutureDatedReplayAcrossFullSignatureWindowBeforeUpstream(t *testing.T) {
	t.Parallel()
	video := []byte("synthetic-mp4-fixture")
	download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(video)
	}))
	defer download.Close()
	start := time.Unix(1_800_000_000, 0).UTC()

	tests := []struct {
		name       string
		method     string
		path       func(providercontract.JobRequest) string
		wantStatus int
		count      func(*fakeProvider) int
	}{
		{
			name: "submit", method: http.MethodPost,
			path:       func(providercontract.JobRequest) string { return "/v1/jobs" },
			wantStatus: http.StatusCreated,
			count:      func(provider *fakeProvider) int { submits, _, _ := provider.counts(); return submits },
		},
		{
			name: "poll", method: http.MethodGet,
			path:       func(request providercontract.JobRequest) string { return "/v1/jobs/" + request.JobID },
			wantStatus: http.StatusOK,
			count:      func(provider *fakeProvider) int { _, polls, _ := provider.counts(); return polls },
		},
		{
			name: "cancel", method: http.MethodPost,
			path:       func(request providercontract.JobRequest) string { return "/v1/jobs/" + request.JobID + "/cancel" },
			wantStatus: http.StatusAccepted,
			count:      func(provider *fakeProvider) int { _, _, cancels := provider.counts(); return cancels },
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &lockedClock{now: start}
			provider := &fakeProvider{outputURL: download.URL + "/result.mp4"}
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := New(testLiveConfig(), provider, store, Options{
				DownloadClient: http.DefaultClient,
				Inspector: fixedInspector{spec: MediaSpec{
					Width: 1280, Height: 720, FPS: 24, DurationMillis: 5_062, Format: "mp4",
				}},
				Now: func() time.Time { return start }, AuthNow: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(adapter.Handler())
			defer server.Close()
			job := testJobRequest(t)
			jobBody, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}

			if tt.name != "submit" {
				setup := signedTestServiceRequestMethodWithBody(
					t, http.MethodPost, server.URL+"/v1/jobs", jobBody, jobBody, start,
					fmt.Sprintf("%032x", 0x10+index),
				)
				setup.Header.Set("Content-Type", "application/json")
				setup.Header.Set("Idempotency-Key", job.JobID)
				response, err := http.DefaultClient.Do(setup)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != http.StatusCreated {
					t.Fatalf("setup HTTP status = %d, want 201", response.StatusCode)
				}
			}

			var actionBody []byte
			if tt.name == "submit" {
				actionBody = jobBody
			}
			nonce := fmt.Sprintf("%032x", 0x20+index)
			newAction := func() *http.Request {
				request := signedTestServiceRequestMethodWithBody(
					t, tt.method, server.URL+tt.path(job), actionBody, actionBody,
					start.Add(119*time.Second), nonce,
				)
				if tt.name == "submit" {
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set("Idempotency-Key", job.JobID)
				}
				return request
			}

			response, err := http.DefaultClient.Do(newAction())
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if response.StatusCode != tt.wantStatus {
				t.Fatalf("first action HTTP %d, want %d: %s", response.StatusCode, tt.wantStatus, body)
			}
			beforeReplay := tt.count(provider)

			clock.Set(start.Add(121 * time.Second))
			assertUnauthenticated(t, newAction())
			if afterReplay := tt.count(provider); afterReplay != beforeReplay {
				t.Fatalf("replay reached upstream: before=%d after=%d", beforeReplay, afterReplay)
			}
		})
	}
}

func assertUnauthenticated(t *testing.T, request *http.Request) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Error *providercontract.Error `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode unauthenticated response: %v", err)
	}
	if response.StatusCode != http.StatusUnauthorized || envelope.Error == nil ||
		envelope.Error.Code != providercontract.CodeUnauthenticated || envelope.Error.Retryable {
		t.Fatalf("anonymous response HTTP %d: %#v", response.StatusCode, envelope.Error)
	}
}

func TestServer_RejectsSimulationAndWrongFrozenModelBeforeSubmit(t *testing.T) {
	t.Parallel()
	provider := &fakeProvider{}
	adapter, _ := testAdapter(t, provider, fixedInspector{})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	tests := []struct {
		name   string
		mutate func(*providercontract.JobRequest)
	}{
		{name: "simulation", mutate: func(request *providercontract.JobRequest) { request.Simulation = "success" }},
		{name: "wrong model", mutate: func(request *providercontract.JobRequest) {
			request.Model.ModelID = "different-model"
			request.Request.ModelHint = "different-model"
			request.BudgetReservation = bindReservation(t, *request)
		}},
		{name: "wrong provider", mutate: func(request *providercontract.JobRequest) {
			request.Model.Provider = "different-provider"
			request.BudgetReservation = bindReservation(t, *request)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := testJobRequest(t)
			tt.mutate(&request)
			body, _ := json.Marshal(request)
			httpRequest, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewReader(body))
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Idempotency-Key", request.JobID)
			response, err := authenticatedTestClient(t).Do(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode < 400 {
				t.Fatalf("HTTP status = %d, want failure", response.StatusCode)
			}
		})
	}
	if provider.submitCount != 0 {
		t.Fatalf("blocked request submitted upstream %d times", provider.submitCount)
	}
}

func TestServer_UnexpectedProviderErrorIsSanitized(t *testing.T) {
	t.Parallel()
	sensitive := strings.Join([]string{"provider", "runtime", "credential"}, "-")
	provider := &fakeProvider{submitErr: errors.New("upstream leaked " + sensitive)}
	adapter, _ := testAdapter(t, provider, fixedInspector{})
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	request := testJobRequest(t)
	body, _ := json.Marshal(request)
	httpRequest, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Idempotency-Key", request.JobID)
	response, err := authenticatedTestClient(t).Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	encoded, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusServiceUnavailable || bytes.Contains(encoded, []byte(sensitive)) {
		t.Fatalf("sanitized failure HTTP %d: %s", response.StatusCode, encoded)
	}
	replayed := postJob(t, server.URL, request)
	if replayed.State != providercontract.StatusUnknown || provider.submitCount != 1 {
		t.Fatalf("ambiguous submit replay = %#v, submits = %d", replayed, provider.submitCount)
	}
}

func testAdapter(t *testing.T, provider providercontract.Provider, inspector MediaInspector) (*Server, *artifactstore.Store) {
	t.Helper()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	adapter, err := New(cfg, provider, store, Options{
		DownloadClient: http.DefaultClient, Inspector: inspector,
		Now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, store
}

func testLiveConfig() runtimeconfig.VolcengineProvider {
	return runtimeconfig.VolcengineProvider{
		ProviderID: "volcengine-agent-plan-large", Region: "cn-beijing",
		VideoModel: "doubao-seedance-2.0", PlanName: "agent-plan-large",
		PricingVersion: "agent-plan-large-included-v1", Currency: "CNY",
		SpeechEndpoint: AgentPlanTTSEndpoint, SpeechModel: AgentPlanTTSModelID,
		SpeechSpeaker:    "zh_female_tianmeitaozi_mars_bigtts",
		MaxDownloadBytes: 1 << 20, DownloadTimeout: 5 * time.Second,
		ServiceAuthSecret: testServiceAuthSecret,
	}
}

func testJobRequest(t *testing.T) providercontract.JobRequest {
	t.Helper()
	hash := sha256.Sum256([]byte("live probe immutable input"))
	capability := sha256.Sum256([]byte("volcengine-agent-plan-large\x00doubao-seedance-2.0"))
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: "provider-job-live-probe-1", RunID: "live-probe-1",
		Capability: providercontract.CapabilityVideo,
		InputHash:  hex.EncodeToString(hash[:]),
		Model: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilityVideo),
			Provider:        "volcengine_ark", ModelID: "doubao-seedance-2.0",
			RouteVersion: "agent-plan-large-v1", CapabilityHash: hex.EncodeToString(capability[:]),
			Verification: providercontract.PendingKey,
		},
		Request: providercontract.GenerationRequest{
			RequestID: "provider-job-live-probe-1", IdempotencyKey: "provider-job-live-probe-1",
			Modality:         providercontract.ModalityVideo,
			Prompt:           "original abstract glass fluid, fixed camera, no people or text",
			PromptSnapshotID: "prompt-live-probe-1",
			Context: providercontract.ContextRefs{
				SeriesSnapshotID: "series-live-probe", EpisodeSnapshotID: "episode-live-probe",
				SceneSnapshotID: "scene-live-probe", ShotSnapshotID: "shot-live-probe",
			},
			Output: providercontract.OutputSpec{
				Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
				FPS: 24, DurationMillis: 5_000, Format: "mp4",
			},
			ModelHint: "doubao-seedance-2.0",
			Budget: providercontract.BudgetEnvelope{
				EstimatedCostMicros: 1_000_000, MaxCostMicros: 1_000_000, MaxAttempts: 1,
			},
		},
		TraceID: "trace-live-probe-1",
	}
	request.BudgetReservation = bindReservation(t, request)
	return request
}

func bindReservation(t *testing.T, request providercontract.JobRequest) providercontract.BudgetReservation {
	t.Helper()
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID: "budget-live-probe-1", Currency: "CNY", AmountMicros: 1_000_000,
		PricingVersion: "agent-plan-large-included-v1", ConfirmedBy: "live-probe-operator",
	}, providercontract.BudgetBindingInput{
		RunID: request.RunID, InputHash: request.InputHash, Model: request.Model, Budget: request.Request.Budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reservation
}

func postJob(t *testing.T, endpoint string, request providercontract.JobRequest) providercontract.JobResponse {
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
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		encoded, _ := io.ReadAll(response.Body)
		t.Fatalf("POST job HTTP %d: %s", response.StatusCode, encoded)
	}
	var result providercontract.JobResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func authenticatedTestClient(t *testing.T) *http.Client {
	t.Helper()
	client, err := AuthenticatedHTTPClient(http.DefaultClient, testServiceAuthSecret)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
