package liveprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

func TestRunProducesSingleSanitizedLiveEvidenceBundle(t *testing.T) {
	t.Parallel()
	artifactRoot := t.TempDir()
	store, err := artifactstore.New(artifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := store.Put(t.Context(), strings.NewReader("synthetic-live-video"))
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	var submits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			submits.Add(1)
			var request providercontract.JobRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode job: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
				JobID: request.JobID, RunID: request.RunID,
				UpstreamTaskID: "cgt-live-probe", RequestID: "request-live-probe",
				State: providercontract.StatusQueued, Model: request.Model,
				Cost: providercontract.Cost{
					EstimatedMicros: request.BudgetReservation.AmountMicros,
					ActualMicros:    &zero, Currency: "CNY",
					PricingVersion: request.BudgetReservation.PricingVersion,
					Verified:       true, BillingMode: "subscription_included",
				},
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/jobs/"):
			_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
				JobID: strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), RunID: "live-probe-test",
				UpstreamTaskID: "cgt-live-probe", RequestID: "request-live-probe",
				State: providercontract.StatusSucceeded,
				Model: providercontract.ModelSnapshot{
					CapabilityAlias: string(providercontract.CapabilityVideo),
					Provider:        "volcengine_ark", ModelID: modelID,
					RouteVersion:   "agent-plan-large-v1",
					CapabilityHash: strings.Repeat("a", 64), Verification: providercontract.PendingKey,
				},
				Artifacts: []providercontract.AssetRef{{
					ID: "artifact-live", Revision: artifact.Digest,
					Kind: providercontract.ModalityVideo, Role: providercontract.AssetRoleOutput,
					URI: artifact.URI, SHA256: artifact.Digest,
					LicenseReference: "request-license-manifest",
					MediaType:        "video/mp4", SizeBytes: artifact.Size,
					Width: 1280, Height: 720, FPS: 24, DurationMillis: 5_062,
				}},
				Usage: providercontract.Usage{VideoTokens: 250_000, GeneratedMillis: 5_000},
				Cost: providercontract.Cost{
					EstimatedMicros: 1_000_000, ActualMicros: &zero, Currency: "CNY",
					PricingVersion: "agent-plan-large-included-v1", Verified: true,
					BillingMode: "subscription_included",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputDir := filepath.Join(t.TempDir(), "evidence")
	clock := &steppingClock{next: time.Unix(1_800_000_000, 0).UTC()}
	result, err := Run(context.Background(), Config{
		AdapterURL: server.URL, ArtifactRoot: artifactRoot, OutputDir: outputDir,
		BuildVersion: "fixed-sha", Region: "cn-beijing", PlanName: "agent-plan-large",
		PollInterval: time.Millisecond, Timeout: time.Second, HTTPClient: server.Client(),
		Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if submits.Load() != 1 || result.TaskID != "cgt-live-probe" ||
		result.RequestID != "request-live-probe" || result.ArtifactSHA256 != artifact.Digest ||
		result.Output.Width != 1280 || result.Output.Height != 720 || result.Output.FPS != 24 ||
		result.Output.DurationMillis != 5_062 || result.Usage.VideoTokens != 250_000 {
		t.Fatalf("probe result = %#v, submits = %d", result, submits.Load())
	}
	for _, name := range []string{"probe.mp4", "generation-manifest.json", "service-bom.json", "probe-result.json"} {
		data, err := os.ReadFile(filepath.Join(outputDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, forbidden := range []string{defaultPrompt, "output_url", "video_url", "Bearer ", "ARK_API_KEY"} {
			if bytes.Contains(bytes.ToLower(data), bytes.ToLower([]byte(forbidden))) {
				t.Fatalf("%s contains forbidden transient value %q", name, forbidden)
			}
		}
	}
	video, err := os.Open(filepath.Join(outputDir, "probe.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	copied, _ := io.ReadAll(video)
	_ = video.Close()
	if string(copied) != "synthetic-live-video" {
		t.Fatalf("copied video = %q", copied)
	}

	if _, err := Run(context.Background(), Config{
		AdapterURL: server.URL, ArtifactRoot: artifactRoot, OutputDir: outputDir,
		HTTPClient: server.Client(), Now: clock.Now,
	}); err == nil || submits.Load() != 1 {
		t.Fatalf("second run error = %v, submits = %d; want lock before submit", err, submits.Load())
	}
}

type steppingClock struct {
	next time.Time
}

func (c *steppingClock) Now() time.Time {
	value := c.next
	c.next = c.next.Add(5 * time.Second)
	return value
}
