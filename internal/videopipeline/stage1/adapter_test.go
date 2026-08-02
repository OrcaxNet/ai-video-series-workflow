package stage1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

func TestAdapterSubmitterUsesRecoveryBeforeTheOnlySubmit(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	created := false
	posts := 0
	gets := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			gets++
			if !created {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": &providercontract.Error{
					Code: providercontract.CodeNotFound, SafeMessage: "job not found",
				}})
				return
			}
			_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
				JobID: "idempotency-attempt-01", UpstreamTaskID: "provider-task-1",
			})
		case http.MethodPost:
			posts++
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
				JobID: "idempotency-attempt-01", UpstreamTaskID: "provider-task-1",
			})
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	adapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(gate, adapter)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt("shot-01", "attempt-01")
	attempt.JobRequest = &providercontract.JobRequest{
		JobID: attempt.IdempotencyKey, Capability: providercontract.CapabilityVideo,
		Model:   providercontract.ModelSnapshot{ModelID: FormalVideoModel},
		Request: providercontract.GenerationRequest{IdempotencyKey: attempt.IdempotencyKey},
	}
	for range 2 {
		result, err := executor.Execute(t.Context(), attempt)
		if err != nil {
			t.Fatal(err)
		}
		if result.ProviderTaskID != "provider-task-1" {
			t.Fatalf("result = %#v", result)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 1 || gets != 2 {
		t.Fatalf("adapter calls: GET=%d POST=%d, want GET=2 POST=1", gets, posts)
	}
}
