package contracts_test

import (
	"os"
	"testing"

	"go.yaml.in/yaml/v4"
)

func TestContractDocumentsAreValidYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		root string
	}{
		{name: "OpenAPI", file: "openapi.yaml", root: "openapi"},
		{name: "AsyncAPI", file: "asyncapi.yaml", root: "asyncapi"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			content, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("ReadFile(%q): %v", tt.file, err)
			}
			var document map[string]any
			if err := yaml.Unmarshal(content, &document); err != nil {
				t.Fatalf("yaml.Unmarshal(%q): %v", tt.file, err)
			}
			if document[tt.root] == nil {
				t.Fatalf("%s root key missing from %s", tt.root, tt.file)
			}
		})
	}
}

func TestOpenAPIContainsProviderFirstOperationsAndErrors(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, required := range []string{
		"/api/v1/providers/status:",
		"/api/v1/providers/capabilities:",
		"/api/v1/generation-plans:",
		"/api/v1/provider-jobs:",
		"/api/v1/provider-jobs/{providerJobId}/cancel:",
		"/api/v1/provider-callbacks/{providerProfileId}:",
		"/api/v1/episodes/{episodeId}/production-batches:",
		"/api/v1/runs/{runId}/cancel:",
		"/api/v1/runs/{runId}/resume:",
		"/api/v1/approvals:",
		"/api/v1/manifests/{scopeType}/{revisionId}:",
		"ATTEMPT_LIMIT_REACHED",
		"LICENSE_BLOCKED",
		"STALE_DEPENDENCY",
		"unauthenticated",
		"quota_exceeded",
		"content_blocked",
		"region_unavailable",
		"model_unavailable",
		"timeout",
	} {
		if !contains(text, required) {
			t.Errorf("openapi.yaml missing %q", required)
		}
	}
	for _, forbidden := range []string{"GPU_WORKER_UNAVAILABLE", "COMFYUI_REJECTED", "Wan2.2-TI2V-5B"} {
		if contains(text, forbidden) {
			t.Errorf("openapi.yaml contains superseded assumption %q", forbidden)
		}
	}
}

func TestAsyncAPIContainsProviderAndCostEvents(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("asyncapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, required := range []string{
		"video.provider-job.state-changed.v1",
		"video.cost-ledger.recorded.v1",
		"rate_limited",
		"REQUIRES_ACTION",
		"UNKNOWN",
	} {
		if !contains(text, required) {
			t.Errorf("asyncapi.yaml missing %q", required)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
