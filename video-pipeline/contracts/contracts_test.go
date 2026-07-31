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

func TestOpenAPIContainsRunnableControlPlaneOperationsAndErrors(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, required := range []string{
		"/api/v1/providers/status:",
		"/api/v1/generation-plans:",
		"/api/v1/sources/{sourceRevisionId}/compilations:",
		"/api/v1/shots/{shotId}/prompt-snapshots:",
		"/api/v1/episodes/{episodeId}/production-batches:",
		"/api/v1/shots/{shotId}/runs:",
		"/api/v1/runs/{runId}/pause:",
		"/api/v1/runs/{runId}/cancel:",
		"/api/v1/runs/{runId}/resume:",
		"/api/v1/runs/{runId}/publication-lock:",
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
		"ExecutionPolicy:",
		"SAFETY_REVIEWER",
	} {
		if !contains(text, required) {
			t.Errorf("openapi.yaml missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GPU_WORKER_UNAVAILABLE", "COMFYUI_REJECTED", "Wan2.2-TI2V-5B",
		"/api/v1/providers/capabilities:",
		"/api/v1/providers/{providerProfileId}/connection-test:",
		"/api/v1/provider-jobs:",
		"/api/v1/provider-callbacks/{providerProfileId}:",
	} {
		if contains(text, forbidden) {
			t.Errorf("openapi.yaml contains superseded assumption %q", forbidden)
		}
	}
}

func TestOpenAPIActorRoleMatchesSafetyApprovalRuntime(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	components, ok := document["components"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI components missing")
	}
	schemas, ok := components["schemas"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI component schemas missing")
	}
	actor, ok := schemas["Actor"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI Actor schema missing")
	}
	properties, ok := actor["properties"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI Actor properties missing")
	}
	role, ok := properties["role"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI Actor.role schema missing")
	}
	enum, ok := role["enum"].([]any)
	if !ok {
		t.Fatalf("OpenAPI Actor.role enum = %#v", role["enum"])
	}
	for _, value := range enum {
		if value == "SAFETY_REVIEWER" {
			return
		}
	}
	t.Fatalf("OpenAPI Actor.role enum does not authorize SAFETY_REVIEWER: %#v", enum)
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
