package contracts_test

import (
	"os"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"go.yaml.in/yaml/v4"
)

func TestLiveAdapterIsInternalOnlyAndRequiresServiceAuthentication(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Ports       []any          `yaml:"ports"`
			Expose      []string       `yaml:"expose"`
			Environment map[string]any `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(content, &compose); err != nil {
		t.Fatal(err)
	}
	provider, ok := compose.Services["volcengine-provider"]
	if !ok {
		t.Fatal("volcengine-provider service is missing")
	}
	if len(provider.Ports) != 0 {
		t.Fatalf("live adapter publishes host ports: %#v", provider.Ports)
	}
	if len(provider.Expose) != 1 || provider.Expose[0] != "8091" {
		t.Fatalf("live adapter internal expose = %#v, want [8091]", provider.Expose)
	}
	if _, ok := provider.Environment["VIDEO_PROVIDER_SERVICE_AUTH_SECRET"]; !ok {
		t.Fatal("live adapter does not receive the internal service authentication secret")
	}
	if endpoint := provider.Environment["VIDEO_VOLCENGINE_TTS_ENDPOINT"]; endpoint != runtimeconfig.AgentPlanTTSEndpoint {
		t.Fatalf("live adapter TTS endpoint = %q, want exact Agent Plan endpoint", endpoint)
	}
}
