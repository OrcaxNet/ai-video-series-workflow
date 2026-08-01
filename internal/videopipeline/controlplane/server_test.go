package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

func TestServer_Readiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		requireDeps   bool
		probeError    error
		wantStatus    int
		wantReadiness string
	}{
		{
			name:          "all dependencies ready",
			requireDeps:   true,
			wantStatus:    http.StatusOK,
			wantReadiness: "ready",
		},
		{
			name:          "critical dependency blocks readiness",
			requireDeps:   true,
			probeError:    errors.New("offline"),
			wantStatus:    http.StatusServiceUnavailable,
			wantReadiness: "not_ready",
		},
		{
			name:          "development may report degraded",
			requireDeps:   false,
			probeError:    errors.New("offline"),
			wantStatus:    http.StatusOK,
			wantReadiness: "degraded",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := runtimeconfig.ControlPlane{
				Version:           "test",
				RequireDeps:       tt.requireDeps,
				DependencyTimeout: time.Second,
			}
			server := NewWithDependencies(cfg, []Dependency{{
				Name:     "postgresql",
				Critical: true,
				Probe: ProbeFunc(func(context.Context) error {
					return tt.probeError
				}),
			}})
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)

			server.Handler().ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			var body healthResponse
			if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.Status != tt.wantReadiness {
				t.Fatalf("readiness = %q, want %q", body.Status, tt.wantReadiness)
			}
		})
	}
}

func TestServer_SystemInfoPinsPureAPIBaseline(t *testing.T) {
	t.Parallel()

	server := NewWithDependencies(runtimeconfig.ControlPlane{
		Version:     "test",
		Environment: "unit",
	}, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, APIBase+"/system/info", nil)
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	baseline, ok := body["executionBaseline"].(map[string]any)
	if !ok {
		t.Fatalf("executionBaseline missing: %#v", body)
	}
	if baseline["generationExecution"] != "remote-provider-api" || baseline["gpuRequired"] != false {
		t.Fatalf("execution baseline = %#v", baseline)
	}
}

func TestServer_ProviderStatusSupportsNoKeyDryRun(t *testing.T) {
	for _, name := range []string{
		"ARK_API_KEY",
		"ANTHROPIC_API_KEY",
		"ANTHROPIC_AUTH_TOKEN",
	} {
		t.Setenv(name, "")
	}

	server := NewWithDependencies(runtimeconfig.ControlPlane{Version: "test"}, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, APIBase+"/providers/status", nil)
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Mode         string                     `json:"mode"`
		Capabilities []providerCapabilityStatus `json:"capabilities"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Mode != "dry-run" || len(body.Capabilities) != 4 {
		t.Fatalf("provider status = %#v", body)
	}
	for _, capability := range body.Capabilities {
		if capability.LiveConfigured || capability.LiveCallsEnabled || !capability.DryRunAvailable || !capability.MockAvailable ||
			capability.LiveEvidence != "not_configured" || capability.MockEvidence != "mock_only" {
			t.Fatalf("no-key capability = %#v", capability)
		}
	}
}

func TestServer_ProviderStatusNeverEchoesSecrets(t *testing.T) {
	const secret = "secret-value-must-not-escape"
	t.Setenv("ARK_API_KEY", secret)
	t.Setenv("ANTHROPIC_API_KEY", secret)

	server := NewWithDependencies(runtimeconfig.ControlPlane{Version: "test"}, nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, APIBase+"/providers/status", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), secret) {
		t.Fatal("provider status leaked a runtime secret")
	}
	var body struct {
		Mode         string                     `json:"mode"`
		Capabilities []providerCapabilityStatus `json:"capabilities"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	for _, capability := range body.Capabilities {
		if !capability.LiveConfigured || capability.LiveCallsEnabled || capability.LiveEvidence != "pending_key_validation" {
			t.Fatalf("configured capability = %#v", capability)
		}
	}
	if body.Mode != "dry-run" {
		t.Fatalf("mode = %q, want dry-run until provider readiness is validated", body.Mode)
	}
}
