// Package controlplane exposes the isolated video-pipeline control-plane
// bootstrap API. Business mutation endpoints are contract-first in OpenAPI;
// this executable skeleton provides dependency-aware health, no-key provider
// status, and pure-API execution discovery.
package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

const APIBase = "/api/v1"

// Probe checks one runtime dependency.
type Probe interface {
	Check(context.Context) error
}

// ProbeFunc adapts a function to Probe.
type ProbeFunc func(context.Context) error

func (f ProbeFunc) Check(ctx context.Context) error { return f(ctx) }

// Dependency is one readiness dependency.
type Dependency struct {
	Name     string
	Critical bool
	Probe    Probe
}

// Server is the dependency-aware HTTP surface.
type Server struct {
	config       runtimeconfig.ControlPlane
	dependencies []Dependency
	store        Store
	workflows    WorkflowController
	artifacts    ArtifactVerifier
	now          func() time.Time
}

// ArtifactVerifier proves that a CAS object exists before its immutable
// reference is committed to PostgreSQL.
type ArtifactVerifier interface {
	Open(string) (io.ReadCloser, error)
}

// New creates a control-plane server with runtime dependency probes.
func New(config runtimeconfig.ControlPlane) *Server {
	return NewWithDependencies(config, []Dependency{
		{Name: "postgresql", Critical: true, Probe: TCPProbe(config.PostgresAddress)},
		{Name: "temporal", Critical: true, Probe: TCPProbe(config.TemporalAddress)},
		{Name: "artifact_store", Critical: true, Probe: DirectoryProbe(config.ArtifactRoot)},
		{Name: "provider_adapter", Critical: true, Probe: HTTPProbe(strings.TrimRight(config.ProviderAdapterURL, "/") + "/health/ready")},
	})
}

// NewWithDependencies is used by tests and alternate deployments.
func NewWithDependencies(config runtimeconfig.ControlPlane, dependencies []Dependency) *Server {
	return &Server{config: config, dependencies: dependencies, now: time.Now}
}

// NewWithRuntime injects product truth and external orchestration boundaries.
func NewWithRuntime(
	config runtimeconfig.ControlPlane,
	dependencies []Dependency,
	store Store,
	workflows WorkflowController,
	artifacts ArtifactVerifier,
) *Server {
	return &Server{
		config:       config,
		dependencies: dependencies,
		store:        store,
		workflows:    workflows,
		artifacts:    artifacts,
		now:          time.Now,
	}
}

// Handler returns the namespaced HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.liveness)
	mux.HandleFunc("GET /health/ready", s.readiness)
	mux.HandleFunc("GET "+APIBase+"/system/health", s.readiness)
	mux.HandleFunc("GET "+APIBase+"/system/info", s.systemInfo)
	mux.HandleFunc("GET "+APIBase+"/providers/status", s.providerStatus)
	mux.HandleFunc("POST "+APIBase+"/series", s.createSeries)
	mux.HandleFunc("POST "+APIBase+"/series/{seriesId}/sources", s.createSourceRevision)
	mux.HandleFunc("POST "+APIBase+"/sources/{sourceRevisionId}/compilations", s.startContentCompilation)
	mux.HandleFunc("GET "+APIBase+"/series/{seriesId}/impacts", s.listRevisionImpacts)
	mux.HandleFunc("POST "+APIBase+"/generation-plans", s.createGenerationPlan)
	mux.HandleFunc("POST "+APIBase+"/episodes/{episodeId}/production-batches", s.startEpisodeProduction)
	mux.HandleFunc("POST "+APIBase+"/shots/{shotId}/runs", s.createGenerationRun)
	mux.HandleFunc("GET "+APIBase+"/runs/{runId}", s.getGenerationRun)
	mux.HandleFunc("POST "+APIBase+"/runs/{runId}/pause", s.pauseGenerationRun)
	mux.HandleFunc("POST "+APIBase+"/runs/{runId}/cancel", s.cancelGenerationRun)
	mux.HandleFunc("POST "+APIBase+"/runs/{runId}/resume", s.resumeGenerationRun)
	mux.HandleFunc("POST "+APIBase+"/runs/{runId}/publication-lock", s.lockRunForPublication)
	mux.HandleFunc("POST "+APIBase+"/approvals", s.createApprovalDecision)
	mux.HandleFunc("GET "+APIBase+"/manifests/{scopeType}/{revisionId}", s.getGenerationManifest)
	mux.HandleFunc("GET "+APIBase+"/operations/{operationId}", s.getOperation)
	return securityHeaders(s.authentication(mux))
}

type healthResponse struct {
	Status       string                      `json:"status"`
	Version      string                      `json:"version"`
	Timestamp    string                      `json:"timestamp"`
	Dependencies map[string]dependencyStatus `json:"dependencies,omitempty"`
}

type dependencyStatus struct {
	Status     string `json:"status"`
	Critical   bool   `json:"critical"`
	LatencyMS  int64  `json:"latencyMs"`
	ErrorCode  string `json:"errorCode,omitempty"`
	Retryable  bool   `json:"retryable,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

func (s *Server) liveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Status:    "alive",
		Version:   s.config.Version,
		Timestamp: s.now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	statuses := make(map[string]dependencyStatus, len(s.dependencies))
	ready := true
	for _, dependency := range s.dependencies {
		started := s.now()
		ctx, cancel := context.WithTimeout(r.Context(), s.config.DependencyTimeout)
		err := dependency.Probe.Check(ctx)
		cancel()

		status := dependencyStatus{
			Status:    "ok",
			Critical:  dependency.Critical,
			LatencyMS: max(s.now().Sub(started).Milliseconds(), 0),
		}
		if err != nil {
			status.Status = "unavailable"
			status.ErrorCode = "DEPENDENCY_UNAVAILABLE"
			status.Retryable = true
			status.Suggestion = "check the dependency endpoint and retry"
			if dependency.Critical {
				ready = false
			}
		}
		statuses[dependency.Name] = status
	}

	code := http.StatusOK
	state := "ready"
	if !ready && s.config.RequireDeps {
		code = http.StatusServiceUnavailable
		state = "not_ready"
	} else if !ready {
		state = "degraded"
	}
	writeJSON(w, code, healthResponse{
		Status:       state,
		Version:      s.config.Version,
		Timestamp:    s.now().UTC().Format(time.RFC3339Nano),
		Dependencies: statuses,
	})
}

func (s *Server) systemInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":     "video-control-plane",
		"version":     s.config.Version,
		"environment": s.config.Environment,
		"apiVersion":  "v1",
		"executionBaseline": map[string]any{
			"stage":                    "M0",
			"generationExecution":      "remote-provider-api",
			"localGenerativeInference": false,
			"gpuRequired":              false,
			"dryRunAvailable":          true,
			"mockProviderAvailable":    true,
			"defaultProviderPriority":  []string{"volcengine", "explicit-claude-api", "mock"},
			"capabilityAliases":        []string{"text.primary", "image.primary", "video.primary", "speech.primary"},
			"requiredGates":            []string{"G1", "G2", "Q1", "G3"},
			"stateBaseline":            []string{"Temporal", "PostgreSQL", "content-addressed-storage"},
		},
	})
}

type providerCapabilityStatus struct {
	Alias             string   `json:"alias"`
	LiveConfigured    bool     `json:"liveConfigured"`
	LiveCallsEnabled  bool     `json:"liveCallsEnabled"`
	DryRunAvailable   bool     `json:"dryRunAvailable"`
	MockAvailable     bool     `json:"mockAvailable"`
	DefaultProvider   string   `json:"defaultProvider"`
	LiveEvidence      string   `json:"liveEvidence"`
	MockEvidence      string   `json:"mockEvidence"`
	MissingSecretRefs []string `json:"missingSecretRefs,omitempty"`
}

func (s *Server) providerStatus(w http.ResponseWriter, _ *http.Request) {
	arkConfigured := anyEnvironmentSet("ARK_API_KEY")
	claudeConfigured := anyEnvironmentSet("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")

	statuses := []providerCapabilityStatus{
		capabilityStatus("text.primary", arkConfigured || claudeConfigured, "volcengine_ark", []string{"ARK_API_KEY", "ANTHROPIC_API_KEY_OR_AUTH_TOKEN"}),
		capabilityStatus("image.primary", arkConfigured, "volcengine_ark", []string{"ARK_API_KEY"}),
		capabilityStatus("video.primary", arkConfigured, "volcengine_ark", []string{"ARK_API_KEY"}),
		capabilityStatus("speech.primary", arkConfigured, "volcengine_agent_plan_tts", []string{"ARK_API_KEY"}),
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": "v1",
		"mode":          "dry-run",
		"capabilities":  statuses,
		"secretPolicy": map[string]any{
			"source":              "explicit-runtime-environment-or-secret-store",
			"databasePlaintext":   false,
			"frontendExposure":    false,
			"developerConfigScan": false,
		},
	})
}

func capabilityStatus(alias string, configured bool, defaultProvider string, refs []string) providerCapabilityStatus {
	status := providerCapabilityStatus{
		Alias:            alias,
		LiveConfigured:   configured,
		LiveCallsEnabled: false,
		DryRunAvailable:  true,
		MockAvailable:    true,
		DefaultProvider:  defaultProvider,
		LiveEvidence:     "not_configured",
		MockEvidence:     "mock_only",
	}
	if configured {
		status.LiveEvidence = "pending_key_validation"
	}
	if !configured {
		status.MissingSecretRefs = refs
	}
	return status
}

func anyEnvironmentSet(names ...string) bool {
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			return true
		}
	}
	return false
}

func allEnvironmentSet(names ...string) bool {
	for _, name := range names {
		if value, ok := os.LookupEnv(name); !ok || value == "" {
			return false
		}
	}
	return true
}

// TCPProbe checks a host:port without exchanging credentials.
func TCPProbe(address string) Probe {
	return ProbeFunc(func(ctx context.Context) error {
		dialer := net.Dialer{}
		connection, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return fmt.Errorf("dial %s: %w", address, err)
		}
		return connection.Close()
	})
}

// DirectoryProbe checks that the artifact root exists and is writable.
func DirectoryProbe(root string) Probe {
	return ProbeFunc(func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.MkdirAll(root, 0o750); err != nil {
			return fmt.Errorf("create artifact root: %w", err)
		}
		probe, err := os.CreateTemp(root, ".health-*")
		if err != nil {
			return fmt.Errorf("write artifact root: %w", err)
		}
		name := probe.Name()
		if closeErr := probe.Close(); closeErr != nil {
			return fmt.Errorf("close artifact probe: %w", closeErr)
		}
		if err := os.Remove(filepath.Clean(name)); err != nil {
			return fmt.Errorf("remove artifact probe: %w", err)
		}
		return nil
	})
}

// HTTPProbe checks a dependency readiness URL.
func HTTPProbe(endpoint string) Probe {
	return ProbeFunc(func(ctx context.Context) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("unexpected status %d", response.StatusCode)
		}
		return nil
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}
