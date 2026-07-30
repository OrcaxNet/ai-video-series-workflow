// Package runtimeconfig owns explicit environment-backed configuration for the
// standalone video-pipeline services. It never scans developer-machine config.
package runtimeconfig

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// LookupEnv matches os.LookupEnv and makes configuration tests hermetic.
type LookupEnv func(string) (string, bool)

// ControlPlane configures the video control-plane health/API process.
type ControlPlane struct {
	Environment        string
	HTTPAddress        string
	PostgresAddress    string
	TemporalAddress    string
	ArtifactRoot       string
	ProviderAdapterURL string
	DependencyTimeout  time.Duration
	ShutdownTimeout    time.Duration
	RequireDeps        bool
	Version            string
}

// OrchestratorWorker configures the Temporal workflow worker.
type OrchestratorWorker struct {
	TemporalAddress    string
	Namespace          string
	TaskQueue          string
	ProviderAdapterURL string
}

// MockProvider configures the deterministic, no-key provider fixture.
type MockProvider struct {
	HTTPAddress  string
	ArtifactRoot string
	ProviderID   string
	Capabilities []string
}

// LoadControlPlane reads namespaced settings with safe local defaults.
func LoadControlPlane() (ControlPlane, error) {
	return loadControlPlane(os.LookupEnv)
}

func loadControlPlane(lookup LookupEnv) (ControlPlane, error) {
	cfg := ControlPlane{
		Environment:        value(lookup, "VIDEO_ENVIRONMENT", "development"),
		HTTPAddress:        value(lookup, "VIDEO_CONTROL_PLANE_HTTP_ADDRESS", ":8080"),
		PostgresAddress:    value(lookup, "VIDEO_POSTGRES_ADDRESS", "postgres:5432"),
		TemporalAddress:    value(lookup, "VIDEO_TEMPORAL_ADDRESS", "temporal:7233"),
		ArtifactRoot:       value(lookup, "VIDEO_ARTIFACT_ROOT", "/var/lib/video-pipeline/artifacts"),
		ProviderAdapterURL: value(lookup, "VIDEO_PROVIDER_ADAPTER_URL", "http://mock-provider:8090"),
		Version:            value(lookup, "VIDEO_BUILD_VERSION", "development"),
		DependencyTimeout:  2 * time.Second,
		ShutdownTimeout:    10 * time.Second,
		RequireDeps:        true,
	}

	var err error
	if cfg.DependencyTimeout, err = duration(lookup, "VIDEO_DEPENDENCY_TIMEOUT", cfg.DependencyTimeout); err != nil {
		return ControlPlane{}, err
	}
	if cfg.ShutdownTimeout, err = duration(lookup, "VIDEO_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return ControlPlane{}, err
	}
	if cfg.RequireDeps, err = boolean(lookup, "VIDEO_REQUIRE_DEPENDENCIES", cfg.RequireDeps); err != nil {
		return ControlPlane{}, err
	}
	if err := validateListenAddress(cfg.HTTPAddress); err != nil {
		return ControlPlane{}, fmt.Errorf("VIDEO_CONTROL_PLANE_HTTP_ADDRESS: %w", err)
	}
	for name, address := range map[string]string{
		"VIDEO_POSTGRES_ADDRESS": cfg.PostgresAddress,
		"VIDEO_TEMPORAL_ADDRESS": cfg.TemporalAddress,
	} {
		if err := validateDialAddress(address); err != nil {
			return ControlPlane{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	if strings.TrimSpace(cfg.ArtifactRoot) == "" {
		return ControlPlane{}, errors.New("VIDEO_ARTIFACT_ROOT is required")
	}
	if err := validateHTTPURL(cfg.ProviderAdapterURL); err != nil {
		return ControlPlane{}, fmt.Errorf("VIDEO_PROVIDER_ADAPTER_URL: %w", err)
	}
	return cfg, nil
}

// LoadOrchestratorWorker reads Temporal worker settings.
func LoadOrchestratorWorker() (OrchestratorWorker, error) {
	cfg := OrchestratorWorker{
		TemporalAddress:    value(os.LookupEnv, "VIDEO_TEMPORAL_ADDRESS", "temporal:7233"),
		Namespace:          value(os.LookupEnv, "VIDEO_TEMPORAL_NAMESPACE", "default"),
		TaskQueue:          value(os.LookupEnv, "VIDEO_TEMPORAL_TASK_QUEUE", "video-production-v1"),
		ProviderAdapterURL: value(os.LookupEnv, "VIDEO_PROVIDER_ADAPTER_URL", "http://mock-provider:8090"),
	}
	if err := validateDialAddress(cfg.TemporalAddress); err != nil {
		return OrchestratorWorker{}, fmt.Errorf("VIDEO_TEMPORAL_ADDRESS: %w", err)
	}
	if strings.TrimSpace(cfg.Namespace) == "" || strings.TrimSpace(cfg.TaskQueue) == "" {
		return OrchestratorWorker{}, errors.New("Temporal namespace and task queue are required")
	}
	if err := validateHTTPURL(cfg.ProviderAdapterURL); err != nil {
		return OrchestratorWorker{}, fmt.Errorf("VIDEO_PROVIDER_ADAPTER_URL: %w", err)
	}
	return cfg, nil
}

// LoadMockProvider reads deterministic fixture settings. It deliberately has
// no credential fields and never scans developer-machine configuration.
func LoadMockProvider() (MockProvider, error) {
	cfg := MockProvider{
		HTTPAddress:  value(os.LookupEnv, "VIDEO_MOCK_PROVIDER_HTTP_ADDRESS", ":8090"),
		ArtifactRoot: value(os.LookupEnv, "VIDEO_ARTIFACT_ROOT", "/var/lib/video-pipeline/artifacts"),
		ProviderID:   value(os.LookupEnv, "VIDEO_MOCK_PROVIDER_ID", "mock-local-v1"),
		Capabilities: splitCSV(value(os.LookupEnv, "VIDEO_MOCK_PROVIDER_CAPABILITIES", "text.primary,image.primary,video.primary,speech.primary")),
	}
	if err := validateListenAddress(cfg.HTTPAddress); err != nil {
		return MockProvider{}, fmt.Errorf("VIDEO_MOCK_PROVIDER_HTTP_ADDRESS: %w", err)
	}
	if strings.TrimSpace(cfg.ArtifactRoot) == "" || strings.TrimSpace(cfg.ProviderID) == "" {
		return MockProvider{}, errors.New("artifact root and provider ID are required")
	}
	if len(cfg.Capabilities) == 0 {
		return MockProvider{}, errors.New("at least one provider capability is required")
	}
	return cfg, nil
}

func value(lookup LookupEnv, name, fallback string) string {
	if got, ok := lookup(name); ok {
		return strings.TrimSpace(got)
	}
	return fallback
}

func duration(lookup LookupEnv, name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}

func boolean(lookup LookupEnv, name string, fallback bool) (bool, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return parsed, nil
}

func validateListenAddress(address string) error {
	if !strings.Contains(address, ":") {
		return errors.New("must include a port")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(port) == "" {
		return errors.New("must be host:port or :port")
	}
	return nil
}

func validateDialAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return errors.New("must be host:port")
	}
	return nil
}

func validateHTTPURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("must be an absolute http(s) URL")
	}
	return nil
}

func splitCSV(raw string) []string {
	var values []string
	seen := map[string]struct{}{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		values = append(values, item)
	}
	return values
}
