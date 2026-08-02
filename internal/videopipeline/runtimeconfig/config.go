// Package runtimeconfig owns explicit environment-backed configuration for the
// standalone video-pipeline services. It never scans developer-machine config.
package runtimeconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
	"github.com/google/uuid"
)

// LookupEnv matches os.LookupEnv and makes configuration tests hermetic.
type LookupEnv func(string) (string, bool)

// AgentPlanTTSEndpoint is the subscription-only HTTP endpoint documented by
// Agent Plan. The standard OpenSpeech route is intentionally not accepted:
// using it can bypass plan attribution and create an unauthenticated or
// separately billed request.
const (
	AgentPlanTTSEndpoint = "https://openspeech.bytedance.com/api/v3/plan/tts/unidirectional"
	// AgentPlanTTSSpeakerID is the TTS 2.0 voice explicitly approved for the
	// FLO-104 canary. Keeping it beside the endpoint lets configuration reject
	// a mismatched speaker before constructing the live Adapter.
	AgentPlanTTSSpeakerID = "zh_female_vv_uranus_bigtts"
)

// ControlPlane configures the video control-plane health/API process.
type ControlPlane struct {
	Environment               string
	HTTPAddress               string
	PostgresAddress           string
	PostgresDSN               string
	TemporalAddress           string
	TemporalNamespace         string
	TemporalTaskQueue         string
	AuthHMACSecret            string
	AuthAudience              string
	ArtifactRoot              string
	ProviderAdapterURL        string
	ProviderServiceAuthSecret string
	LiveCallsEnabled          bool
	DependencyTimeout         time.Duration
	ShutdownTimeout           time.Duration
	RequireDeps               bool
	Version                   string
}

// OrchestratorWorker configures the Temporal workflow worker.
type OrchestratorWorker struct {
	TemporalAddress           string
	Namespace                 string
	TaskQueue                 string
	ProviderAdapterURL        string
	ProviderServiceAuthSecret string
	SpeechProviderAdapterURL  string
	PostgresDSN               string
	ArtifactRoot              string
}

// MockProvider configures the deterministic, no-key provider fixture.
type MockProvider struct {
	HTTPAddress  string
	ArtifactRoot string
	ProviderID   string
	Capabilities []string
}

// VolcengineProvider configures the credential-isolated Agent Plan adapter.
// APIKey is retained only in memory and must never be logged or serialized.
type VolcengineProvider struct {
	HTTPAddress                    string
	ArtifactRoot                   string
	ProviderID                     string
	BaseURL                        string
	APIKey                         string
	ServiceAuthSecret              string
	Region                         string
	VideoModel                     string
	SpeechEndpoint                 string
	SpeechModel                    string
	SpeechSpeaker                  string
	SpeechRetryJobID               string
	SpeechRetryRecord              string
	SpeechCanaryJobID              string
	SpeechCanaryInputHash          string
	SpeechCanaryCueID              string
	SpeechCanaryVoiceAssetID       string
	SpeechCanaryParentVoiceVersion string
	SpeechCanaryVoiceVersion       string
	SpeechCanaryVoiceHash          string
	SpeechCanaryLicenseSnapshotID  string
	SpeechCanaryLicenseHash        string
	SpeechCanaryMaximumAFPMilli    int64
	SpeechCanaryMaximumCashMicros  int64
	SpeechBatchAuthorization       *speechcontract.BatchAuthorization
	MaxSpeechBytes                 int64
	PlanName                       string
	PricingVersion                 string
	Currency                       string
	MaxDownloadBytes               int64
	RequestTimeout                 time.Duration
	DownloadTimeout                time.Duration
}

// LoadControlPlane reads namespaced settings with safe local defaults.
func LoadControlPlane() (ControlPlane, error) {
	return loadControlPlane(os.LookupEnv)
}

func loadControlPlane(lookup LookupEnv) (ControlPlane, error) {
	cfg := ControlPlane{
		Environment:               value(lookup, "VIDEO_ENVIRONMENT", "development"),
		HTTPAddress:               value(lookup, "VIDEO_CONTROL_PLANE_HTTP_ADDRESS", ":8080"),
		PostgresAddress:           value(lookup, "VIDEO_POSTGRES_ADDRESS", "postgres:5432"),
		TemporalAddress:           value(lookup, "VIDEO_TEMPORAL_ADDRESS", "temporal:7233"),
		TemporalNamespace:         value(lookup, "VIDEO_TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue:         value(lookup, "VIDEO_TEMPORAL_TASK_QUEUE", "video-production-v1"),
		AuthAudience:              value(lookup, "VIDEO_AUTH_AUDIENCE", "video-control-plane"),
		ArtifactRoot:              value(lookup, "VIDEO_ARTIFACT_ROOT", "/var/lib/video-pipeline/artifacts"),
		ProviderAdapterURL:        value(lookup, "VIDEO_PROVIDER_ADAPTER_URL", "http://mock-provider:8090"),
		ProviderServiceAuthSecret: value(lookup, "VIDEO_PROVIDER_SERVICE_AUTH_SECRET", ""),
		Version:                   value(lookup, "VIDEO_BUILD_VERSION", "development"),
		DependencyTimeout:         2 * time.Second,
		ShutdownTimeout:           10 * time.Second,
		RequireDeps:               true,
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
	if cfg.LiveCallsEnabled, err = boolean(lookup, "VIDEO_LIVE_CALLS_ENABLED", false); err != nil {
		return ControlPlane{}, err
	}
	if cfg.LiveCallsEnabled && len(cfg.ProviderServiceAuthSecret) < 32 {
		return ControlPlane{}, errors.New("VIDEO_PROVIDER_SERVICE_AUTH_SECRET must contain at least 32 bytes when live calls are enabled")
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
	if strings.TrimSpace(cfg.TemporalNamespace) == "" || strings.TrimSpace(cfg.TemporalTaskQueue) == "" {
		return ControlPlane{}, errors.New("Temporal namespace and task queue are required")
	}
	cfg.AuthHMACSecret = value(lookup, "VIDEO_AUTH_HMAC_SECRET", "")
	if cfg.AuthHMACSecret != "" && len(cfg.AuthHMACSecret) < 32 {
		return ControlPlane{}, errors.New("VIDEO_AUTH_HMAC_SECRET must contain at least 32 bytes")
	}
	if strings.TrimSpace(cfg.AuthAudience) == "" {
		return ControlPlane{}, errors.New("VIDEO_AUTH_AUDIENCE is required")
	}
	if strings.EqualFold(cfg.Environment, "production") && cfg.AuthHMACSecret == "" {
		return ControlPlane{}, errors.New("VIDEO_AUTH_HMAC_SECRET is required in production")
	}
	if err := validateHTTPURL(cfg.ProviderAdapterURL); err != nil {
		return ControlPlane{}, fmt.Errorf("VIDEO_PROVIDER_ADAPTER_URL: %w", err)
	}
	if cfg.PostgresDSN, err = postgresDSN(lookup, cfg.PostgresAddress); err != nil {
		return ControlPlane{}, err
	}
	return cfg, nil
}

// LoadOrchestratorWorker reads Temporal worker settings.
func LoadOrchestratorWorker() (OrchestratorWorker, error) {
	cfg := OrchestratorWorker{
		TemporalAddress:           value(os.LookupEnv, "VIDEO_TEMPORAL_ADDRESS", "temporal:7233"),
		Namespace:                 value(os.LookupEnv, "VIDEO_TEMPORAL_NAMESPACE", "default"),
		TaskQueue:                 value(os.LookupEnv, "VIDEO_TEMPORAL_TASK_QUEUE", "video-production-v1"),
		ProviderAdapterURL:        value(os.LookupEnv, "VIDEO_PROVIDER_ADAPTER_URL", "http://mock-provider:8090"),
		ProviderServiceAuthSecret: value(os.LookupEnv, "VIDEO_PROVIDER_SERVICE_AUTH_SECRET", ""),
		SpeechProviderAdapterURL:  value(os.LookupEnv, "VIDEO_SPEECH_PROVIDER_ADAPTER_URL", "http://mock-provider:8090"),
		ArtifactRoot:              value(os.LookupEnv, "VIDEO_ARTIFACT_ROOT", "/var/lib/video-pipeline/artifacts"),
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
	if err := validateHTTPURL(cfg.SpeechProviderAdapterURL); err != nil {
		return OrchestratorWorker{}, fmt.Errorf("VIDEO_SPEECH_PROVIDER_ADAPTER_URL: %w", err)
	}
	var err error
	postgresAddress := value(os.LookupEnv, "VIDEO_POSTGRES_ADDRESS", "postgres:5432")
	if err := validateDialAddress(postgresAddress); err != nil {
		return OrchestratorWorker{}, fmt.Errorf("VIDEO_POSTGRES_ADDRESS: %w", err)
	}
	if cfg.PostgresDSN, err = postgresDSN(os.LookupEnv, postgresAddress); err != nil {
		return OrchestratorWorker{}, err
	}
	if strings.TrimSpace(cfg.ArtifactRoot) == "" {
		return OrchestratorWorker{}, errors.New("VIDEO_ARTIFACT_ROOT is required")
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

// LoadVolcengineProvider reads only explicit runtime configuration. It does
// not scan arkcli profiles, shell history, or developer-machine config files.
func LoadVolcengineProvider() (VolcengineProvider, error) {
	return loadVolcengineProvider(os.LookupEnv)
}

func loadVolcengineProvider(lookup LookupEnv) (VolcengineProvider, error) {
	cfg := VolcengineProvider{
		HTTPAddress:                    value(lookup, "VIDEO_VOLCENGINE_PROVIDER_HTTP_ADDRESS", ":8091"),
		ArtifactRoot:                   value(lookup, "VIDEO_ARTIFACT_ROOT", "/var/lib/video-pipeline/artifacts"),
		ProviderID:                     value(lookup, "VIDEO_VOLCENGINE_PROVIDER_ID", "volcengine-agent-plan-large"),
		BaseURL:                        value(lookup, "VIDEO_VOLCENGINE_BASE_URL", "https://ark.cn-beijing.volces.com/api/plan/v3"),
		APIKey:                         value(lookup, "ARK_API_KEY", ""),
		ServiceAuthSecret:              value(lookup, "VIDEO_PROVIDER_SERVICE_AUTH_SECRET", ""),
		Region:                         value(lookup, "VIDEO_VOLCENGINE_REGION", "cn-beijing"),
		VideoModel:                     value(lookup, "VIDEO_VOLCENGINE_VIDEO_MODEL", "doubao-seedance-2.0"),
		SpeechEndpoint:                 value(lookup, "VIDEO_VOLCENGINE_TTS_ENDPOINT", AgentPlanTTSEndpoint),
		SpeechModel:                    "doubao-seed-tts-2.0",
		SpeechSpeaker:                  value(lookup, "VIDEO_VOLCENGINE_TTS_SPEAKER", AgentPlanTTSSpeakerID),
		SpeechRetryJobID:               value(lookup, "VIDEO_VOLCENGINE_TTS_RETRY_JOB_ID", ""),
		SpeechRetryRecord:              value(lookup, "VIDEO_VOLCENGINE_TTS_RETRY_RECORD_SHA256", ""),
		SpeechCanaryJobID:              value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_JOB_ID", ""),
		SpeechCanaryInputHash:          value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_INPUT_SHA256", ""),
		SpeechCanaryCueID:              value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_CUE_ID", ""),
		SpeechCanaryVoiceAssetID:       value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_VOICE_ASSET_ID", ""),
		SpeechCanaryParentVoiceVersion: value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_PARENT_VOICE_VERSION_ID", ""),
		SpeechCanaryVoiceVersion:       value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_VOICE_VERSION_ID", ""),
		SpeechCanaryVoiceHash:          value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_VOICE_SHA256", ""),
		SpeechCanaryLicenseSnapshotID:  value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_LICENSE_SNAPSHOT_ID", ""),
		SpeechCanaryLicenseHash:        value(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_LICENSE_SHA256", ""),
		PlanName:                       value(lookup, "VIDEO_VOLCENGINE_PLAN", "agent-plan-large"),
		PricingVersion:                 value(lookup, "VIDEO_VOLCENGINE_PRICING_VERSION", "agent-plan-large-included-v1"),
		Currency:                       value(lookup, "VIDEO_VOLCENGINE_CURRENCY", "CNY"),
		MaxDownloadBytes:               256 << 20,
		MaxSpeechBytes:                 32 << 20,
		RequestTimeout:                 2 * time.Minute,
		DownloadTimeout:                2 * time.Minute,
	}
	var err error
	if cfg.MaxDownloadBytes, err = positiveInt64(lookup, "VIDEO_VOLCENGINE_MAX_DOWNLOAD_BYTES", cfg.MaxDownloadBytes); err != nil {
		return VolcengineProvider{}, err
	}
	if cfg.MaxSpeechBytes, err = positiveInt64(lookup, "VIDEO_VOLCENGINE_MAX_SPEECH_BYTES", cfg.MaxSpeechBytes); err != nil {
		return VolcengineProvider{}, err
	}
	if cfg.SpeechCanaryMaximumAFPMilli, err = nonNegativeInt64(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_MAX_AFP_MILLI", 0); err != nil {
		return VolcengineProvider{}, err
	}
	if cfg.SpeechCanaryMaximumCashMicros, err = nonNegativeInt64(lookup, "VIDEO_VOLCENGINE_TTS_CANARY_MAX_CASH_MICROS", 0); err != nil {
		return VolcengineProvider{}, err
	}
	if raw := value(lookup, "VIDEO_VOLCENGINE_TTS_BATCH_AUTHORIZATION_JSON", ""); raw != "" {
		var authorization speechcontract.BatchAuthorization
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&authorization); err != nil {
			return VolcengineProvider{}, fmt.Errorf("VIDEO_VOLCENGINE_TTS_BATCH_AUTHORIZATION_JSON: %w", err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return VolcengineProvider{}, errors.New("VIDEO_VOLCENGINE_TTS_BATCH_AUTHORIZATION_JSON must contain exactly one JSON value")
		}
		if err := authorization.Validate(); err != nil {
			return VolcengineProvider{}, fmt.Errorf("VIDEO_VOLCENGINE_TTS_BATCH_AUTHORIZATION_JSON: %w", err)
		}
		cfg.SpeechBatchAuthorization = &authorization
	}
	if cfg.RequestTimeout, err = duration(lookup, "VIDEO_VOLCENGINE_REQUEST_TIMEOUT", cfg.RequestTimeout); err != nil {
		return VolcengineProvider{}, err
	}
	if cfg.DownloadTimeout, err = duration(lookup, "VIDEO_VOLCENGINE_DOWNLOAD_TIMEOUT", cfg.DownloadTimeout); err != nil {
		return VolcengineProvider{}, err
	}
	if err := validateListenAddress(cfg.HTTPAddress); err != nil {
		return VolcengineProvider{}, fmt.Errorf("VIDEO_VOLCENGINE_PROVIDER_HTTP_ADDRESS: %w", err)
	}
	if err := validateHTTPURL(cfg.BaseURL); err != nil {
		return VolcengineProvider{}, fmt.Errorf("VIDEO_VOLCENGINE_BASE_URL: %w", err)
	}
	if err := validateHTTPURL(cfg.SpeechEndpoint); err != nil {
		return VolcengineProvider{}, fmt.Errorf("VIDEO_VOLCENGINE_TTS_ENDPOINT: %w", err)
	}
	if cfg.SpeechEndpoint != AgentPlanTTSEndpoint {
		return VolcengineProvider{}, errors.New("VIDEO_VOLCENGINE_TTS_ENDPOINT must use the exact Agent Plan subscription endpoint")
	}
	if cfg.SpeechSpeaker != AgentPlanTTSSpeakerID {
		return VolcengineProvider{}, errors.New("VIDEO_VOLCENGINE_TTS_SPEAKER must use the approved TTS 2.0 speaker")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return VolcengineProvider{}, errors.New("ARK_API_KEY is required for the live provider adapter")
	}
	if len(cfg.ServiceAuthSecret) < 32 {
		return VolcengineProvider{}, errors.New("VIDEO_PROVIDER_SERVICE_AUTH_SECRET must contain at least 32 bytes")
	}
	if strings.TrimSpace(cfg.ArtifactRoot) == "" || strings.TrimSpace(cfg.ProviderID) == "" ||
		strings.TrimSpace(cfg.Region) == "" || strings.TrimSpace(cfg.VideoModel) == "" ||
		strings.TrimSpace(cfg.SpeechModel) == "" || strings.TrimSpace(cfg.SpeechSpeaker) == "" ||
		strings.TrimSpace(cfg.PlanName) == "" || strings.TrimSpace(cfg.PricingVersion) == "" ||
		strings.TrimSpace(cfg.Currency) == "" {
		return VolcengineProvider{}, errors.New("live provider identity, route, plan, pricing, currency, and artifact root are required")
	}
	if (cfg.SpeechRetryJobID == "") != (cfg.SpeechRetryRecord == "") {
		return VolcengineProvider{}, errors.New("TTS retry requires both an exact job ID and provider record SHA-256")
	}
	if cfg.SpeechRetryJobID != "" {
		if !strings.HasPrefix(cfg.SpeechRetryJobID, "speech-") || !lowercaseSHA256(cfg.SpeechRetryRecord) {
			return VolcengineProvider{}, errors.New("TTS retry requires a speech job ID and lowercase provider record SHA-256")
		}
	}
	canaryValues := []string{
		cfg.SpeechCanaryJobID, cfg.SpeechCanaryInputHash, cfg.SpeechCanaryCueID,
		cfg.SpeechCanaryVoiceAssetID, cfg.SpeechCanaryParentVoiceVersion,
		cfg.SpeechCanaryVoiceVersion, cfg.SpeechCanaryVoiceHash,
		cfg.SpeechCanaryLicenseSnapshotID, cfg.SpeechCanaryLicenseHash,
	}
	canaryConfigured := cfg.SpeechCanaryMaximumAFPMilli != 0 ||
		cfg.SpeechCanaryMaximumCashMicros != 0
	for _, candidate := range canaryValues {
		canaryConfigured = canaryConfigured || candidate != ""
	}
	if canaryConfigured {
		for _, candidate := range canaryValues {
			if candidate == "" {
				return VolcengineProvider{}, errors.New("TTS canary requires the complete frozen job, cue, voice, and license identity")
			}
		}
		if !strings.HasPrefix(cfg.SpeechCanaryJobID, "speech-v2-") ||
			!lowercaseSHA256(cfg.SpeechCanaryInputHash) ||
			!lowercaseSHA256(cfg.SpeechCanaryVoiceHash) ||
			!lowercaseSHA256(cfg.SpeechCanaryLicenseHash) ||
			cfg.SpeechCanaryMaximumAFPMilli <= 0 ||
			cfg.SpeechCanaryMaximumCashMicros != 0 {
			return VolcengineProvider{}, errors.New("TTS canary identity or zero-cash budget is invalid")
		}
		for _, candidate := range []string{
			cfg.SpeechCanaryVoiceAssetID, cfg.SpeechCanaryParentVoiceVersion,
			cfg.SpeechCanaryVoiceVersion, cfg.SpeechCanaryLicenseSnapshotID,
		} {
			if _, err := uuid.Parse(candidate); err != nil {
				return VolcengineProvider{}, errors.New("TTS canary asset and license identities must be UUIDs")
			}
		}
		if cfg.SpeechRetryJobID != "" {
			return VolcengineProvider{}, errors.New("TTS canary and legacy reconciliation cannot be enabled together")
		}
	}
	if cfg.SpeechBatchAuthorization != nil {
		if canaryConfigured || cfg.SpeechRetryJobID != "" {
			return VolcengineProvider{}, errors.New("TTS batch authorization cannot be combined with canary or reconciliation settings")
		}
		batch := cfg.SpeechBatchAuthorization
		if !sameProvider(batch.Provider, "volcengine_ark") || batch.ModelID != cfg.SpeechModel ||
			batch.RouteVersion != "agent-plan-large-tts-v2" ||
			batch.ResourceID != "seed-tts-2.0" || batch.Speaker != cfg.SpeechSpeaker {
			return VolcengineProvider{}, errors.New("TTS batch authorization route does not match the configured Agent Plan adapter")
		}
	}
	return cfg, nil
}

func sameProvider(left, right string) bool {
	normalize := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "volcengine" {
			return "volcengine_ark"
		}
		return value
	}
	return normalize(left) == normalize(right)
}

func lowercaseSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
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

func positiveInt64(lookup LookupEnv, name string, fallback int64) (int64, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func nonNegativeInt64(lookup LookupEnv, name string, fallback int64) (int64, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
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

func postgresDSN(lookup LookupEnv, address string) (string, error) {
	if raw, ok := lookup("VIDEO_POSTGRES_DSN"); ok && strings.TrimSpace(raw) != "" {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" || parsed.Path == "" {
			return "", errors.New("VIDEO_POSTGRES_DSN must be a PostgreSQL URL")
		}
		return parsed.String(), nil
	}
	user := value(lookup, "VIDEO_POSTGRES_USER", "video")
	password := value(lookup, "VIDEO_POSTGRES_PASSWORD", "video-local-only")
	database := value(lookup, "VIDEO_POSTGRES_DATABASE", "video_pipeline")
	if user == "" || password == "" || database == "" {
		return "", errors.New("PostgreSQL user, password, and database must be non-empty")
	}
	dsn := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     address,
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	return dsn.String(), nil
}
