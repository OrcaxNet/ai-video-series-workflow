package runtimeconfig

import (
	"strings"
	"testing"
	"time"
)

func TestLoadControlPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		values  map[string]string
		wantErr bool
		check   func(*testing.T, ControlPlane)
	}{
		{
			name:   "defaults",
			values: map[string]string{},
			check: func(t *testing.T, cfg ControlPlane) {
				t.Helper()
				if cfg.HTTPAddress != ":8080" || cfg.DependencyTimeout != 2*time.Second || !cfg.RequireDeps {
					t.Fatalf("unexpected defaults: %#v", cfg)
				}
			},
		},
		{
			name: "overrides",
			values: map[string]string{
				"VIDEO_CONTROL_PLANE_HTTP_ADDRESS": "127.0.0.1:18080",
				"VIDEO_DEPENDENCY_TIMEOUT":         "750ms",
				"VIDEO_REQUIRE_DEPENDENCIES":       "false",
			},
			check: func(t *testing.T, cfg ControlPlane) {
				t.Helper()
				if cfg.HTTPAddress != "127.0.0.1:18080" || cfg.DependencyTimeout != 750*time.Millisecond || cfg.RequireDeps {
					t.Fatalf("unexpected overrides: %#v", cfg)
				}
			},
		},
		{
			name: "rejects invalid dependency address",
			values: map[string]string{
				"VIDEO_TEMPORAL_ADDRESS": "temporal",
			},
			wantErr: true,
		},
		{
			name: "rejects invalid provider URL",
			values: map[string]string{
				"VIDEO_PROVIDER_ADAPTER_URL": "file:///tmp/provider",
			},
			wantErr: true,
		},
		{
			name: "requires authentication secret in production",
			values: map[string]string{
				"VIDEO_ENVIRONMENT": "production",
			},
			wantErr: true,
		},
		{
			name: "accepts production authentication configuration",
			values: map[string]string{
				"VIDEO_ENVIRONMENT":      "production",
				"VIDEO_AUTH_HMAC_SECRET": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			},
			check: func(t *testing.T, cfg ControlPlane) {
				t.Helper()
				if cfg.AuthAudience != "video-control-plane" || len(cfg.AuthHMACSecret) != 32 {
					t.Fatalf("unexpected auth config")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lookup := func(name string) (string, bool) {
				value, ok := tt.values[name]
				return value, ok
			}
			cfg, err := loadControlPlane(lookup)
			if tt.wantErr {
				if err == nil {
					t.Fatal("loadControlPlane() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("loadControlPlane() error = %v", err)
			}
			tt.check(t, cfg)
		})
	}
}

func TestLoadMockProviderDefaults(t *testing.T) {
	t.Setenv("VIDEO_MOCK_PROVIDER_HTTP_ADDRESS", "127.0.0.1:19090")
	t.Setenv("VIDEO_ARTIFACT_ROOT", t.TempDir())
	t.Setenv("VIDEO_MOCK_PROVIDER_ID", "fixture")
	t.Setenv("VIDEO_MOCK_PROVIDER_CAPABILITIES", "text.primary,image.primary,video.primary,speech.primary")

	cfg, err := LoadMockProvider()
	if err != nil {
		t.Fatalf("LoadMockProvider() error = %v", err)
	}
	if cfg.ProviderID != "fixture" || len(cfg.Capabilities) != 4 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}

func TestLoadVolcengineProviderRequiresExplicitKeyAndUsesAgentPlanDefaults(t *testing.T) {
	base := map[string]string{
		"VIDEO_ARTIFACT_ROOT": t.TempDir(),
	}
	lookup := func(name string) (string, bool) {
		value, ok := base[name]
		return value, ok
	}
	if _, err := loadVolcengineProvider(lookup); err == nil {
		t.Fatal("loadVolcengineProvider() error = nil without ARK_API_KEY")
	}
	base["ARK_API_KEY"] = "test-runtime-credential"
	if _, err := loadVolcengineProvider(lookup); err == nil {
		t.Fatal("loadVolcengineProvider() error = nil without service authentication secret")
	}
	base["VIDEO_PROVIDER_SERVICE_AUTH_SECRET"] = "test-service-auth-secret-32-bytes-long"
	cfg, err := loadVolcengineProvider(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != "https://ark.cn-beijing.volces.com/api/plan/v3" ||
		cfg.VideoModel != "doubao-seedance-2.0" || cfg.PlanName != "agent-plan-large" ||
		cfg.SpeechEndpoint != "https://openspeech.bytedance.com/api/v3/tts/unidirectional" ||
		cfg.SpeechModel != "doubao-seed-tts-2.0" || cfg.SpeechSpeaker != "zh_female_vv_uranus_bigtts" ||
		cfg.APIKey != "test-runtime-credential" || cfg.ServiceAuthSecret != "test-service-auth-secret-32-bytes-long" ||
		cfg.MaxDownloadBytes != 256<<20 || cfg.MaxSpeechBytes != 32<<20 {
		t.Fatalf("unexpected live defaults: %#v", cfg)
	}
}

func TestLoadVolcengineProviderErrorNeverContainsCredential(t *testing.T) {
	credential := "sensitive-runtime-value"
	values := map[string]string{
		"ARK_API_KEY":               credential,
		"VIDEO_VOLCENGINE_BASE_URL": "file:///invalid",
	}
	_, err := loadVolcengineProvider(func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	if err == nil {
		t.Fatal("loadVolcengineProvider() error = nil")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("configuration error leaked credential: %v", err)
	}
}

func TestLoadVolcengineProviderRequiresExactSpeechRetryPair(t *testing.T) {
	tests := []struct {
		name      string
		jobID     string
		record    string
		wantError bool
	}{
		{name: "disabled"},
		{name: "exact pair", jobID: "speech-job-1", record: strings.Repeat("a", 64)},
		{name: "missing record", jobID: "speech-job-1", wantError: true},
		{name: "missing job", record: strings.Repeat("a", 64), wantError: true},
		{name: "non speech job", jobID: "video-job-1", record: strings.Repeat("a", 64), wantError: true},
		{name: "invalid record", jobID: "speech-job-1", record: strings.Repeat("A", 64), wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{
				"ARK_API_KEY":                              "test-runtime-credential",
				"VIDEO_PROVIDER_SERVICE_AUTH_SECRET":       "test-service-auth-secret-32-bytes-long",
				"VIDEO_ARTIFACT_ROOT":                      t.TempDir(),
				"VIDEO_VOLCENGINE_TTS_RETRY_JOB_ID":        tt.jobID,
				"VIDEO_VOLCENGINE_TTS_RETRY_RECORD_SHA256": tt.record,
			}
			cfg, err := loadVolcengineProvider(func(name string) (string, bool) {
				value, ok := values[name]
				return value, ok
			})
			if (err != nil) != tt.wantError {
				t.Fatalf("loadVolcengineProvider() error = %v, wantError = %v", err, tt.wantError)
			}
			if err == nil && (cfg.SpeechRetryJobID != tt.jobID || cfg.SpeechRetryRecord != tt.record) {
				t.Fatalf("retry config = %q/%q", cfg.SpeechRetryJobID, cfg.SpeechRetryRecord)
			}
		})
	}
}
