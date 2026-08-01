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
		cfg.SpeechEndpoint != AgentPlanTTSEndpoint ||
		cfg.SpeechModel != "doubao-seed-tts-2.0" || cfg.SpeechSpeaker != "zh_female_tianmeitaozi_mars_bigtts" ||
		cfg.APIKey != "test-runtime-credential" || cfg.ServiceAuthSecret != "test-service-auth-secret-32-bytes-long" ||
		cfg.MaxDownloadBytes != 256<<20 || cfg.MaxSpeechBytes != 32<<20 {
		t.Fatalf("unexpected live defaults: %#v", cfg)
	}
}

func TestLoadVolcengineProviderRejectsNonPlanSpeechEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "standard route", endpoint: "https://openspeech.bytedance.com/api/v3/tts/unidirectional"},
		{name: "query drift", endpoint: AgentPlanTTSEndpoint + "?billing=other"},
		{name: "trailing slash", endpoint: AgentPlanTTSEndpoint + "/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{
				"ARK_API_KEY":                        "test-runtime-credential",
				"VIDEO_PROVIDER_SERVICE_AUTH_SECRET": "test-service-auth-secret-32-bytes-long",
				"VIDEO_ARTIFACT_ROOT":                t.TempDir(),
				"VIDEO_VOLCENGINE_TTS_ENDPOINT":      tt.endpoint,
			}
			_, err := loadVolcengineProvider(func(name string) (string, bool) {
				value, ok := values[name]
				return value, ok
			})
			if err == nil || !strings.Contains(err.Error(), "exact Agent Plan subscription endpoint") {
				t.Fatalf("loadVolcengineProvider() error = %v", err)
			}
		})
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

func TestLoadVolcengineProviderRequiresCompleteZeroCashSpeechCanary(t *testing.T) {
	valid := map[string]string{
		"ARK_API_KEY":                                         "test-runtime-credential",
		"VIDEO_PROVIDER_SERVICE_AUTH_SECRET":                  "test-service-auth-secret-32-bytes-long",
		"VIDEO_ARTIFACT_ROOT":                                 t.TempDir(),
		"VIDEO_VOLCENGINE_TTS_CANARY_JOB_ID":                  "speech-v2-0123456789abcdef0123456789abcdef",
		"VIDEO_VOLCENGINE_TTS_CANARY_INPUT_SHA256":            strings.Repeat("1", 64),
		"VIDEO_VOLCENGINE_TTS_CANARY_CUE_ID":                  "cue-001",
		"VIDEO_VOLCENGINE_TTS_CANARY_VOICE_ASSET_ID":          "10400000-0000-4000-8000-00000000000f",
		"VIDEO_VOLCENGINE_TTS_CANARY_PARENT_VOICE_VERSION_ID": "10400000-0000-4000-8000-000000000010",
		"VIDEO_VOLCENGINE_TTS_CANARY_VOICE_VERSION_ID":        "10400000-0000-4000-8000-000000000011",
		"VIDEO_VOLCENGINE_TTS_CANARY_VOICE_SHA256":            strings.Repeat("2", 64),
		"VIDEO_VOLCENGINE_TTS_CANARY_LICENSE_SNAPSHOT_ID":     "10400000-0000-4000-8000-000000000012",
		"VIDEO_VOLCENGINE_TTS_CANARY_LICENSE_SHA256":          strings.Repeat("3", 64),
		"VIDEO_VOLCENGINE_TTS_CANARY_MAX_AFP_MILLI":           "2228",
		"VIDEO_VOLCENGINE_TTS_CANARY_MAX_CASH_MICROS":         "0",
	}
	load := func(values map[string]string) (VolcengineProvider, error) {
		return loadVolcengineProvider(func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		})
	}
	cfg, err := load(valid)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SpeechCanaryCueID != "cue-001" || cfg.SpeechCanaryMaximumAFPMilli != 2_228 ||
		cfg.SpeechCanaryMaximumCashMicros != 0 {
		t.Fatalf("speech canary config = %#v", cfg)
	}

	tests := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "missing frozen voice", mutate: func(values map[string]string) { delete(values, "VIDEO_VOLCENGINE_TTS_CANARY_VOICE_SHA256") }},
		{name: "cash allowed", mutate: func(values map[string]string) { values["VIDEO_VOLCENGINE_TTS_CANARY_MAX_CASH_MICROS"] = "1" }},
		{name: "invalid job", mutate: func(values map[string]string) { values["VIDEO_VOLCENGINE_TTS_CANARY_JOB_ID"] = "speech-old" }},
		{name: "canary limit without identity", mutate: func(values map[string]string) {
			for name := range values {
				if strings.HasPrefix(name, "VIDEO_VOLCENGINE_TTS_CANARY_") {
					delete(values, name)
				}
			}
			values["VIDEO_VOLCENGINE_TTS_CANARY_MAX_AFP_MILLI"] = "2228"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := make(map[string]string, len(valid))
			for name, value := range valid {
				values[name] = value
			}
			tt.mutate(values)
			if _, err := load(values); err == nil {
				t.Fatal("invalid speech canary config unexpectedly passed")
			}
		})
	}
}
