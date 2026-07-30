package runtimeconfig

import (
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
