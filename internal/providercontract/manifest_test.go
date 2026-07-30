package providercontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGenerationManifestWritesImmutableSanitizedEvidence(t *testing.T) {
	t.Parallel()
	request := testGenerationRequest()
	request.Prompt = "sensitive prompt must not persist"
	started := time.Unix(1_800_000_000, 0).UTC()
	job := Job{
		ID:                "provider-job-1",
		RequestID:         request.RequestID,
		IdempotencyKey:    request.IdempotencyKey,
		Status:            StatusSucceeded,
		Provider:          "volcengine_ark",
		ProviderModel:     "runtime-model-version",
		ProviderRegion:    "cn-beijing",
		ProviderRequestID: "provider-request-1",
		CreatedAt:         started,
		UpdatedAt:         started.Add(30 * time.Second),
		Output: &Output{
			Assets: []AssetRef{{
				ID:               "output-video",
				Revision:         "rev-1",
				Kind:             ModalityVideo,
				Role:             AssetRoleOutput,
				URI:              "https://signed.example.invalid/private-output",
				SHA256:           strings.Repeat("a", 64),
				LicenseReference: "fixture-license",
			}},
			Actual: OutputSpec{
				Resolution:     "720p",
				AspectRatio:    "16:9",
				FPS:            24,
				DurationMillis: 5_000,
				Format:         "mp4",
			},
			Usage: Usage{VideoTokens: 250_000, ProviderCostMicros: 7_000_000},
		},
	}
	manifest, err := NewGenerationManifest(ManifestBuildInput{
		ManifestID:  "manifest-character-dialogue-01-attempt-1",
		ShotID:      "character-dialogue-01",
		Evidence:    EvidenceLiveProvider,
		Request:     request,
		Job:         job,
		Attempt:     1,
		StartedAt:   started,
		CompletedAt: started.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "evidence", "manifest.json")
	hash, err := WriteGenerationManifest(path, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(hash) != 64 {
		t.Fatalf("manifest hash = %q", hash)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{request.Prompt, request.Assets[0].URI, job.Output.Assets[0].URI} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("manifest persisted forbidden runtime value %q", forbidden)
		}
	}
	var decoded GenerationManifest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Actual == nil || decoded.Actual.Resolution != "720p" ||
		decoded.Provider.Region != "cn-beijing" {
		t.Fatalf("decoded manifest = %#v", decoded)
	}
	if _, err := WriteGenerationManifest(path, manifest); ErrorCodeOf(err) != CodeConflict {
		t.Fatalf("second write error = %v, want conflict", err)
	}
}

func TestGenerationManifestRejectsPendingOutputHash(t *testing.T) {
	t.Parallel()
	asset := AssetEvidence{
		ID:               "output",
		Revision:         "rev-1",
		Kind:             ModalityVideo,
		Role:             AssetRoleOutput,
		SHA256:           "pending_download",
		LicenseReference: "fixture-license",
	}
	if err := asset.Validate(); err == nil {
		t.Fatal("pending output hash was accepted")
	}
}

func TestCommittedGenerationManifestSchema(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../docs/flo-110/generation-manifest.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("manifest schema is not valid JSON: %v", err)
	}
	if strings.Contains(string(data), `"uri"`) {
		t.Fatal("manifest schema must not persist runtime URIs")
	}
	if strings.Contains(string(data), `"temperature"`) {
		t.Fatal("manifest schema must not persist caller-supplied temperature")
	}
}
