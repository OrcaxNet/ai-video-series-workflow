package analyzerseal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerify(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Manifest)
		wantErr string
	}{
		{name: "valid production seal"},
		{
			name: "path traversal", wantErr: "escapes its root",
			mutate: func(manifest *Manifest) { manifest.Components[0].Path = "../outside" },
		},
		{
			name: "noncommercial component", wantErr: "not production-licensed",
			mutate: func(manifest *Manifest) { manifest.Components[0].SPDXLicense = "CC-BY-NC-4.0" },
		},
		{
			name: "component hash drift", wantErr: "SHA-256 drifted",
			mutate: func(manifest *Manifest) { manifest.Components[0].SHA256 = strings.Repeat("f", 64) },
		},
		{
			name: "missing AV sync", wantErr: "missing required component kind",
			mutate: func(manifest *Manifest) {
				for index, component := range manifest.Components {
					if component.Kind == "av_sync" {
						manifest.Components = append(manifest.Components[:index], manifest.Components[index+1:]...)
						return
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			manifest := writeSealFixture(t, root)
			if test.mutate != nil {
				test.mutate(&manifest)
			}
			manifestPath := filepath.Join(root, "seal.json")
			writeJSON(t, manifestPath, manifest)
			_, evidence, err := Verify(root, manifestPath)
			if test.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				if !validDigest(evidence.SealSHA256) ||
					evidence.ExecutableSHA256 != manifest.Analyzer.SHA256 ||
					len(evidence.Components) != len(requiredKinds) {
					t.Fatalf("evidence = %#v", evidence)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Verify() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func writeSealFixture(t *testing.T, root string) Manifest {
	t.Helper()
	write := func(name, contents string, mode os.FileMode) Artifact {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(contents), mode); err != nil {
			t.Fatal(err)
		}
		return Artifact{Path: name, SHA256: testDigest([]byte(contents)), Version: "fixture-v1"}
	}
	analyzer := write("analyzer", "#!/bin/sh\nexit 0\n", 0o750)
	analyzer.Executable = true
	config := write("config.json", "{}\n", 0o640)
	manifest := Manifest{
		SchemaVersion: SchemaVersion, Analyzer: analyzer, Config: config,
		Offline: Offline{
			Network: "disabled", CommandSchema: "flo154.audio-analyzer-command.v1",
		},
	}
	for _, kind := range requiredKinds {
		artifact := write(kind+".bin", kind+" fixture\n", 0o640)
		manifest.Components = append(manifest.Components, Component{
			Name: kind, Kind: kind, Path: artifact.Path, SHA256: artifact.SHA256,
			Version: "fixture-v1", SPDXLicense: "MIT", CommercialUse: true,
			Source: "https://example.invalid/" + kind,
		})
	}
	return manifest
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
}

func testDigest(data []byte) string {
	value := sha256.Sum256(data)
	return hex.EncodeToString(value[:])
}
