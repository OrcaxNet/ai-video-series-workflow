// Package analyzerseal verifies the immutable, production-licensed local
// analyzer installation used by FLO-154. It never downloads components or
// opens a network connection.
package analyzerseal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const SchemaVersion = "flo154.analyzer-seal.v1"

var requiredKinds = []string{
	"asr_model", "tokenizer", "normalizer", "vad", "face_mouth",
	"av_sync", "ffmpeg", "ffprobe", "license_snapshot",
}

var productionLicenses = map[string]struct{}{
	"Apache-2.0": {}, "BSD-2-Clause": {}, "BSD-3-Clause": {},
	"GPL-3.0-or-later": {}, "ISC": {}, "LGPL-2.1-or-later": {}, "MIT": {},
	"MPL-2.0": {}, "LicenseRef-Project-Internal": {},
}

type Manifest struct {
	SchemaVersion string      `json:"schemaVersion"`
	Analyzer      Artifact    `json:"analyzer"`
	Config        Artifact    `json:"config"`
	Components    []Component `json:"components"`
	Offline       Offline     `json:"offline"`
}

type Artifact struct {
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	Version    string `json:"version"`
	Executable bool   `json:"executable,omitempty"`
}

type Component struct {
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	Path          string `json:"path"`
	SHA256        string `json:"sha256"`
	Version       string `json:"version"`
	SPDXLicense   string `json:"spdxLicense"`
	CommercialUse bool   `json:"commercialUse"`
	Source        string `json:"source"`
}

type Offline struct {
	Network               string `json:"network"`
	CommandSchema         string `json:"commandSchema"`
	ReferenceTextProvided bool   `json:"referenceTextProvided"`
}

type Evidence struct {
	SealSHA256       string            `json:"sealSha256"`
	ExecutableSHA256 string            `json:"executableSha256"`
	ConfigSHA256     string            `json:"configSha256"`
	Components       map[string]string `json:"components"`
}

func Verify(root, manifestPath string) (Manifest, Evidence, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Manifest{}, Evidence{}, err
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return Manifest{}, Evidence{}, fmt.Errorf("read analyzer seal: %w", err)
	}
	if len(manifestBytes) == 0 || len(manifestBytes) > 1<<20 {
		return Manifest{}, Evidence{}, errors.New("analyzer seal size is invalid")
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, Evidence{}, fmt.Errorf("decode analyzer seal: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Manifest{}, Evidence{}, errors.New("analyzer seal must contain exactly one JSON value")
	}
	if manifest.SchemaVersion != SchemaVersion {
		return Manifest{}, Evidence{}, fmt.Errorf("analyzer seal schemaVersion must be %s", SchemaVersion)
	}
	if manifest.Offline.Network != "disabled" ||
		manifest.Offline.CommandSchema != "flo154.audio-analyzer-command.v1" ||
		manifest.Offline.ReferenceTextProvided {
		return Manifest{}, Evidence{}, errors.New("analyzer seal must freeze no-network execution without reference text")
	}
	if err := verifyArtifact(root, manifest.Analyzer, true); err != nil {
		return Manifest{}, Evidence{}, fmt.Errorf("analyzer executable: %w", err)
	}
	if err := verifyArtifact(root, manifest.Config, false); err != nil {
		return Manifest{}, Evidence{}, fmt.Errorf("analyzer config: %w", err)
	}
	components := make(map[string]string, len(manifest.Components))
	kinds := make(map[string]struct{}, len(manifest.Components))
	for index, component := range manifest.Components {
		if strings.TrimSpace(component.Name) == "" || strings.TrimSpace(component.Kind) == "" ||
			strings.TrimSpace(component.Version) == "" || strings.TrimSpace(component.Source) == "" {
			return Manifest{}, Evidence{}, fmt.Errorf("analyzer component %d identity is incomplete", index)
		}
		if _, duplicate := components[component.Name]; duplicate {
			return Manifest{}, Evidence{}, fmt.Errorf("duplicate analyzer component %q", component.Name)
		}
		if _, allowed := productionLicenses[component.SPDXLicense]; !allowed || !component.CommercialUse {
			return Manifest{}, Evidence{}, fmt.Errorf("analyzer component %q is not production-licensed", component.Name)
		}
		if err := verifyArtifact(root, Artifact{
			Path: component.Path, SHA256: component.SHA256, Version: component.Version,
		}, false); err != nil {
			return Manifest{}, Evidence{}, fmt.Errorf("analyzer component %q: %w", component.Name, err)
		}
		components[component.Name] = component.SHA256
		kinds[component.Kind] = struct{}{}
	}
	for _, kind := range requiredKinds {
		if _, ok := kinds[kind]; !ok {
			return Manifest{}, Evidence{}, fmt.Errorf("analyzer seal is missing required component kind %q", kind)
		}
	}
	sealDigest := digest(manifestBytes)
	return manifest, Evidence{
		SealSHA256: sealDigest, ExecutableSHA256: manifest.Analyzer.SHA256,
		ConfigSHA256: manifest.Config.SHA256, Components: componentsByKind(manifest.Components),
	}, nil
}

func verifyArtifact(root string, artifact Artifact, executable bool) error {
	if strings.TrimSpace(artifact.Version) == "" || !validDigest(artifact.SHA256) {
		return errors.New("version or SHA-256 is invalid")
	}
	path, err := resolve(root, artifact.Path)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("sealed artifact must be a regular non-symlink file")
	}
	if executable && info.Mode().Perm()&0o111 == 0 {
		return errors.New("sealed analyzer is not executable")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, 4<<30)); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
		return errors.New("sealed artifact SHA-256 drifted")
	}
	return nil
}

func resolve(root, relative string) (string, error) {
	if strings.TrimSpace(relative) == "" || filepath.IsAbs(relative) {
		return "", errors.New("sealed artifact path must be relative")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("sealed artifact path escapes its root")
	}
	path := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("sealed artifact path escapes its root")
	}
	return path, nil
}

func componentsByKind(components []Component) map[string]string {
	grouped := make(map[string][]string)
	for _, component := range components {
		grouped[component.Kind] = append(grouped[component.Kind], component.SHA256)
	}
	result := make(map[string]string, len(grouped))
	for kind, values := range grouped {
		sort.Strings(values)
		result[kind] = digest([]byte(strings.Join(values, "\x00")))
	}
	return result
}

func digest(data []byte) string {
	value := sha256.Sum256(data)
	return hex.EncodeToString(value[:])
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
