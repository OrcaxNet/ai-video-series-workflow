package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestScanRepositoryCoversRootHiddenAndUntrackedFiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		path    string
		tracked bool
		content string
	}{
		{
			name:    "QA root credential probe",
			path:    "qa-secret-probe.txt",
			tracked: false,
			content: strings.Join([]string{"ARK_API_KEY", strings.Repeat("b", 20)}, "="),
		},
		{name: "tracked root file", path: "root-fixture.txt", tracked: true},
		{name: "tracked hidden directory", path: ".github/workflows/fixture.yml", tracked: true},
		{name: "future untracked file", path: "future/provider/fixture.txt", tracked: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			runGit(t, root, "init", "--quiet")
			safePath := filepath.Join(root, "README.md")
			if err := os.WriteFile(safePath, []byte("safe fixture\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runGit(t, root, "add", "README.md")
			fixturePath := filepath.Join(root, filepath.FromSlash(tt.path))
			if err := os.MkdirAll(filepath.Dir(fixturePath), 0o700); err != nil {
				t.Fatal(err)
			}
			synthetic := tt.content
			if synthetic == "" {
				synthetic = strings.Join([]string{"Bearer", strings.Repeat("x", 20)}, " ")
			}
			if err := os.WriteFile(fixturePath, []byte(synthetic), 0o600); err != nil {
				t.Fatal(err)
			}
			if tt.tracked {
				runGit(t, root, "add", tt.path)
			}
			if _, err := scanRepository(context.Background(), root); err == nil ||
				!strings.Contains(err.Error(), tt.path) {
				t.Fatalf("scanRepository() error = %v, want path %q", err, tt.path)
			}
		})
	}
}

func TestScanRepositoryPassesSafeInventory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("safe\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "README.md")
	result, err := scanRepository(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "passed" || len(result.Checks) != 1 {
		t.Fatalf("scan result = %#v", result)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
