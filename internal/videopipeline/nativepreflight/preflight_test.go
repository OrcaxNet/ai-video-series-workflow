package nativepreflight

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestContainsReferenceTextFailsClosed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{name: "none", value: map[string]any{"referencePrompt": nil}, want: false},
		{name: "text", value: map[string]any{"nested": []any{map[string]any{"text": "answer"}}}, want: true},
		{name: "prompt", value: map[string]any{"referencePrompt": "answer"}, want: true},
		{name: "subtitle", value: map[string]any{"subtitle": map[string]any{}}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := containsReferenceText(test.value); got != test.want {
				t.Fatalf("containsReferenceText() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestGitCommitRejectsTrackedDriftButAllowsUntrackedEvidence(t *testing.T) {
	root := t.TempDir()
	runGit := func(arguments ...string) {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", arguments, err, output)
		}
	}
	runGit("init", "--quiet")
	runGit("config", "user.name", "FLO-154 Test")
	runGit("config", "user.email", "flo154-test@example.invalid")
	tracked := filepath.Join(root, "source.go")
	if err := os.WriteFile(tracked, []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "source.go")
	runGit("commit", "--quiet", "-m", "fixture")

	commit, err := gitCommit(t.Context(), root)
	if err != nil || len(commit) != 40 {
		t.Fatalf("gitCommit(clean) = %q, %v", commit, err)
	}
	if err := os.WriteFile(filepath.Join(root, "evidence.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitCommit(t.Context(), root); err != nil {
		t.Fatalf("gitCommit(untracked evidence) error = %v", err)
	}
	if err := os.WriteFile(tracked, []byte("package changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitCommit(t.Context(), root); err == nil || !strings.Contains(err.Error(), "uncommitted tracked") {
		t.Fatalf("gitCommit(dirty) error = %v", err)
	}
}

func TestSealedAnalyzerProgramIsAbsoluteForIsolatedWorkdir(t *testing.T) {
	root := t.TempDir()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relativeRoot, err := filepath.Rel(workingDirectory, root)
	if err != nil {
		t.Fatal(err)
	}
	program, err := sealedAnalyzerProgram(relativeRoot, "bin/flo154-analyzer")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(program) || program != filepath.Join(root, "bin/flo154-analyzer") {
		t.Fatalf("sealedAnalyzerProgram() = %q", program)
	}
}
