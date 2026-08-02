package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRejectsInvalidInvocationBeforeDatabase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{name: "empty"},
		{name: "unknown", args: []string{"submit"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := run(t.Context(), test.args, &bytes.Buffer{}, func(string) (string, bool) { return "", false })
			if err == nil || !strings.Contains(err.Error(), "usage:") {
				t.Fatalf("error=%v, want usage", err)
			}
		})
	}
}

func TestRunRequiresPostgresForValidOperation(t *testing.T) {
	t.Parallel()
	err := run(t.Context(), []string{"materialize", "activation", "package", "projection", "2026-08-03T00:00:00Z"},
		&bytes.Buffer{}, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "VIDEO_POSTGRES_DSN") {
		t.Fatalf("error=%v, want missing PostgreSQL DSN", err)
	}
}
