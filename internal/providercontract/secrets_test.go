package providercontract

import (
	"strings"
	"testing"
)

func TestRedact(t *testing.T) {
	t.Parallel()
	literal := strings.Join([]string{"runtime", "credential", strings.Repeat("z", 20)}, "-")
	input := "upstream returned Authorization: Bearer " + strings.Repeat("x", 20) +
		" and " + literal
	got := Redact(input, literal)
	if strings.Contains(got, literal) || strings.Contains(got, strings.Repeat("x", 20)) {
		t.Fatalf("Redact() leaked credential: %q", got)
	}
	if !strings.Contains(got, RedactionMarker) {
		t.Fatalf("Redact() = %q, want marker", got)
	}
}

func TestContainsPotentialSecret(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		text string
		want bool
	}{
		{name: "runtime reference allowed", text: `api_key=os.Getenv("ARK_API_KEY")`, want: false},
		{name: "shell reference allowed", text: `Authorization: Bearer $ARK_API_KEY`, want: false},
		{name: "literal bearer rejected", text: "Bearer " + strings.Repeat("a", 20), want: true},
		{name: "literal key rejected", text: "ARK_API_KEY=" + strings.Repeat("b", 20), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ContainsPotentialSecret(tt.text); got != tt.want {
				t.Fatalf("ContainsPotentialSecret(%q) = %t, want %t", tt.text, got, tt.want)
			}
		})
	}
}
