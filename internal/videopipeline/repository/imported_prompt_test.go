package repository

import (
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/google/uuid"
)

func TestImportedPromptHashBindsExactPackageAndPrompt(t *testing.T) {
	assets := []uuid.UUID{uuid.New(), uuid.New()}
	output := providercontract.OutputSpec{
		Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
		FPS: 24, DurationMillis: 5000, Format: "mp4",
	}
	inputs := map[string]string{"shot_spec": strings.Repeat("1", 64)}
	packageA := strings.Repeat("a", 64)
	first, err := ImportedPromptHash(
		uuid.NewString(), uuid.NewString(), strings.Repeat("2", 64), assets,
		"positive", "negative", output, inputs, packageA,
	)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := ImportedPromptHash(
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		strings.Repeat("2", 64), assets, "positive", "negative", output, inputs, packageA,
	)
	if err != nil {
		t.Fatal(err)
	}
	boundAgain, err := ImportedPromptHash(
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		strings.Repeat("2", 64), assets, "positive", "negative", output, inputs, packageA,
	)
	if err != nil {
		t.Fatal(err)
	}
	if repeat != boundAgain {
		t.Fatal("canonical imported prompt hash is not deterministic")
	}
	changedPackage, _ := ImportedPromptHash(
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		strings.Repeat("2", 64), assets, "positive", "negative", output, inputs,
		strings.Repeat("b", 64),
	)
	changedPrompt, _ := ImportedPromptHash(
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		strings.Repeat("2", 64), assets, "changed", "negative", output, inputs, packageA,
	)
	if first == "" || repeat == changedPackage || repeat == changedPrompt {
		t.Fatal("imported prompt digest did not bind package and executable prompt")
	}
}
