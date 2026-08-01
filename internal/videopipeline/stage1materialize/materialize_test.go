package stage1materialize

import (
	"fmt"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/google/uuid"
)

func TestOutputInputProviderSpecPreservesProductFields(t *testing.T) {
	got := (outputInput{
		Width: 1280, Height: 720, AspectRatio: "16:9", FPS: 24,
		DurationMillis: 5000, Format: "mp4", GenerateAudio: false,
	}).providerSpec()
	want := providercontract.OutputSpec{
		Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
		FPS: 24, DurationMillis: 5000, Format: "mp4", GenerateAudio: false,
	}
	if got != want {
		t.Fatalf("provider output mismatch: got %#v want %#v", got, want)
	}
}

func TestValidateIDsRejectsDuplicateReservedIdentity(t *testing.T) {
	product := productWithUniqueIDs()
	if err := validateIDs(product); err != nil {
		t.Fatalf("valid fixed identity set rejected: %v", err)
	}
	product.Shots[9].RunID = product.Shots[0].RunID
	if err := validateIDs(product); err == nil {
		t.Fatal("duplicate frozen run identity was accepted")
	}
}

func TestModelRouteKeepsPendingKeyEvidence(t *testing.T) {
	route := modelRoute(routeInput{
		CapabilityAlias: "video.primary", Provider: "volcengine_ark",
		ModelID: "doubao-seedance-2.0", RouteVersion: "fixed-v1",
		Verification: providercontract.PendingKey,
	}, fmt.Sprintf("%064x", 1))
	if route.Provider != "VOLCENGINE" || route.Verification != providercontract.PendingKey {
		t.Fatalf("route evidence boundary changed: %#v", route)
	}
}

func productWithUniqueIDs() productInput {
	next := func() string { return uuid.New().String() }
	product := productInput{Reserved: reservedIDs{
		SeriesID: next(), SourceRevisionID: next(), EpisodeID: next(), EpisodeRevisionID: next(),
		SceneID: next(), SceneRevisionID: next(), ScriptRevisionID: next(), StoryboardRevisionID: next(),
		GenerationProfileID: next(), GenerationProfileRevisionID: next(), GenerationPlanID: next(),
		VisualAssetID: next(), VisualAssetVersionID: next(), VisualLicenseSnapshotID: next(),
		VoiceAssetID: next(), VoiceAssetVersionID: next(), VoiceLicenseSnapshotID: next(),
		SafetyEvidenceArtifactID: next(), G1DecisionID: next(), G2DecisionID: next(), SafetyDecisionID: next(),
		VideoBudgetApprovalID: next(), SpeechBudgetApprovalID: next(),
		VideoProviderProfileID: next(), SpeechProviderProfileID: next(),
		SeriesContextRevisionID: next(), EpisodeContextRevisionID: next(), SceneContextRevisionID: next(),
	}}
	for index := 0; index < 10; index++ {
		product.Shots = append(product.Shots, shotInput{
			DBShotID: next(), ShotSpecRevisionID: next(), PromptSnapshotID: next(),
			EffectiveContextSnapshotID: next(), RunID: next(),
		})
	}
	return product
}
