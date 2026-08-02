package stage1materialize

import (
	"os"
	"testing"
	"time"
)

func TestPrepareFLO100FormalPack(t *testing.T) {
	root := os.Getenv("VIDEO_TEST_FLO100_PACK_PATH")
	if root == "" {
		t.Skip("VIDEO_TEST_FLO100_PACK_PATH is not configured")
	}
	validUntil := time.Date(2026, time.August, 31, 15, 59, 59, 0, time.UTC)
	prepared, err := prepareFormal(FormalOptions{
		Root: root, ExpectedPackageHash: FormalExpectedPackageHash(),
		Approval: Approval{
			CommentID:  "5b92b347-3ce9-4e7b-831a-1f00d1454d78",
			ActorID:    "16bbc49e-750f-432d-9ba4-b33ef6812026",
			ValidUntil: validUntil,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.batches) != 3 || len(prepared.assets.Versions) != 8 {
		t.Fatalf("prepared batches/assets = %d/%d, want 3/8", len(prepared.batches), len(prepared.assets.Versions))
	}
	var shots, keys int
	for _, batch := range prepared.batches {
		shots += len(batch.product.Shots)
		keys += len(batch.plan.Idempotency.Keys)
		if err := batch.readiness.Validate(); err != nil {
			t.Fatalf("%s readiness: %v", batch.product.BatchID, err)
		}
	}
	if shots != 30 || keys != 30 {
		t.Fatalf("prepared shots/keys = %d/%d, want 30/30", shots, keys)
	}
}

func TestPrepareFLO100RequiresIndependentPackageHash(t *testing.T) {
	_, err := prepareFormal(FormalOptions{ExpectedPackageHash: "not-pinned"})
	if err == nil {
		t.Fatal("untrusted package hash was accepted")
	}
}
