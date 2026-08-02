package stage1

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/google/uuid"
)

// QA-STAGE1-PACKAGE-REVISION-06: a speech-v2 child may change only the
// approved speech revision. Naming the parent hash must not authorize changes
// to the already-executed video lineage or the finalization target.
func TestQASpeechV2PackageRevisionRejectsNonSpeechDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ExecutionPackage)
	}{
		{
			name: "primary prompt lineage",
			mutate: func(p *ExecutionPackage) {
				p.PrimaryJobs[0].PromptSnapshotHash = strings.Repeat("a", 64)
			},
		},
		{
			name: "episode finalization target",
			mutate: func(p *ExecutionPackage) {
				p.PostProduction.EpisodeRevisionID = uuid.NewString()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := testExecutionPackage(t)
			gate := completePrimaryPackage(
				t,
				filepath.Join(t.TempDir(), "ledger.json"),
				parent,
				RequiredPrimaryJobs,
				"TERMINAL_SUCCEEDED",
				true,
			)
			child := testSpeechV2ExecutionPackage(t, parent)
			tt.mutate(&child)
			child, err := SealExecutionPackage(child)
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Validate(testPlan()); err != nil {
				t.Fatalf("drift probe must remain otherwise well formed: %v", err)
			}

			if err := gate.BindExecutionPackageRevision(child, parent); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
				t.Fatalf("non-speech drift error = %v", err)
			}
			ledger, snapshotErr := gate.Snapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if ledger.ExecutionPackageHash != parent.ContentHash || ledger.SupersededExecutionPackageHash != "" {
				t.Fatalf(
					"non-speech drift changed ledger: parent=%s child=%s superseded=%s",
					parent.ContentHash,
					ledger.ExecutionPackageHash,
					ledger.SupersededExecutionPackageHash,
				)
			}
		})
	}
}
