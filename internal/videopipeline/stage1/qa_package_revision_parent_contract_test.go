package stage1

import (
	"path/filepath"
	"strings"
	"testing"
)

// QA-STAGE1-PACKAGE-REVISION-ERROR-07: every unverifiable revision parent
// must use the same non-retryable forbidden contract before changing ledger
// state. A well-formed child that names another parent is still untrusted.
func TestQASpeechV2PackageRevisionWrongParentHashIsForbidden(t *testing.T) {
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
	child.ParentExecutionPackageHash = strings.Repeat("a", 64)
	child, err := SealExecutionPackage(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Validate(testPlan()); err != nil {
		t.Fatalf("wrong-parent probe must remain a well-formed standalone child: %v", err)
	}

	err = gate.BindExecutionPackageRevision(child, parent)
	assertForbiddenRevisionParent(t, err)
	ledger, snapshotErr := gate.Snapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if ledger.ExecutionPackageHash != parent.ContentHash || ledger.SupersededExecutionPackageHash != "" {
		t.Fatalf("wrong parent changed ledger: %#v", ledger)
	}
}
