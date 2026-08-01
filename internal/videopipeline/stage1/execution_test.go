package stage1

import "testing"

func TestExecutionPackageFreezesTenRunsAndCompletePostProduction(t *testing.T) {
	t.Parallel()
	package_ := testExecutionPackage(t)
	if err := package_.Validate(testPlan()); err != nil {
		t.Fatal(err)
	}
	if len(package_.PrimaryJobs) != RequiredPrimaryJobs ||
		len(package_.PostProduction.RunIDs) != RequiredPrimaryJobs {
		t.Fatalf("frozen package = %#v", package_)
	}
	for index, job := range package_.PrimaryJobs {
		if package_.PostProduction.RunIDs[index] != job.Run.RunID ||
			job.PromptSnapshotID == "" || job.PromptSnapshotHash == "" ||
			job.BudgetApprovalID == "" || job.ShotSpecRevisionID == "" {
			t.Fatalf("frozen job %d is incomplete: %#v", index+1, job)
		}
	}
}

func TestExecutionPackageRejectsAnyFrozenIdentityOrPostProductionDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ExecutionPackage)
	}{
		{name: "run", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].Run.RunSpecDigest = p.PrimaryJobs[0].Run.RunSpecDigest[:63] + "f"
		}},
		{name: "prompt", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].PromptSnapshotHash = p.PrimaryJobs[0].PromptSnapshotHash[:63] + "f"
		}},
		{name: "budget", mutate: func(p *ExecutionPackage) { p.PrimaryJobs[0].BudgetMaximumMicros++ }},
		{name: "post-production order", mutate: func(p *ExecutionPackage) {
			p.PostProduction.RunIDs[0], p.PostProduction.RunIDs[1] = p.PostProduction.RunIDs[1], p.PostProduction.RunIDs[0]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			package_ := testExecutionPackage(t)
			test.mutate(&package_)
			if err := package_.Validate(testPlan()); err == nil {
				t.Fatal("tampered execution package was accepted")
			}
		})
	}
}
