package stage1

import (
	"strings"
	"testing"
)

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

func TestControlledRetryPackageBindsNewIdentityApprovalAndFinalizationRun(t *testing.T) {
	t.Parallel()
	primary := testExecutionPackage(t)
	retry := testControlledRetryPackage(t, primary)
	if err := retry.Validate(testPlan(), primary); err != nil {
		t.Fatal(err)
	}
	if retry.Job.Run.RunID == primary.PrimaryJobs[0].Run.RunID ||
		retry.Job.AttemptID == primary.PrimaryJobs[0].AttemptID ||
		retry.Job.IdempotencyKey == primary.PrimaryJobs[0].IdempotencyKey ||
		retry.PostProduction.RunIDs[0] != retry.Job.Run.RunID {
		t.Fatalf("controlled retry package = %#v", retry)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ControlledRetryPackage)
	}{
		{name: "parent package", mutate: func(p *ControlledRetryPackage) { p.ParentExecutionPackageHash = strings.Repeat("0", 64) }},
		{name: "original attempt", mutate: func(p *ControlledRetryPackage) { p.Approval.OriginalAttemptID = "other" }},
		{name: "failure class", mutate: func(p *ControlledRetryPackage) { p.Approval.FailureClass = "" }},
		{name: "same run", mutate: func(p *ControlledRetryPackage) {
			p.Job.Run = primary.PrimaryJobs[0].Run
			p.Job.IdempotencyKey = primary.PrimaryJobs[0].IdempotencyKey
		}},
		{name: "finalization old run", mutate: func(p *ControlledRetryPackage) { p.PostProduction.RunIDs[0] = primary.PrimaryJobs[0].Run.RunID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := retry
			candidate.PostProduction.RunIDs = append([]string(nil), retry.PostProduction.RunIDs...)
			test.mutate(&candidate)
			candidate, err := SealControlledRetryPackage(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := candidate.Validate(testPlan(), primary); err == nil {
				t.Fatal("drifted controlled retry package was accepted")
			}
		})
	}
}
