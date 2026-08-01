package stage1

import (
	"strings"
	"testing"

	"github.com/google/uuid"
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

func TestExecutionPackageRevisionRequiresSpeechV2AndDistinctParent(t *testing.T) {
	t.Parallel()
	parent := testExecutionPackage(t)
	valid := testSpeechV2ExecutionPackage(t, parent)
	if valid.ParentExecutionPackageHash != parent.ContentHash ||
		valid.ContentHash == parent.ContentHash {
		t.Fatalf("speech-v2 package revision = %#v", valid)
	}

	tests := []struct {
		name   string
		mutate func(*ExecutionPackage)
	}{
		{name: "missing parent", mutate: func(p *ExecutionPackage) { p.ParentExecutionPackageHash = "" }},
		{name: "invalid parent", mutate: func(p *ExecutionPackage) { p.ParentExecutionPackageHash = "invalid" }},
		{name: "legacy speech", mutate: func(p *ExecutionPackage) {
			p.PostProduction.Config.SpeechIdentityVersion = ""
			p.PostProduction.Config.SpeechVoice = nil
			p.PostProduction.Config.SpeechAuthorizedCueID = ""
			p.PostProduction.Config.SpeechMaximumAFPMilli = 0
			p.PostProduction.Config.SpeechMaxAttempts = 0
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			candidate, err := SealExecutionPackage(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := candidate.Validate(testPlan()); err == nil {
				t.Fatal("invalid execution package revision unexpectedly passed")
			}
		})
	}
}

func TestSpeechV2RevisionRejectsEveryFrozenNonSpeechProjection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ExecutionPackage)
	}{
		{name: "shot revision", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].ShotSpecRevisionID = uuid.NewString()
		}},
		{name: "run spec", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].Run.RunSpecDigest = strings.Repeat("a", 64)
		}},
		{name: "prompt snapshot", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].PromptSnapshotHash = strings.Repeat("a", 64)
		}},
		{name: "generation plan", mutate: func(p *ExecutionPackage) {
			planID := uuid.NewString()
			for index := range p.PrimaryJobs {
				p.PrimaryJobs[index].GenerationPlanID = planID
			}
			p.PostProduction.GenerationPlanID = planID
		}},
		{name: "video budget approval", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].BudgetApprovalID = uuid.NewString()
		}},
		{name: "video budget ceiling", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].BudgetMaximumMicros++
		}},
		{name: "video provider profile", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].ProviderProfileID = uuid.NewString()
		}},
		{name: "video route", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].Route.CapabilityHash = strings.Repeat("a", 64)
		}},
		{name: "video workflow identity", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].WorkflowID = "other-stage1-workflow"
		}},
		{name: "episode revision", mutate: func(p *ExecutionPackage) {
			p.PostProduction.EpisodeRevisionID = uuid.NewString()
		}},
		{name: "run order", mutate: func(p *ExecutionPackage) {
			p.PrimaryJobs[0], p.PrimaryJobs[1] = p.PrimaryJobs[1], p.PrimaryJobs[0]
			p.PostProduction.RunIDs[0], p.PostProduction.RunIDs[1] =
				p.PostProduction.RunIDs[1], p.PostProduction.RunIDs[0]
		}},
		{name: "base speech budget approval", mutate: func(p *ExecutionPackage) {
			p.PostProduction.Config.SpeechBudgetApprovalID = uuid.NewString()
		}},
		{name: "base speech budget ceiling", mutate: func(p *ExecutionPackage) {
			p.PostProduction.Config.SpeechBudgetMaximumMicros++
		}},
		{name: "subtitle language", mutate: func(p *ExecutionPackage) {
			p.PostProduction.Config.SubtitleLanguage = "en-US"
		}},
		{name: "background audio", mutate: func(p *ExecutionPackage) {
			p.PostProduction.Config.BackgroundAudioAssetVersionID = uuid.NewString()
		}},
		{name: "finalization trace", mutate: func(p *ExecutionPackage) {
			p.PostProduction.TraceID = "other-speech-v2-trace"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parent := testExecutionPackage(t)
			child := testSpeechV2ExecutionPackage(t, parent)
			test.mutate(&child)
			child, err := SealExecutionPackage(child)
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Validate(testPlan()); err != nil {
				t.Fatalf("drift probe must remain a well-formed standalone package: %v", err)
			}
			if err := child.ValidateSpeechV2Revision(testPlan(), parent); err == nil {
				t.Fatal("non-speech drift was accepted by the canonical parent projection")
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
