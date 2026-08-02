package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestPreparedProductTruthEqualComparesDurationPricingByValue(t *testing.T) {
	pricing := DurationPricingBinding{DurationMS: 4_500, ExpectedAFPMilli: 2_254_230,
		PricingSnapshotID: "agent-plan-large-v1", NormalizationVersion: "duration-normalized-afp/v1"}
	base := PreparedProductTruth{GenerationPlanID: "plan-1", DurationPricing: &pricing}
	samePricing := pricing
	differentPointer := base
	differentPointer.DurationPricing = &samePricing
	differentValue := base
	driftedPricing := pricing
	driftedPricing.ExpectedAFPMilli++
	differentValue.DurationPricing = &driftedPricing
	differentScalar := base
	differentScalar.GenerationPlanID = "plan-2"

	for _, test := range []struct {
		name  string
		other PreparedProductTruth
		want  bool
	}{
		{name: "same pointer", other: base, want: true},
		{name: "same pricing value at another address", other: differentPointer, want: true},
		{name: "pricing value drift", other: differentValue, want: false},
		{name: "nil pricing mismatch", other: PreparedProductTruth{GenerationPlanID: "plan-1"}, want: false},
		{name: "scalar drift", other: differentScalar, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := base.Equal(test.other); got != test.want {
				t.Fatalf("Equal()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestEpisodeProductionWorkflow_LocksAfterG3(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerHappyPathActivities(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Gate3DecisionSignal, Gate3Decision{
			DecisionID: "decision-g3-1",
			Approved:   true,
			ActorID:    "reviewer-1",
		})
	}, time.Second)

	input := EpisodeProductionInput{
		SchemaVersion:        "v1",
		SeriesID:             "series-1",
		EpisodeRevisionID:    "episode-revision-1",
		ShotSpecRevisionIDs:  []string{"shot-revision-1", "shot-revision-2"},
		GenerationProfileRef: "profile-revision-1",
		Gate2DecisionID:      "decision-g2-1",
		ProviderRoute:        testProviderRoute(),
		BudgetApprovalID:     "budget-approval-1",
		BudgetMaximumMicros:  500,
		BudgetCurrency:       "CNY",
		TraceID:              "trace-1",
	}
	env.ExecuteWorkflow(EpisodeProductionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result EpisodeProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	if result.State != "LOCKED" || len(result.LockedRunIDs) != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestEpisodeProductionWorkflow_RejectsDuplicateShot(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(EpisodeProductionWorkflow, EpisodeProductionInput{
		SchemaVersion:        "v1",
		SeriesID:             "series-1",
		EpisodeRevisionID:    "episode-1",
		ShotSpecRevisionIDs:  []string{"shot-1", "shot-1"},
		GenerationProfileRef: "profile-1",
		Gate2DecisionID:      "decision-1",
		ProviderRoute:        testProviderRoute(),
		BudgetApprovalID:     "budget-1",
		BudgetMaximumMicros:  500,
		BudgetCurrency:       "CNY",
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want validation error")
	}
}

func TestEpisodeProductionWorkflow_WaitsForExactQ1Decision(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerHappyPathActivities(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID:         "decision-q1-wrong",
			ShotSpecRevisionID: "another-shot",
			RunID:              "run-shot-revision-1-1",
			Approved:           true,
			ActorID:            "reviewer-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID:         "decision-q1-1",
			ShotSpecRevisionID: "shot-revision-1",
			RunID:              "run-shot-revision-1-1",
			Approved:           true,
			ActorID:            "reviewer-1",
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Gate3DecisionSignal, Gate3Decision{
			DecisionID: "decision-g3-1",
			Approved:   true,
			ActorID:    "producer-1",
		})
	}, 3*time.Second)

	input := EpisodeProductionInput{
		SchemaVersion:        "v1",
		SeriesID:             "series-1",
		EpisodeRevisionID:    "episode-revision-1",
		ShotSpecRevisionIDs:  []string{"shot-revision-1"},
		GenerationProfileRef: "profile-revision-1",
		Gate2DecisionID:      "decision-g2-1",
		ProviderRoute:        testProviderRoute(),
		BudgetApprovalID:     "budget-approval-1",
		BudgetMaximumMicros:  500,
		BudgetCurrency:       "CNY",
		TraceID:              "trace-q1",
		RequireShotApproval:  true,
	}
	env.ExecuteWorkflow(EpisodeProductionWorkflow, input)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result EpisodeProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "LOCKED" || result.Shots["shot-revision-1"].State != "APPROVED" {
		t.Fatalf("result = %#v", result)
	}
}

func TestEpisodeProductionWorkflow_Q1RejectionDoesNotApproveRejectedRuns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerHappyPathActivities(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID: "decision-q1-1", ShotSpecRevisionID: "shot-revision-1",
			RunID: "run-shot-revision-1-1", Approved: false, ReasonCode: "CONTINUITY", ActorID: "reviewer-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID: "decision-q1-2", ShotSpecRevisionID: "shot-revision-1",
			RunID: "run-shot-revision-1-2", Approved: false, ReasonCode: "CONTINUITY", ActorID: "reviewer-1",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(EpisodeProductionWorkflow, EpisodeProductionInput{
		SchemaVersion: "v1", SeriesID: "series-1", EpisodeRevisionID: "episode-revision-1",
		ShotSpecRevisionIDs: []string{"shot-revision-1"}, GenerationProfileRef: "profile-revision-1",
		Gate2DecisionID: "decision-g2-1", ProviderRoute: testProviderRoute(),
		BudgetApprovalID: "budget-approval-1", BudgetMaximumMicros: 500, BudgetCurrency: "CNY",
		TraceID: "trace-q1-reject", RequireShotApproval: true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result EpisodeProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "NEEDS_INTERVENTION" || len(result.LockedRunIDs) != 0 {
		t.Fatalf("rejected versions polluted approved baseline: %#v", result)
	}
	if state := result.Shots["shot-revision-1"]; state.State != "Q1_REJECTED" || state.FailureCode != "CONTINUITY" {
		t.Fatalf("shot state = %#v", state)
	}
}

func TestEpisodeProductionWorkflow_FinalizesBeforeGate3(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerHappyPathActivities(env)
	manifestHash := strings.Repeat("a", 64)
	env.RegisterActivityWithOptions(
		func(_ context.Context, input FinalizeEpisodeInput) (postproduction.Result, error) {
			if input.RunIDs[0] != "run-shot-revision-1-1" {
				return postproduction.Result{}, fmt.Errorf("unexpected run order: %v", input.RunIDs)
			}
			return postproduction.Result{
				SchemaVersion:     postproduction.SchemaVersion,
				Evidence:          postproduction.EvidenceMockOnly,
				EpisodeRevisionID: input.EpisodeRevisionID,
				ManifestHash:      manifestHash,
			}, nil
		},
		activity.RegisterOptions{Name: ActivityFinalizeEpisode},
	)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Gate3DecisionSignal, Gate3Decision{
			DecisionID: "decision-g3-post", Approved: true, ActorID: "producer-1",
		})
	}, time.Second)
	env.ExecuteWorkflow(EpisodeProductionWorkflow, EpisodeProductionInput{
		SchemaVersion: "v1", SeriesID: "series-1", EpisodeRevisionID: "episode-revision-1",
		ShotSpecRevisionIDs: []string{"shot-revision-1"}, GenerationProfileRef: "profile-revision-1",
		Gate2DecisionID: "decision-g2-1", ProviderRoute: testProviderRoute(),
		BudgetApprovalID: "budget-approval-1", BudgetMaximumMicros: 500, BudgetCurrency: "CNY",
		TraceID: "trace-postproduction",
		PostProduction: &PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidenceMockOnly,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN", BurnSubtitles: true,
		},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result EpisodeProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "LOCKED" || result.PostProduction == nil ||
		result.PostProduction.ManifestHash != manifestHash {
		t.Fatalf("post-production result = %#v", result)
	}
}

func TestStage1FinalizationWorkflow_FinalizesThenCreatesGate3(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	manifestHash := strings.Repeat("b", 64)
	var calls []string
	env.RegisterActivityWithOptions(
		func(_ context.Context, input FinalizeEpisodeInput) (postproduction.Result, error) {
			calls = append(calls, "finalize")
			return postproduction.Result{
				SchemaVersion:     postproduction.SchemaVersion,
				Evidence:          postproduction.EvidenceLive,
				EpisodeRevisionID: input.EpisodeRevisionID,
				ManifestHash:      manifestHash,
			}, nil
		},
		activity.RegisterOptions{Name: ActivityFinalizeEpisode},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, input CreateGate3Input) error {
			calls = append(calls, "gate3")
			if input.PostProductionManifestHash != manifestHash ||
				input.GenerationPlanID != "plan-stage1" || !input.PersistProductTruth ||
				len(input.RunIDs) != 2 || input.RunIDs[0] != "run-1" || input.RunIDs[1] != "run-2" {
				return fmt.Errorf("unexpected G3 input: %#v", input)
			}
			return nil
		},
		activity.RegisterOptions{Name: ActivityCreateGate3},
	)

	env.ExecuteWorkflow(Stage1FinalizationWorkflow, stage1FinalizationTestInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result Stage1FinalizationResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Gate3Created || result.PostProduction.ManifestHash != manifestHash {
		t.Fatalf("result = %#v", result)
	}
	if got := strings.Join(calls, ","); got != "finalize,gate3" {
		t.Fatalf("activity order = %q", got)
	}
}

func TestStage1FinalizationWorkflow_DoesNotCreateGate3WhenFinalizationFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var gateCalls int
	env.RegisterActivityWithOptions(
		func(context.Context, FinalizeEpisodeInput) (postproduction.Result, error) {
			return postproduction.Result{}, fmt.Errorf("speech submission failed")
		},
		activity.RegisterOptions{Name: ActivityFinalizeEpisode},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, CreateGate3Input) error {
			gateCalls++
			return nil
		},
		activity.RegisterOptions{Name: ActivityCreateGate3},
	)

	env.ExecuteWorkflow(Stage1FinalizationWorkflow, stage1FinalizationTestInput())
	if env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil")
	}
	if gateCalls != 0 {
		t.Fatalf("G3 calls = %d, want 0", gateCalls)
	}
}

func stage1FinalizationTestInput() FinalizeEpisodeInput {
	return FinalizeEpisodeInput{
		EpisodeRevisionID:   "episode-stage1",
		RunIDs:              []string{"run-1", "run-2"},
		GenerationPlanID:    "plan-stage1",
		TraceID:             "trace-stage1-finalization",
		PersistProductTruth: true,
		Config: PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidenceLive,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN", BurnSubtitles: true,
		},
	}
}

func registerHappyPathActivities(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(context.Context, EpisodeProductionInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityValidateBatch})
	env.RegisterActivityWithOptions(func(_ context.Context, input CompilePromptInput) (PromptSnapshotRef, error) {
		sum := sha256.Sum256([]byte(input.ShotSpecRevisionID))
		return PromptSnapshotRef{ID: "prompt-" + input.ShotSpecRevisionID, Digest: hex.EncodeToString(sum[:])}, nil
	}, activity.RegisterOptions{Name: ActivityCompilePrompt})
	env.RegisterActivityWithOptions(func(_ context.Context, input CreateRunInput) (GenerationRunRef, error) {
		sum := sha256.Sum256([]byte(input.ShotSpecRevisionID))
		return GenerationRunRef{
			RunID:         fmt.Sprintf("run-%s-%d", input.ShotSpecRevisionID, input.CreativeAttempt),
			RunSpecDigest: hex.EncodeToString(sum[:]),
			Attempt:       input.CreativeAttempt,
		}, nil
	}, activity.RegisterOptions{Name: ActivityCreateRun})
	env.RegisterActivityWithOptions(func(_ context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
		sum := sha256.Sum256([]byte(input.Run.RunID))
		digest := hex.EncodeToString(sum[:])
		return ProviderResult{
			UpstreamTaskID: "upstream-task-1",
			RequestID:      "request-1",
			ArtifactDigest: digest,
			ArtifactURI:    "cas://sha256/" + digest,
			Model:          input.Route,
		}, nil
	}, activity.RegisterOptions{Name: ActivityExecuteProviderJob})
	env.RegisterActivityWithOptions(func(context.Context, RunQCInput) (QCResult, error) {
		return QCResult{Passed: true}, nil
	}, activity.RegisterOptions{Name: ActivityRunAutomaticQC})
	env.RegisterActivityWithOptions(func(context.Context, CreateReviewInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityCreateShotReview})
	env.RegisterActivityWithOptions(func(context.Context, EscalateShotInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityEscalateShot})
	env.RegisterActivityWithOptions(func(context.Context, CreateGate3Input) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityCreateGate3})
}

func testProviderRoute() providercontract.ModelSnapshot {
	sum := sha256.Sum256([]byte("video-capability"))
	return providercontract.ModelSnapshot{
		CapabilityAlias: "video.primary",
		Provider:        "mock",
		ModelID:         "fixture-video-v1",
		RouteVersion:    "mock-routes-v1",
		CapabilityHash:  hex.EncodeToString(sum[:]),
		Verification:    "mock_only",
	}
}

func testSpeechRoute() providercontract.ModelSnapshot {
	sum := sha256.Sum256([]byte("speech-capability"))
	return providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilitySpeech),
		Provider:        "mock",
		ModelID:         "fixture-speech-v1",
		RouteVersion:    "mock-routes-v1",
		CapabilityHash:  hex.EncodeToString(sum[:]),
		Verification:    "mock_only",
	}
}
