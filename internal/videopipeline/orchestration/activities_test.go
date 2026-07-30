package orchestration

import (
	"context"
	"encoding/json"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

type activityJournalFixture struct {
	replay          json.RawMessage
	replayCompleted bool
	beginStep       WorkflowStep
	inputHash       string
	completedStep   WorkflowStep
	output          json.RawMessage
}

func (j *activityJournalFixture) BeginWorkflowStep(
	_ context.Context,
	step WorkflowStep,
	inputHash string,
) (json.RawMessage, bool, error) {
	j.beginStep = step
	j.inputHash = inputHash
	return j.replay, j.replayCompleted, nil
}

func (j *activityJournalFixture) CompleteWorkflowStep(
	_ context.Context,
	step WorkflowStep,
	_ string,
	output json.RawMessage,
) error {
	j.completedStep = step
	j.output = append(json.RawMessage(nil), output...)
	return nil
}

func TestActivities_CompilePromptCommitsDurableJournalResult(t *testing.T) {
	journal := &activityJournalFixture{}
	activities := NewActivitiesWithJournal("http://provider.invalid", journal)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CompilePrompt)

	encoded, err := env.ExecuteActivity(activities.CompilePrompt, CompilePromptInput{
		ShotSpecRevisionID:   "shot-1",
		GenerationProfileRef: "profile-1",
		TraceID:              "trace-1",
	})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result PromptSnapshotRef
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.ID == "" || result.Digest == "" {
		t.Fatalf("result = %#v", result)
	}
	if journal.beginStep.WorkflowID == "" || journal.beginStep.ActivityID == "" ||
		journal.beginStep.ActivityType == "" || journal.inputHash == "" {
		t.Fatalf("journal begin = %#v, inputHash=%q", journal.beginStep, journal.inputHash)
	}
	if journal.completedStep != journal.beginStep || len(journal.output) == 0 {
		t.Fatalf("journal completion = %#v, output=%s", journal.completedStep, journal.output)
	}
}

func TestActivities_CompilePromptReplaysWithoutRevalidation(t *testing.T) {
	expected := PromptSnapshotRef{ID: "prompt-replayed", Digest: "replayed-digest"}
	replay, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	journal := &activityJournalFixture{replay: replay, replayCompleted: true}
	activities := NewActivitiesWithJournal("http://provider.invalid", journal)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CompilePrompt)

	encoded, err := env.ExecuteActivity(activities.CompilePrompt, CompilePromptInput{TraceID: "trace-2"})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result PromptSnapshotRef
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result != expected {
		t.Fatalf("result = %#v, want %#v", result, expected)
	}
	if len(journal.output) != 0 {
		t.Fatalf("replay wrote a second completion: %s", journal.output)
	}
}
