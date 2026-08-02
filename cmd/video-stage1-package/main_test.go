package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
)

func TestRunVerifyRevisionUsesCanonicalRevisionContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		batch     bool
		mutate    func(*stage1.ExecutionPackage)
		wantError bool
	}{
		{name: "valid speech batch child", batch: true},
		{name: "legacy single cue child", batch: false},
		{
			name: "wrong parent", batch: true, wantError: true,
			mutate: func(child *stage1.ExecutionPackage) {
				wrong := strings.Repeat("9", 64)
				child.ParentExecutionPackageHash = wrong
				child.PostProduction.Config.SpeechBatchAuthorization.ParentExecutionPackageHash = wrong
			},
		},
		{
			name: "invalid batch authorization", batch: true, wantError: true,
			mutate: func(child *stage1.ExecutionPackage) {
				child.PostProduction.Config.SpeechBatchAuthorization.MaximumSubmits++
			},
		},
		{
			name: "non speech drift", batch: true, wantError: true,
			mutate: func(child *stage1.ExecutionPackage) {
				child.PostProduction.Config.SubtitleLanguage = "en-US"
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			planPath := filepath.Join("..", "..", "video-pipeline", "config", "flo104-stage1-readiness.json")
			var plan stage1.Plan
			if err := decodeFile(planPath, &plan); err != nil {
				t.Fatal(err)
			}
			base := cliTestExecutionPackage(t, plan)
			singleCue := cliTestSpeechV2Package(t, plan, base)
			parent, child := base, singleCue
			if test.batch {
				parent = singleCue
				child = cliTestSpeechBatchPackage(t, plan, parent)
			}
			if test.mutate != nil {
				test.mutate(&child)
				var err error
				child, err = stage1.SealExecutionPackage(child)
				if err != nil {
					t.Fatal(err)
				}
			}

			directory := t.TempDir()
			parentPath := writeCLITestJSON(t, directory, "parent.json", parent)
			childPath := writeCLITestJSON(t, directory, "child.json", child)
			var output bytes.Buffer
			err := run([]string{"verify-revision", planPath, parentPath, childPath}, &output)
			if test.wantError {
				if err == nil {
					t.Fatal("verify-revision accepted a drifted child")
				}
				return
			}
			if err != nil {
				t.Fatalf("verify-revision: %v", err)
			}
			var verified stage1.ExecutionPackage
			if err := json.Unmarshal(output.Bytes(), &verified); err != nil {
				t.Fatalf("decode verified package: %v", err)
			}
			if verified.ContentHash != child.ContentHash {
				t.Fatalf("verified content hash = %q, want %q", verified.ContentHash, child.ContentHash)
			}
		})
	}
}

func cliTestExecutionPackage(t *testing.T, plan stage1.Plan) stage1.ExecutionPackage {
	t.Helper()
	jobs := make([]stage1.FrozenJob, len(plan.PrimaryShotIDs))
	runIDs := make([]string, len(jobs))
	generationPlanID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-plan")).String()
	for index, shotID := range plan.PrimaryShotIDs {
		runID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-run:"+shotID)).String()
		jobs[index] = stage1.FrozenJob{
			ShotID:             shotID,
			ShotSpecRevisionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-shot:"+shotID)).String(),
			AttemptID:          fmt.Sprintf("cli-stage1-attempt-%02d", index+1),
			IdempotencyKey:     "provider-job-" + runID,
			Run: orchestration.GenerationRunRef{
				RunID: runID, RunSpecDigest: fmt.Sprintf("%064x", index+1), Attempt: 1,
			},
			PromptSnapshotID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-prompt:"+shotID)).String(),
			PromptSnapshotHash:  fmt.Sprintf("%064x", index+101),
			GenerationPlanID:    generationPlanID,
			BudgetApprovalID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-video-budget")).String(),
			BudgetMaximumMicros: 1_000,
			BudgetCurrency:      "CNY",
			ProviderProfileID:   uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-video-profile")).String(),
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilityVideo), Provider: "volcengine_ark",
				ModelID: plan.VideoModel, RouteVersion: "agent-plan-large-v1",
				CapabilityHash: strings.Repeat("c", 64), Verification: providercontract.PendingKey,
			},
			EstimatedVideoTokens: 100_000,
			PredictedAFPMilli:    plan.ReferenceJobAFPMilli,
			WorkflowID:           "flo104-stage1", ActivityID: "submit-" + shotID,
			TraceID: "flo104-stage1-" + shotID,
		}
		runIDs[index] = runID
	}
	package_ := stage1.ExecutionPackage{
		SchemaVersion: stage1.ExecutionPackageSchemaVersion,
		BatchID:       plan.BatchID,
		PrimaryJobs:   jobs,
		PostProduction: orchestration.FinalizeEpisodeInput{
			EpisodeRevisionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-episode")).String(),
			RunIDs:            runIDs,
			GenerationPlanID:  generationPlanID,
			Config: orchestration.PostProductionConfig{
				Enabled: true, Evidence: postproduction.EvidenceLive,
				SpeechRoute: providercontract.ModelSnapshot{
					CapabilityAlias: string(providercontract.CapabilitySpeech), Provider: "volcengine_ark",
					ModelID: "doubao-seed-tts-2.0", RouteVersion: "agent-plan-large-tts-v2",
					CapabilityHash: strings.Repeat("d", 64), Verification: providercontract.PendingKey,
				},
				SpeechProviderProfileID:   uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-speech-profile")).String(),
				SpeechBudgetApprovalID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-speech-budget")).String(),
				SpeechBudgetMaximumMicros: 1_000,
				SpeechBudgetCurrency:      "CNY",
				SubtitleLanguage:          "zh-CN", BurnSubtitles: true, EnforcePoCDuration: true,
			},
			TraceID: "flo104-stage1-finalize", PersistProductTruth: true,
		},
	}
	return sealCLITestPackage(t, plan, package_)
}

func cliTestSpeechV2Package(
	t *testing.T,
	plan stage1.Plan,
	parent stage1.ExecutionPackage,
) stage1.ExecutionPackage {
	t.Helper()
	child := parent
	child.ParentExecutionPackageHash = parent.ContentHash
	child.PostProduction.Config.SpeechIdentityVersion = postproduction.SpeechIdentityV2
	child.PostProduction.Config.SpeechVoice = &postproduction.SpeechVoiceBinding{
		AssetID:              uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-voice")).String(),
		ParentAssetVersionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-voice-v1")).String(),
		AssetVersionID:       uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-voice-v2")).String(),
		AssetVersionHash:     strings.Repeat("e", 64),
		LicenseSnapshotID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-license")).String(),
		LicenseSnapshotHash:  strings.Repeat("f", 64),
		Provider:             "volcengine_ark", ModelID: child.PostProduction.Config.SpeechRoute.ModelID,
		ResourceID: "seed-tts-2.0", Speaker: "zh_female_vv_uranus_bigtts",
	}
	child.PostProduction.Config.SpeechAuthorizedCueID = "cue-001"
	child.PostProduction.Config.SpeechMaximumAFPMilli = 2_228
	child.PostProduction.Config.SpeechMaxAttempts = 1
	child.PostProduction.TraceID += "-speech-v2"
	return sealCLITestPackage(t, plan, child)
}

func cliTestSpeechBatchPackage(
	t *testing.T,
	plan stage1.Plan,
	parent stage1.ExecutionPackage,
) stage1.ExecutionPackage {
	t.Helper()
	child := parent
	child.ParentExecutionPackageHash = parent.ContentHash
	child.PostProduction.Config.SpeechAuthorizedCueID = ""
	child.PostProduction.Config.SpeechMaximumAFPMilli = 0
	child.PostProduction.Config.SpeechMaximumCashMicros = 0
	child.PostProduction.Config.SpeechMaxAttempts = 0
	inputHash := strings.Repeat("1", 64)
	voice := child.PostProduction.Config.SpeechVoice
	child.PostProduction.Config.SpeechBatchAuthorization = &speechcontract.BatchAuthorization{
		SchemaVersion:              speechcontract.SchemaVersion,
		ParentExecutionPackageHash: parent.ContentHash,
		ApprovalCommentID:          uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-approval-comment")).String(),
		ApprovalActorID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-stage1-approval-actor")).String(),
		ValidUntil:                 "2026-08-31T15:59:59Z",
		Provider:                   voice.Provider, ModelID: voice.ModelID,
		RouteVersion: child.PostProduction.Config.SpeechRoute.RouteVersion,
		ResourceID:   voice.ResourceID, Speaker: voice.Speaker,
		VoiceAssetID:              voice.AssetID,
		ParentVoiceAssetVersionID: voice.ParentAssetVersionID,
		VoiceAssetVersionID:       voice.AssetVersionID,
		VoiceAssetVersionHash:     voice.AssetVersionHash,
		LicenseSnapshotID:         voice.LicenseSnapshotID,
		LicenseSnapshotHash:       voice.LicenseSnapshotHash,
		MaximumSubmits:            1, EstimatedAFPMilli: 135, MaximumAFPMilli: 149,
		Cues: []speechcontract.CueAuthorization{{
			CueID: "cue-002", JobID: "speech-v2-" + inputHash[:32], InputHash: inputHash,
			UnicodeCharacters: 1, EstimatedAFPMilli: 135, MaximumAFPMilli: 149, MaxAttempts: 1,
		}},
	}
	child.PostProduction.TraceID += "-speech-batch-v1"
	return sealCLITestPackage(t, plan, child)
}

func sealCLITestPackage(
	t *testing.T,
	plan stage1.Plan,
	package_ stage1.ExecutionPackage,
) stage1.ExecutionPackage {
	t.Helper()
	sealed, err := stage1.SealExecutionPackage(package_)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.Validate(plan); err != nil {
		t.Fatal(err)
	}
	return sealed
}

func writeCLITestJSON(t *testing.T, directory, name string, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
