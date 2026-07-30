package providercontract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCommittedLivePlan(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../docs/flo-110/live-test-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan LiveTestPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("committed live plan is invalid: %v", err)
	}
}

func TestLivePlanRequiresExecutableMeasurementContract(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../docs/flo-110/live-test-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var valid LiveTestPlan
	if err := json.Unmarshal(data, &valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*LiveTestPlan)
	}{
		{name: "missing denominator", mutate: func(plan *LiveTestPlan) {
			plan.Measurement.SuccessDenominator = ""
		}},
		{name: "undefined percentile", mutate: func(plan *LiveTestPlan) {
			plan.Measurement.PercentileMethod = "interpolated"
		}},
		{name: "missing quality threshold", mutate: func(plan *LiveTestPlan) {
			plan.QualityRubric.MinimumWeightedAverageMilli = 0
		}},
		{name: "missing manifest provenance", mutate: func(plan *LiveTestPlan) {
			for index, field := range plan.ManifestFields {
				if field == "provider.request_id" {
					plan.ManifestFields = append(plan.ManifestFields[:index], plan.ManifestFields[index+1:]...)
					break
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			var plan LiveTestPlan
			if err := json.Unmarshal(encoded, &plan); err != nil {
				t.Fatal(err)
			}
			tt.mutate(&plan)
			if err := plan.Validate(); err == nil {
				t.Fatal("mutated plan unexpectedly passed validation")
			}
		})
	}
}

func TestNearestRankPercentile(t *testing.T) {
	t.Parallel()
	values := make([]int64, 20)
	for index := range values {
		values[index] = int64(index + 1)
	}
	got, err := NearestRankPercentile(values, 95)
	if err != nil {
		t.Fatal(err)
	}
	if got != 19 {
		t.Fatalf("p95 = %d, want 19", got)
	}
}

func TestEvaluateLiveEvidenceUsesDeclaredDenominatorAndQualityGate(t *testing.T) {
	t.Parallel()
	plan := loadTestLivePlan(t)
	evidence := validLiveEvidence(t, plan)
	evaluation, err := EvaluateLiveEvidence(plan, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.SuccessRateBPS != 10_000 || !evaluation.QualityPassed ||
		!evaluation.AcceptancePassed || evaluation.PlannedShots != 15 ||
		evaluation.ColdSamples != 1 || evaluation.HotSamples != 13 ||
		evaluation.UnclassifiedSamples != 1 ||
		evaluation.ColdP95LatencyMillis != 1_001 ||
		evaluation.HotP95LatencyMillis != 1_014 {
		t.Fatalf("evaluation = %#v", evaluation)
	}
}

func TestEvaluateLiveEvidenceRejectsUnboundOrForgedEvidence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, *LiveEvidence)
	}{
		{
			name: "duplicate reviewer",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				evidence.Shots[0].Reviews[1].ReviewerID = evidence.Shots[0].Reviews[0].ReviewerID
			},
		},
		{
			name: "arbitrary hash without manifest",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				evidence.Shots[0].Attempts[0].ManifestBytes = nil
				evidence.Shots[0].Attempts[0].ManifestSHA256 = strings.Repeat("a", 64)
			},
		},
		{
			name: "manifest and attempt mismatch",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				evidence.Shots[0].Attempts[0].Status = StatusFailed
			},
		},
		{
			name: "caller supplied cold label",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				evidence.Shots[0].Attempts[0].Temperature = "cold"
			},
		},
		{
			name: "manifest bytes changed after hashing",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				evidence.Shots[0].Attempts[0].ManifestBytes = append(
					evidence.Shots[0].Attempts[0].ManifestBytes,
					' ',
				)
			},
		},
		{
			name: "manifest provenance does not match plan",
			mutate: func(t *testing.T, evidence *LiveEvidence) {
				rewriteAttemptManifest(t, &evidence.Shots[0].Attempts[0], func(manifest *GenerationManifest) {
					manifest.Provider.Name = "unplanned-provider"
				})
			},
		},
		{
			name: "noncanonical manifest with matching hash",
			mutate: func(_ *testing.T, evidence *LiveEvidence) {
				attempt := &evidence.Shots[0].Attempts[0]
				attempt.ManifestBytes = append(attempt.ManifestBytes, ' ')
				sum := sha256.Sum256(attempt.ManifestBytes)
				attempt.ManifestSHA256 = hex.EncodeToString(sum[:])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan := loadTestLivePlan(t)
			evidence := validLiveEvidence(t, plan)
			tt.mutate(t, &evidence)
			if evaluation, err := EvaluateLiveEvidence(plan, evidence); err == nil ||
				evaluation.AcceptancePassed {
				t.Fatalf("forged evidence passed: evaluation=%#v err=%v", evaluation, err)
			}
		})
	}
}

func TestEvaluateLiveEvidenceDerivesTemperatureFromInvocationTimes(t *testing.T) {
	t.Parallel()
	plan := loadTestLivePlan(t)
	evidence := validLiveEvidence(t, plan)

	// Collapse the only cold gap and shift all later attempts so every
	// invocation is inside the hot window. A caller-provided label is not
	// needed or trusted; without a derived cold sample the gate must fail.
	previousCompleted := evidence.Shots[0].Attempts[0].CompletedAt
	for index := 1; index < len(evidence.Shots); index++ {
		attempt := &evidence.Shots[index].Attempts[0]
		started := previousCompleted.Add(time.Minute)
		completed := started.Add(time.Duration(attempt.LatencyMillis) * time.Millisecond)
		rewriteAttemptManifestTimes(t, attempt, started, completed)
		previousCompleted = completed
	}
	if evaluation, err := EvaluateLiveEvidence(plan, evidence); err == nil ||
		evaluation.AcceptancePassed {
		t.Fatalf("hot-only sequence passed: evaluation=%#v err=%v", evaluation, err)
	}
}

func loadTestLivePlan(t *testing.T) LiveTestPlan {
	t.Helper()
	data, err := os.ReadFile("../../docs/flo-110/live-test-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan LiveTestPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func validLiveEvidence(t *testing.T, plan LiveTestPlan) LiveEvidence {
	t.Helper()
	evidence := LiveEvidence{}
	base := time.Unix(1_800_000_000, 0).UTC()
	previousCompleted := base
	index := 0
	for _, category := range plan.Categories {
		for _, shot := range category.Shots {
			started := base
			switch index {
			case 0:
			case 1:
				started = previousCompleted.Add(
					time.Duration(plan.Measurement.ColdIdleMinSeconds) * time.Second,
				)
			default:
				started = previousCompleted.Add(time.Minute)
			}
			latencyMillis := int64(1_000 + index)
			completed := started.Add(time.Duration(latencyMillis) * time.Millisecond)
			assets := plannedAssetRefs(t, shot.ReferenceAssetRevisionIDs)
			request := GenerationRequest{
				RequestID:        "request-" + shot.ID,
				IdempotencyKey:   "idempotency-" + shot.ID,
				Modality:         ModalityVideo,
				Prompt:           "fixture prompt",
				PromptSnapshotID: shot.PromptSnapshotID,
				Context: ContextRefs{
					SeriesSnapshotID:  "series-context-v1",
					EpisodeSnapshotID: "episode-context-v1",
					SceneSnapshotID:   "scene-context-v1",
					ShotSnapshotID:    "shot-context-" + shot.ID,
				},
				Assets: assets,
				Output: shot.Output,
				Budget: BudgetEnvelope{
					EstimatedCostMicros: 1_000,
					MaxCostMicros:       shot.MaxCostMicros,
					MaxAttempts:         shot.MaxAttempts,
				},
			}
			job := Job{
				ID:                fmt.Sprintf("provider-job-%02d", index+1),
				RequestID:         request.RequestID,
				IdempotencyKey:    request.IdempotencyKey,
				Status:            StatusSucceeded,
				Provider:          plan.Provider,
				ProviderModel:     "runtime-model-version",
				ProviderRegion:    "cn-beijing",
				ProviderRequestID: fmt.Sprintf("provider-request-%02d", index+1),
				CreatedAt:         started,
				UpdatedAt:         completed,
				Output: &Output{
					Assets: []AssetRef{{
						ID:               "output-" + shot.ID,
						Revision:         "rev-1",
						Kind:             ModalityVideo,
						Role:             AssetRoleOutput,
						SHA256:           strings.Repeat("a", 64),
						LicenseReference: "fixture-license",
					}},
					Actual: OutputSpec{
						Resolution:     "720p",
						AspectRatio:    "16:9",
						FPS:            24,
						DurationMillis: 5_000,
						Format:         "mp4",
					},
					Usage: Usage{VideoTokens: 100, ProviderCostMicros: 1_000},
				},
			}
			manifest, err := NewGenerationManifest(ManifestBuildInput{
				ManifestID:  fmt.Sprintf("manifest-%s-attempt-1", shot.ID),
				ShotID:      shot.ID,
				Evidence:    EvidenceLiveProvider,
				Request:     request,
				Job:         job,
				Attempt:     1,
				StartedAt:   started,
				CompletedAt: completed,
			})
			if err != nil {
				t.Fatal(err)
			}
			manifestBytes, manifestHash := encodeTestManifest(t, manifest)
			firstScores := make(map[string]int)
			secondScores := make(map[string]int)
			for _, dimension := range plan.QualityRubric.Dimensions {
				firstScores[dimension.Name] = 4
				secondScores[dimension.Name] = 4
			}
			evidence.Shots = append(evidence.Shots, LiveShotEvidence{
				ShotID: shot.ID,
				Attempts: []LiveAttemptResult{{
					Number:         1,
					Status:         StatusSucceeded,
					StartedAt:      started,
					CompletedAt:    completed,
					LatencyMillis:  latencyMillis,
					RetryCount:     0,
					UsageTokens:    100,
					CostMicros:     1_000,
					ManifestSHA256: manifestHash,
					ManifestBytes:  manifestBytes,
				}},
				Reviews: []LiveQualityReview{
					{ReviewerID: "reviewer-1", Scores: firstScores},
					{ReviewerID: "reviewer-2", Scores: secondScores},
				},
			})
			previousCompleted = completed
			index++
		}
	}
	return evidence
}

func plannedAssetRefs(t *testing.T, revisions []string) []AssetRef {
	t.Helper()
	assets := make([]AssetRef, 0, len(revisions))
	for _, value := range revisions {
		index := strings.LastIndex(value, "@")
		if index <= 0 || index == len(value)-1 {
			t.Fatalf("invalid planned asset revision %q", value)
		}
		assets = append(assets, AssetRef{
			ID:               value[:index],
			Revision:         value[index+1:],
			Kind:             ModalityImage,
			Role:             AssetRoleReferenceImage,
			SHA256:           strings.Repeat("b", 64),
			LicenseReference: "fixture-license",
		})
	}
	return assets
}

func encodeTestManifest(t *testing.T, manifest GenerationManifest) ([]byte, string) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

func rewriteAttemptManifestTimes(
	t *testing.T,
	attempt *LiveAttemptResult,
	started, completed time.Time,
) {
	t.Helper()
	rewriteAttemptManifest(t, attempt, func(manifest *GenerationManifest) {
		manifest.Attempt.StartedAt = started
		manifest.Attempt.CompletedAt = completed
		manifest.Attempt.LatencyMillis = completed.Sub(started).Milliseconds()
	})
	attempt.StartedAt = started
	attempt.CompletedAt = completed
	attempt.LatencyMillis = completed.Sub(started).Milliseconds()
}

func rewriteAttemptManifest(
	t *testing.T,
	attempt *LiveAttemptResult,
	mutate func(*GenerationManifest),
) {
	t.Helper()
	var manifest GenerationManifest
	if err := json.Unmarshal(attempt.ManifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	attempt.ManifestBytes, attempt.ManifestSHA256 = encodeTestManifest(t, manifest)
}
