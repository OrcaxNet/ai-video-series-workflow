package providercontract

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
	data, err := os.ReadFile("../../docs/flo-110/live-test-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan LiveTestPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	evidence := LiveEvidence{}
	index := 0
	for _, category := range plan.Categories {
		for _, shot := range category.Shots {
			scores := make(map[string]int)
			for _, dimension := range plan.QualityRubric.Dimensions {
				scores[dimension.Name] = 4
			}
			temperature := "hot"
			if index == 0 {
				temperature = "cold"
			}
			evidence.Shots = append(evidence.Shots, LiveShotEvidence{
				ShotID: shot.ID,
				Attempts: []LiveAttemptResult{{
					Number:         1,
					Temperature:    temperature,
					Status:         StatusSucceeded,
					LatencyMillis:  int64(1_000 + index),
					RetryCount:     0,
					UsageTokens:    100,
					CostMicros:     1_000,
					ManifestSHA256: strings.Repeat("a", 64),
				}},
				Reviews: []LiveQualityReview{
					{ReviewerID: "reviewer-1", Scores: scores},
					{ReviewerID: "reviewer-2", Scores: scores},
				},
			})
			index++
		}
	}
	evaluation, err := EvaluateLiveEvidence(plan, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.SuccessRateBPS != 10_000 || !evaluation.QualityPassed ||
		!evaluation.AcceptancePassed || evaluation.PlannedShots != 15 {
		t.Fatalf("evaluation = %#v", evaluation)
	}
}
