package providercontract

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

type LiveEvidence struct {
	Shots []LiveShotEvidence `json:"shots"`
}

type LiveShotEvidence struct {
	ShotID   string              `json:"shot_id"`
	Attempts []LiveAttemptResult `json:"attempts"`
	Reviews  []LiveQualityReview `json:"quality_reviews,omitempty"`
}

type LiveAttemptResult struct {
	Number         int       `json:"number"`
	Temperature    string    `json:"temperature"`
	Status         JobStatus `json:"status"`
	LatencyMillis  int64     `json:"latency_millis"`
	RetryCount     int       `json:"retry_count"`
	UsageTokens    int64     `json:"usage_tokens"`
	CostMicros     int64     `json:"cost_micros"`
	ManifestSHA256 string    `json:"manifest_sha256"`
}

type LiveQualityReview struct {
	ReviewerID     string         `json:"reviewer_id"`
	Scores         map[string]int `json:"scores"`
	SevereArtifact bool           `json:"severe_artifact"`
}

type LiveEvaluation struct {
	PlannedShots         int   `json:"planned_shots"`
	SucceededShots       int   `json:"succeeded_shots"`
	SuccessRateBPS       int   `json:"success_rate_basis_points"`
	TotalRetries         int   `json:"total_retries"`
	ColdP95LatencyMillis int64 `json:"cold_p95_latency_millis"`
	HotP95LatencyMillis  int64 `json:"hot_p95_latency_millis"`
	TotalUsageTokens     int64 `json:"total_usage_tokens"`
	TotalCostMicros      int64 `json:"total_cost_micros"`
	QualityPassed        bool  `json:"quality_passed"`
	AcceptancePassed     bool  `json:"acceptance_passed"`
}

// EvaluateLiveEvidence applies the plan's declared denominator, nearest-rank
// percentile algorithm, retry accounting, and quality thresholds.
func EvaluateLiveEvidence(plan LiveTestPlan, evidence LiveEvidence) (LiveEvaluation, error) {
	if err := plan.Validate(); err != nil {
		return LiveEvaluation{}, fmt.Errorf("invalid live plan: %w", err)
	}
	planned := make(map[string]LiveTestShot)
	for _, category := range plan.Categories {
		for _, shot := range category.Shots {
			planned[shot.ID] = shot
		}
	}
	if len(evidence.Shots) != len(planned) {
		return LiveEvaluation{}, fmt.Errorf("evidence contains %d shots, want %d", len(evidence.Shots), len(planned))
	}
	evaluation := LiveEvaluation{PlannedShots: len(planned), QualityPassed: true}
	seen := make(map[string]struct{}, len(evidence.Shots))
	var coldLatencies, hotLatencies []int64
	for _, shotEvidence := range evidence.Shots {
		shot, ok := planned[shotEvidence.ShotID]
		if !ok {
			return LiveEvaluation{}, fmt.Errorf("unexpected evidence shot %q", shotEvidence.ShotID)
		}
		if _, duplicate := seen[shotEvidence.ShotID]; duplicate {
			return LiveEvaluation{}, fmt.Errorf("duplicate evidence shot %q", shotEvidence.ShotID)
		}
		seen[shotEvidence.ShotID] = struct{}{}
		if len(shotEvidence.Attempts) < 1 || len(shotEvidence.Attempts) > shot.MaxAttempts {
			return LiveEvaluation{}, fmt.Errorf("shot %q has invalid attempt count", shotEvidence.ShotID)
		}
		succeeded := false
		var shotCostMicros int64
		for index, attempt := range shotEvidence.Attempts {
			if succeeded {
				return LiveEvaluation{}, fmt.Errorf("shot %q retried after success", shotEvidence.ShotID)
			}
			if attempt.Number != index+1 || !attempt.Status.Terminal() ||
				attempt.LatencyMillis <= 0 || attempt.RetryCount != index ||
				attempt.UsageTokens < 0 || attempt.CostMicros < 0 ||
				attempt.UsageTokens > shot.MaxVideoTokens ||
				(attempt.Temperature != "cold" && attempt.Temperature != "hot") ||
				!validSHA256(attempt.ManifestSHA256) {
				return LiveEvaluation{}, fmt.Errorf("shot %q attempt %d is invalid", shotEvidence.ShotID, index+1)
			}
			if attempt.Temperature == "cold" {
				coldLatencies = append(coldLatencies, attempt.LatencyMillis)
			} else {
				hotLatencies = append(hotLatencies, attempt.LatencyMillis)
			}
			var sumOK bool
			evaluation.TotalUsageTokens, sumOK = checkedAddNonNegative(evaluation.TotalUsageTokens, attempt.UsageTokens)
			if !sumOK {
				return LiveEvaluation{}, errors.New("usage token total overflowed")
			}
			evaluation.TotalCostMicros, sumOK = checkedAddNonNegative(evaluation.TotalCostMicros, attempt.CostMicros)
			if !sumOK {
				return LiveEvaluation{}, errors.New("cost total overflowed")
			}
			shotCostMicros, sumOK = checkedAddNonNegative(shotCostMicros, attempt.CostMicros)
			if !sumOK || shotCostMicros > shot.MaxCostMicros {
				return LiveEvaluation{}, fmt.Errorf("shot %q exceeded its cost ceiling", shotEvidence.ShotID)
			}
			if attempt.Status == StatusSucceeded {
				succeeded = true
			}
		}
		evaluation.TotalRetries += len(shotEvidence.Attempts) - 1
		if succeeded {
			evaluation.SucceededShots++
			if err := validateQualityReviews(plan.QualityRubric, shotEvidence); err != nil {
				evaluation.QualityPassed = false
			}
		}
	}
	if plan.Acceptance.RequireColdAndHot && (len(coldLatencies) == 0 || len(hotLatencies) == 0) {
		return LiveEvaluation{}, errors.New("evidence requires both cold and hot latency samples")
	}
	evaluation.ColdP95LatencyMillis, _ = NearestRankPercentile(coldLatencies, 95)
	evaluation.HotP95LatencyMillis, _ = NearestRankPercentile(hotLatencies, 95)
	evaluation.SuccessRateBPS = evaluation.SucceededShots * 10_000 / evaluation.PlannedShots
	evaluation.AcceptancePassed = evaluation.SuccessRateBPS >= plan.Acceptance.MinimumSuccessRateBPS &&
		evaluation.TotalRetries <= plan.Acceptance.MaximumTotalRetries &&
		evaluation.TotalCostMicros <= plan.HardBudgetMicros &&
		evaluation.QualityPassed
	return evaluation, nil
}

func NearestRankPercentile(values []int64, percentile int) (int64, error) {
	if len(values) == 0 {
		return 0, errors.New("percentile requires at least one value")
	}
	if percentile < 1 || percentile > 100 {
		return 0, errors.New("percentile must be between 1 and 100")
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := (percentile*len(sorted) + 99) / 100
	return sorted[rank-1], nil
}

func validateQualityReviews(rubric QualityRubric, shot LiveShotEvidence) error {
	if len(shot.Reviews) < rubric.RequiredReviewers {
		return errors.New("not enough quality reviewers")
	}
	for _, review := range shot.Reviews[:rubric.RequiredReviewers] {
		if review.ReviewerID == "" || review.SevereArtifact {
			return errors.New("quality review failed")
		}
		weighted := 0
		for _, dimension := range rubric.Dimensions {
			score, ok := review.Scores[dimension.Name]
			if !ok || score < rubric.MinimumDimensionScore || score > rubric.ScaleMax {
				return errors.New("quality dimension failed")
			}
			weighted += score * dimension.WeightBPS
		}
		if weighted/10 < rubric.MinimumWeightedAverageMilli {
			return errors.New("quality weighted average failed")
		}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
