package providercontract

import (
	"errors"
	"fmt"
	"strings"
)

const PendingKey = "pending_key"

type LiveTestPlan struct {
	SchemaVersion    string              `json:"schema_version"`
	Status           string              `json:"status"`
	Provider         string              `json:"provider"`
	VerifiedAt       string              `json:"verified_at"`
	SoftBudgetMicros int64               `json:"soft_budget_micros"`
	HardBudgetMicros int64               `json:"hard_budget_micros"`
	Measurement      MeasurementProtocol `json:"measurement_protocol"`
	Acceptance       AcceptanceCriteria  `json:"acceptance_criteria"`
	QualityRubric    QualityRubric       `json:"quality_rubric"`
	ManifestFields   []string            `json:"manifest_required_fields"`
	Categories       []LiveTestCategory  `json:"categories"`
	RequiredMetrics  []string            `json:"required_metrics"`
}

type MeasurementProtocol struct {
	ColdIdleMinSeconds  int    `json:"cold_idle_min_seconds"`
	HotWindowMaxSeconds int    `json:"hot_window_max_seconds"`
	SuccessDenominator  string `json:"success_denominator"`
	SuccessNumerator    string `json:"success_numerator"`
	PercentileMethod    string `json:"percentile_method"`
	LatencyPopulation   string `json:"latency_population"`
	LatencyStartEvent   string `json:"latency_start_event"`
	LatencyEndEvent     string `json:"latency_end_event"`
}

type AcceptanceCriteria struct {
	MinimumSuccessRateBPS int  `json:"minimum_success_rate_basis_points"`
	MaximumTotalRetries   int  `json:"maximum_total_retries"`
	RequireColdAndHot     bool `json:"require_cold_and_hot_samples"`
}

type QualityRubric struct {
	ScaleMin                    int                `json:"scale_min"`
	ScaleMax                    int                `json:"scale_max"`
	RequiredReviewers           int                `json:"required_reviewers"`
	MinimumDimensionScore       int                `json:"minimum_dimension_score"`
	MinimumWeightedAverageMilli int                `json:"minimum_weighted_average_milli"`
	BlockOnSevereArtifact       bool               `json:"block_on_severe_artifact"`
	Dimensions                  []QualityDimension `json:"dimensions"`
}

type QualityDimension struct {
	Name      string `json:"name"`
	WeightBPS int    `json:"weight_basis_points"`
}

type LiveTestCategory struct {
	Name  string         `json:"name"`
	Shots []LiveTestShot `json:"shots"`
}

type LiveTestShot struct {
	ID                        string     `json:"id"`
	Prompt                    string     `json:"prompt"`
	PromptSnapshotID          string     `json:"prompt_snapshot_id"`
	ReferenceAssetRevisionIDs []string   `json:"reference_asset_revision_ids"`
	Output                    OutputSpec `json:"output"`
	MaxAttempts               int        `json:"max_attempts"`
	MaxVideoTokens            int64      `json:"max_video_tokens"`
	MaxCostMicros             int64      `json:"max_cost_micros"`
}

func (p LiveTestPlan) Validate() error {
	switch {
	case p.SchemaVersion != "1":
		return errors.New("schema_version must be 1")
	case p.Status != PendingKey:
		return errors.New("status must be pending_key before live evidence exists")
	case p.Provider != "volcengine_ark":
		return errors.New("provider must be volcengine_ark")
	case p.VerifiedAt == "":
		return errors.New("verified_at is required")
	case p.SoftBudgetMicros <= 0 || p.HardBudgetMicros <= 0 ||
		p.SoftBudgetMicros >= p.HardBudgetMicros:
		return errors.New("soft and hard budgets are invalid")
	case p.Measurement.ColdIdleMinSeconds <= 0 || p.Measurement.HotWindowMaxSeconds <= 0:
		return errors.New("cold and hot measurement windows must be positive")
	case p.Measurement.SuccessDenominator != "all_planned_shots":
		return errors.New("success denominator must be all_planned_shots")
	case p.Measurement.SuccessNumerator != "shots_succeeded_within_max_attempts":
		return errors.New("success numerator must count shots succeeded within max attempts")
	case p.Measurement.PercentileMethod != "nearest_rank":
		return errors.New("percentile method must be nearest_rank")
	case p.Measurement.LatencyPopulation != "all_terminal_attempts":
		return errors.New("latency population must be all_terminal_attempts")
	case p.Measurement.LatencyStartEvent != "submit_start" ||
		p.Measurement.LatencyEndEvent != "terminal_state_observed":
		return errors.New("latency event boundaries are invalid")
	case p.Acceptance.MinimumSuccessRateBPS < 1 ||
		p.Acceptance.MinimumSuccessRateBPS > 10_000 ||
		p.Acceptance.MaximumTotalRetries < 0 || !p.Acceptance.RequireColdAndHot:
		return errors.New("acceptance criteria are invalid")
	case len(p.Categories) != 3:
		return fmt.Errorf("exactly 3 shot categories are required, got %d", len(p.Categories))
	}
	if err := p.QualityRubric.Validate(); err != nil {
		return err
	}
	requiredManifestFields := map[string]bool{
		"manifest_id": false, "evidence": false, "shot_id": false,
		"request_id": false, "idempotency_key": false, "prompt_snapshot_id": false,
		"context": false, "input_assets": false, "requested_output": false,
		"provider.name": false, "provider.model": false, "provider.region": false,
		"provider.request_id": false, "provider.job_id": false,
		"attempt.number":     false,
		"attempt.started_at": false, "attempt.completed_at": false,
		"attempt.latency_millis": false, "status": false, "actual_output": false,
		"output_assets": false, "usage": false, "budget": false,
	}
	for _, field := range p.ManifestFields {
		if _, ok := requiredManifestFields[field]; ok {
			requiredManifestFields[field] = true
		}
	}
	for field, present := range requiredManifestFields {
		if !present {
			return fmt.Errorf("required manifest field %q is missing", field)
		}
	}
	requiredCategories := map[string]bool{
		"character_dialogue":    false,
		"action_continuity":     false,
		"scene_prop_continuity": false,
	}
	seen := make(map[string]struct{})
	var totalMaxCost int64
	for _, category := range p.Categories {
		if category.Name == "" {
			return errors.New("category name is required")
		}
		if _, required := requiredCategories[category.Name]; !required {
			return fmt.Errorf("unexpected category %q", category.Name)
		}
		if requiredCategories[category.Name] {
			return fmt.Errorf("duplicate category %q", category.Name)
		}
		requiredCategories[category.Name] = true
		if len(category.Shots) != 5 {
			return fmt.Errorf("category %q has %d shots; exactly 5 are required", category.Name, len(category.Shots))
		}
		for _, shot := range category.Shots {
			if shot.ID == "" || shot.Prompt == "" || shot.PromptSnapshotID == "" {
				return fmt.Errorf("category %q contains an incomplete shot", category.Name)
			}
			if _, duplicate := seen[shot.ID]; duplicate {
				return fmt.Errorf("duplicate shot id %q", shot.ID)
			}
			seen[shot.ID] = struct{}{}
			if len(shot.ReferenceAssetRevisionIDs) == 0 {
				return fmt.Errorf("shot %q has no authorized asset revision reference", shot.ID)
			}
			if shot.Output.Width != 1280 || shot.Output.Height != 720 ||
				shot.Output.AspectRatio != "16:9" || shot.Output.FPS != 24 ||
				shot.Output.DurationMillis < 4_000 || shot.Output.DurationMillis > 6_000 ||
				shot.Output.Format != "mp4" {
				return fmt.Errorf("shot %q does not match the PoC output contract", shot.ID)
			}
			if shot.MaxAttempts < 1 || shot.MaxAttempts > 2 ||
				shot.MaxVideoTokens <= 0 || shot.MaxCostMicros <= 0 {
				return fmt.Errorf("shot %q has an invalid retry or cost ceiling", shot.ID)
			}
			var ok bool
			totalMaxCost, ok = checkedAddNonNegative(totalMaxCost, shot.MaxCostMicros)
			if !ok {
				return errors.New("shot cost ceilings overflow the supported range")
			}
		}
	}
	for category, present := range requiredCategories {
		if !present {
			return fmt.Errorf("required category %q is missing", category)
		}
	}
	if totalMaxCost > p.HardBudgetMicros {
		return fmt.Errorf(
			"shot ceilings total %d micros and exceed hard budget %d",
			totalMaxCost,
			p.HardBudgetMicros,
		)
	}
	required := map[string]bool{
		"cold_latency_ms": false,
		"hot_latency_ms":  false,
		"success_rate":    false,
		"retry_count":     false,
		"quality_scores":  false,
		"usage_tokens":    false,
		"cost_micros":     false,
		"manifest_hashes": false,
	}
	for _, metric := range p.RequiredMetrics {
		if _, ok := required[metric]; ok {
			required[metric] = true
		}
	}
	for metric, present := range required {
		if !present {
			return fmt.Errorf("required metric %q is missing", metric)
		}
	}
	return nil
}

func (r QualityRubric) Validate() error {
	switch {
	case r.ScaleMin != 1 || r.ScaleMax != 5:
		return errors.New("quality score scale must be 1 through 5")
	case r.RequiredReviewers < 1:
		return errors.New("quality rubric requires at least one reviewer")
	case r.MinimumDimensionScore < r.ScaleMin || r.MinimumDimensionScore > r.ScaleMax:
		return errors.New("minimum dimension score is outside the quality scale")
	case r.MinimumWeightedAverageMilli < r.ScaleMin*1000 ||
		r.MinimumWeightedAverageMilli > r.ScaleMax*1000:
		return errors.New("minimum weighted average is outside the quality scale")
	case !r.BlockOnSevereArtifact:
		return errors.New("severe artifacts must block quality acceptance")
	case len(r.Dimensions) == 0:
		return errors.New("quality rubric dimensions are required")
	}
	seen := make(map[string]struct{}, len(r.Dimensions))
	totalWeight := 0
	for _, dimension := range r.Dimensions {
		if strings.TrimSpace(dimension.Name) == "" || dimension.WeightBPS <= 0 {
			return errors.New("quality rubric dimension is invalid")
		}
		if _, duplicate := seen[dimension.Name]; duplicate {
			return fmt.Errorf("duplicate quality dimension %q", dimension.Name)
		}
		seen[dimension.Name] = struct{}{}
		totalWeight += dimension.WeightBPS
	}
	if totalWeight != 10_000 {
		return fmt.Errorf("quality weights total %d basis points, want 10000", totalWeight)
	}
	return nil
}
