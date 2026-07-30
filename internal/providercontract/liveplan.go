package providercontract

import (
	"errors"
	"fmt"
)

const PendingKey = "pending_key"

type LiveTestPlan struct {
	SchemaVersion    string             `json:"schema_version"`
	Status           string             `json:"status"`
	Provider         string             `json:"provider"`
	VerifiedAt       string             `json:"verified_at"`
	SoftBudgetMicros int64              `json:"soft_budget_micros"`
	HardBudgetMicros int64              `json:"hard_budget_micros"`
	Categories       []LiveTestCategory `json:"categories"`
	RequiredMetrics  []string           `json:"required_metrics"`
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
	case len(p.Categories) != 3:
		return fmt.Errorf("exactly 3 shot categories are required, got %d", len(p.Categories))
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
			totalMaxCost += shot.MaxCostMicros
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
