package providercontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const maxManifestEvidenceBytes = 2 << 20

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
	Temperature    string    `json:"temperature,omitempty"`
	Status         JobStatus `json:"status"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	LatencyMillis  int64     `json:"latency_millis"`
	RetryCount     int       `json:"retry_count"`
	UsageTokens    int64     `json:"usage_tokens"`
	CostMicros     int64     `json:"cost_micros"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	ManifestBytes  []byte    `json:"manifest_bytes"`
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
	ColdSamples          int   `json:"cold_samples"`
	HotSamples           int   `json:"hot_samples"`
	UnclassifiedSamples  int   `json:"unclassified_samples"`
	TotalUsageTokens     int64 `json:"total_usage_tokens"`
	TotalCostMicros      int64 `json:"total_cost_micros"`
	QualityPassed        bool  `json:"quality_passed"`
	AcceptancePassed     bool  `json:"acceptance_passed"`
}

type verifiedLiveAttempt struct {
	result   LiveAttemptResult
	manifest GenerationManifest
	groupKey string
}

// EvaluateLiveEvidence applies the plan's declared denominator, nearest-rank
// percentile algorithm, retry accounting, and quality thresholds. Every
// attempt is derived from exact Manifest bytes; duplicate caller fields are
// cross-checked and never treated as authoritative.
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
	manifestHashes := make(map[string]struct{})
	providerJobs := make(map[string]struct{})
	providerRequests := make(map[string]struct{})
	var verifiedAttempts []verifiedLiveAttempt
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
				attempt.Temperature != "" ||
				attempt.StartedAt.IsZero() || !attempt.CompletedAt.After(attempt.StartedAt) ||
				attempt.LatencyMillis <= 0 || attempt.RetryCount != index ||
				attempt.UsageTokens < 0 || attempt.CostMicros < 0 ||
				attempt.UsageTokens > shot.MaxVideoTokens {
				return LiveEvaluation{}, fmt.Errorf("shot %q attempt %d is invalid", shotEvidence.ShotID, index+1)
			}
			manifest, hash, err := verifyAttemptManifest(plan, shot, attempt)
			if err != nil {
				return LiveEvaluation{}, fmt.Errorf(
					"shot %q attempt %d manifest: %w",
					shotEvidence.ShotID,
					index+1,
					err,
				)
			}
			if _, duplicate := manifestHashes[hash]; duplicate {
				return LiveEvaluation{}, fmt.Errorf("duplicate manifest hash %q", hash)
			}
			manifestHashes[hash] = struct{}{}
			jobKey := strings.Join([]string{
				manifest.Provider.Name,
				manifest.Provider.Region,
				manifest.Provider.JobID,
			}, "\x00")
			if _, duplicate := providerJobs[jobKey]; duplicate {
				return LiveEvaluation{}, fmt.Errorf("duplicate provider job provenance for %q", manifest.Provider.JobID)
			}
			providerJobs[jobKey] = struct{}{}
			requestKey := strings.Join([]string{
				manifest.Provider.Name,
				manifest.Provider.Region,
				manifest.Provider.RequestID,
			}, "\x00")
			if _, duplicate := providerRequests[requestKey]; duplicate {
				return LiveEvaluation{}, fmt.Errorf(
					"duplicate provider request provenance for %q",
					manifest.Provider.RequestID,
				)
			}
			providerRequests[requestKey] = struct{}{}
			groupKey, err := measurementGroupKey(manifest)
			if err != nil {
				return LiveEvaluation{}, err
			}
			verifiedAttempts = append(verifiedAttempts, verifiedLiveAttempt{
				result:   attempt,
				manifest: manifest,
				groupKey: groupKey,
			})
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
			passed, err := validateQualityReviews(plan.QualityRubric, shotEvidence)
			if err != nil {
				return LiveEvaluation{}, fmt.Errorf("shot %q quality reviews: %w", shotEvidence.ShotID, err)
			}
			if !passed {
				evaluation.QualityPassed = false
			}
		}
	}
	coldLatencies, hotLatencies, unclassified, err := deriveTemperatureLatencies(
		plan.Measurement,
		verifiedAttempts,
	)
	if err != nil {
		return LiveEvaluation{}, err
	}
	if plan.Acceptance.RequireColdAndHot && (len(coldLatencies) == 0 || len(hotLatencies) == 0) {
		return LiveEvaluation{}, errors.New("evidence requires both cold and hot latency samples")
	}
	evaluation.ColdP95LatencyMillis, _ = NearestRankPercentile(coldLatencies, 95)
	evaluation.HotP95LatencyMillis, _ = NearestRankPercentile(hotLatencies, 95)
	evaluation.ColdSamples = len(coldLatencies)
	evaluation.HotSamples = len(hotLatencies)
	evaluation.UnclassifiedSamples = unclassified
	evaluation.SuccessRateBPS = evaluation.SucceededShots * 10_000 / evaluation.PlannedShots
	evaluation.AcceptancePassed = evaluation.SuccessRateBPS >= plan.Acceptance.MinimumSuccessRateBPS &&
		evaluation.TotalRetries <= plan.Acceptance.MaximumTotalRetries &&
		evaluation.TotalCostMicros <= plan.HardBudgetMicros &&
		evaluation.QualityPassed
	return evaluation, nil
}

func verifyAttemptManifest(
	plan LiveTestPlan,
	shot LiveTestShot,
	attempt LiveAttemptResult,
) (GenerationManifest, string, error) {
	if len(attempt.ManifestBytes) == 0 {
		return GenerationManifest{}, "", errors.New("manifest bytes are required")
	}
	if len(attempt.ManifestBytes) > maxManifestEvidenceBytes {
		return GenerationManifest{}, "", errors.New("manifest bytes exceed the evidence size limit")
	}
	sum := sha256.Sum256(attempt.ManifestBytes)
	hash := hex.EncodeToString(sum[:])
	if !strings.EqualFold(hash, attempt.ManifestSHA256) {
		return GenerationManifest{}, "", errors.New("manifest SHA-256 does not match its bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(attempt.ManifestBytes))
	decoder.DisallowUnknownFields()
	var manifest GenerationManifest
	if err := decoder.Decode(&manifest); err != nil {
		return GenerationManifest{}, "", fmt.Errorf("decode strict manifest JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return GenerationManifest{}, "", err
	}
	if err := manifest.Validate(); err != nil {
		return GenerationManifest{}, "", fmt.Errorf("validate manifest: %w", err)
	}
	canonical, err := marshalGenerationManifest(manifest)
	if err != nil {
		return GenerationManifest{}, "", err
	}
	if !bytes.Equal(canonical, attempt.ManifestBytes) {
		return GenerationManifest{}, "", errors.New("manifest bytes are not canonical writer output")
	}
	switch {
	case manifest.Evidence != EvidenceLiveProvider:
		return GenerationManifest{}, "", errors.New("manifest is not live provider evidence")
	case manifest.ShotID != shot.ID:
		return GenerationManifest{}, "", errors.New("manifest shot_id does not match the plan")
	case manifest.Attempt.Number != attempt.Number:
		return GenerationManifest{}, "", errors.New("manifest attempt number does not match evidence")
	case manifest.Status != attempt.Status:
		return GenerationManifest{}, "", errors.New("manifest status does not match evidence")
	case !manifest.Attempt.StartedAt.Equal(attempt.StartedAt):
		return GenerationManifest{}, "", errors.New("manifest start time does not match evidence")
	case !manifest.Attempt.CompletedAt.Equal(attempt.CompletedAt):
		return GenerationManifest{}, "", errors.New("manifest completion time does not match evidence")
	case manifest.Attempt.LatencyMillis != attempt.LatencyMillis:
		return GenerationManifest{}, "", errors.New("manifest latency does not match evidence")
	case manifest.Usage.VideoTokens != attempt.UsageTokens:
		return GenerationManifest{}, "", errors.New("manifest usage does not match evidence")
	case manifest.Usage.ProviderCostMicros != attempt.CostMicros:
		return GenerationManifest{}, "", errors.New("manifest cost does not match evidence")
	case manifest.Provider.Name != plan.Provider:
		return GenerationManifest{}, "", errors.New("manifest provider does not match the plan")
	case manifest.Modality != ModalityVideo:
		return GenerationManifest{}, "", errors.New("manifest modality is not video")
	case manifest.PromptSnapshot != shot.PromptSnapshotID:
		return GenerationManifest{}, "", errors.New("manifest prompt snapshot does not match the plan")
	case manifest.Requested != shot.Output:
		return GenerationManifest{}, "", errors.New("manifest requested output does not match the plan")
	case manifest.Budget.MaxAttempts != shot.MaxAttempts ||
		manifest.Budget.MaxCostMicros != shot.MaxCostMicros:
		return GenerationManifest{}, "", errors.New("manifest budget does not match the plan")
	case manifest.Usage.VideoTokens > shot.MaxVideoTokens:
		return GenerationManifest{}, "", errors.New("manifest usage exceeds the shot ceiling")
	case manifest.Usage.ProviderCostMicros > shot.MaxCostMicros:
		return GenerationManifest{}, "", errors.New("manifest cost exceeds the shot ceiling")
	}
	if manifest.Status == StatusSucceeded {
		actual := manifest.Actual
		if actual == nil ||
			actual.Resolution != fmt.Sprintf("%dp", shot.Output.Height) ||
			actual.AspectRatio != shot.Output.AspectRatio ||
			actual.FPS != shot.Output.FPS ||
			actual.DurationMillis != shot.Output.DurationMillis ||
			actual.Format != shot.Output.Format {
			return GenerationManifest{}, "", errors.New("manifest actual output does not match the PoC contract")
		}
	}
	if err := validateManifestAssetRevisions(shot.ReferenceAssetRevisionIDs, manifest.InputAssets); err != nil {
		return GenerationManifest{}, "", err
	}
	return manifest, hash, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("manifest contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing manifest JSON: %w", err)
	}
	return nil
}

func validateManifestAssetRevisions(expected []string, actual []AssetEvidence) error {
	if len(expected) != len(actual) {
		return errors.New("manifest input asset count does not match the plan")
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, revision := range expected {
		if strings.TrimSpace(revision) == "" {
			return errors.New("plan contains an empty asset revision")
		}
		if _, duplicate := expectedSet[revision]; duplicate {
			return fmt.Errorf("plan contains duplicate asset revision %q", revision)
		}
		expectedSet[revision] = struct{}{}
	}
	actualSet := make(map[string]struct{}, len(actual))
	for _, asset := range actual {
		revision := asset.ID + "@" + asset.Revision
		if _, duplicate := actualSet[revision]; duplicate {
			return fmt.Errorf("manifest contains duplicate input asset %q", revision)
		}
		actualSet[revision] = struct{}{}
		if _, ok := expectedSet[revision]; !ok {
			return fmt.Errorf("manifest input asset %q does not match the plan", revision)
		}
	}
	return nil
}

func measurementGroupKey(manifest GenerationManifest) (string, error) {
	key := struct {
		Provider  string     `json:"provider"`
		Model     string     `json:"model"`
		Region    string     `json:"region"`
		Requested OutputSpec `json:"requested"`
	}{
		Provider:  manifest.Provider.Name,
		Model:     manifest.Provider.Model,
		Region:    manifest.Provider.Region,
		Requested: manifest.Requested,
	}
	data, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("encode measurement group: %w", err)
	}
	return string(data), nil
}

// deriveTemperatureLatencies assigns temperature from the verified invocation
// sequence. The first attempt in each model/spec group is an unclassified
// baseline. Later attempts are cold after the configured idle period, hot
// inside the configured window, or unclassified between those thresholds.
func deriveTemperatureLatencies(
	protocol MeasurementProtocol,
	attempts []verifiedLiveAttempt,
) ([]int64, []int64, int, error) {
	groups := make(map[string][]verifiedLiveAttempt)
	for _, attempt := range attempts {
		groups[attempt.groupKey] = append(groups[attempt.groupKey], attempt)
	}
	var cold, hot []int64
	unclassified := 0
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool {
			if group[i].manifest.Attempt.StartedAt.Equal(group[j].manifest.Attempt.StartedAt) {
				return group[i].manifest.ManifestID < group[j].manifest.ManifestID
			}
			return group[i].manifest.Attempt.StartedAt.Before(group[j].manifest.Attempt.StartedAt)
		})
		if len(group) > 0 {
			unclassified++
		}
		for index := 1; index < len(group); index++ {
			previous := group[index-1].manifest.Attempt
			current := group[index].manifest.Attempt
			if current.StartedAt.Before(previous.CompletedAt) {
				return nil, nil, 0, errors.New("model/spec invocation sequence overlaps")
			}
			idle := current.StartedAt.Sub(previous.CompletedAt)
			switch {
			case idle >= time.Duration(protocol.ColdIdleMinSeconds)*time.Second:
				cold = append(cold, group[index].result.LatencyMillis)
			case idle <= time.Duration(protocol.HotWindowMaxSeconds)*time.Second:
				hot = append(hot, group[index].result.LatencyMillis)
			default:
				unclassified++
			}
		}
	}
	return cold, hot, unclassified, nil
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

func validateQualityReviews(rubric QualityRubric, shot LiveShotEvidence) (bool, error) {
	if len(shot.Reviews) < rubric.RequiredReviewers {
		return false, errors.New("not enough quality reviewers")
	}
	passed := true
	reviewers := make(map[string]struct{}, len(shot.Reviews))
	for _, review := range shot.Reviews {
		reviewerID := strings.TrimSpace(review.ReviewerID)
		if reviewerID == "" {
			return false, errors.New("quality reviewer ID is required")
		}
		if _, duplicate := reviewers[reviewerID]; duplicate {
			return false, fmt.Errorf("duplicate quality reviewer %q", reviewerID)
		}
		reviewers[reviewerID] = struct{}{}
		if review.SevereArtifact {
			passed = false
		}
		if len(review.Scores) != len(rubric.Dimensions) {
			return false, fmt.Errorf("reviewer %q submitted an incomplete score set", reviewerID)
		}
		weighted := 0
		for _, dimension := range rubric.Dimensions {
			score, ok := review.Scores[dimension.Name]
			if !ok || score < rubric.ScaleMin || score > rubric.ScaleMax {
				return false, fmt.Errorf("reviewer %q submitted an invalid quality score", reviewerID)
			}
			if score < rubric.MinimumDimensionScore {
				passed = false
			}
			weighted += score * dimension.WeightBPS
		}
		if weighted/10 < rubric.MinimumWeightedAverageMilli {
			passed = false
		}
	}
	if len(reviewers) < rubric.RequiredReviewers {
		return false, errors.New("not enough independent quality reviewers")
	}
	return passed, nil
}
