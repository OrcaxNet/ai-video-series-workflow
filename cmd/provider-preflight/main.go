// Command provider-preflight validates the FLO-110 provider boundary without
// requiring credentials. A deliberately gated live-auth mode performs one
// minimal text request only after an operator supplies runtime configuration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const defaultPlanPath = "docs/flo-110/live-test-plan.json"

type checkResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type report struct {
	Evidence string        `json:"evidence"`
	Status   string        `json:"status"`
	Checks   []checkResult `json:"checks"`
	LiveJob  *liveJob      `json:"live_job,omitempty"`
}

type liveJob struct {
	Provider          string                     `json:"provider"`
	Model             string                     `json:"model"`
	JobID             string                     `json:"job_id"`
	ProviderRequestID string                     `json:"provider_request_id,omitempty"`
	Status            providercontract.JobStatus `json:"status"`
	Usage             providercontract.Usage     `json:"usage"`
}

func main() {
	var (
		mode        = flag.String("mode", "mock", "mock, validate-plan, scan, or live-auth")
		planPath    = flag.String("plan", defaultPlanPath, "path to the pending-key live test plan")
		confirmLive = flag.Bool("confirm-live", false, "confirm that live-auth may make one billable request")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var (
		result report
		err    error
	)
	switch *mode {
	case "mock":
		result, err = runMock(ctx)
	case "validate-plan":
		result, err = validatePlan(*planPath)
	case "scan":
		result, err = scanRepository([]string{
			"cmd/provider-preflight",
			"docs/flo-110",
			"internal/providercontract",
			"scripts/flo110-preflight.sh",
		})
	case "live-auth":
		result, err = runLiveAuth(ctx, *confirmLive)
	default:
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		safe := providercontract.Redact(
			err.Error(),
			os.Getenv("ARK_API_KEY"),
			os.Getenv("ANTHROPIC_API_KEY"),
			os.Getenv("ANTHROPIC_AUTH_TOKEN"),
		)
		fmt.Fprintln(os.Stderr, safe)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "encode preflight report:", err)
		os.Exit(1)
	}
}

func runMock(ctx context.Context) (report, error) {
	result := report{Evidence: "mock_only", Status: "passed"}
	errorScenarios := []struct {
		name     string
		scenario providercontract.FakeScenario
		want     providercontract.ErrorCode
	}{
		{name: "401 authentication", scenario: providercontract.FakeUnauthorized, want: providercontract.CodeUnauthenticated},
		{name: "403 authorization", scenario: providercontract.FakeForbidden, want: providercontract.CodeForbidden},
		{name: "429 rate limit", scenario: providercontract.FakeRateLimited, want: providercontract.CodeRateLimited},
		{name: "5xx recovery policy", scenario: providercontract.FakeServerError, want: providercontract.CodeUnavailable},
		{name: "timeout", scenario: providercontract.FakeTimeout, want: providercontract.CodeTimeout},
		{name: "quota exhausted", scenario: providercontract.FakeQuotaExceeded, want: providercontract.CodeQuotaExceeded},
		{name: "content blocked", scenario: providercontract.FakeContentBlocked, want: providercontract.CodeContentBlocked},
	}
	for _, scenario := range errorScenarios {
		_, err := providercontract.NewFakeProvider(scenario.scenario).Submit(ctx, mockRequest())
		if got := providercontract.ErrorCodeOf(err); got != scenario.want {
			return report{}, fmt.Errorf("%s returned %q, expected %q", scenario.name, got, scenario.want)
		}
		result.Checks = append(result.Checks, checkResult{Name: scenario.name, Status: "passed"})
	}

	success := providercontract.NewFakeProvider(providercontract.FakeSuccess)
	request := mockRequest()
	first, err := success.Submit(ctx, request)
	if err != nil {
		return report{}, err
	}
	replayed, err := success.Submit(ctx, request)
	if err != nil || first.ID != replayed.ID {
		return report{}, errors.New("idempotent replay created a different job")
	}
	if _, err := success.Poll(ctx, first.ID); err != nil {
		return report{}, err
	}
	completed, err := success.Poll(ctx, first.ID)
	if err != nil || completed.Status != providercontract.StatusSucceeded || completed.Output == nil {
		return report{}, errors.New("success lifecycle did not reach a durable output")
	}
	result.Checks = append(result.Checks,
		checkResult{Name: "success lifecycle", Status: "passed"},
		checkResult{Name: "idempotent submit", Status: "passed"},
	)

	callbackProvider := providercontract.NewFakeProvider(providercontract.FakeDuplicateCallback)
	callbackJob, err := callbackProvider.Submit(ctx, mockRequest())
	if err != nil {
		return report{}, err
	}
	callback := providercontract.Callback{
		EventID:   "event-preflight-1",
		JobID:     callbackJob.ID,
		Status:    providercontract.StatusRunning,
		CreatedAt: time.Unix(1_800_000_010, 0).UTC(),
	}
	if applied, _, err := callbackProvider.ApplyCallback(callback); err != nil || !applied {
		return report{}, errors.New("first callback was not applied")
	}
	if applied, _, err := callbackProvider.ApplyCallback(callback); err != nil || applied {
		return report{}, errors.New("duplicate callback was not deduplicated")
	}
	result.Checks = append(result.Checks, checkResult{Name: "duplicate callback", Status: "passed"})

	cancelProvider := providercontract.NewFakeProvider(providercontract.FakeSuccess)
	cancelJob, err := cancelProvider.Submit(ctx, mockRequest())
	if err != nil {
		return report{}, err
	}
	cancelled, err := cancelProvider.Cancel(ctx, cancelJob.ID)
	if err != nil || cancelled.Status != providercontract.StatusCancelled {
		return report{}, errors.New("queued job was not cancelled")
	}
	result.Checks = append(result.Checks, checkResult{Name: "cancel lifecycle", Status: "passed"})

	recoveryProvider := providercontract.NewFakeProvider(providercontract.FakeRecovery)
	if _, err := recoveryProvider.Submit(ctx, request); providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
		return report{}, errors.New("recovery fixture did not simulate an ambiguous failure")
	}
	recovered, err := recoveryProvider.Submit(ctx, request)
	if err != nil || recovered.ID == "" {
		return report{}, errors.New("idempotent recovery did not return the accepted job")
	}
	result.Checks = append(result.Checks, checkResult{Name: "ambiguous-submit recovery", Status: "passed"})
	return result, nil
}

func validatePlan(path string) (report, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return report{}, fmt.Errorf("read live test plan: %w", err)
	}
	var plan providercontract.LiveTestPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return report{}, fmt.Errorf("decode live test plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return report{}, fmt.Errorf("validate live test plan: %w", err)
	}
	shots := 0
	for _, category := range plan.Categories {
		shots += len(category.Shots)
	}
	return report{
		Evidence: "plan_only_pending_key",
		Status:   "passed",
		Checks: []checkResult{{
			Name:   "live test plan",
			Status: "passed",
			Detail: fmt.Sprintf("%d categories, %d shots, hard budget %.6f CNY", len(plan.Categories), shots, float64(plan.HardBudgetMicros)/1_000_000),
		}},
	}, nil
}

func scanRepository(paths []string) (report, error) {
	result := report{Evidence: "static_scan", Status: "passed"}
	for _, root := range paths {
		info, err := os.Stat(root)
		if err != nil {
			return report{}, fmt.Errorf("stat %s: %w", root, err)
		}
		if !info.IsDir() {
			if err := scanFile(root); err != nil {
				return report{}, err
			}
			result.Checks = append(result.Checks, checkResult{Name: root, Status: "passed"})
			continue
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			return scanFile(path)
		})
		if err != nil {
			return report{}, err
		}
		result.Checks = append(result.Checks, checkResult{Name: root, Status: "passed"})
	}
	return result, nil
}

func scanFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > 2<<20 {
		return fmt.Errorf("%s exceeds the scanner size limit", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if providercontract.ContainsPotentialSecret(string(data)) {
		return fmt.Errorf("potential credential literal found in %s", path)
	}
	return nil
}

func runLiveAuth(ctx context.Context, confirmed bool) (report, error) {
	if !confirmed {
		return report{}, errors.New("live-auth requires --confirm-live because it may be billable")
	}
	apiKey := strings.TrimSpace(os.Getenv("ARK_API_KEY"))
	model := strings.TrimSpace(os.Getenv("ARK_LLM_MODEL"))
	if apiKey == "" || model == "" {
		return report{}, errors.New("live-auth requires runtime ARK_API_KEY and ARK_LLM_MODEL")
	}
	provider, err := providercontract.NewVolcengineProvider(providercontract.VolcengineConfig{
		APIKey: apiKey,
		Models: providercontract.VolcengineModels{Text: model},
	})
	if err != nil {
		return report{}, err
	}
	requestID := fmt.Sprintf("flo110-live-%d", time.Now().Unix())
	job, err := provider.Submit(ctx, providercontract.GenerationRequest{
		RequestID:        requestID,
		IdempotencyKey:   requestID,
		Modality:         providercontract.ModalityText,
		Prompt:           "Reply with exactly: ok",
		PromptSnapshotID: "flo110-live-auth-v1",
		Budget: providercontract.BudgetEnvelope{
			EstimatedCostMicros: 10_000,
			MaxCostMicros:       100_000,
			MaxAttempts:         1,
		},
	})
	if err != nil {
		return report{}, err
	}
	var usage providercontract.Usage
	if job.Output != nil {
		usage = job.Output.Usage
	}
	return report{
		Evidence: "live_provider_call",
		Status:   "passed",
		Checks: []checkResult{{
			Name:   "runtime authentication and model availability",
			Status: "passed",
		}},
		LiveJob: &liveJob{
			Provider:          job.Provider,
			Model:             job.ProviderModel,
			JobID:             job.ID,
			ProviderRequestID: job.ProviderRequestID,
			Status:            job.Status,
			Usage:             usage,
		},
	}, nil
}

func mockRequest() providercontract.GenerationRequest {
	return providercontract.GenerationRequest{
		RequestID:        "request-preflight-1",
		IdempotencyKey:   "idempotency-preflight-1",
		Modality:         providercontract.ModalityVideo,
		Prompt:           "fixture prompt",
		PromptSnapshotID: "prompt-fixture-v1",
		Context: providercontract.ContextRefs{
			SeriesSnapshotID:  "series-context-v1",
			EpisodeSnapshotID: "episode-context-v1",
			SceneSnapshotID:   "scene-context-v1",
			ShotSnapshotID:    "shot-context-v1",
		},
		Assets: []providercontract.AssetRef{{
			ID:               "asset-character",
			Revision:         "rev-fixture-1",
			Kind:             providercontract.ModalityImage,
			Role:             providercontract.AssetRoleReferenceImage,
			URI:              "https://example.invalid/reference.png",
			SHA256:           "c0a7fbcf5858bf143029646d0c6e290837322165eed55443a0560ae230a4f858",
			LicenseReference: "fixture-license",
		}},
		Output: providercontract.OutputSpec{
			Width:          1280,
			Height:         720,
			AspectRatio:    "16:9",
			FPS:            24,
			DurationMillis: 5_000,
			Format:         "mp4",
		},
		Budget: providercontract.BudgetEnvelope{
			EstimatedCostMicros: 23_000_000,
			MaxCostMicros:       46_000_000,
			MaxAttempts:         2,
		},
	}
}
