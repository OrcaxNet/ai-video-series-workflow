// Command video-stage1-runner is the only executable submit/completion path
// for the formal FLO-104 Stage 1 batch. Every invocation is one operation read
// from stdin; the command has no automatic retry loop.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.LookupEnv); err != nil {
		log.Fatalf("stage 1 runner failed: %v", err)
	}
}

func run(
	ctx context.Context,
	args []string,
	input io.Reader,
	output io.Writer,
	lookup func(string) (string, bool),
) error {
	if len(args) != 1 || args[0] != "submit" && args[0] != "retry" && args[0] != "poll" &&
		args[0] != "complete" && args[0] != "finalize-input" && args[0] != "finalize" {
		return errors.New("usage: video-stage1-runner <submit|retry|poll|complete|finalize-input|finalize> < invocation.json")
	}
	planPath, err := requiredEnv(lookup, "VIDEO_STAGE1_PLAN_PATH")
	if err != nil {
		return err
	}
	ledgerPath, err := requiredEnv(lookup, "VIDEO_STAGE1_LEDGER_PATH")
	if err != nil {
		return err
	}
	executionPackagePath, err := requiredEnv(lookup, "VIDEO_STAGE1_EXECUTION_PACKAGE_PATH")
	if err != nil {
		return err
	}
	postgresDSN, err := requiredEnv(lookup, "VIDEO_POSTGRES_DSN")
	if err != nil {
		return err
	}
	adapterURL, err := requiredEnv(lookup, "VIDEO_PROVIDER_ADAPTER_URL")
	if err != nil {
		return err
	}
	serviceAuth, err := requiredEnv(lookup, "VIDEO_PROVIDER_SERVICE_AUTH_SECRET")
	if err != nil {
		return err
	}
	artifactRoot, err := requiredEnv(lookup, "VIDEO_ARTIFACT_ROOT")
	if err != nil {
		return err
	}

	planFile, err := os.Open(planPath)
	if err != nil {
		return fmt.Errorf("open immutable stage 1 plan: %w", err)
	}
	defer planFile.Close()
	var plan stage1.Plan
	if err := decodeOne(planFile, &plan); err != nil {
		return fmt.Errorf("decode immutable stage 1 plan: %w", err)
	}
	executionPackageFile, err := os.Open(executionPackagePath)
	if err != nil {
		return fmt.Errorf("open immutable stage 1 execution package: %w", err)
	}
	defer executionPackageFile.Close()
	var executionPackage stage1.ExecutionPackage
	if err := decodeOne(executionPackageFile, &executionPackage); err != nil {
		return fmt.Errorf("decode immutable stage 1 execution package: %w", err)
	}
	if err := validateImmutableExecutionPackage(plan, executionPackage); err != nil {
		return err
	}
	if err := requireExactLiveBuild(executionPackage); err != nil {
		return err
	}
	parentExecutionPackage, err := loadRevisionParent(plan, executionPackage, lookup)
	if err != nil {
		return err
	}
	gate, err := stage1.Open(plan, ledgerPath)
	if err != nil {
		return err
	}
	store, err := repository.Open(ctx, postgresDSN, repository.PoolConfig{})
	if err != nil {
		return fmt.Errorf("connect to stage 1 PostgreSQL product truth: %w", err)
	}
	defer store.Close()
	client, err := volcengineprovider.AuthenticatedHTTPClient(
		&http.Client{Timeout: 2 * time.Minute}, serviceAuth,
	)
	if err != nil {
		return err
	}
	adapter, err := stage1.NewAdapterSubmitter(adapterURL, client)
	if err != nil {
		return err
	}
	artifacts, err := artifactstore.New(artifactRoot)
	if err != nil {
		return err
	}
	var runner *stage1.Runner
	retryPath, hasRetryPath := lookup("VIDEO_STAGE1_RETRY_PACKAGE_PATH")
	if strings.TrimSpace(retryPath) != "" {
		retryFile, openErr := os.Open(retryPath)
		if openErr != nil {
			if args[0] == "retry" || args[0] == "finalize-input" {
				return fmt.Errorf("open immutable stage 1 controlled retry package: %w", openErr)
			}
		} else {
			defer retryFile.Close()
			var retryPackage stage1.ControlledRetryPackage
			if decodeErr := decodeOne(retryFile, &retryPackage); decodeErr != nil {
				return fmt.Errorf("decode immutable stage 1 controlled retry package: %w", decodeErr)
			}
			runner, err = stage1.NewRunnerWithControlledRetry(
				gate, adapter, artifacts, store, executionPackage, retryPackage,
			)
		}
	}
	if runner == nil && err == nil {
		if args[0] == "retry" {
			if !hasRetryPath || strings.TrimSpace(retryPath) == "" {
				return errors.New("VIDEO_STAGE1_RETRY_PACKAGE_PATH is required for a controlled retry")
			}
			return errors.New("stage 1 controlled retry package is unavailable")
		}
		if parentExecutionPackage != nil {
			runner, err = stage1.NewRunnerWithExecutionPackageRevision(
				gate, adapter, artifacts, store, *parentExecutionPackage, executionPackage,
			)
		} else {
			runner, err = stage1.NewRunner(gate, adapter, artifacts, store, executionPackage)
		}
	}
	if err != nil {
		return err
	}

	var result any
	switch args[0] {
	case "submit":
		var request stage1.SubmitInput
		if err := decodeOne(input, &request); err != nil {
			return fmt.Errorf("decode stage 1 submit invocation: %w", err)
		}
		result, err = runner.Submit(ctx, request)
	case "retry":
		var request stage1.SubmitInput
		if err := decodeOne(input, &request); err != nil {
			return fmt.Errorf("decode stage 1 controlled retry invocation: %w", err)
		}
		result, err = runner.SubmitControlledRetry(ctx, request)
	case "poll":
		var request stage1.PollInput
		if err := decodeOne(input, &request); err != nil {
			return fmt.Errorf("decode stage 1 poll invocation: %w", err)
		}
		result, err = runner.Poll(ctx, request)
	case "complete":
		var request stage1.CompleteInput
		if err := decodeOne(input, &request); err != nil {
			return fmt.Errorf("decode stage 1 completion invocation: %w", err)
		}
		result, err = runner.Complete(ctx, request)
	case "finalize-input":
		if err := requireEmptyInput(input); err != nil {
			return err
		}
		result, err = runner.FinalizationInput()
	case "finalize":
		if err := requireEmptyInput(input); err != nil {
			return err
		}
		finalizationInput, inputErr := runner.FinalizationInput()
		if inputErr != nil {
			return inputErr
		}
		temporalAddress, envErr := requiredEnv(lookup, "VIDEO_TEMPORAL_ADDRESS")
		if envErr != nil {
			return envErr
		}
		temporalNamespace, envErr := requiredEnv(lookup, "VIDEO_TEMPORAL_NAMESPACE")
		if envErr != nil {
			return envErr
		}
		temporalTaskQueue, envErr := requiredEnv(lookup, "VIDEO_TEMPORAL_TASK_QUEUE")
		if envErr != nil {
			return envErr
		}
		temporalClient, dialErr := temporalclient.Dial(temporalclient.Options{
			HostPort: temporalAddress, Namespace: temporalNamespace,
		})
		if dialErr != nil {
			return fmt.Errorf("connect to Stage 1 finalization Temporal: %w", dialErr)
		}
		defer temporalClient.Close()
		workflowID := stage1FinalizationWorkflowID(executionPackage)
		workflowRun, startErr := temporalClient.ExecuteWorkflow(
			ctx,
			stage1FinalizationStartOptions(workflowID, temporalTaskQueue),
			orchestration.Stage1FinalizationWorkflowName,
			finalizationInput,
		)
		if startErr != nil {
			return fmt.Errorf("start or recover Stage 1 finalization workflow: %w", startErr)
		}
		var finalization orchestration.Stage1FinalizationResult
		if getErr := workflowRun.Get(ctx, &finalization); getErr != nil {
			return fmt.Errorf("await Stage 1 finalization workflow: %w", getErr)
		}
		result = finalization
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

func stage1FinalizationWorkflowID(executionPackage stage1.ExecutionPackage) string {
	return "stage1-finalization-" + executionPackage.BatchID + "-" +
		executionPackage.ContentHash[:16] + "-" + postproduction.AlgorithmRevision
}

func stage1FinalizationStartOptions(
	workflowID string,
	taskQueue string,
) temporalclient.StartWorkflowOptions {
	return temporalclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		// The immutable workflow ID must recover an in-flight run and may start
		// again only after a fail-closed terminal failure. A successful formal
		// finalization remains permanently non-duplicable.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}
}

func requireEmptyInput(reader io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(reader, 1024))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != "" {
		return errors.New("stage 1 finalize-input accepts no caller-supplied fields")
	}
	return nil
}

func validateImmutableExecutionPackage(plan stage1.Plan, package_ stage1.ExecutionPackage) error {
	if err := package_.Validate(plan); err != nil {
		cause := fmt.Errorf("validate immutable stage 1 execution package: %w", err)
		if package_.RequiresRevisionParent() {
			return stage1.UnverifiableRevisionParentError(cause)
		}
		return cause
	}
	return nil
}

func requireExactLiveBuild(package_ stage1.ExecutionPackage) error {
	if package_.LiveActivation == nil {
		return nil
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("live execution requires verifiable Go VCS build information")
	}
	var revision, modified string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	if revision != package_.LiveActivation.SourceCodeCommit || modified != "false" {
		return errors.New("live execution binary is not the exact clean source commit authorized by the execution package")
	}
	return nil
}

func loadRevisionParent(
	plan stage1.Plan,
	child stage1.ExecutionPackage,
	lookup func(string) (string, bool),
) (*stage1.ExecutionPackage, error) {
	if !child.RequiresRevisionParent() {
		return nil, nil
	}
	parentPath, err := requiredEnv(lookup, "VIDEO_STAGE1_PARENT_EXECUTION_PACKAGE_PATH")
	if err != nil {
		return nil, stage1.UnverifiableRevisionParentError(err)
	}
	parentFile, err := os.Open(parentPath)
	if err != nil {
		return nil, stage1.UnverifiableRevisionParentError(
			fmt.Errorf("open immutable stage 1 parent execution package: %w", err),
		)
	}
	defer parentFile.Close()
	var parent stage1.ExecutionPackage
	if err := decodeOne(parentFile, &parent); err != nil {
		return nil, stage1.UnverifiableRevisionParentError(
			fmt.Errorf("decode immutable stage 1 parent execution package: %w", err),
		)
	}
	if err := child.ValidateRevision(plan, parent); err != nil {
		return nil, stage1.UnverifiableRevisionParentError(
			fmt.Errorf("validate immutable stage 1 execution package revision: %w", err),
		)
	}
	return &parent, nil
}

func decodeOne(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("input must contain exactly one JSON value")
	}
	return nil
}

func requiredEnv(lookup func(string) (string, bool), name string) (string, error) {
	value, present := lookup(name)
	if !present || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
