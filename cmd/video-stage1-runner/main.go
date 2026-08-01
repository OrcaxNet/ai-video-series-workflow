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
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
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
	if len(args) != 1 || args[0] != "submit" && args[0] != "retry" &&
		args[0] != "complete" && args[0] != "finalize-input" {
		return errors.New("usage: video-stage1-runner <submit|retry|complete|finalize-input> < invocation.json")
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
	if err := executionPackage.Validate(plan); err != nil {
		return fmt.Errorf("validate immutable stage 1 execution package: %w", err)
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
		runner, err = stage1.NewRunner(gate, adapter, artifacts, store, executionPackage)
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
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
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
