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
	if len(args) != 1 || args[0] != "submit" && args[0] != "complete" {
		return errors.New("usage: video-stage1-runner <submit|complete> < invocation.json")
	}
	planPath, err := requiredEnv(lookup, "VIDEO_STAGE1_PLAN_PATH")
	if err != nil {
		return err
	}
	ledgerPath, err := requiredEnv(lookup, "VIDEO_STAGE1_LEDGER_PATH")
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
	gate, err := stage1.Open(plan, ledgerPath)
	if err != nil {
		return err
	}
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
	runner, err := stage1.NewRunner(gate, adapter, artifacts)
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
	case "complete":
		var request stage1.CompleteInput
		if err := decodeOne(input, &request); err != nil {
			return fmt.Errorf("decode stage 1 completion invocation: %w", err)
		}
		result, err = runner.Complete(ctx, request)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
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
