// Command video-stage1-authorize-speech derives an ordered speech-v2 job
// allowlist and emits a prompt-free child Stage 1 package. It has no Provider
// Adapter URL and cannot submit speech or video jobs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1materialize"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr, os.LookupEnv); err != nil {
		log.Fatalf("Stage 1 speech authorization failed: %v", err)
	}
}

func run(
	ctx context.Context,
	args []string,
	stderr io.Writer,
	lookup func(string) (string, bool),
) error {
	flags := flag.NewFlagSet("video-stage1-authorize-speech", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var planPath, parentPath, authorizationPath, outputPath, reportPath string
	var approvalComment, approvalActor, approvalValidUntil string
	flags.StringVar(&planPath, "plan", "", "Stage 1 readiness plan")
	flags.StringVar(&parentPath, "parent", "", "current execution package")
	flags.StringVar(&authorizationPath, "authorization", "", "approved ordered speech batch input")
	flags.StringVar(&outputPath, "output", "", "authorized child execution package output")
	flags.StringVar(&reportPath, "report", "", "no-cost speech authorization report output")
	flags.StringVar(&approvalComment, "approval-comment", "", "approval comment UUID")
	flags.StringVar(&approvalActor, "approval-actor", "", "approving actor UUID")
	flags.StringVar(&approvalValidUntil, "approval-valid-until", "", "approval expiry RFC3339")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || planPath == "" || parentPath == "" || authorizationPath == "" ||
		outputPath == "" || reportPath == "" {
		return errors.New("plan, parent, authorization, output, and report are required")
	}
	validUntil, err := time.Parse(time.RFC3339, approvalValidUntil)
	if err != nil {
		return errors.New("approval-valid-until must be RFC3339")
	}
	var plan stage1.Plan
	if err := decodeFile(planPath, &plan); err != nil {
		return fmt.Errorf("read Stage 1 plan: %w", err)
	}
	var parent stage1.ExecutionPackage
	if err := decodeFile(parentPath, &parent); err != nil {
		return fmt.Errorf("read parent execution package: %w", err)
	}
	var authorization stage1materialize.SpeechBatchRevisionInput
	if err := decodeFile(authorizationPath, &authorization); err != nil {
		return fmt.Errorf("read speech batch authorization: %w", err)
	}
	dsn, err := requiredEnv(lookup, "VIDEO_POSTGRES_DSN")
	if err != nil {
		return err
	}
	artifactRoot, err := requiredEnv(lookup, "VIDEO_ARTIFACT_ROOT")
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	cas, err := artifactstore.New(artifactRoot)
	if err != nil {
		return err
	}
	revised, report, err := stage1materialize.AuthorizeSpeechBatch(
		ctx, pool, cas, plan, parent, authorization, stage1materialize.Approval{
			CommentID: approvalComment, ActorID: approvalActor, ValidUntil: validUntil.UTC(),
		},
	)
	if err != nil {
		return err
	}
	if err := writeJSONAtomically(outputPath, revised); err != nil {
		return fmt.Errorf("write revised execution package: %w", err)
	}
	if err := writeJSONAtomically(reportPath, report); err != nil {
		return fmt.Errorf("write speech authorization report: %w", err)
	}
	return nil
}

func decodeFile(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("file must contain exactly one JSON value")
	}
	return nil
}

func writeJSONAtomically(path string, value any) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".stage1-speech-authorization-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	encoder := json.NewEncoder(temporary)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func requiredEnv(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
