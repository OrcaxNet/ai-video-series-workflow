// Command video-stage1-materialize imports one fixed, ADMIN-approved Stage 1
// product package into PostgreSQL/CAS and emits a prompt-free execution file.
// It deliberately has no Provider Adapter URL, client, or submit operation.
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
		log.Fatalf("stage 1 materialization failed: %v", err)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer, lookup func(string) (string, bool)) error {
	flags := flag.NewFlagSet("video-stage1-materialize", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var files stage1materialize.Files
	var planPath, outputPath, reportPath, formalRoot, expectedPackageHash string
	var liveAuthorizationPath, expectedLiveAuthorizationHash, sourceCodeCommit, livePlanOutput string
	var approvalComment, approvalActor, approvalValidUntil string
	flags.StringVar(&files.Product, "product", "", "fixed product-input JSON")
	flags.StringVar(&files.Source, "source", "", "fixed source text")
	flags.StringVar(&files.Safety, "safety", "", "fixed safety evidence JSON")
	flags.StringVar(&files.Visual, "visual", "", "fixed visual reference PNG")
	flags.StringVar(&planPath, "plan", "", "Stage 1 readiness plan")
	flags.StringVar(&formalRoot, "formal-pack", "", "FLO-100 formal offline package directory (A/B/C mode)")
	flags.StringVar(&expectedPackageHash, "expected-package-hash", "", "independently pinned FLO-100 package content hash")
	flags.StringVar(&liveAuthorizationPath, "live-authorization", "", "external FLO-100 A-only live authorization JSON")
	flags.StringVar(&expectedLiveAuthorizationHash, "expected-live-authorization-hash", "", "independently pinned live authorization SHA-256")
	flags.StringVar(&sourceCodeCommit, "source-code-commit", "", "candidate live implementation full Git SHA")
	flags.StringVar(&livePlanOutput, "live-plan-output", "", "subscription-live readiness plan output")
	flags.StringVar(&outputPath, "output", "", "prompt-free execution package output, or output directory in formal-pack mode")
	flags.StringVar(&reportPath, "report", "", "offline materialization report output")
	flags.StringVar(&approvalComment, "approval-comment", "", "ADMIN approval comment UUID")
	flags.StringVar(&approvalActor, "approval-actor", "", "ADMIN actor UUID")
	flags.StringVar(&approvalValidUntil, "approval-valid-until", "", "ADMIN approval expiry RFC3339")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || outputPath == "" || reportPath == "" {
		return errors.New("output and report are required")
	}
	liveMode := liveAuthorizationPath != ""
	formalMode := formalRoot != "" && !liveMode
	if liveMode {
		if formalRoot == "" || expectedPackageHash == "" || expectedLiveAuthorizationHash == "" ||
			sourceCodeCommit == "" || livePlanOutput == "" {
			return errors.New("formal-pack, expected-package-hash, expected-live-authorization-hash, source-code-commit, and live-plan-output are required in live mode")
		}
		if files.Product != "" || files.Source != "" || files.Safety != "" || files.Visual != "" || planPath != "" {
			return errors.New("live mode cannot be combined with legacy product, source, safety, visual, or plan inputs")
		}
	} else if formalMode {
		if files.Product != "" || files.Source != "" || files.Safety != "" || files.Visual != "" || planPath != "" {
			return errors.New("formal-pack mode cannot be combined with legacy product, source, safety, visual, or plan inputs")
		}
		if expectedPackageHash == "" {
			return errors.New("expected-package-hash is required in formal-pack mode")
		}
	} else if files.Product == "" || files.Source == "" || files.Safety == "" || files.Visual == "" || planPath == "" {
		return errors.New("product, source, safety, visual, and plan are required in legacy mode")
	}
	validUntil, err := time.Parse(time.RFC3339, approvalValidUntil)
	if err != nil {
		return errors.New("approval-valid-until must be RFC3339")
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
	approval := stage1materialize.Approval{
		CommentID: approvalComment, ActorID: approvalActor, ValidUntil: validUntil.UTC(),
	}
	if liveMode {
		plan, package_, report, err := stage1materialize.MaterializeFLO100Live(
			ctx, pool, cas, stage1materialize.LiveOptions{
				Formal: stage1materialize.FormalOptions{
					Root: formalRoot, ExpectedPackageHash: expectedPackageHash, Approval: approval,
				},
				AuthorizationPath:         liveAuthorizationPath,
				ExpectedAuthorizationHash: expectedLiveAuthorizationHash,
				SourceCodeCommit:          sourceCodeCommit,
			},
		)
		if err != nil {
			return err
		}
		if err := writeJSONAtomically(livePlanOutput, plan); err != nil {
			return fmt.Errorf("write live readiness plan: %w", err)
		}
		if err := writeJSONAtomically(outputPath, package_); err != nil {
			return fmt.Errorf("write live execution package: %w", err)
		}
		if err := writeJSONAtomically(reportPath, report); err != nil {
			return fmt.Errorf("write live verification report: %w", err)
		}
		return nil
	}
	if formalMode {
		packages, report, err := stage1materialize.MaterializeFLO100(ctx, pool, cas, stage1materialize.FormalOptions{
			Root: formalRoot, ExpectedPackageHash: expectedPackageHash, Approval: approval,
		})
		if err != nil {
			return err
		}
		for _, package_ := range packages {
			path := filepath.Join(outputPath, stage1materialize.FormalBatchOutputName(package_.BatchID))
			if err := writeJSONAtomically(path, package_); err != nil {
				return fmt.Errorf("write %s execution package: %w", package_.BatchID, err)
			}
		}
		if err := writeJSONAtomically(reportPath, report); err != nil {
			return fmt.Errorf("write formal verification report: %w", err)
		}
		return nil
	}
	var plan stage1.Plan
	if err := decodeFile(planPath, &plan); err != nil {
		return fmt.Errorf("read Stage 1 plan: %w", err)
	}
	package_, report, err := stage1materialize.Materialize(ctx, pool, cas, plan, files, approval)
	if err != nil {
		return err
	}
	if err := writeJSONAtomically(outputPath, package_); err != nil {
		return fmt.Errorf("write execution package: %w", err)
	}
	if err := writeJSONAtomically(reportPath, report); err != nil {
		return fmt.Errorf("write verification report: %w", err)
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
	temporary, err := os.CreateTemp(directory, ".stage1-materialize-*")
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
