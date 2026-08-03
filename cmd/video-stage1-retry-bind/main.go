// Command video-stage1-retry-bind binds the manually approved FLO-100 Batch A
// +1 package to PostgreSQL after a primary terminal failure. It has no Provider
// client and cannot cross the paid network boundary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1materialize"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.LookupEnv); err != nil {
		log.Fatalf("stage 1 controlled retry bind failed: %v", err)
	}
}

func run(
	ctx context.Context,
	args []string,
	output io.Writer,
	lookup func(string) (string, bool),
) error {
	if len(args) != 3 && len(args) != 4 {
		return errors.New("usage: video-stage1-retry-bind <plan.json> <primary-package.json> <controlled-retry-package.json> [flo167-supersession-package.json]")
	}
	var plan stage1.Plan
	var primary stage1.ExecutionPackage
	var retry stage1.ControlledRetryPackage
	if err := decodeFile(args[0], &plan); err != nil {
		return fmt.Errorf("read Stage 1 plan: %w", err)
	}
	if err := decodeFile(args[1], &primary); err != nil {
		return fmt.Errorf("read primary execution package: %w", err)
	}
	if err := decodeFile(args[2], &retry); err != nil {
		return fmt.Errorf("read controlled retry package: %w", err)
	}
	dsn, ok := lookup("VIDEO_POSTGRES_DSN")
	if !ok || dsn == "" {
		return errors.New("VIDEO_POSTGRES_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	result := map[string]any{
		"status": "BOUND", "controlledRetryPackageHash": retry.ContentHash,
		"retryRunId": retry.Job.Run.RunID,
	}
	if len(args) == 4 {
		var supersession stage1.FLO167SupersessionPackage
		if err := decodeFile(args[3], &supersession); err != nil {
			return fmt.Errorf("read FLO-167 supersession package: %w", err)
		}
		if err := stage1materialize.BindFLO167ControlledRetry(
			ctx, pool, plan, primary, supersession, retry,
		); err != nil {
			return err
		}
		result["supersessionPackageHash"] = supersession.ContentHash
	} else if err := stage1materialize.BindFLO100LiveControlledRetry(
		ctx, pool, plan, primary, retry,
	); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
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
