// Command video-stage1-flo167 is the provider-free production entry point for
// materializing and authorizing the immutable FLO-167 continuation authority.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1materialize"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.LookupEnv); err != nil {
		log.Fatalf("FLO-167 command failed: %v", err)
	}
}

func run(ctx context.Context, args []string, output io.Writer, lookup func(string) (string, bool)) error {
	if len(args) == 0 || args[0] != "materialize" && args[0] != "authorize" {
		return usageError()
	}
	dsn, err := requiredEnv(lookup, "VIDEO_POSTGRES_DSN")
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect FLO-167 PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping FLO-167 PostgreSQL: %w", err)
	}
	switch args[0] {
	case "materialize":
		if len(args) != 5 {
			return usageError()
		}
		var package_ stage1.FLO167SupersessionPackage
		if err := decodeFile(args[2], &package_); err != nil {
			return fmt.Errorf("read FLO-167 supersession package: %w", err)
		}
		var projection stage1.FLO167CanonicalProjection
		if err := decodeFile(args[3], &projection); err != nil {
			return fmt.Errorf("read FLO-167 projection: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, args[4])
		if err != nil {
			return errors.New("FLO-167 created-at must be RFC3339")
		}
		err = stage1materialize.MaterializeFLO167Supersession(ctx, pool, stage1materialize.FLO167Materialization{
			LegacyActivationID: args[1], Package: package_, Projection: projection, CreatedAt: createdAt,
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(map[string]string{"state": "supersession_package_pending_v3", "packageHash": package_.ContentHash, "projectionHash": projection.ContentHash})
	case "authorize":
		if len(args) != 5 {
			return usageError()
		}
		payload, err := os.ReadFile(args[2])
		if err != nil {
			return fmt.Errorf("read FLO-167 authorization: %w", err)
		}
		issuedAt, err := time.Parse(time.RFC3339, args[3])
		if err != nil {
			return errors.New("FLO-167 issued-at must be RFC3339")
		}
		validUntil, err := time.Parse(time.RFC3339, args[4])
		if err != nil {
			return errors.New("FLO-167 valid-until must be RFC3339")
		}
		if err := stage1materialize.AuthorizeFLO167Supersession(ctx, pool, stage1materialize.FLO167Authorization{
			LegacyActivationID: args[1], Payload: payload, IssuedAt: issuedAt, ValidUntil: validUntil,
		}); err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(map[string]string{"state": "v3_authorized_A02_A10"})
	}
	return usageError()
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

func requiredEnv(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func usageError() error {
	return errors.New("usage: video-stage1-flo167 materialize <legacy-activation-id> <package.json> <projection.json> <created-at-rfc3339> OR authorize <legacy-activation-id> <authorization.json> <issued-at-rfc3339> <valid-until-rfc3339>")
}
