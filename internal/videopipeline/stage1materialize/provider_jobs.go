package stage1materialize

import (
	"context"
	"errors"
	"fmt"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// countExecutionPackageProviderJobs scopes the no-Provider postcondition to
// the exact immutable generation runs named by the package. Unrelated runs may
// legitimately allocate jobs concurrently and must not make an already
// committed offline revision report a false failure.
func countExecutionPackageProviderJobs(
	ctx context.Context,
	pool *pgxpool.Pool,
	executionPackage stage1.ExecutionPackage,
) (int64, error) {
	if pool == nil {
		return 0, errors.New("PostgreSQL pool is required")
	}
	runIDs := make([]uuid.UUID, 0, len(executionPackage.PostProduction.RunIDs))
	for _, runIDRaw := range executionPackage.PostProduction.RunIDs {
		runID, err := uuid.Parse(runIDRaw)
		if err != nil {
			return 0, fmt.Errorf("execution package runId %q is invalid", runIDRaw)
		}
		runIDs = append(runIDs, runID)
	}
	if len(runIDs) == 0 {
		return 0, errors.New("execution package has no immutable generation runs")
	}
	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga
		  ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = ANY($1)`,
		runIDs,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count execution package Provider jobs: %w", err)
	}
	return count, nil
}
