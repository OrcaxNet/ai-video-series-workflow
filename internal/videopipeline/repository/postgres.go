// Package repository implements the PostgreSQL product-truth boundary for the
// video control plane. External Provider, Temporal, and CAS calls intentionally
// remain outside these transactions.
package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	idempotencyTTL  = 24 * time.Hour
	workflowStepTTL = 90 * 24 * time.Hour
	// Ten paid candidates may contend on one approval in the frozen MVP.
	// Keep one retry per admissible winner plus room for the final domain
	// budget decision; exhaustion is still mapped to a stable conflict.
	maxTxAttempts   = 12
	defaultMaxConns = 20
	defaultMinConns = 2
)

type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// Postgres persists immutable revisions and mutable execution projections.
type Postgres struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// Open creates and verifies a bounded PostgreSQL pool. The DSN is accepted only
// from runtime configuration and is never retained in audit or error payloads.
func Open(ctx context.Context, dsn string, settings PoolConfig) (*Postgres, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("PostgreSQL DSN is required")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("parse PostgreSQL configuration")
	}
	if settings.MaxConns <= 0 {
		settings.MaxConns = defaultMaxConns
	}
	if settings.MinConns < 0 || settings.MinConns > settings.MaxConns {
		return nil, errors.New("invalid PostgreSQL pool limits")
	}
	if settings.MinConns == 0 {
		settings.MinConns = defaultMinConns
	}
	if settings.MaxConnLifetime <= 0 {
		settings.MaxConnLifetime = 30 * time.Minute
	}
	if settings.MaxConnIdleTime <= 0 {
		settings.MaxConnIdleTime = 5 * time.Minute
	}
	config.MaxConns = settings.MaxConns
	config.MinConns = settings.MinConns
	config.MaxConnLifetime = settings.MaxConnLifetime
	config.MaxConnIdleTime = settings.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	store := &Postgres{pool: pool, now: time.Now}
	if err := store.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("verify PostgreSQL connection: %w", err)
	}
	return store, nil
}

// NewForPool supports integration tests and dependency injection.
func NewForPool(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool, now: time.Now}
}

func (p *Postgres) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}

func (p *Postgres) Ping(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return errors.New("PostgreSQL pool is not configured")
	}
	return p.pool.Ping(ctx)
}

// ValidateWorkerUpgradeReadiness prevents a v6 worker from replaying an
// in-flight Temporal execution whose Prompt predates executable lineage.
// Deployments must drain/cancel those runs and recompile their Prompt before
// the new worker is allowed to consume the task queue.
func (p *Postgres) ValidateWorkerUpgradeReadiness(ctx context.Context) error {
	var incompatible int
	if err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.prompt_snapshots ps ON ps.id = gr.prompt_snapshot_id
		JOIN video_pipeline.effective_context_snapshots ecs
		  ON ecs.id = ps.effective_context_snapshot_id
		WHERE gr.state IN (
		  'DRAFT', 'VALIDATED', 'QUEUED', 'RUNNING', 'UNKNOWN',
		  'RECONCILING', 'REQUIRES_ACTION', 'CANCEL_REQUESTED', 'PAUSED'
		)
		  AND (
		    ps.compiler_version NOT IN (
		      'control-plane-compiler-v1',
		      'control-plane-compiler-v2-native-audio',
		      'stage1-product-input-v1'
		    )
		    OR (
		      ps.compiler_version = 'stage1-product-input-v1'
		      AND NOT EXISTS (
		        SELECT 1
		        FROM video_pipeline.audit_events imported
		        WHERE imported.action = 'prompt_snapshot.imported'
		          AND imported.aggregate_type = 'PROMPT_SNAPSHOT'
		          AND imported.aggregate_id = ps.id
		          AND imported.payload->>'derivedPromptHash' = ps.content_hash
		          AND imported.payload->>'inputPackageHash' ~ '^[0-9a-f]{64}$'
		          AND imported.payload->>'originalPromptHash' ~ '^[0-9a-f]{64}$'
		      )
		    )
		    OR ps.output_spec = '{}'::jsonb
		    OR ps.input_revision_hashes = '{}'::jsonb
		    OR (
		      SELECT COUNT(*)
		      FROM video_pipeline.prompt_snapshot_inputs psi
		      WHERE psi.prompt_snapshot_id = ps.id
		    ) <> 2 + cardinality(ecs.context_revision_ids)
		    OR (
		      SELECT COUNT(*)
		      FROM video_pipeline.prompt_snapshot_assets psa
		      WHERE psa.prompt_snapshot_id = ps.id
		    ) <> cardinality(ps.asset_version_refs)
		    OR EXISTS (
		      SELECT 1
		      FROM video_pipeline.generation_attempts ga
		      JOIN video_pipeline.provider_jobs pj
		        ON pj.generation_attempt_id = ga.id
		      WHERE ga.generation_run_id = gr.id
		        AND NOT (pj.request_snapshot ? 'prepared')
		    )
		  )`,
	).Scan(&incompatible); err != nil {
		return fmt.Errorf("check v6 worker upgrade readiness: %w", err)
	}
	if incompatible != 0 {
		return fmt.Errorf(
			"v6 worker startup refused: %d active generation run(s) use legacy Prompt or Provider reservation lineage; drain or cancel in-flight Temporal executions and recompile before retrying",
			incompatible,
		)
	}
	return nil
}

// BeginWorkflowStep reserves one stable Temporal Activity identity or returns
// its previously committed result. An incomplete entry is safe to reconcile:
// provider submission uses the same run/job idempotency key on every retry.
func (p *Postgres) BeginWorkflowStep(
	ctx context.Context,
	step orchestration.WorkflowStep,
	inputHash string,
) (json.RawMessage, bool, error) {
	type result struct {
		body      json.RawMessage
		completed bool
	}
	stored, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (result, error) {
		now := p.now().UTC()
		scope, key, err := workflowStepIdentity(step)
		if err != nil {
			return result{}, err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.idempotency_records
				(scope, idempotency_key, request_hash, created_at, expires_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (scope, idempotency_key) DO NOTHING`,
			scope, key, inputHash, now, now.Add(workflowStepTTL),
		)
		if err != nil {
			return result{}, fmt.Errorf("reserve workflow step: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return result{}, nil
		}

		var existingHash string
		var response []byte
		if err := tx.QueryRow(ctx, `
			SELECT request_hash, response_body
			FROM video_pipeline.idempotency_records
			WHERE scope = $1 AND idempotency_key = $2
			FOR UPDATE`,
			scope, key,
		).Scan(&existingHash, &response); err != nil {
			return result{}, fmt.Errorf("read workflow step: %w", err)
		}
		if existingHash != inputHash {
			return result{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"Temporal Activity identity was reused with different immutable input",
			)
		}
		if len(response) == 0 {
			return result{}, nil
		}
		return result{body: json.RawMessage(response), completed: true}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return stored.body, stored.completed, nil
}

// CompleteWorkflowStep atomically commits the replay result with immutable
// audit and outbox evidence. Raw input is deliberately excluded; only its hash
// and the output hash are audit-visible.
func (p *Postgres) CompleteWorkflowStep(
	ctx context.Context,
	step orchestration.WorkflowStep,
	inputHash string,
	output json.RawMessage,
) error {
	_, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		now := p.now().UTC()
		scope, key, err := workflowStepIdentity(step)
		if err != nil {
			return struct{}{}, err
		}
		if !json.Valid(output) {
			return struct{}{}, errors.New("workflow step output is not valid JSON")
		}

		var existingHash string
		var existingResponse []byte
		if err := tx.QueryRow(ctx, `
			SELECT request_hash, response_body
			FROM video_pipeline.idempotency_records
			WHERE scope = $1 AND idempotency_key = $2
			FOR UPDATE`,
			scope, key,
		).Scan(&existingHash, &existingResponse); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return struct{}{}, errors.New("workflow step was not reserved")
			}
			return struct{}{}, fmt.Errorf("lock workflow step: %w", err)
		}
		if existingHash != inputHash {
			return struct{}{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"Temporal Activity identity was reused with different immutable input",
			)
		}
		if len(existingResponse) != 0 {
			return struct{}{}, nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.idempotency_records
			SET response_status = 200, response_body = $3
			WHERE scope = $1 AND idempotency_key = $2`,
			scope, key, output,
		); err != nil {
			return struct{}{}, fmt.Errorf("complete workflow step: %w", err)
		}

		aggregateID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(step.WorkflowID+"\x00"+step.ActivityID))
		auditID := uuid.NewSHA1(aggregateID, []byte("audit"))
		eventID := uuid.NewSHA1(aggregateID, []byte("outbox"))
		outputDigest := sha256.Sum256(output)
		if err := insertAuditAndOutbox(
			ctx,
			tx,
			auditID,
			eventID,
			controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
			"workflow_step.completed",
			"WORKFLOW_STEP",
			aggregateID,
			nil,
			nil,
			"",
			step.TraceID,
			map[string]any{
				"workflowId":   step.WorkflowID,
				"activityId":   step.ActivityID,
				"activityType": step.ActivityType,
				"inputHash":    inputHash,
				"outputHash":   hex.EncodeToString(outputDigest[:]),
			},
			now,
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

func workflowStepIdentity(step orchestration.WorkflowStep) (string, string, error) {
	if strings.TrimSpace(step.WorkflowID) == "" ||
		strings.TrimSpace(step.ActivityID) == "" ||
		strings.TrimSpace(step.ActivityType) == "" ||
		strings.TrimSpace(step.TraceID) == "" {
		return "", "", errors.New("workflow step identity and trace ID are required")
	}
	return "temporal-workflow:" + step.WorkflowID, step.ActivityID, nil
}

func (p *Postgres) CreateSeries(
	ctx context.Context,
	command controlplane.CreateSeriesCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	seriesID := uuid.New()
	operationID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()
	operation := controlplane.Operation{
		OperationID:   operationID.String(),
		OperationType: "CREATE_SERIES",
		AggregateType: "SERIES",
		AggregateID:   seriesID.String(),
		State:         "SUCCEEDED",
		TraceID:       traceID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{Value: replay, Replayed: true}, nil
		}

		profileID, err := uuid.Parse(command.GenerationProfileRevisionID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeProfileInactive,
				"generation profile revision is invalid",
				"select an ACTIVE immutable generation profile",
			)
		}
		var profileStatus string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM video_pipeline.generation_profiles WHERE id = $1`,
			profileID,
		).Scan(&profileStatus); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
					controlplane.CodeProfileInactive,
					"generation profile revision was not found",
					"create and activate the referenced generation profile",
				)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("read generation profile: %w", err)
		}
		if profileStatus != "ACTIVE" {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeProfileInactive,
				"generation profile is not ACTIVE",
				"approve and activate the profile before creating a series",
			)
		}

		rights, err := json.Marshal(command.RightsDeclaration)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("encode rights declaration: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.series
				(id, schema_version, title, status, default_profile_id, rights_declaration, created_by, created_at, updated_at)
			VALUES ($1, $2, $3, 'DRAFT', $4, $5, $6, $7, $7)`,
			seriesID, command.SchemaVersion, command.Title, profileID, rights, command.Actor.ActorID, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("insert series: %w", err)
		}
		if err := insertOperation(ctx, tx, operation, command.Actor.ActorID, nil); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "series.created", "SERIES", seriesID, nil, intPointer(1), "", traceID, map[string]any{
			"schemaVersion": command.SchemaVersion,
			"title":         command.Title,
		}, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, operation.OperationID, operation, 202); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) CreateSourceRevision(
	ctx context.Context,
	seriesIDRaw string,
	expectedRevision int,
	command controlplane.CreateSourceRevisionCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	seriesID, err := uuid.Parse(seriesIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("series", seriesIDRaw)
	}
	sourceID := uuid.New()
	operationID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()
	operation := controlplane.Operation{
		OperationID:   operationID.String(),
		OperationType: "CREATE_SOURCE_REVISION",
		AggregateType: "SERIES",
		AggregateID:   seriesID.String(),
		State:         "SUCCEEDED",
		TraceID:       traceID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{Value: replay, Replayed: true}, nil
		}

		var ignored uuid.UUID
		if err := tx.QueryRow(ctx,
			`SELECT id FROM video_pipeline.series WHERE id = $1 FOR UPDATE`,
			seriesID,
		).Scan(&ignored); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("series", seriesIDRaw)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("lock series: %w", err)
		}

		var maximum int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(revision), 0) FROM video_pipeline.source_revisions WHERE series_id = $1`,
			seriesID,
		).Scan(&maximum); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("read source revision: %w", err)
		}
		aggregateRevision := maximum + 1
		if expectedRevision != aggregateRevision {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				fmt.Sprintf("If-Match revision %d does not match current aggregate revision %d", expectedRevision, aggregateRevision),
			)
		}

		rightsSnapshotID, err := uuid.Parse(command.RightsSnapshotID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"rights snapshot identifier is invalid",
				"bind an ALLOWED, current source license snapshot",
			)
		}
		if err := requireAllowedLicense(ctx, tx, rightsSnapshotID, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		var parent any
		var parentID uuid.UUID
		if command.ParentRevisionID != "" {
			parsedParentID, parseErr := uuid.Parse(command.ParentRevisionID)
			if parseErr != nil {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"parentRevisionId is invalid",
				)
			}
			var parentRevision int
			if err := tx.QueryRow(ctx,
				`SELECT revision FROM video_pipeline.source_revisions WHERE id = $1 AND series_id = $2 FOR SHARE`,
				parsedParentID, seriesID,
			).Scan(&parentRevision); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
						controlplane.CodeRevisionConflict,
						"parent source revision does not belong to the series",
					)
				}
				return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("read parent source revision: %w", err)
			}
			if parentRevision != maximum {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"parentRevisionId is not the current immutable source revision",
				)
			}
			parentID = parsedParentID
			parent = parsedParentID
		}

		rights := map[string]any{"licenseSnapshotId": rightsSnapshotID.String()}
		rightsJSON, err := json.Marshal(rights)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("encode rights snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.source_revisions
				(id, series_id, revision, parent_revision_id, status, content_hash, artifact_uri, language, rights_snapshot, created_by, created_at)
			VALUES ($1, $2, $3, $4, 'DRAFT', $5, $6, $7, $8, $9, $10)`,
			sourceID, seriesID, maximum+1, parent, command.ArtifactHash, command.ArtifactURI,
			command.Language, rightsJSON, command.Actor.ActorID, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, translateWriteError("insert source revision", err)
		}
		if parentID != uuid.Nil {
			if err := markFreshnessImpacts(ctx, tx, sourceID, parentID, traceID, now); err != nil {
				return controlplane.Stored[controlplane.Operation]{}, err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE video_pipeline.series SET updated_at = $2 WHERE id = $1`,
			seriesID, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("update series timestamp: %w", err)
		}
		if err := insertOperation(ctx, tx, operation, command.Actor.ActorID, &expectedRevision); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "source_revision.created", "SOURCE_REVISION", sourceID,
			intPointer(maximum), intPointer(maximum+1), "", traceID, map[string]any{
				"seriesId":       seriesID.String(),
				"contentHash":    command.ArtifactHash,
				"artifactUri":    command.ArtifactURI,
				"rightsSnapshot": rightsSnapshotID.String(),
			}, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, operation.OperationID, operation, 202); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) CreateGenerationPlan(
	ctx context.Context,
	command controlplane.CreateGenerationPlanCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.GenerationPlan], error) {
	planID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.GenerationPlan], error) {
		var replay controlplane.GenerationPlan
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.GenerationPlan]{Value: replay, Replayed: true}, nil
		}
		seriesID, err := uuid.Parse(command.SeriesID)
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, controlplane.NewNotFoundError("series", command.SeriesID)
		}
		shotIDs, err := parseUUIDs(command.ShotSpecRevisionIDs)
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "shot revision identifiers are invalid", "refresh the shot plan",
			)
		}
		episodeRevisionID, err := uuid.Parse(command.EpisodeRevisionID)
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, controlplane.NewPolicyError(
				controlplane.CodeContentBlocked,
				"an exact episode revision is required for content safety approval",
				"bind a current episode revision and authorized SAFETY decision",
			)
		}
		var shotCount int
		var durationMS int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*), COALESCE(SUM(ssr.duration_ms), 0)
			FROM video_pipeline.shot_spec_revisions ssr
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			WHERE ssr.id = ANY($1::uuid[])
			  AND ep.series_id = $2
			  AND ($3::uuid IS NULL OR EXISTS (
			      SELECT 1
			      FROM video_pipeline.episode_revisions er
			      WHERE er.id = $3 AND er.episode_id = ep.id
			  ))
			  AND ssr.freshness IN ('FRESH', 'REVALIDATED')
			  AND ssr.lifecycle_state IN ('READY', 'APPROVED', 'REVIEW')`,
			shotIDs, seriesID, episodeRevisionID,
		).Scan(&shotCount, &durationMS); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, fmt.Errorf("validate generation plan shots: %w", err)
		}
		if shotCount != len(shotIDs) {
			return controlplane.Stored[controlplane.GenerationPlan]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"one or more shot revisions are missing, stale, or not ready",
				"refresh dependencies and approve the exact shot revisions",
			)
		}

		pricingVersion, limits, err := validateRouteSnapshot(ctx, tx, command.RouteSnapshot, now)
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		providerCallCount := shotCount * command.CandidatesPerShot
		if err := requireExecutionPolicy(limits, command.ExecutionPolicy, providerCallCount); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		if err := requireContentSafetyDecision(
			ctx, tx, command.ExecutionPolicy, seriesID, episodeRevisionID, shotIDs, now,
		); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		unitsMaximum := float64(durationMS) / 1000 * float64(command.CandidatesPerShot)
		unitsMinimum := unitsMaximum
		estimate := controlplane.CostEstimate{
			UnitsMinimum:       unitsMinimum,
			UnitsMaximum:       unitsMaximum,
			Unit:               "generated_second",
			PricingRuleVersion: pricingVersion,
			ValidUntil:         now.Add(15 * time.Minute),
		}
		decision := "AMOUNT_UNKNOWN_REQUIRES_CONFIRMATION"
		state := "READY_FOR_CONFIRMATION"
		if unitPrice, ok := numericLimit(limits, "unitPriceMicros"); ok && unitPrice >= 0 {
			amount := saturatingMicros(unitsMaximum, unitPrice)
			estimate.AmountMinimum = &amount
			estimate.AmountMaximum = &amount
			estimate.Currency = command.BudgetLimit.Currency
			if amount > command.BudgetLimit.AmountMicros {
				decision = "EXCEEDS_BUDGET"
				state = "BLOCKED"
			} else {
				decision = "WITHIN_BUDGET"
			}
		}

		planHash, err := digestValue(struct {
			SeriesID           string
			EpisodeRevisionID  string
			Shots              []string
			Candidates         int
			Route              controlplane.ModelRouteSnapshot
			Budget             controlplane.BudgetLimit
			SpeechBudget       *controlplane.BudgetLimit
			ExecutionPolicy    controlplane.ExecutionPolicy
			PricingRuleVersion string
		}{
			command.SeriesID, command.EpisodeRevisionID, command.ShotSpecRevisionIDs,
			command.CandidatesPerShot, command.RouteSnapshot, command.BudgetLimit,
			command.SpeechBudgetLimit, command.ExecutionPolicy, pricingVersion,
		})
		if err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		plan := controlplane.GenerationPlan{
			GenerationPlanID:  planID.String(),
			State:             state,
			DryRun:            true,
			ShotCount:         shotCount,
			ProviderCallCount: providerCallCount,
			RouteSnapshot:     command.RouteSnapshot,
			ExecutionPolicy:   command.ExecutionPolicy,
			Estimate:          estimate,
			SpeechBudgetLimit: cloneBudgetLimit(command.SpeechBudgetLimit),
			BudgetDecision:    decision,
			PlanHash:          planHash,
		}
		operation := controlplane.Operation{
			OperationID:   planID.String(),
			OperationType: "CREATE_GENERATION_PLAN",
			AggregateType: "SERIES",
			AggregateID:   seriesID.String(),
			State:         "SUCCEEDED",
			TraceID:       traceID,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := insertOperation(ctx, tx, operation, command.Actor.ActorID, nil); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "generation_plan.created", "GENERATION_PLAN", planID,
			nil, nil, "", traceID, map[string]any{
				"seriesId":            command.SeriesID,
				"episodeRevisionId":   command.EpisodeRevisionID,
				"shotSpecRevisionIds": command.ShotSpecRevisionIDs,
				"candidatesPerShot":   command.CandidatesPerShot,
				"pricingRuleVersion":  pricingVersion,
				"planHash":            planHash,
				"state":               state,
				"budgetDecision":      decision,
				"budgetLimit":         command.BudgetLimit,
				"speechBudgetLimit":   command.SpeechBudgetLimit,
				"executionPolicy":     command.ExecutionPolicy,
			}, now); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, plan.GenerationPlanID, plan, 201); err != nil {
			return controlplane.Stored[controlplane.GenerationPlan]{}, err
		}
		return controlplane.Stored[controlplane.GenerationPlan]{Value: plan}, nil
	})
}

func (p *Postgres) StartContentCompilation(
	ctx context.Context,
	sourceRevisionIDRaw string,
	command controlplane.StartContentCompilationCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	sourceRevisionID, err := uuid.Parse(sourceRevisionIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{},
			controlplane.NewNotFoundError("source revision", sourceRevisionIDRaw)
	}
	operationID := uuid.New()
	now := p.now().UTC()
	return withSerializable(ctx, p.pool, func(
		tx pgx.Tx,
	) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{
				Value: replay, Replayed: true,
			}, nil
		}
		var seriesID uuid.UUID
		var sourceHash, sourceStatus string
		if err := tx.QueryRow(ctx, `
			SELECT series_id, content_hash, status
			FROM video_pipeline.source_revisions
			WHERE id = $1
			FOR SHARE`,
			sourceRevisionID,
		).Scan(&seriesID, &sourceHash, &sourceStatus); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{},
					controlplane.NewNotFoundError("source revision", sourceRevisionIDRaw)
			}
			return controlplane.Stored[controlplane.Operation]{},
				fmt.Errorf("read compilation source revision: %w", err)
		}
		if sourceStatus != "APPROVED" || sourceHash != command.SourceHash {
			return controlplane.Stored[controlplane.Operation]{},
				controlplane.NewPolicyError(
					controlplane.CodeStaleDependency,
					"content compilation must bind the exact approved source revision",
					"approve the current source hash and retry",
				)
		}
		if _, _, err := validateRouteSnapshot(
			ctx, tx, command.TextRouteSnapshot, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		compilationIDs := make([]string, 0, len(command.Stages))
		for _, rawStage := range command.Stages {
			stage := strings.ToUpper(strings.TrimSpace(rawStage))
			inputHash, err := digestValue(map[string]any{
				"schemaVersion":     "v1",
				"sourceRevisionId":  sourceRevisionID.String(),
				"sourceHash":        sourceHash,
				"stage":             stage,
				"textRouteSnapshot": command.TextRouteSnapshot,
			})
			if err != nil {
				return controlplane.Stored[controlplane.Operation]{}, err
			}
			compilationID := uuid.NewSHA1(
				sourceRevisionID,
				[]byte("content-compilation:"+stage+":"+inputHash),
			)
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.content_compilation_runs
					(id, series_id, source_revision_id, stage, generator_model,
					 input_hash, state, trace_id, created_by)
				VALUES ($1, $2, $3, $4, $5, $6, 'VALIDATED', $7, $8)
				ON CONFLICT (source_revision_id, stage, input_hash) DO NOTHING`,
				compilationID, seriesID, sourceRevisionID, stage,
				command.TextRouteSnapshot, inputHash, traceID, command.Actor.ActorID,
			); err != nil {
				return controlplane.Stored[controlplane.Operation]{},
					fmt.Errorf("insert content compilation run: %w", err)
			}
			compilationIDs = append(compilationIDs, compilationID.String())
		}
		operation := controlplane.Operation{
			OperationID:   operationID.String(),
			OperationType: "START_CONTENT_COMPILATION",
			AggregateType: "SOURCE_REVISION",
			AggregateID:   sourceRevisionID.String(),
			State:         "ACCEPTED", TraceID: traceID,
			CreatedAt: now, UpdatedAt: now,
		}
		if err := insertOperation(
			ctx, tx, operation, command.Actor.ActorID, nil,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(
			ctx, tx, uuid.NewSHA1(operationID, []byte("audit")),
			uuid.NewSHA1(operationID, []byte("outbox")),
			command.Actor,
			"content_compilation.requested",
			"CONTENT_COMPILATION",
			operationID,
			nil, nil, "", traceID,
			map[string]any{
				"sourceRevisionId":  sourceRevisionID.String(),
				"sourceHash":        sourceHash,
				"stages":            command.Stages,
				"compilationRunIds": compilationIDs,
				"textRouteSnapshot": command.TextRouteSnapshot,
			},
			now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(
			ctx, tx, idempotency, operation.OperationID, operation, 202,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) PrepareProduction(
	ctx context.Context,
	episodeIDRaw string,
	expectedRevision int,
	command controlplane.StartProductionCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	episodeID, err := uuid.Parse(episodeIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("episode", episodeIDRaw)
	}
	operationID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()
	workflowID := "episode-production-" + operationID.String()
	operation := controlplane.Operation{
		OperationID:        operationID.String(),
		OperationType:      "START_EPISODE_PRODUCTION",
		AggregateType:      "EPISODE",
		AggregateID:        episodeID.String(),
		State:              "ACCEPTED",
		TemporalWorkflowID: workflowID,
		TraceID:            traceID,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{Value: replay, Replayed: true}, nil
		}

		episodeRevisionID, err := uuid.Parse(command.EpisodeRevisionID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired, "episode revision identifier is invalid", "select the exact G2-approved episode revision",
			)
		}
		var seriesID uuid.UUID
		var episodeRevision int
		var episodeStatus string
		if err := tx.QueryRow(ctx, `
			SELECT ep.series_id, er.revision, er.status
			FROM video_pipeline.episodes ep
			JOIN video_pipeline.episode_revisions er ON er.episode_id = ep.id
			WHERE ep.id = $1 AND er.id = $2
			FOR UPDATE OF ep, er`,
			episodeID, episodeRevisionID,
		).Scan(&seriesID, &episodeRevision, &episodeStatus); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("episode revision", command.EpisodeRevisionID)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("lock episode revision: %w", err)
		}
		if expectedRevision != episodeRevision {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				fmt.Sprintf("If-Match revision %d does not match episode revision %d", expectedRevision, episodeRevision),
			)
		}
		if episodeStatus != "G2_APPROVED" {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired, "episode revision is not G2_APPROVED", "approve the exact script and storyboard revision at G2",
			)
		}
		if err := requireApprovedDecision(
			ctx, tx, command.Gate2DecisionID, "G2", seriesID, episodeID,
			"EPISODE_REVISION", episodeRevisionID,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := requireActiveProfile(ctx, tx, command.GenerationProfileRevisionID); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		_, limits, err := validateRouteSnapshot(ctx, tx, command.RouteSnapshot, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if command.PostProduction != nil && command.PostProduction.Enabled {
			post := command.PostProduction
			if post.SpeechRouteSnapshot.CapabilityAlias != "speech.primary" {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
					controlplane.CodeCapability,
					"post-production route is not speech.primary",
					"freeze a current speech capability snapshot",
				)
			}
			if _, _, err := validateRouteSnapshot(
				ctx, tx, post.SpeechRouteSnapshot, now,
			); err != nil {
				return controlplane.Stored[controlplane.Operation]{}, err
			}
			if err := requirePostProductionEvidenceMode(
				ctx, tx, post.SpeechRouteSnapshot.ProviderProfileID, post.Evidence,
			); err != nil {
				return controlplane.Stored[controlplane.Operation]{}, err
			}
		}

		planRecord, err := readPlan(ctx, tx, command.GenerationPlanID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if planRecord.SeriesID != seriesID.String() ||
			planRecord.Plan.State == "BLOCKED" ||
			planRecord.Plan.PlanHash == "" {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"generation plan is blocked or belongs to a different series",
				"create and confirm a current within-budget generation plan",
			)
		}
		if planRecord.EpisodeRevisionID != "" && planRecord.EpisodeRevisionID != command.EpisodeRevisionID {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"production episode revision differs from the immutable generation plan",
				"create a new plan for the exact episode revision",
			)
		}
		if !equalStrings(planRecord.ShotSpecRevisionIDs, command.ShotSpecRevisionIDs) {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"production shot revisions differ from the immutable generation plan",
				"use the exact ordered shot revisions or create a new plan",
			)
		}
		if !sameRoute(planRecord.Plan.RouteSnapshot, command.RouteSnapshot) {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeCapability,
				"production route differs from the immutable generation plan",
				"create a new plan for the selected provider route",
			)
		}
		if planRecord.ExecutionPolicy != command.ExecutionPolicy {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"production execution policy differs from the immutable generation plan",
				"use the exact territory, product form, and safety policy from the plan",
			)
		}
		if err := requireBudgetApproval(
			ctx, tx, command.BudgetApprovalID, seriesID, episodeID,
			command.GenerationPlanID, "VIDEO", planRecord.BudgetLimit,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if command.PostProduction != nil && command.PostProduction.Enabled {
			post := command.PostProduction
			if !sameBudgetLimit(planRecord.SpeechBudgetLimit, post.SpeechBudgetLimit) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
					controlplane.CodeBudgetExceeded,
					"post-production speech budget differs from the immutable generation plan",
					"create a new plan with the exact TTS amount and currency",
				)
			}
			if err := requireBudgetApproval(
				ctx, tx, post.SpeechBudgetApprovalID, seriesID, episodeID,
				command.GenerationPlanID, "SPEECH", post.SpeechBudgetLimit,
			); err != nil {
				return controlplane.Stored[controlplane.Operation]{}, err
			}
		}
		shotIDs, err := parseUUIDs(command.ShotSpecRevisionIDs)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "shot revision identifiers are invalid", "refresh the exact shot revisions",
			)
		}
		if err := requireExecutionPolicy(limits, command.ExecutionPolicy, planRecord.Plan.ProviderCallCount); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := requireContentSafetyDecision(
			ctx, tx, command.ExecutionPolicy, seriesID, episodeRevisionID, shotIDs, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		var readyCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM video_pipeline.shot_spec_revisions ssr
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			WHERE ssr.id = ANY($1::uuid[])
			  AND sc.episode_id = $2
			  AND ssr.gate2_decision_id = $3
			  AND ssr.generation_profile_id = $4
			  AND ssr.freshness IN ('FRESH', 'REVALIDATED')
			  AND ssr.lifecycle_state IN ('READY', 'APPROVED', 'REVIEW')`,
			shotIDs, episodeID, command.Gate2DecisionID, command.GenerationProfileRevisionID,
		).Scan(&readyCount); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("validate production shot revisions: %w", err)
		}
		if readyCount != len(shotIDs) || readyCount != planRecord.Plan.ShotCount {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"production batch differs from the approved fresh generation plan",
				"rebuild and approve the plan using the exact current shot revisions",
			)
		}
		if err := requireShotAssetLicenses(ctx, tx, shotIDs, now, command.ExecutionPolicy); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}

		if err := insertOperation(ctx, tx, operation, command.Actor.ActorID, &expectedRevision); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "episode.production.requested", "EPISODE", episodeID,
			intPointer(episodeRevision), intPointer(episodeRevision), "", traceID, map[string]any{
				"operationId":       operation.OperationID,
				"workflowId":        workflowID,
				"episodeRevisionId": command.EpisodeRevisionID,
				"generationPlanId":  command.GenerationPlanID,
				"shotCount":         len(shotIDs),
				"executionPolicy":   command.ExecutionPolicy,
				"postProduction":    command.PostProduction,
			}, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, operation.OperationID, operation, 202); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) GetGenerationPlan(ctx context.Context, planIDRaw string) (controlplane.GenerationPlanRecord, error) {
	planID, err := uuid.Parse(planIDRaw)
	if err != nil {
		return controlplane.GenerationPlanRecord{}, controlplane.NewNotFoundError("generation plan", planIDRaw)
	}
	var aggregateID uuid.UUID
	var responseBody []byte
	var auditPayload []byte
	err = p.pool.QueryRow(ctx, `
		SELECT op.aggregate_id, idem.response_body, audit.payload
		FROM video_pipeline.operation_requests op
		JOIN video_pipeline.idempotency_records idem ON idem.operation_id = op.id
		JOIN LATERAL (
			SELECT payload
			FROM video_pipeline.audit_events
			WHERE aggregate_type = 'GENERATION_PLAN' AND aggregate_id = op.id
			ORDER BY occurred_at DESC
			LIMIT 1
		) audit ON true
		WHERE op.id = $1
		  AND op.operation_type = 'CREATE_GENERATION_PLAN'
		  AND op.state = 'SUCCEEDED'
		ORDER BY idem.created_at DESC
		LIMIT 1`,
		planID,
	).Scan(&aggregateID, &responseBody, &auditPayload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.GenerationPlanRecord{}, controlplane.NewNotFoundError("generation plan", planIDRaw)
		}
		return controlplane.GenerationPlanRecord{}, fmt.Errorf("read generation plan: %w", err)
	}
	var plan controlplane.GenerationPlan
	if err := json.Unmarshal(responseBody, &plan); err != nil {
		return controlplane.GenerationPlanRecord{}, fmt.Errorf("decode generation plan: %w", err)
	}
	return decodePlanRecord(plan, aggregateID, auditPayload)
}

func (p *Postgres) CreateGenerationRun(
	ctx context.Context,
	shotIDRaw string,
	expectedRevision int,
	command controlplane.CreateGenerationRunCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	shotID, err := uuid.Parse(shotIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("shot", shotIDRaw)
	}
	budgetApprovalID, err := uuid.Parse(command.BudgetApprovalID)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"budgetApprovalId is invalid",
			"use the exact approved budget decision",
		)
	}
	canonicalBudgetApprovalID := budgetApprovalID.String()
	runID := uuid.New()
	attemptID := uuid.New()
	operationID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()
	workflowID := "shot-generation-" + runID.String()
	operation := controlplane.Operation{
		OperationID:        operationID.String(),
		OperationType:      "CREATE_GENERATION_RUN",
		AggregateType:      "GENERATION_RUN",
		AggregateID:        runID.String(),
		State:              "ACCEPTED",
		TemporalWorkflowID: workflowID,
		TraceID:            traceID,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{Value: replay, Replayed: true}, nil
		}

		shotSpecID, err := uuid.Parse(command.ShotSpecRevisionID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "shotSpecRevisionId is invalid", "select a current immutable shot revision",
			)
		}
		var revision int
		var freshness, lifecycle string
		var gate2ID, shotProfileID, seriesID, episodeID uuid.UUID
		var assetRefs []uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT ssr.revision, ssr.freshness, ssr.lifecycle_state, ssr.gate2_decision_id,
			       ssr.asset_version_refs, ssr.generation_profile_id, ep.series_id, ep.id
			FROM video_pipeline.shot_spec_revisions ssr
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			WHERE ssr.id = $1 AND ssr.shot_id = $2
			FOR UPDATE`,
			shotSpecID, shotID,
		).Scan(&revision, &freshness, &lifecycle, &gate2ID, &assetRefs, &shotProfileID, &seriesID, &episodeID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("shot revision", command.ShotSpecRevisionID)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("lock shot revision: %w", err)
		}
		if expectedRevision != revision {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				fmt.Sprintf("If-Match revision %d does not match shot revision %d", expectedRevision, revision),
			)
		}
		if freshness == "STALE" {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "shot revision is stale", "revalidate or regenerate the shot revision",
			)
		}
		if lifecycle != "READY" && lifecycle != "APPROVED" && lifecycle != "REVIEW" {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired, "shot revision is not ready for generation", "complete G2 for the exact shot revision",
			)
		}
		if err := requireApprovedDecision(
			ctx, tx, gate2ID.String(), "G2", seriesID, episodeID,
			"", uuid.Nil,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := requireActiveProfile(ctx, tx, command.GenerationProfileRevisionID); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if shotProfileID.String() != command.GenerationProfileRevisionID {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeProfileInactive,
				"run profile differs from the immutable shot specification",
				"create a new shot revision for the selected generation profile",
			)
		}
		_, limits, err := validateRouteSnapshot(ctx, tx, command.RouteSnapshot, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		planRecord, err := readPlan(ctx, tx, command.GenerationPlanID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if planRecord.SeriesID != seriesID.String() ||
			!containsString(planRecord.ShotSpecRevisionIDs, command.ShotSpecRevisionID) {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"run shot revision is not bound to the immutable generation plan",
				"create or select a plan containing the exact shot revision",
			)
		}
		if planRecord.Plan.State == "BLOCKED" || !sameRoute(planRecord.Plan.RouteSnapshot, command.RouteSnapshot) {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"run route or budget differs from the approved generation plan",
				"use the exact within-budget plan route",
			)
		}
		if planRecord.ExecutionPolicy != command.ExecutionPolicy {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"run execution policy differs from the immutable generation plan",
				"use the exact territory, product form, and safety policy from the plan",
			)
		}
		if err := requireExecutionPolicy(limits, command.ExecutionPolicy, 1); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		planEpisodeRevisionID, err := uuid.Parse(planRecord.EpisodeRevisionID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeContentBlocked,
				"generation plan has no exact episode revision safety binding",
				"create a new plan with an authorized SAFETY decision",
			)
		}
		planShotIDs, err := parseUUIDs(planRecord.ShotSpecRevisionIDs)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeContentBlocked,
				"generation plan safety bindings are invalid",
				"create a new plan with exact immutable shot revisions",
			)
		}
		if err := requireContentSafetyDecision(
			ctx, tx, command.ExecutionPolicy, seriesID, planEpisodeRevisionID, planShotIDs, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := requireAssetLicenses(ctx, tx, assetRefs, now, command.ExecutionPolicy); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := requireBudgetApproval(
			ctx, tx, canonicalBudgetApprovalID, seriesID, episodeID,
			command.GenerationPlanID, "VIDEO", planRecord.BudgetLimit,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}

		promptID, err := uuid.Parse(command.PromptSnapshotID)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "promptSnapshotId is invalid", "compile a prompt for the exact shot revision",
			)
		}
		var promptHash string
		if err := tx.QueryRow(ctx,
			`SELECT content_hash FROM video_pipeline.prompt_snapshots WHERE id = $1 AND shot_spec_revision_id = $2`,
			promptID, shotSpecID,
		).Scan(&promptHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
					controlplane.CodeStaleDependency,
					"prompt snapshot does not belong to the exact shot revision",
					"compile and approve a current prompt snapshot",
				)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("read prompt snapshot: %w", err)
		}
		profileID, _ := uuid.Parse(command.GenerationProfileRevisionID)
		providerRoute := providerRouteSnapshot(command.RouteSnapshot)
		runDigest, err := generationRunSpecDigest(
			command.ShotSpecRevisionID,
			command.PromptSnapshotID,
			promptHash,
			command.GenerationProfileRevisionID,
			command.GenerationPlanID,
			providerRoute,
			command.CreativeAttempt,
		)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.generation_runs
				(id, shot_spec_revision_id, prompt_snapshot_id, generation_profile_id, temporal_workflow_id,
				 run_spec_digest, creative_attempt, state, fallback_reason, dry_run, budget_approval_id,
				 trace_id, created_by, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'VALIDATED', NULLIF($8, ''), false, $9, $10, $11, $12)`,
			runID, shotSpecID, promptID, profileID, workflowID, runDigest, command.CreativeAttempt,
			command.FallbackReasonCode, canonicalBudgetApprovalID, traceID, command.Actor.ActorID, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, translateWriteError("insert generation run", err)
		}
		modelSnapshot, err := json.Marshal(providerRoute)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("encode model snapshot: %w", err)
		}
		attemptKind := "PROVIDER_REQUEST"
		if command.CreativeAttempt > 1 {
			attemptKind = "CREATIVE_REVISION"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.generation_attempts
				(id, generation_run_id, sequence, attempt_kind, state, input_hash, model_snapshot, parameter_diff, created_at)
			VALUES ($1, $2, 1, $3, 'VALIDATED', $4, $5, '{}'::jsonb, $6)`,
			attemptID, runID, attemptKind, runDigest, modelSnapshot, now,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("insert generation attempt: %w", err)
		}
		if err := insertOperation(ctx, tx, operation, command.Actor.ActorID, &expectedRevision); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "generation_run.created", "GENERATION_RUN", runID,
			nil, nil, "", traceID, map[string]any{
				"shotSpecRevisionId": command.ShotSpecRevisionID,
				"promptSnapshotId":   command.PromptSnapshotID,
				"runSpecDigest":      runDigest,
				"creativeAttempt":    command.CreativeAttempt,
				"generationPlanId":   command.GenerationPlanID,
				"executionPolicy":    command.ExecutionPolicy,
				"operationId":        operation.OperationID,
			}, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, operation.OperationID, operation, 202); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) GetGenerationRun(ctx context.Context, runIDRaw string) (controlplane.GenerationRun, error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.GenerationRun{}, controlplane.NewNotFoundError("generation run", runIDRaw)
	}
	var run controlplane.GenerationRun
	var failureClass, failureCode, temporalWorkflowID *string
	err = p.pool.QueryRow(ctx, `
		SELECT id, shot_spec_revision_id, run_spec_digest, creative_attempt, state,
		       failure_class, failure_code, temporal_workflow_id, trace_id, created_at
		FROM video_pipeline.generation_runs
		WHERE id = $1`,
		runID,
	).Scan(
		&run.RunID, &run.ShotSpecRevisionID, &run.RunSpecDigest, &run.CreativeAttempt,
		&run.State, &failureClass, &failureCode, &temporalWorkflowID, &run.TraceID, &run.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.GenerationRun{}, controlplane.NewNotFoundError("generation run", runIDRaw)
		}
		return controlplane.GenerationRun{}, fmt.Errorf("read generation run: %w", err)
	}
	run.FailureClass = valueOrEmpty(failureClass)
	run.FailureCode = valueOrEmpty(failureCode)
	run.TemporalWorkflowID = valueOrEmpty(temporalWorkflowID)
	return run, nil
}

func (p *Postgres) GetShotWorkflowRecord(
	ctx context.Context,
	runIDRaw string,
) (controlplane.ShotWorkflowRecord, error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.ShotWorkflowRecord{}, controlplane.NewNotFoundError("generation run", runIDRaw)
	}
	var record controlplane.ShotWorkflowRecord
	var failureClass, failureCode, temporalWorkflowID *string
	var routeJSON []byte
	var planID string
	err = p.pool.QueryRow(ctx, `
		SELECT gr.id, gr.shot_spec_revision_id, gr.run_spec_digest, gr.creative_attempt,
		       gr.state, gr.failure_class, gr.failure_code, gr.temporal_workflow_id,
		       gr.trace_id, gr.created_at, ps.id, ps.content_hash,
		       ga.model_snapshot, gr.budget_approval_id, audit.payload->>'generationPlanId'
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.prompt_snapshots ps ON ps.id = gr.prompt_snapshot_id
		JOIN video_pipeline.generation_attempts ga
		  ON ga.generation_run_id = gr.id AND ga.sequence = 1
		JOIN LATERAL (
			SELECT payload
			FROM video_pipeline.audit_events
			WHERE aggregate_type = 'GENERATION_RUN'
			  AND aggregate_id = gr.id
			  AND action = 'generation_run.created'
			ORDER BY occurred_at
			LIMIT 1
		) audit ON true
		WHERE gr.id = $1`,
		runID,
	).Scan(
		&record.Run.RunID, &record.Run.ShotSpecRevisionID, &record.Run.RunSpecDigest,
		&record.Run.CreativeAttempt, &record.Run.State, &failureClass, &failureCode,
		&temporalWorkflowID, &record.Run.TraceID, &record.Run.CreatedAt,
		&record.PromptSnapshotID, &record.PromptHash, &routeJSON,
		&record.BudgetApprovalID, &planID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.ShotWorkflowRecord{}, controlplane.NewNotFoundError("generation run workflow projection", runIDRaw)
		}
		return controlplane.ShotWorkflowRecord{}, fmt.Errorf("read shot workflow projection: %w", err)
	}
	record.Run.FailureClass = valueOrEmpty(failureClass)
	record.Run.FailureCode = valueOrEmpty(failureCode)
	record.Run.TemporalWorkflowID = valueOrEmpty(temporalWorkflowID)
	var attemptRoute providercontract.ModelSnapshot
	if err := json.Unmarshal(routeJSON, &attemptRoute); err != nil {
		return controlplane.ShotWorkflowRecord{}, fmt.Errorf(
			"decode shot workflow Provider route: %w", err,
		)
	}
	plan, err := p.GetGenerationPlan(ctx, planID)
	if err != nil {
		return controlplane.ShotWorkflowRecord{}, fmt.Errorf("read shot workflow plan: %w", err)
	}
	if attemptRoute != providerRouteSnapshot(plan.Plan.RouteSnapshot) {
		return controlplane.ShotWorkflowRecord{}, controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"shot workflow Provider route differs from its immutable generation plan",
		)
	}
	record.RouteSnapshot = plan.Plan.RouteSnapshot
	record.BudgetLimit = plan.BudgetLimit
	return record, nil
}

func (p *Postgres) RequestRunPause(
	ctx context.Context,
	runIDRaw string,
	expectedRevision int,
	actor controlplane.Actor,
	reasonCode string,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	return p.transitionRun(ctx, runIDRaw, expectedRevision, actor, reasonCode, "", idempotency, traceID, runTransition{
		OperationType: "PAUSE_GENERATION_RUN",
		Action:        "generation_run.pause_requested",
		TargetState:   "PAUSED",
		AllowedStates: map[string]struct{}{"QUEUED": {}, "RUNNING": {}, "RECONCILING": {}},
	})
}

func (p *Postgres) RequestRunCancellation(
	ctx context.Context,
	runIDRaw string,
	expectedRevision int,
	actor controlplane.Actor,
	reasonCode string,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	return p.transitionRun(ctx, runIDRaw, expectedRevision, actor, reasonCode, "", idempotency, traceID, runTransition{
		OperationType: "CANCEL_GENERATION_RUN",
		Action:        "generation_run.cancel_requested",
		TargetState:   "CANCEL_REQUESTED",
		AllowedStates: map[string]struct{}{
			"VALIDATED": {}, "QUEUED": {}, "RUNNING": {}, "PAUSED": {}, "UNKNOWN": {}, "RECONCILING": {}, "REQUIRES_ACTION": {},
		},
	})
}

func (p *Postgres) RequestRunResume(
	ctx context.Context,
	runIDRaw string,
	expectedRevision int,
	actor controlplane.Actor,
	recoveryMode string,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.Operation], error) {
	if recoveryMode != "RECONCILE_HISTORY" && recoveryMode != "RETRY_INFRASTRUCTURE" {
		if recoveryMode == "RESUME_PAUSED" {
			return p.transitionRun(ctx, runIDRaw, expectedRevision, actor, "", recoveryMode, idempotency, traceID, runTransition{
				OperationType: "RESUME_GENERATION_RUN",
				Action:        "generation_run.resumed",
				TargetState:   "RUNNING",
				ClearFailure:  true,
				AllowedStates: map[string]struct{}{"PAUSED": {}},
			})
		}
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewPolicyError(
			controlplane.CodeRecoveryActive,
			"unsupported recoveryMode",
			"use RESUME_PAUSED, RECONCILE_HISTORY, or RETRY_INFRASTRUCTURE",
		)
	}
	return p.transitionRun(ctx, runIDRaw, expectedRevision, actor, "", recoveryMode, idempotency, traceID, runTransition{
		OperationType: "RESUME_GENERATION_RUN",
		Action:        "generation_run.recovery_requested",
		TargetState:   "RECONCILING",
		AllowedStates: map[string]struct{}{"UNKNOWN": {}, "REQUIRES_ACTION": {}},
	})
}

type runTransition struct {
	OperationType string
	Action        string
	TargetState   string
	ClearFailure  bool
	AllowedStates map[string]struct{}
}

func (p *Postgres) transitionRun(
	ctx context.Context,
	runIDRaw string,
	expectedRevision int,
	actor controlplane.Actor,
	reasonCode string,
	recoveryMode string,
	idempotency controlplane.Idempotency,
	traceID string,
	transition runTransition,
) (controlplane.Stored[controlplane.Operation], error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("generation run", runIDRaw)
	}
	operationID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.Operation], error) {
		var replay controlplane.Operation
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.Operation]{Value: replay, Replayed: true}, nil
		}

		var currentState, workflowID string
		var creativeAttempt int
		if err := tx.QueryRow(ctx, `
			SELECT state, creative_attempt, temporal_workflow_id
			FROM video_pipeline.generation_runs
			WHERE id = $1
			FOR UPDATE`,
			runID,
		).Scan(&currentState, &creativeAttempt, &workflowID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.Operation]{}, controlplane.NewNotFoundError("generation run", runIDRaw)
			}
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("lock generation run: %w", err)
		}
		if creativeAttempt != expectedRevision {
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				fmt.Sprintf("If-Match revision %d does not match run creative revision %d", expectedRevision, creativeAttempt),
			)
		}
		if _, allowed := transition.AllowedStates[currentState]; !allowed {
			code := controlplane.CodeRunTerminal
			if currentState == "RECONCILING" {
				code = controlplane.CodeRecoveryActive
			}
			return controlplane.Stored[controlplane.Operation]{}, controlplane.NewConflictError(
				code, fmt.Sprintf("run state %s cannot transition to %s", currentState, transition.TargetState),
			)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_runs
			SET state = $2,
			    failure_class = CASE WHEN $4 THEN NULL ELSE failure_class END,
			    failure_code = CASE
			      WHEN $4 THEN NULL
			      WHEN $3 = '' THEN failure_code
			      ELSE $3
			    END
			WHERE id = $1`,
			runID, transition.TargetState, reasonCode, transition.ClearFailure,
		); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, fmt.Errorf("update generation run state: %w", err)
		}
		operation := controlplane.Operation{
			OperationID:        operationID.String(),
			OperationType:      transition.OperationType,
			AggregateType:      "GENERATION_RUN",
			AggregateID:        runID.String(),
			State:              "ACCEPTED",
			TemporalWorkflowID: workflowID,
			TraceID:            traceID,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		if err := insertOperation(ctx, tx, operation, actor.ActorID, &expectedRevision); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, actor, transition.Action, "GENERATION_RUN", runID,
			intPointer(creativeAttempt), intPointer(creativeAttempt), reasonCode, traceID, map[string]any{
				"operationId":   operation.OperationID,
				"previousState": currentState,
				"targetState":   transition.TargetState,
				"recoveryMode":  recoveryMode,
			}, now); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, operation.OperationID, operation, 202); err != nil {
			return controlplane.Stored[controlplane.Operation]{}, err
		}
		return controlplane.Stored[controlplane.Operation]{Value: operation}, nil
	})
}

func (p *Postgres) CreateApprovalDecision(
	ctx context.Context,
	command controlplane.CreateApprovalDecisionCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.ApprovalDecision], error) {
	storedExplanation, err := approvalExplanation(command)
	if err != nil {
		return controlplane.Stored[controlplane.ApprovalDecision]{}, err
	}
	decisionID := uuid.New()
	auditID := uuid.New()
	eventID := uuid.New()
	now := p.now().UTC()
	decision := controlplane.ApprovalDecision{
		CreateApprovalDecisionCommand: command,
		DecisionID:                    decisionID.String(),
		DecidedAt:                     now,
		TraceID:                       traceID,
	}

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.ApprovalDecision], error) {
		var replay controlplane.ApprovalDecision
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.ApprovalDecision]{Value: replay, Replayed: true}, nil
		}
		seriesID, err := uuid.Parse(command.SeriesID)
		if err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, controlplane.NewNotFoundError("series", command.SeriesID)
		}
		var seriesExists bool
		if err := tx.QueryRow(ctx,
			`SELECT true FROM video_pipeline.series WHERE id = $1 FOR SHARE`,
			seriesID,
		).Scan(&seriesExists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.ApprovalDecision]{}, controlplane.NewNotFoundError("series", command.SeriesID)
			}
			return controlplane.Stored[controlplane.ApprovalDecision]{}, fmt.Errorf("read approval series: %w", err)
		}
		var episodeID any
		var parsedEpisodeID uuid.UUID
		if command.EpisodeID != "" {
			parsed, err := uuid.Parse(command.EpisodeID)
			if err != nil {
				return controlplane.Stored[controlplane.ApprovalDecision]{}, controlplane.NewNotFoundError("episode", command.EpisodeID)
			}
			var episodeExists bool
			if err := tx.QueryRow(ctx,
				`SELECT true FROM video_pipeline.episodes WHERE id = $1 AND series_id = $2 FOR SHARE`,
				parsed, seriesID,
			).Scan(&episodeExists); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return controlplane.Stored[controlplane.ApprovalDecision]{}, controlplane.NewNotFoundError("episode in series", command.EpisodeID)
				}
				return controlplane.Stored[controlplane.ApprovalDecision]{}, fmt.Errorf("read approval episode: %w", err)
			}
			parsedEpisodeID = parsed
			episodeID = parsed
		}
		if err := validateApprovalBindings(ctx, tx, command.Gate, command.Decision, command.Bindings); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		if err := validateApprovalScope(ctx, tx, seriesID, parsedEpisodeID, command.Bindings); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.approval_decisions
				(id, series_id, episode_id, gate, decision, reason_code, explanation, actor_id, actor_role, decided_at, trace_id)
			VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11)`,
			decisionID, seriesID, episodeID, command.Gate, command.Decision, command.ReasonCode,
			storedExplanation, command.Actor.ActorID, command.Actor.Role, now, traceID,
		); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, fmt.Errorf("insert approval decision: %w", err)
		}
		for _, binding := range command.Bindings {
			revisionID, _ := uuid.Parse(binding.RevisionID)
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.approval_bindings
					(decision_id, object_type, revision_id, content_hash)
				VALUES ($1, $2, $3, $4)`,
				decisionID, strings.ToUpper(binding.ObjectType), revisionID, binding.ContentHash,
			); err != nil {
				return controlplane.Stored[controlplane.ApprovalDecision]{}, fmt.Errorf("insert approval binding: %w", err)
			}
		}
		if err := applyApprovedGate(ctx, tx, decisionID, command, traceID, now); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		if err := insertAuditAndOutbox(ctx, tx, auditID, eventID, command.Actor, "approval.decided", "APPROVAL_DECISION", decisionID,
			nil, nil, command.ReasonCode, traceID, map[string]any{
				"seriesId":      command.SeriesID,
				"episodeId":     command.EpisodeID,
				"gate":          command.Gate,
				"decision":      command.Decision,
				"policyVersion": command.PolicyVersion,
				"evidenceHash":  command.EvidenceHash,
				"validUntil":    command.ValidUntil,
				"bindings":      command.Bindings,
			}, now); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		if err := completeIdempotency(ctx, tx, idempotency, "", decision, 201); err != nil {
			return controlplane.Stored[controlplane.ApprovalDecision]{}, err
		}
		return controlplane.Stored[controlplane.ApprovalDecision]{Value: decision}, nil
	})
}

func (p *Postgres) LockPublication(
	ctx context.Context,
	runIDRaw string,
	command controlplane.LockPublicationCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.PublicationLock], error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.PublicationLock]{},
			controlplane.NewNotFoundError("generation run", runIDRaw)
	}
	manifestID, err := uuid.Parse(command.ManifestID)
	if err != nil {
		return controlplane.Stored[controlplane.PublicationLock]{},
			controlplane.NewNotFoundError("generation manifest", command.ManifestID)
	}
	qcReportID, err := uuid.Parse(command.QCReportID)
	if err != nil {
		return controlplane.Stored[controlplane.PublicationLock]{},
			controlplane.NewNotFoundError("QC report", command.QCReportID)
	}
	gate3DecisionID, err := uuid.Parse(command.Gate3DecisionID)
	if err != nil {
		return controlplane.Stored[controlplane.PublicationLock]{},
			controlplane.NewNotFoundError("G3 decision", command.Gate3DecisionID)
	}
	now := p.now().UTC()
	return withSerializable(ctx, p.pool, func(
		tx pgx.Tx,
	) (controlplane.Stored[controlplane.PublicationLock], error) {
		var replay controlplane.PublicationLock
		replayed, err := reserveIdempotency(ctx, tx, idempotency, &replay, now)
		if err != nil {
			return controlplane.Stored[controlplane.PublicationLock]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.PublicationLock]{
				Value: replay, Replayed: true,
			}, nil
		}
		var (
			seriesID, episodeID            uuid.UUID
			runState, manifestHash, qcHash string
			qcState, gate, decision        string
			manifestGateID                 *uuid.UUID
			manifestLockedAt               *time.Time
			gateBindingMatches             bool
		)
		if err := tx.QueryRow(ctx, `
			SELECT ep.series_id, ep.id, gr.state,
			       gm.manifest_hash, gm.gate_decision_id, gm.locked_at,
			       qr.report_hash, qr.state, ad.gate, ad.decision,
			       EXISTS (
			         SELECT 1
			         FROM video_pipeline.approval_bindings ab
			         WHERE ab.decision_id = ad.id
			           AND ab.object_type = 'MANIFEST'
			           AND ab.revision_id = gm.id
			           AND ab.content_hash = gm.manifest_hash
			       )
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr
			  ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			JOIN video_pipeline.run_artifacts ra
			  ON ra.generation_run_id = gr.id AND ra.role = 'MANIFEST'
			JOIN video_pipeline.generation_manifests gm
			  ON gm.artifact_id = ra.artifact_id AND gm.id = $2
			JOIN video_pipeline.artifacts a
			  ON a.id = gm.artifact_id AND a.status = 'ACTIVE'
			JOIN video_pipeline.qc_reports qr
			  ON qr.generation_run_id = gr.id AND qr.id = $3
			JOIN video_pipeline.approval_decisions ad ON ad.id = $4
			WHERE gr.id = $1
			FOR UPDATE OF gr, gm, qr, ad`,
			runID, manifestID, qcReportID, gate3DecisionID,
		).Scan(
			&seriesID, &episodeID, &runState,
			&manifestHash, &manifestGateID, &manifestLockedAt,
			&qcHash, &qcState, &gate, &decision, &gateBindingMatches,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.Stored[controlplane.PublicationLock]{},
					controlplane.NewPolicyError(
						controlplane.CodeGateRequired,
						"publication lock inputs do not resolve to one immutable run lineage",
						"bind the succeeded run, ACTIVE manifest, passing QC, and exact G3 decision",
					)
			}
			return controlplane.Stored[controlplane.PublicationLock]{},
				fmt.Errorf("lock publication lineage: %w", err)
		}
		if runState != "SUCCEEDED" ||
			manifestHash != command.ManifestHash ||
			manifestGateID == nil || *manifestGateID != gate3DecisionID ||
			manifestLockedAt == nil ||
			qcHash != command.QCReportHash || qcState != "PASSED" ||
			gate != "G3" || decision != "APPROVED" || !gateBindingMatches {
			return controlplane.Stored[controlplane.PublicationLock]{},
				controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"publication lock requires exact succeeded Run/Manifest/QC/G3 bindings",
					"approve G3 for the current manifest and use the matching passing QC report",
				)
		}
		var decisionSeriesID uuid.UUID
		var decisionEpisodeID *uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT series_id, episode_id
			FROM video_pipeline.approval_decisions
			WHERE id = $1
			FOR SHARE`,
			gate3DecisionID,
		).Scan(&decisionSeriesID, &decisionEpisodeID); err != nil {
			return controlplane.Stored[controlplane.PublicationLock]{},
				fmt.Errorf("read G3 publication scope: %w", err)
		}
		if decisionSeriesID != seriesID ||
			decisionEpisodeID == nil || *decisionEpisodeID != episodeID {
			return controlplane.Stored[controlplane.PublicationLock]{},
				controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"G3 publication decision belongs to a different series or episode",
					"approve the exact current episode manifest",
				)
		}
		lockID := uuid.NewSHA1(runID, []byte(strings.Join([]string{
			command.ManifestID, command.ManifestHash,
			command.QCReportID, command.QCReportHash,
			command.Gate3DecisionID,
		}, ":")))
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.publication_locks
				(id, generation_run_id, manifest_id, manifest_hash,
				 qc_report_id, qc_report_hash, gate3_decision_id,
				 locked_by, locked_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (generation_run_id) DO NOTHING`,
			lockID, runID, manifestID, command.ManifestHash,
			qcReportID, command.QCReportHash, gate3DecisionID,
			command.Actor.ActorID, now,
		)
		if err != nil {
			return controlplane.Stored[controlplane.PublicationLock]{},
				fmt.Errorf("insert publication lock: %w", err)
		}
		var lock controlplane.PublicationLock
		var storedLockID, storedRunID, storedManifestID, storedQCID, storedGateID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT id, generation_run_id, manifest_id, manifest_hash,
			       qc_report_id, qc_report_hash, gate3_decision_id, locked_at
			FROM video_pipeline.publication_locks
			WHERE generation_run_id = $1
			FOR SHARE`,
			runID,
		).Scan(
			&storedLockID, &storedRunID,
			&storedManifestID, &lock.ManifestHash,
			&storedQCID, &lock.QCReportHash, &storedGateID, &lock.LockedAt,
		); err != nil {
			return controlplane.Stored[controlplane.PublicationLock]{},
				fmt.Errorf("read publication lock: %w", err)
		}
		lock.PublicationLockID = storedLockID.String()
		lock.RunID = storedRunID.String()
		lock.ManifestID = storedManifestID.String()
		lock.QCReportID = storedQCID.String()
		lock.Gate3DecisionID = storedGateID.String()
		if lock.PublicationLockID != lockID.String() ||
			lock.ManifestID != command.ManifestID ||
			lock.ManifestHash != command.ManifestHash ||
			lock.QCReportID != command.QCReportID ||
			lock.QCReportHash != command.QCReportHash ||
			lock.Gate3DecisionID != command.Gate3DecisionID {
			return controlplane.Stored[controlplane.PublicationLock]{},
				controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"generation run already has a different immutable publication lock",
				)
		}
		if tag.RowsAffected() == 1 {
			if err := insertAuditAndOutbox(
				ctx, tx, uuid.NewSHA1(lockID, []byte("audit")),
				uuid.NewSHA1(lockID, []byte("outbox")),
				command.Actor, "publication_lock.created",
				"PUBLICATION_LOCK", lockID,
				nil, nil, "", traceID,
				map[string]any{
					"runId":           runID.String(),
					"manifestId":      command.ManifestID,
					"manifestHash":    command.ManifestHash,
					"qcReportId":      command.QCReportID,
					"qcReportHash":    command.QCReportHash,
					"gate3DecisionId": command.Gate3DecisionID,
				},
				now,
			); err != nil {
				return controlplane.Stored[controlplane.PublicationLock]{}, err
			}
		}
		if err := completeIdempotency(
			ctx, tx, idempotency, "", lock, 201,
		); err != nil {
			return controlplane.Stored[controlplane.PublicationLock]{}, err
		}
		return controlplane.Stored[controlplane.PublicationLock]{Value: lock}, nil
	})
}

func (p *Postgres) ListRevisionImpacts(ctx context.Context, seriesIDRaw, sourceRevisionIDRaw string) ([]controlplane.FreshnessImpact, error) {
	seriesID, err := uuid.Parse(seriesIDRaw)
	if err != nil {
		return nil, controlplane.NewNotFoundError("series", seriesIDRaw)
	}
	sourceID, err := uuid.Parse(sourceRevisionIDRaw)
	if err != nil {
		return nil, controlplane.NewNotFoundError("source revision", sourceRevisionIDRaw)
	}
	var exists bool
	if err := p.pool.QueryRow(ctx,
		`SELECT true FROM video_pipeline.source_revisions WHERE id = $1 AND series_id = $2`,
		sourceID, seriesID,
	).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, controlplane.NewNotFoundError("source revision", sourceRevisionIDRaw)
		}
		return nil, fmt.Errorf("verify source revision: %w", err)
	}

	rows, err := p.pool.Query(ctx, `
		SELECT affected_type, affected_revision_id, state, reason_code
		FROM video_pipeline.freshness_impacts
		WHERE source_revision_id = $1
		ORDER BY affected_type, affected_revision_id`,
		sourceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query revision impacts: %w", err)
	}
	defer rows.Close()
	impacts := make([]controlplane.FreshnessImpact, 0)
	for rows.Next() {
		var impact controlplane.FreshnessImpact
		if err := rows.Scan(&impact.AffectedType, &impact.AffectedRevisionID, &impact.State, &impact.ReasonCode); err != nil {
			return nil, fmt.Errorf("scan revision impact: %w", err)
		}
		impacts = append(impacts, impact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate revision impacts: %w", err)
	}
	return impacts, nil
}

func markFreshnessImpacts(
	ctx context.Context,
	tx pgx.Tx,
	newSourceRevisionID uuid.UUID,
	replacedSourceRevisionID uuid.UUID,
	traceID string,
	now time.Time,
) error {
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE consumers AS (
			SELECT rd.consumer_type, rd.consumer_revision_id
			FROM video_pipeline.revision_dependencies rd
			WHERE rd.producer_revision_id = $1
			UNION
			SELECT rd.consumer_type, rd.consumer_revision_id
			FROM video_pipeline.revision_dependencies rd
			JOIN consumers c
			  ON rd.producer_type = c.consumer_type
			 AND rd.producer_revision_id = c.consumer_revision_id
		)
		SELECT consumer_type, consumer_revision_id
		FROM consumers
		ORDER BY consumer_type, consumer_revision_id`,
		replacedSourceRevisionID,
	)
	if err != nil {
		return fmt.Errorf("resolve changed-source consumers: %w", err)
	}
	type impactTarget struct {
		objectType string
		id         uuid.UUID
	}
	var targets []impactTarget
	for rows.Next() {
		var target impactTarget
		if err := rows.Scan(&target.objectType, &target.id); err != nil {
			rows.Close()
			return fmt.Errorf("scan changed-source consumer: %w", err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate changed-source consumers: %w", err)
	}
	rows.Close()

	for _, target := range targets {
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.freshness_impacts
				(id, source_revision_id, affected_type, affected_revision_id, state, reason_code, created_at)
			VALUES ($1, $2, $3, $4, 'STALE', 'UPSTREAM_REVISION_CHANGED', $5)
			ON CONFLICT (source_revision_id, affected_type, affected_revision_id)
			DO UPDATE SET state = 'STALE', reason_code = EXCLUDED.reason_code, resolved_at = NULL`,
			uuid.New(), newSourceRevisionID, target.objectType, target.id, now,
		); err != nil {
			return fmt.Errorf("persist freshness impact: %w", err)
		}
		if target.objectType == "SHOT_SPEC_REVISION" {
			if _, err := tx.Exec(ctx,
				`UPDATE video_pipeline.shot_spec_revisions SET freshness = 'STALE' WHERE id = $1`,
				target.id,
			); err != nil {
				return fmt.Errorf("mark shot revision stale: %w", err)
			}
		}
		aggregateID := uuid.NewSHA1(newSourceRevisionID, []byte(target.objectType+":"+target.id.String()))
		if err := insertAuditAndOutbox(
			ctx,
			tx,
			uuid.NewSHA1(aggregateID, []byte("audit")),
			uuid.NewSHA1(aggregateID, []byte("outbox")),
			controlplane.Actor{ActorID: "revision-impact-resolver", Role: "OPERATOR"},
			"dependency.stale",
			target.objectType,
			target.id,
			nil,
			nil,
			"UPSTREAM_REVISION_CHANGED",
			traceID,
			map[string]any{
				"sourceRevisionId":         newSourceRevisionID.String(),
				"replacedSourceRevisionId": replacedSourceRevisionID.String(),
			},
			now,
		); err != nil {
			return err
		}
	}
	return nil
}

func (p *Postgres) GetManifest(ctx context.Context, scopeType, revisionIDRaw string) (controlplane.GenerationManifest, error) {
	revisionID, err := uuid.Parse(revisionIDRaw)
	if err != nil {
		return controlplane.GenerationManifest{}, controlplane.NewNotFoundError("manifest", revisionIDRaw)
	}
	var manifest controlplane.GenerationManifest
	var payload []byte
	err = p.pool.QueryRow(ctx, `
		SELECT gm.id, gm.schema_version, gm.scope_type, gm.scope_revision_id,
		       gm.manifest_hash, a.artifact_uri, gm.payload, gm.locked_at
		FROM video_pipeline.generation_manifests gm
		JOIN video_pipeline.artifacts a ON a.id = gm.artifact_id
		WHERE gm.scope_type = $1 AND gm.scope_revision_id = $2
		  AND a.status = 'ACTIVE'
		ORDER BY gm.created_at DESC
		LIMIT 1`,
		strings.ToUpper(scopeType), revisionID,
	).Scan(
		&manifest.ManifestID, &manifest.SchemaVersion, &manifest.ScopeType, &manifest.ScopeRevisionID,
		&manifest.ManifestHash, &manifest.ArtifactURI, &payload, &manifest.LockedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.GenerationManifest{}, controlplane.NewNotFoundError("manifest", revisionIDRaw)
		}
		return controlplane.GenerationManifest{}, fmt.Errorf("read generation manifest: %w", err)
	}
	if err := json.Unmarshal(payload, &manifest.Payload); err != nil {
		return controlplane.GenerationManifest{}, fmt.Errorf("decode generation manifest: %w", err)
	}
	manifest.ProviderExecutions = objectSlice(manifest.Payload["providerExecutions"])
	manifest.Inputs = stringSlice(manifest.Payload["inputs"])
	manifest.Outputs = stringSlice(manifest.Payload["outputs"])
	manifest.CostSummary = objectMap(manifest.Payload["costSummary"])
	return manifest, nil
}

func (p *Postgres) GetOperation(ctx context.Context, operationIDRaw string) (controlplane.Operation, error) {
	operationID, err := uuid.Parse(operationIDRaw)
	if err != nil {
		return controlplane.Operation{}, controlplane.NewNotFoundError("operation", operationIDRaw)
	}
	var operation controlplane.Operation
	var workflowID, runID *string
	err = p.pool.QueryRow(ctx, `
		SELECT id, operation_type, aggregate_type, aggregate_id, state,
		       temporal_workflow_id, temporal_run_id, trace_id, created_at, updated_at
		FROM video_pipeline.operation_requests
		WHERE id = $1`,
		operationID,
	).Scan(
		&operation.OperationID, &operation.OperationType, &operation.AggregateType, &operation.AggregateID,
		&operation.State, &workflowID, &runID, &operation.TraceID, &operation.CreatedAt, &operation.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.Operation{}, controlplane.NewNotFoundError("operation", operationIDRaw)
		}
		return controlplane.Operation{}, fmt.Errorf("read operation: %w", err)
	}
	operation.TemporalWorkflowID = valueOrEmpty(workflowID)
	operation.TemporalRunID = valueOrEmpty(runID)
	return operation, nil
}

func (p *Postgres) FindActiveEpisodeWorkflow(ctx context.Context, episodeIDRaw string) (string, error) {
	episodeID, err := uuid.Parse(episodeIDRaw)
	if err != nil {
		return "", controlplane.NewNotFoundError("episode", episodeIDRaw)
	}
	var workflowID string
	err = p.pool.QueryRow(ctx, `
		SELECT temporal_workflow_id
		FROM video_pipeline.operation_requests
		WHERE aggregate_type = 'EPISODE'
		  AND aggregate_id = $1
		  AND operation_type = 'START_EPISODE_PRODUCTION'
		  AND state IN ('ACCEPTED', 'RUNNING')
		  AND temporal_workflow_id IS NOT NULL
		ORDER BY created_at DESC
		LIMIT 1`,
		episodeID,
	).Scan(&workflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", controlplane.NewNotFoundError("active episode workflow", episodeIDRaw)
		}
		return "", fmt.Errorf("find active episode workflow: %w", err)
	}
	return workflowID, nil
}

func (p *Postgres) MarkOperationStarted(ctx context.Context, operationIDRaw, workflowID, runID string) error {
	operationID, err := uuid.Parse(operationIDRaw)
	if err != nil {
		return controlplane.NewNotFoundError("operation", operationIDRaw)
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var aggregateType, operationType string
		var aggregateID uuid.UUID
		if err := tx.QueryRow(ctx, `
			UPDATE video_pipeline.operation_requests
			SET state = 'RUNNING', temporal_workflow_id = $2,
			    temporal_run_id = NULLIF($3, ''), updated_at = $4
			WHERE id = $1 AND state = 'ACCEPTED'
			RETURNING aggregate_type, aggregate_id, operation_type`,
			operationID, workflowID, runID, p.now().UTC(),
		).Scan(&aggregateType, &aggregateID, &operationType); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return struct{}{}, controlplane.NewConflictError(controlplane.CodeConflict, "operation is no longer ACCEPTED")
			}
			return struct{}{}, fmt.Errorf("mark operation running: %w", err)
		}
		if aggregateType == "GENERATION_RUN" &&
			(operationType == "CREATE_GENERATION_RUN" || operationType == "RESUME_GENERATION_RUN") {
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_runs
				SET state = CASE WHEN state = 'VALIDATED' THEN 'QUEUED' ELSE state END,
				    temporal_workflow_id = $2, temporal_run_id = NULLIF($3, ''),
				    started_at = COALESCE(started_at, $4)
				WHERE id = $1`,
				aggregateID, workflowID, runID, p.now().UTC(),
			); err != nil {
				return struct{}{}, fmt.Errorf("mark shot run queued: %w", err)
			}
		}
		return struct{}{}, nil
	})
	return err
}

func (p *Postgres) MarkOperationSucceeded(ctx context.Context, operationIDRaw string) error {
	operationID, err := uuid.Parse(operationIDRaw)
	if err != nil {
		return controlplane.NewNotFoundError("operation", operationIDRaw)
	}
	tag, err := p.pool.Exec(ctx, `
		UPDATE video_pipeline.operation_requests
		SET state = 'SUCCEEDED', updated_at = $2
		WHERE id = $1 AND state IN ('ACCEPTED', 'RUNNING', 'CANCEL_REQUESTED')`,
		operationID, p.now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("mark operation succeeded: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.NewConflictError(controlplane.CodeConflict, "operation is not active")
	}
	return nil
}

func (p *Postgres) MarkOperationFailed(ctx context.Context, operationIDRaw, failureCode string) error {
	operationID, err := uuid.Parse(operationIDRaw)
	if err != nil {
		return controlplane.NewNotFoundError("operation", operationIDRaw)
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var aggregateType, operationType string
		var aggregateID uuid.UUID
		if err := tx.QueryRow(ctx, `
			UPDATE video_pipeline.operation_requests
			SET state = 'FAILED', updated_at = $2
			WHERE id = $1 AND state IN ('ACCEPTED', 'RUNNING')
			RETURNING aggregate_type, aggregate_id, operation_type`,
			operationID, p.now().UTC(),
		).Scan(&aggregateType, &aggregateID, &operationType); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return struct{}{}, controlplane.NewConflictError(controlplane.CodeConflict, "operation is not active")
			}
			return struct{}{}, fmt.Errorf("mark operation failed (%s): %w", failureCode, err)
		}
		if aggregateType == "GENERATION_RUN" && operationType == "CREATE_GENERATION_RUN" {
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_runs
				SET state = 'FAILED', failure_class = 'INFRASTRUCTURE',
				    failure_code = $2, finished_at = $3
				WHERE id = $1 AND state IN ('VALIDATED', 'QUEUED', 'RUNNING')`,
				aggregateID, failureCode, p.now().UTC(),
			); err != nil {
				return struct{}{}, fmt.Errorf("fail generation run after workflow start failure: %w", err)
			}
		}
		return struct{}{}, nil
	})
	return err
}

func withSerializable[T any](ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) (T, error)) (T, error) {
	var zero T
	for attempt := 1; attempt <= maxTxAttempts; attempt++ {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return zero, fmt.Errorf("begin serializable transaction: %w", err)
		}
		value, runErr := fn(tx)
		if runErr != nil {
			_ = tx.Rollback(ctx)
			if retryableTransaction(runErr) {
				if attempt < maxTxAttempts {
					continue
				}
				return zero, controlplane.NewConflictError(
					controlplane.CodeConflict,
					"transaction contention did not converge after bounded retries",
				)
			}
			return zero, runErr
		}
		if err := tx.Commit(ctx); err != nil {
			if retryableTransaction(err) {
				if attempt < maxTxAttempts {
					continue
				}
				return zero, controlplane.NewConflictError(
					controlplane.CodeConflict,
					"transaction contention did not converge after bounded retries",
				)
			}
			return zero, fmt.Errorf("commit serializable transaction: %w", err)
		}
		return value, nil
	}
	return zero, controlplane.NewConflictError(controlplane.CodeConflict, "transaction could not be serialized")
}

func retryableTransaction(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return false
	}
	return postgresError.Code == "40001" || postgresError.Code == "40P01"
}

func reserveIdempotency[T any](
	ctx context.Context,
	tx pgx.Tx,
	idempotency controlplane.Idempotency,
	replay *T,
	now time.Time,
) (bool, error) {
	if idempotency.Scope == "" || idempotency.Key == "" || idempotency.RequestHash == "" {
		return false, controlplane.NewInternalError("idempotency metadata is incomplete", nil)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.idempotency_records
			(scope, idempotency_key, request_hash, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (scope, idempotency_key) DO NOTHING`,
		idempotency.Scope, idempotency.Key, idempotency.RequestHash, now, now.Add(idempotencyTTL),
	)
	if err != nil {
		return false, fmt.Errorf("reserve idempotency key: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return false, nil
	}

	var existingHash string
	var responseBody []byte
	if err := tx.QueryRow(ctx, `
		SELECT request_hash, response_body
		FROM video_pipeline.idempotency_records
		WHERE scope = $1 AND idempotency_key = $2
		FOR UPDATE`,
		idempotency.Scope, idempotency.Key,
	).Scan(&existingHash, &responseBody); err != nil {
		return false, fmt.Errorf("read idempotency record: %w", err)
	}
	if existingHash != idempotency.RequestHash {
		return false, controlplane.NewConflictError(
			controlplane.CodeConflict,
			"Idempotency-Key was already used with a different canonical request",
		)
	}
	if len(responseBody) == 0 {
		return false, controlplane.NewConflictError(
			controlplane.CodeConflict,
			"the original idempotent operation has not produced a response",
		)
	}
	if err := json.Unmarshal(responseBody, replay); err != nil {
		return false, fmt.Errorf("decode idempotent response: %w", err)
	}
	return true, nil
}

func completeIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	idempotency controlplane.Idempotency,
	operationID string,
	value any,
	status int,
) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode idempotent response: %w", err)
	}
	var operation any
	if operationID != "" {
		parsed, err := uuid.Parse(operationID)
		if err != nil {
			return fmt.Errorf("parse idempotent operation ID: %w", err)
		}
		operation = parsed
	}
	tag, err := tx.Exec(ctx, `
		UPDATE video_pipeline.idempotency_records
		SET response_status = $3, response_body = $4, operation_id = $6
		WHERE scope = $1 AND idempotency_key = $2 AND request_hash = $5`,
		idempotency.Scope, idempotency.Key, status, body, idempotency.RequestHash, operation,
	)
	if err != nil {
		return fmt.Errorf("complete idempotency record: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("idempotency record disappeared before commit")
	}
	return nil
}

func insertOperation(
	ctx context.Context,
	tx pgx.Tx,
	operation controlplane.Operation,
	requestedBy string,
	expectedRevision *int,
) error {
	var workflowID any
	if operation.TemporalWorkflowID != "" {
		workflowID = operation.TemporalWorkflowID
	}
	var runID any
	if operation.TemporalRunID != "" {
		runID = operation.TemporalRunID
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.operation_requests
			(id, operation_type, aggregate_type, aggregate_id, expected_revision, state,
			 temporal_workflow_id, temporal_run_id, trace_id, requested_by, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		operation.OperationID, operation.OperationType, operation.AggregateType, operation.AggregateID,
		expectedRevision, operation.State, workflowID, runID, operation.TraceID, requestedBy,
		operation.CreatedAt, operation.UpdatedAt,
	); err != nil {
		return fmt.Errorf("insert operation request: %w", err)
	}
	return nil
}

func insertAuditAndOutbox(
	ctx context.Context,
	tx pgx.Tx,
	auditID uuid.UUID,
	eventID uuid.UUID,
	actor controlplane.Actor,
	action string,
	aggregateType string,
	aggregateID uuid.UUID,
	beforeRevision *int,
	afterRevision *int,
	reasonCode string,
	traceID string,
	payload map[string]any,
	now time.Time,
) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode audit payload: %w", err)
	}
	eventType, err := eventTypeForAction(action)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.audit_events
			(id, occurred_at, actor_id, actor_role, action, aggregate_type, aggregate_id,
			 before_revision, after_revision, reason_code, trace_id, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), $11, $12)`,
		auditID, now, actor.ActorID, actor.Role, action, aggregateType, aggregateID,
		beforeRevision, afterRevision, reasonCode, traceID, payloadJSON,
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.outbox_events
			(event_id, event_type, aggregate_type, aggregate_id, aggregate_revision,
			 trace_id, occurred_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		eventID, eventType, aggregateType, aggregateID, afterRevision, traceID, now, payloadJSON,
	); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func eventTypeForAction(action string) (string, error) {
	eventTypes := map[string]string{
		"series.created":                       "video.series.created.v1",
		"source_revision.created":              "video.revision.created.v1",
		"content_compilation.requested":        "video.content-compilation.requested.v1",
		"generation_plan.created":              "video.generation-plan.created.v1",
		"episode.production.requested":         "video.production.requested.v1",
		"generation_run.created":               "video.run.state-changed.v1",
		"generation_run.cancel_requested":      "video.run.state-changed.v1",
		"generation_run.recovery_requested":    "video.run.state-changed.v1",
		"generation_run.pause_requested":       "video.run.state-changed.v1",
		"generation_run.resumed":               "video.run.state-changed.v1",
		"generation_run.workflow_finalized":    "video.run.state-changed.v1",
		"prompt_snapshot.created":              "video.revision.created.v1",
		"provider_job.completed":               "video.provider-job.state-changed.v1",
		"provider_job.cancellation_reconciled": "video.provider-job.state-changed.v1",
		"qc_report.created":                    "video.qc.completed.v1",
		"manifest.created":                     "video.revision.created.v1",
		"approval.decided":                     "video.approval.decided.v1",
		"manifest.locked":                      "video.manifest.locked.v1",
		"publication_lock.created":             "video.publication-lock.created.v1",
		"dependency.stale":                     "video.dependency.stale.v1",
		"workflow_step.completed":              "video.workflow-step.completed.v1",
		"episode.postproduction.completed":     "video.episode.postproduction-completed.v1",
	}
	eventType, ok := eventTypes[action]
	if !ok {
		return "", fmt.Errorf("outbox event type is not registered for audit action %q", action)
	}
	return eventType, nil
}

func requireAllowedLicense(ctx context.Context, tx pgx.Tx, licenseID uuid.UUID, now time.Time) error {
	var status string
	var commercialUse bool
	var expiresAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT policy_status, commercial_use, expires_at
		FROM video_pipeline.license_snapshots
		WHERE id = $1
		FOR SHARE`,
		licenseID,
	).Scan(&status, &commercialUse, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked, "license snapshot was not found", "attach a reviewed ALLOWED license snapshot",
			)
		}
		return fmt.Errorf("read license snapshot: %w", err)
	}
	if status != "ALLOWED" || !commercialUse || (expiresAt != nil && !expiresAt.After(now)) {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"license is blocked, expired, or does not permit commercial use",
			"replace or review the license before queueing work",
		)
	}
	return nil
}

func requireActiveProfile(ctx context.Context, tx pgx.Tx, profileIDRaw string) error {
	profileID, err := uuid.Parse(profileIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeProfileInactive, "generation profile identifier is invalid", "select an ACTIVE profile revision",
		)
	}
	var status string
	if err := tx.QueryRow(ctx,
		`SELECT status FROM video_pipeline.generation_profiles WHERE id = $1 FOR SHARE`,
		profileID,
	).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.NewPolicyError(
				controlplane.CodeProfileInactive, "generation profile was not found", "select an ACTIVE profile revision",
			)
		}
		return fmt.Errorf("read generation profile: %w", err)
	}
	if status != "ACTIVE" {
		return controlplane.NewPolicyError(
			controlplane.CodeProfileInactive, "generation profile is not ACTIVE", "activate the exact profile revision",
		)
	}
	return nil
}

func validateRouteSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	route controlplane.ModelRouteSnapshot,
	now time.Time,
) (string, map[string]any, error) {
	profileID, err := uuid.Parse(route.ProviderProfileID)
	if err != nil {
		return "", nil, controlplane.NewPolicyError(
			controlplane.CodeCapability, "provider profile identifier is invalid", "select an ACTIVE capability route",
		)
	}
	var pricingVersion *string
	var limitsJSON []byte
	var enabled bool
	var mode, health string
	var expiresAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT pcs.pricing_rule_version, pcs.limits, pp.enabled, pp.mode, pp.health, pcs.expires_at
		FROM video_pipeline.provider_capability_snapshots pcs
		JOIN video_pipeline.provider_profiles pp ON pp.id = pcs.provider_profile_id
		WHERE pcs.provider_profile_id = $1
		  AND pcs.capability_alias = $2
		  AND pcs.model_id = $3
		  AND COALESCE(pcs.endpoint_id, '') = $4
		  AND pcs.route_version = $5
		  AND pcs.capability_hash = $6
		  AND pcs.status = 'ACTIVE'
		FOR SHARE OF pcs, pp`,
		profileID, route.CapabilityAlias, route.ModelID, route.EndpointID, route.RouteVersion, route.CapabilityHash,
	).Scan(&pricingVersion, &limitsJSON, &enabled, &mode, &health, &expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, controlplane.NewPolicyError(
				controlplane.CodeCapability,
				"provider/model route is not an ACTIVE immutable capability snapshot",
				"refresh capability discovery and select an allowed route",
			)
		}
		return "", nil, fmt.Errorf("validate provider route: %w", err)
	}
	if !enabled || health == "UNAVAILABLE" || (expiresAt != nil && !expiresAt.After(now)) {
		return "", nil, controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"provider route is disabled, unavailable, or expired",
			"repair the provider profile or select another approved route",
		)
	}
	if mode == "LIVE" && health != "READY" {
		return "", nil, controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"LIVE provider route is not READY",
			"complete credential and model-access preflight before queueing",
		)
	}
	var limits map[string]any
	if err := json.Unmarshal(limitsJSON, &limits); err != nil {
		return "", nil, fmt.Errorf("decode provider capability limits: %w", err)
	}
	version := "unknown-price"
	if pricingVersion != nil && *pricingVersion != "" {
		version = *pricingVersion
	}
	return version, limits, nil
}

func requirePostProductionEvidenceMode(
	ctx context.Context,
	tx pgx.Tx,
	profileIDRaw string,
	evidence string,
) error {
	profileID, err := uuid.Parse(profileIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"post-production provider profile identifier is invalid",
			"select the frozen speech capability profile",
		)
	}
	var mode, health string
	if err := tx.QueryRow(ctx, `
		SELECT mode, health
		FROM video_pipeline.provider_profiles
		WHERE id = $1
		FOR SHARE`,
		profileID,
	).Scan(&mode, &health); err != nil {
		return fmt.Errorf("read post-production provider mode: %w", err)
	}
	if err := validatePostProductionEvidenceMode(evidence, mode, health); err != nil {
		return err
	}
	return nil
}

func validatePostProductionEvidenceMode(evidence, mode, health string) error {
	valid := false
	switch evidence {
	case "mock_only":
		valid = mode == "MOCK"
	case "live_provider_call":
		valid = mode == "LIVE" && health == "READY"
	case "pending_key":
		valid = mode == "DRY_RUN" || mode == "LIVE"
	}
	if valid {
		return nil
	}
	return controlplane.NewPolicyError(
		controlplane.CodeCapability,
		"post-production evidence does not match the provider profile mode",
		"use MOCK only for mock_only, LIVE/READY only for live evidence, and DRY_RUN/LIVE for pending_key",
	)
}

func requireApprovedDecision(
	ctx context.Context,
	tx pgx.Tx,
	decisionIDRaw string,
	gate string,
	seriesID uuid.UUID,
	episodeID uuid.UUID,
	requiredType string,
	requiredID uuid.UUID,
) error {
	decisionID, err := uuid.Parse(decisionIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(controlplane.CodeGateRequired, gate+" decision identifier is invalid", "approve the exact revision")
	}
	var decision string
	if err := tx.QueryRow(ctx, `
		SELECT decision
		FROM video_pipeline.approval_decisions
		WHERE id = $1
		  AND gate = $2
		  AND series_id = $3
		  AND episode_id = $4
		FOR SHARE`,
		decisionID, gate, seriesID, episodeID,
	).Scan(&decision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.NewPolicyError(controlplane.CodeGateRequired, gate+" decision was not found", "approve the exact revision")
		}
		return fmt.Errorf("read approval decision: %w", err)
	}
	if decision != "APPROVED" {
		return controlplane.NewPolicyError(controlplane.CodeGateRequired, gate+" decision is not APPROVED", "obtain approval for the exact immutable inputs")
	}
	if requiredType != "" {
		if err := requireDecisionBindings(ctx, tx, decisionIDRaw, requiredType, []uuid.UUID{requiredID}); err != nil {
			return err
		}
	}
	return nil
}

func requireDecisionBindings(
	ctx context.Context,
	tx pgx.Tx,
	decisionIDRaw string,
	objectType string,
	revisionIDs []uuid.UUID,
) error {
	decisionID, err := uuid.Parse(decisionIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeGateRequired, "decision identifier is invalid", "approve the exact immutable revisions",
		)
	}
	var count int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.approval_bindings
		WHERE decision_id = $1
		  AND object_type = $2
		  AND revision_id = ANY($3::uuid[])`,
		decisionID, objectType, revisionIDs,
	).Scan(&count); err != nil {
		return fmt.Errorf("read approval bindings: %w", err)
	}
	if count != len(revisionIDs) {
		return controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"approval decision does not bind every exact immutable revision",
			"approve the current episode and shot revision set",
		)
	}
	return nil
}

func requireBudgetApproval(
	ctx context.Context,
	tx pgx.Tx,
	approvalIDRaw string,
	seriesID uuid.UUID,
	episodeID uuid.UUID,
	generationPlanIDRaw string,
	budgetScope string,
	required controlplane.BudgetLimit,
) error {
	return requireBudgetApprovalWithLock(
		ctx, tx, approvalIDRaw, seriesID, episodeID,
		generationPlanIDRaw, budgetScope, required, false,
	)
}

func requireBudgetApprovalForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	approvalIDRaw string,
	seriesID uuid.UUID,
	episodeID uuid.UUID,
	generationPlanIDRaw string,
	budgetScope string,
	required controlplane.BudgetLimit,
) error {
	return requireBudgetApprovalWithLock(
		ctx, tx, approvalIDRaw, seriesID, episodeID,
		generationPlanIDRaw, budgetScope, required, true,
	)
}

func requireBudgetApprovalWithLock(
	ctx context.Context,
	tx pgx.Tx,
	approvalIDRaw string,
	seriesID uuid.UUID,
	episodeID uuid.UUID,
	generationPlanIDRaw string,
	budgetScope string,
	required controlplane.BudgetLimit,
	forUpdate bool,
) error {
	approvalID, err := uuid.Parse(approvalIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded, "budget approval identifier is invalid", "confirm the immutable generation plan budget",
		)
	}
	generationPlanID, err := uuid.Parse(generationPlanIDRaw)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded, "generation plan identifier is invalid", "confirm the immutable generation plan budget",
		)
	}
	var state string
	var approvedPlanID *uuid.UUID
	var approvedScope, approvedCurrency *string
	var approvedMicros *int64
	const sharedApprovalQuery = `
		SELECT state, generation_plan_id, budget_scope,
		       budget_limit_micros, budget_currency
		FROM video_pipeline.review_tasks
		WHERE id = $1
		  AND review_type = 'BUDGET'
		  AND series_id = $2
		  AND (episode_id IS NULL OR episode_id = $3)
		FOR SHARE`
	const exclusiveApprovalQuery = `
		SELECT state, generation_plan_id, budget_scope,
		       budget_limit_micros, budget_currency
		FROM video_pipeline.review_tasks
		WHERE id = $1
		  AND review_type = 'BUDGET'
		  AND series_id = $2
		  AND (episode_id IS NULL OR episode_id = $3)
		FOR UPDATE`
	approvalQuery := sharedApprovalQuery
	if forUpdate {
		approvalQuery = exclusiveApprovalQuery
	}
	if err := tx.QueryRow(ctx, approvalQuery,
		approvalID, seriesID, episodeID,
	).Scan(
		&state, &approvedPlanID, &approvedScope, &approvedMicros, &approvedCurrency,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded, "budget approval was not found", "approve the plan upper estimate before queueing",
			)
		}
		return fmt.Errorf("read budget approval: %w", err)
	}
	if state != "APPROVED" {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded, "budget approval is not APPROVED", "approve or reduce the plan budget",
		)
	}
	if approvedPlanID == nil || approvedScope == nil ||
		approvedMicros == nil || approvedCurrency == nil {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"legacy budget approval is not bound to an immutable plan envelope",
			"approve the exact current plan, budget scope, amount, and currency",
		)
	}
	if *approvedPlanID != generationPlanID || *approvedScope != budgetScope {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"budget approval belongs to an old plan or a different spend scope",
			"approve the exact current plan and provider spend scope",
		)
	}
	if *approvedCurrency != required.Currency {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"budget approval currency differs from the immutable plan",
			"approve the exact plan currency before paid submission",
		)
	}
	if *approvedMicros != required.AmountMicros {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"budget approval amount differs from the immutable plan limit",
			"approve the exact frozen plan upper limit before paid submission",
		)
	}
	return nil
}

func requireShotAssetLicenses(
	ctx context.Context,
	tx pgx.Tx,
	shotIDs []uuid.UUID,
	now time.Time,
	policy controlplane.ExecutionPolicy,
) error {
	rows, err := tx.Query(ctx,
		`SELECT asset_version_refs FROM video_pipeline.shot_spec_revisions WHERE id = ANY($1::uuid[])`,
		shotIDs,
	)
	if err != nil {
		return fmt.Errorf("read shot asset references: %w", err)
	}
	defer rows.Close()
	var assetIDs []uuid.UUID
	for rows.Next() {
		var refs []uuid.UUID
		if err := rows.Scan(&refs); err != nil {
			return fmt.Errorf("scan shot asset references: %w", err)
		}
		assetIDs = append(assetIDs, refs...)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate shot asset references: %w", err)
	}
	return requireAssetLicenses(ctx, tx, assetIDs, now, policy)
}

func requireAssetLicenses(
	ctx context.Context,
	tx pgx.Tx,
	assetIDs []uuid.UUID,
	now time.Time,
	policy controlplane.ExecutionPolicy,
) error {
	if len(assetIDs) == 0 {
		return nil
	}
	var invalidLicense, invalidConsent int
	if err := tx.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE
				av.id IS NULL
				OR av.status NOT IN ('APPROVED', 'LOCKED')
				OR ls.id IS NULL
				OR ls.policy_status <> 'ALLOWED'
				OR ($4 = 'COMMERCIAL_RELEASE' AND NOT ls.commercial_use)
				OR NOT ($3 = ANY(ls.territories))
				OR (ls.expires_at IS NOT NULL AND ls.expires_at <= $2)
			),
			COUNT(*) FILTER (WHERE
				av.consent_asset_id IS NOT NULL
				AND (
					ca.id IS NULL
					OR ca.status <> 'ACTIVE'
					OR NOT ($3 = ANY(ca.territories))
					OR (ca.expires_at IS NOT NULL AND ca.expires_at <= $2)
				)
			)
		FROM unnest($1::uuid[]) AS requested(id)
		LEFT JOIN video_pipeline.asset_versions av ON av.id = requested.id
		LEFT JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
		LEFT JOIN video_pipeline.consent_assets ca ON ca.id = av.consent_asset_id`,
		assetIDs, now, policy.TargetTerritory, policy.ProductForm,
	).Scan(&invalidLicense, &invalidConsent); err != nil {
		return fmt.Errorf("validate asset licenses: %w", err)
	}
	if invalidLicense > 0 {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"one or more asset revisions are missing, unapproved, unlicensed, expired, or incompatible with the target territory/product form",
			"replace or approve every exact asset revision for the requested territory and product form before queueing",
		)
	}
	if invalidConsent > 0 {
		return controlplane.NewPolicyError(
			controlplane.CodeConsentRequired,
			"one or more bound consent records are revoked, expired, unavailable, or incompatible with the target territory",
			"renew or replace the consent record for the requested territory before queueing",
		)
	}
	return nil
}

type contentSafetyDecisionEnvelope struct {
	PolicyVersion string    `json:"policyVersion"`
	EvidenceHash  string    `json:"evidenceHash"`
	ValidUntil    time.Time `json:"validUntil"`
	Explanation   string    `json:"explanation,omitempty"`
}

func approvalExplanation(command controlplane.CreateApprovalDecisionCommand) (string, error) {
	if strings.ToUpper(command.Gate) != "SAFETY" {
		return command.Explanation, nil
	}
	if strings.TrimSpace(command.PolicyVersion) == "" ||
		len(command.EvidenceHash) != sha256.Size*2 ||
		command.ValidUntil == nil {
		return "", controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"SAFETY decision is missing its immutable policy, evidence, or validity envelope",
			"submit an authorized SAFETY decision with policyVersion, evidenceHash, and validUntil",
		)
	}
	var episodeBindings, shotBindings, evidenceBindings int
	for _, binding := range command.Bindings {
		switch strings.ToUpper(binding.ObjectType) {
		case "EPISODE_REVISION":
			episodeBindings++
		case "SHOT_SPEC_REVISION":
			shotBindings++
		case "ARTIFACT":
			evidenceBindings++
			if binding.ContentHash != command.EvidenceHash {
				return "", controlplane.NewPolicyError(
					controlplane.CodeContentBlocked,
					"SAFETY evidence hash does not match its immutable artifact binding",
					"bind the exact evidence artifact",
				)
			}
		}
	}
	if command.Decision == "APPROVED" &&
		(episodeBindings != 1 || shotBindings == 0 || evidenceBindings != 1) {
		return "", controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"SAFETY approval has incomplete immutable input bindings",
			"bind exactly one episode revision, every shot revision, and one evidence artifact",
		)
	}
	encoded, err := json.Marshal(contentSafetyDecisionEnvelope{
		PolicyVersion: command.PolicyVersion,
		EvidenceHash:  command.EvidenceHash,
		ValidUntil:    command.ValidUntil.UTC(),
		Explanation:   command.Explanation,
	})
	if err != nil {
		return "", fmt.Errorf("encode content safety decision evidence: %w", err)
	}
	return string(encoded), nil
}

func requireContentSafetyDecision(
	ctx context.Context,
	tx pgx.Tx,
	policy controlplane.ExecutionPolicy,
	seriesID uuid.UUID,
	episodeRevisionID uuid.UUID,
	shotIDs []uuid.UUID,
	now time.Time,
) error {
	decisionID, err := uuid.Parse(policy.ContentSafetyDecisionID)
	if err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety decision identifier is missing or invalid",
			"obtain an authorized SAFETY decision bound to the exact immutable inputs",
		)
	}
	var (
		decisionSeriesID  uuid.UUID
		decisionEpisodeID *uuid.UUID
		decision          string
		actorRole         string
		explanation       string
	)
	if err := tx.QueryRow(ctx, `
		SELECT series_id, episode_id, decision, actor_role, COALESCE(explanation, '')
		FROM video_pipeline.approval_decisions
		WHERE id = $1 AND gate = 'SAFETY'
		FOR SHARE`,
		decisionID,
	).Scan(&decisionSeriesID, &decisionEpisodeID, &decision, &actorRole, &explanation); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.NewPolicyError(
				controlplane.CodeContentBlocked,
				"authorized content safety approval was not found",
				"create a SAFETY decision before planning or queueing provider work",
			)
		}
		return fmt.Errorf("read content safety decision: %w", err)
	}
	if decision != "APPROVED" ||
		(strings.ToUpper(actorRole) != "SAFETY_REVIEWER" && strings.ToUpper(actorRole) != "ADMIN") {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety decision is not an authorized approval",
			"obtain approval from SAFETY_REVIEWER or ADMIN",
		)
	}
	var evidence contentSafetyDecisionEnvelope
	if err := json.Unmarshal([]byte(explanation), &evidence); err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety decision evidence is malformed",
			"replace it with an immutable SAFETY decision",
		)
	}
	if evidence.PolicyVersion != policy.ContentSafetyPolicyVersion ||
		len(evidence.EvidenceHash) != sha256.Size*2 ||
		!evidence.ValidUntil.After(now) {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety decision is expired or incompatible with the requested policy version",
			"obtain a current SAFETY decision for the exact policy version",
		)
	}
	var expectedEpisodeID uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT episode_id FROM video_pipeline.episode_revisions WHERE id = $1`,
		episodeRevisionID,
	).Scan(&expectedEpisodeID); err != nil {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety episode revision binding is missing",
			"bind the exact immutable episode revision",
		)
	}
	if decisionSeriesID != seriesID || decisionEpisodeID == nil || *decisionEpisodeID != expectedEpisodeID {
		return controlplane.NewPolicyError(
			controlplane.CodeForbidden,
			"content safety decision belongs to a different series or episode",
			"obtain a SAFETY decision in the requested production scope",
		)
	}
	rows, err := tx.Query(ctx, `
		SELECT object_type, revision_id, content_hash
		FROM video_pipeline.approval_bindings
		WHERE decision_id = $1`,
		decisionID,
	)
	if err != nil {
		return fmt.Errorf("read content safety bindings: %w", err)
	}
	defer rows.Close()
	boundShots := make(map[uuid.UUID]struct{}, len(shotIDs))
	episodeBound := false
	evidenceBound := false
	for rows.Next() {
		var objectType, contentHash string
		var revisionID uuid.UUID
		if err := rows.Scan(&objectType, &revisionID, &contentHash); err != nil {
			return fmt.Errorf("scan content safety binding: %w", err)
		}
		switch objectType {
		case "EPISODE_REVISION":
			episodeBound = episodeBound || revisionID == episodeRevisionID
		case "SHOT_SPEC_REVISION":
			boundShots[revisionID] = struct{}{}
		case "ARTIFACT":
			evidenceBound = evidenceBound || contentHash == evidence.EvidenceHash
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate content safety bindings: %w", err)
	}
	if !episodeBound || !evidenceBound || len(boundShots) != len(shotIDs) {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety decision is not bound to the exact episode, shots, and evidence artifact",
			"review and approve the complete immutable input set",
		)
	}
	for _, shotID := range shotIDs {
		if _, ok := boundShots[shotID]; !ok {
			return controlplane.NewPolicyError(
				controlplane.CodeContentBlocked,
				"content safety decision does not cover every requested shot revision",
				"obtain a SAFETY decision for the exact shot set",
			)
		}
	}
	return nil
}

func requireExecutionPolicy(
	limits map[string]any,
	policy controlplane.ExecutionPolicy,
	requestedCalls int,
) error {
	if policy.TargetTerritory == "" || !containsLimitString(limits, "allowedTerritories", policy.TargetTerritory) {
		return controlplane.NewPolicyError(
			controlplane.CodeRegionUnavailable,
			"the frozen provider capability does not allow the target territory",
			"select a route whose immutable capability snapshot allows the target territory",
		)
	}
	if policy.ProductForm == "" || !containsLimitString(limits, "productForms", policy.ProductForm) {
		return controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"the frozen provider capability does not allow the requested product form",
			"select a compatible route or product form",
		)
	}
	if policy.ContentSafetyDecisionID == "" ||
		!containsLimitString(limits, "contentSafetyPolicyVersions", policy.ContentSafetyPolicyVersion) {
		return controlplane.NewPolicyError(
			controlplane.CodeContentBlocked,
			"content safety approval is missing or incompatible with the route policy",
			"obtain fail-closed safety approval for a policy version accepted by the route",
		)
	}
	remainingCalls, ok := numericLimit(limits, "remainingCalls")
	if !ok || remainingCalls < float64(requestedCalls) {
		return controlplane.NewPolicyError(
			controlplane.CodeQuotaExceeded,
			"the frozen provider quota cannot cover the requested calls",
			"reduce the batch or confirm a capability snapshot with sufficient quota",
		)
	}
	return nil
}

func containsLimitString(limits map[string]any, key, expected string) bool {
	values, ok := limits[key].([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		if text, ok := value.(string); ok && text == expected {
			return true
		}
	}
	return false
}

func validateApprovalBindings(
	ctx context.Context,
	tx pgx.Tx,
	gate string,
	decision string,
	bindings []controlplane.ApprovalBinding,
) error {
	var shotIDs, runIDs, episodeRevisionIDs, manifestIDs []uuid.UUID
	for _, binding := range bindings {
		revisionID, err := uuid.Parse(binding.RevisionID)
		if err != nil {
			return controlplane.NewPolicyError(
				controlplane.CodeStaleDependency, "approval binding revisionId is invalid", "bind an exact immutable revision",
			)
		}
		query, err := approvalBindingQuery(strings.ToUpper(binding.ObjectType))
		if err != nil {
			return err
		}
		var storedHash string
		var policyState *string
		if err := tx.QueryRow(ctx, query, revisionID).Scan(&storedHash, &policyState); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return controlplane.NewPolicyError(
					controlplane.CodeStaleDependency,
					"approval binding "+binding.ObjectType+" "+binding.RevisionID+" was not found",
					"refresh and bind the exact immutable revision",
				)
			}
			return fmt.Errorf("read approval binding: %w", err)
		}
		if storedHash != binding.ContentHash {
			return controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"approval binding hash does not match persisted immutable content",
			)
		}
		if decision == "APPROVED" && policyState != nil {
			switch strings.ToUpper(binding.ObjectType) {
			case "EPISODE_REVISION":
				switch gate {
				case "G1":
					if *policyState != "DRAFT" && *policyState != "G1_APPROVED" {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired, "G1 requires a DRAFT episode revision", "select the current draft episode revision",
						)
					}
				case "G2":
					if *policyState != "G1_APPROVED" && *policyState != "G2_APPROVED" {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired, "G2 requires a G1_APPROVED episode revision", "complete G1 before G2",
						)
					}
				case "G3":
					if *policyState != "G2_APPROVED" {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired, "G3 requires a G2_APPROVED episode revision", "complete G2 and production before G3",
						)
					}
				case "SAFETY":
					if *policyState != "G2_APPROVED" {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired,
							"SAFETY requires a G2_APPROVED episode revision",
							"complete G2 before content safety approval",
						)
					}
				}
			case "SHOT_SPEC_REVISION":
				if *policyState == "STALE" {
					return controlplane.NewPolicyError(
						controlplane.CodeStaleDependency, "stale shot revision cannot be approved", "revalidate or regenerate the shot",
					)
				}
			case "GENERATION_RUN":
				if gate == "Q1" || gate == "G3" {
					if *policyState != "SUCCEEDED" {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired,
							"generation run must be SUCCEEDED before approval",
							"finish generation, CAS commit, and automatic QC first",
						)
					}
					var qcPassed bool
					if err := tx.QueryRow(ctx, `
						SELECT EXISTS (
							SELECT 1 FROM video_pipeline.qc_reports
							WHERE generation_run_id = $1 AND state = 'PASSED'
						)`,
						revisionID,
					).Scan(&qcPassed); err != nil {
						return fmt.Errorf("read run QC gate: %w", err)
					}
					if !qcPassed {
						return controlplane.NewPolicyError(
							controlplane.CodeGateRequired, "generation run has no passing QC report", "complete automatic QC before approval",
						)
					}
				}
			case "MANIFEST":
				if gate == "G3" && *policyState != "UNLOCKED" {
					return controlplane.NewPolicyError(
						controlplane.CodeGateRequired, "G3 can only lock an unlocked immutable manifest", "bind the final unlocked manifest candidate",
					)
				}
			case "ARTIFACT":
				if gate == "SAFETY" && *policyState != "ACTIVE" {
					return controlplane.NewPolicyError(
						controlplane.CodeContentBlocked,
						"SAFETY evidence artifact is not active",
						"bind an immutable active evidence artifact",
					)
				}
			}
		}
		switch strings.ToUpper(binding.ObjectType) {
		case "SHOT_SPEC_REVISION":
			shotIDs = append(shotIDs, revisionID)
		case "GENERATION_RUN":
			runIDs = append(runIDs, revisionID)
		case "EPISODE_REVISION":
			episodeRevisionIDs = append(episodeRevisionIDs, revisionID)
		case "MANIFEST":
			manifestIDs = append(manifestIDs, revisionID)
		}
	}
	if decision == "APPROVED" && gate == "Q1" {
		if len(runIDs) != 1 || len(shotIDs) != 1 {
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"Q1 requires exactly one generation run and shot revision",
				"bind the exact run and shot revision pair",
			)
		}
		var linked bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM video_pipeline.generation_runs
				WHERE id = $1 AND shot_spec_revision_id = $2
			)`,
			runIDs[0], shotIDs[0],
		).Scan(&linked); err != nil {
			return fmt.Errorf("validate Q1 run/shot binding: %w", err)
		}
		if !linked {
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"Q1 generation run does not belong to the bound shot revision",
				"bind the exact run and shot revision pair",
			)
		}
	}
	if decision == "APPROVED" && gate == "G3" {
		if len(manifestIDs) == 0 || len(episodeRevisionIDs) == 0 {
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"G3 requires episode revision and manifest bindings",
				"bind the exact final episode revision and manifest",
			)
		}
		var linkedCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM video_pipeline.generation_manifests gm
			JOIN video_pipeline.artifacts a ON a.id = gm.artifact_id
			WHERE gm.id = ANY($1::uuid[])
			  AND gm.scope_type = 'EPISODE'
			  AND gm.scope_revision_id = ANY($2::uuid[])
			  AND a.status = 'ACTIVE'`,
			manifestIDs, episodeRevisionIDs,
		).Scan(&linkedCount); err != nil {
			return fmt.Errorf("validate G3 manifest/episode binding: %w", err)
		}
		if linkedCount != len(manifestIDs) {
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"G3 manifest is not scoped to the bound episode revision",
				"bind the final episode manifest and exact episode revision",
			)
		}
	}
	return nil
}

func validateApprovalScope(
	ctx context.Context,
	tx pgx.Tx,
	expectedSeriesID uuid.UUID,
	expectedEpisodeID uuid.UUID,
	bindings []controlplane.ApprovalBinding,
) error {
	for _, binding := range bindings {
		query, scoped := approvalBindingScopeQuery(strings.ToUpper(binding.ObjectType))
		if !scoped {
			continue
		}
		revisionID, _ := uuid.Parse(binding.RevisionID)
		var seriesID uuid.UUID
		var episodeID *uuid.UUID
		if err := tx.QueryRow(ctx, query, revisionID).Scan(&seriesID, &episodeID); err != nil {
			return fmt.Errorf("read approval binding scope: %w", err)
		}
		if seriesID != expectedSeriesID || (expectedEpisodeID != uuid.Nil && episodeID != nil && *episodeID != expectedEpisodeID) {
			return controlplane.NewPolicyError(
				controlplane.CodeForbidden,
				"approval binding is outside the declared series or episode scope",
				"bind only immutable objects owned by the approval scope",
			)
		}
	}
	return nil
}

func approvalBindingScopeQuery(objectType string) (string, bool) {
	switch objectType {
	case "SOURCE_REVISION":
		return `SELECT series_id, NULL::uuid FROM video_pipeline.source_revisions WHERE id = $1`, true
	case "EPISODE_REVISION":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.episode_revisions er
		        JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
		        WHERE er.id = $1`, true
	case "SCENE_REVISION":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.scene_revisions sr
		        JOIN video_pipeline.scenes sc ON sc.id = sr.scene_id
		        JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		        WHERE sr.id = $1`, true
	case "ENTITY_REVISION":
		return `SELECT series_id, NULL::uuid FROM video_pipeline.entity_revisions WHERE id = $1`, true
	case "CONTEXT_REVISION":
		return `SELECT cr.series_id,
		               CASE cr.scope_type
		                 WHEN 'EPISODE' THEN cr.scope_id
		                 WHEN 'SCENE' THEN (
		                   SELECT sc.episode_id FROM video_pipeline.scenes sc WHERE sc.id = cr.scope_id
		                 )
		                 WHEN 'SHOT' THEN (
		                   SELECT sc.episode_id
		                   FROM video_pipeline.shots sh
		                   JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		                   WHERE sh.id = cr.scope_id
		                 )
		                 ELSE NULL::uuid
		               END
		        FROM video_pipeline.context_revisions cr
		        WHERE cr.id = $1`, true
	case "ASSET_VERSION":
		return `SELECT a.series_id, NULL::uuid
		        FROM video_pipeline.asset_versions av
		        JOIN video_pipeline.assets a ON a.id = av.asset_id
		        WHERE av.id = $1`, true
	case "SHOT_SPEC_REVISION":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.shot_spec_revisions ssr
		        JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		        JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		        JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		        WHERE ssr.id = $1`, true
	case "PROMPT_SNAPSHOT":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.prompt_snapshots ps
		        JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = ps.shot_spec_revision_id
		        JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		        JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		        JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		        WHERE ps.id = $1`, true
	case "GENERATION_RUN":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.generation_runs gr
		        JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
		        JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		        JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		        JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		        WHERE gr.id = $1`, true
	case "QC_REPORT":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.qc_reports qr
		        JOIN video_pipeline.generation_runs gr ON gr.id = qr.generation_run_id
		        JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
		        JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		        JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		        JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		        WHERE qr.id = $1`, true
	case "MANIFEST":
		return `SELECT ep.series_id, ep.id
		        FROM video_pipeline.generation_manifests gm
		        JOIN video_pipeline.episode_revisions er
		          ON gm.scope_type = 'EPISODE' AND er.id = gm.scope_revision_id
		        JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
		        WHERE gm.id = $1`, true
	default:
		return "", false
	}
}

func applyApprovedGate(
	ctx context.Context,
	tx pgx.Tx,
	decisionID uuid.UUID,
	command controlplane.CreateApprovalDecisionCommand,
	traceID string,
	now time.Time,
) error {
	if command.Decision != "APPROVED" {
		reviewState := "REJECTED"
		if command.Decision == "CANCELLED" {
			reviewState = "CANCELLED"
		}
		for _, binding := range command.Bindings {
			if command.Gate != "Q1" || binding.ObjectType != "GENERATION_RUN" {
				continue
			}
			runID, _ := uuid.Parse(binding.RevisionID)
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.review_tasks
				SET state = $2, reason_codes = ARRAY[$3], decided_at = $4
				WHERE generation_run_id = $1 AND review_type = 'Q1' AND state = 'OPEN'`,
				runID, reviewState, command.ReasonCode, now,
			); err != nil {
				return fmt.Errorf("close Q1 review: %w", err)
			}
		}
		if command.Gate == "G3" && command.EpisodeID != "" {
			episodeID, _ := uuid.Parse(command.EpisodeID)
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.review_tasks
				SET state = $2, reason_codes = ARRAY[$3], decided_at = $4
				WHERE episode_id = $1 AND review_type = 'G3' AND state = 'OPEN'`,
				episodeID, reviewState, command.ReasonCode, now,
			); err != nil {
				return fmt.Errorf("close G3 review: %w", err)
			}
		}
		return nil
	}
	for _, binding := range command.Bindings {
		revisionID, _ := uuid.Parse(binding.RevisionID)
		switch binding.ObjectType {
		case "EPISODE_REVISION":
			target := map[string]string{"G1": "G1_APPROVED", "G2": "G2_APPROVED", "G3": "G3_LOCKED"}[command.Gate]
			if target == "" {
				continue
			}
			if _, err := tx.Exec(ctx,
				`UPDATE video_pipeline.episode_revisions SET status = $2 WHERE id = $1`,
				revisionID, target,
			); err != nil {
				return fmt.Errorf("advance episode gate state: %w", err)
			}
		case "SHOT_SPEC_REVISION":
			if command.Gate == "Q1" {
				if _, err := tx.Exec(ctx,
					`UPDATE video_pipeline.shot_spec_revisions SET lifecycle_state = 'APPROVED' WHERE id = $1`,
					revisionID,
				); err != nil {
					return fmt.Errorf("approve shot revision: %w", err)
				}
			}
		case "GENERATION_RUN":
			if command.Gate == "Q1" {
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.review_tasks
					SET state = 'APPROVED', reason_codes = ARRAY[$2], decided_at = $3
					WHERE generation_run_id = $1 AND review_type = 'Q1' AND state = 'OPEN'`,
					revisionID, command.ReasonCode, now,
				); err != nil {
					return fmt.Errorf("approve Q1 review: %w", err)
				}
			}
		case "MANIFEST":
			if command.Gate != "G3" {
				continue
			}
			var manifestHash string
			if err := tx.QueryRow(ctx, `
				UPDATE video_pipeline.generation_manifests
				SET gate_decision_id = $2, locked_at = $3
				WHERE id = $1 AND locked_at IS NULL
				RETURNING manifest_hash`,
				revisionID, decisionID, now,
			).Scan(&manifestHash); err != nil {
				return fmt.Errorf("lock G3 manifest: %w", err)
			}
			if err := insertAuditAndOutbox(
				ctx,
				tx,
				uuid.NewSHA1(decisionID, []byte("manifest-audit:"+revisionID.String())),
				uuid.NewSHA1(decisionID, []byte("manifest-outbox:"+revisionID.String())),
				command.Actor,
				"manifest.locked",
				"MANIFEST",
				revisionID,
				nil,
				nil,
				command.ReasonCode,
				traceID,
				map[string]any{
					"decisionId":   decisionID.String(),
					"manifestHash": manifestHash,
				},
				now,
			); err != nil {
				return err
			}
		}
	}
	if command.Gate == "G3" && command.EpisodeID != "" {
		episodeID, _ := uuid.Parse(command.EpisodeID)
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.review_tasks
			SET state = 'APPROVED', reason_codes = ARRAY[$2], decided_at = $3
			WHERE episode_id = $1 AND review_type = 'G3' AND state = 'OPEN'`,
			episodeID, command.ReasonCode, now,
		); err != nil {
			return fmt.Errorf("approve G3 review: %w", err)
		}
	}
	return nil
}

func approvalBindingQuery(objectType string) (string, error) {
	switch objectType {
	case "SOURCE_REVISION":
		return `SELECT content_hash, status FROM video_pipeline.source_revisions WHERE id = $1 FOR SHARE`, nil
	case "EPISODE_REVISION":
		return `SELECT content_hash, status FROM video_pipeline.episode_revisions WHERE id = $1 FOR SHARE`, nil
	case "SCENE_REVISION":
		return `SELECT content_hash, status FROM video_pipeline.scene_revisions WHERE id = $1 FOR SHARE`, nil
	case "ENTITY_REVISION":
		return `SELECT content_hash, status FROM video_pipeline.entity_revisions WHERE id = $1 FOR SHARE`, nil
	case "CONTEXT_REVISION":
		return `SELECT content_hash, status FROM video_pipeline.context_revisions WHERE id = $1 FOR SHARE`, nil
	case "ASSET_VERSION":
		return `SELECT content_hash, status FROM video_pipeline.asset_versions WHERE id = $1 FOR SHARE`, nil
	case "SHOT_SPEC_REVISION":
		return `SELECT content_hash, freshness FROM video_pipeline.shot_spec_revisions WHERE id = $1 FOR SHARE`, nil
	case "PROMPT_SNAPSHOT":
		return `SELECT content_hash, NULL::text FROM video_pipeline.prompt_snapshots WHERE id = $1 FOR SHARE`, nil
	case "GENERATION_RUN":
		return `SELECT run_spec_digest, state FROM video_pipeline.generation_runs WHERE id = $1 FOR SHARE`, nil
	case "QC_REPORT":
		return `SELECT report_hash, state FROM video_pipeline.qc_reports WHERE id = $1 FOR SHARE`, nil
	case "MANIFEST":
		return `SELECT manifest_hash, CASE WHEN locked_at IS NULL THEN 'UNLOCKED' ELSE 'LOCKED' END
		        FROM video_pipeline.generation_manifests gm
		        JOIN video_pipeline.artifacts a ON a.id = gm.artifact_id
		        WHERE gm.id = $1 AND a.status = 'ACTIVE'
		        FOR SHARE OF gm, a`, nil
	case "ARTIFACT":
		return `SELECT content_hash, status FROM video_pipeline.artifacts WHERE id = $1 FOR SHARE`, nil
	default:
		return "", controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"approval binding objectType is not allowlisted",
			"use an immutable revision type supported by the approval contract",
		)
	}
}

func readPlan(ctx context.Context, tx pgx.Tx, planIDRaw string) (controlplane.GenerationPlanRecord, error) {
	planID, err := uuid.Parse(planIDRaw)
	if err != nil {
		return controlplane.GenerationPlanRecord{}, controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded, "generation plan identifier is invalid", "create and confirm a current generation plan",
		)
	}
	var aggregateID uuid.UUID
	var responseBody []byte
	var auditPayload []byte
	err = tx.QueryRow(ctx, `
		SELECT op.aggregate_id, idem.response_body, audit.payload
		FROM video_pipeline.operation_requests op
		JOIN video_pipeline.idempotency_records idem ON idem.operation_id = op.id
		JOIN LATERAL (
			SELECT payload
			FROM video_pipeline.audit_events
			WHERE aggregate_type = 'GENERATION_PLAN' AND aggregate_id = op.id
			ORDER BY occurred_at DESC
			LIMIT 1
		) audit ON true
		WHERE op.id = $1
		  AND op.operation_type = 'CREATE_GENERATION_PLAN'
		  AND op.state = 'SUCCEEDED'
		ORDER BY idem.created_at DESC
		LIMIT 1
		FOR SHARE OF op, idem`,
		planID,
	).Scan(&aggregateID, &responseBody, &auditPayload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.GenerationPlanRecord{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded, "generation plan was not found", "create and confirm a current generation plan",
			)
		}
		return controlplane.GenerationPlanRecord{}, fmt.Errorf("read generation plan: %w", err)
	}
	var plan controlplane.GenerationPlan
	if err := json.Unmarshal(responseBody, &plan); err != nil {
		return controlplane.GenerationPlanRecord{}, fmt.Errorf("decode generation plan: %w", err)
	}
	return decodePlanRecord(plan, aggregateID, auditPayload)
}

func decodePlanRecord(
	plan controlplane.GenerationPlan,
	seriesID uuid.UUID,
	auditPayload []byte,
) (controlplane.GenerationPlanRecord, error) {
	var audit struct {
		EpisodeRevisionID   string                       `json:"episodeRevisionId"`
		ShotSpecRevisionIDs []string                     `json:"shotSpecRevisionIds"`
		CandidatesPerShot   int                          `json:"candidatesPerShot"`
		PricingRuleVersion  string                       `json:"pricingRuleVersion"`
		BudgetLimit         controlplane.BudgetLimit     `json:"budgetLimit"`
		SpeechBudgetLimit   *controlplane.BudgetLimit    `json:"speechBudgetLimit"`
		ExecutionPolicy     controlplane.ExecutionPolicy `json:"executionPolicy"`
	}
	if err := json.Unmarshal(auditPayload, &audit); err != nil {
		return controlplane.GenerationPlanRecord{}, fmt.Errorf("decode generation plan evidence: %w", err)
	}
	if len(audit.ShotSpecRevisionIDs) == 0 || audit.CandidatesPerShot < 1 {
		return controlplane.GenerationPlanRecord{}, errors.New("generation plan evidence is incomplete")
	}
	return controlplane.GenerationPlanRecord{
		Plan:                plan,
		SeriesID:            seriesID.String(),
		EpisodeRevisionID:   audit.EpisodeRevisionID,
		ShotSpecRevisionIDs: append([]string(nil), audit.ShotSpecRevisionIDs...),
		CandidatesPerShot:   audit.CandidatesPerShot,
		PricingRuleVersion:  audit.PricingRuleVersion,
		BudgetLimit:         audit.BudgetLimit,
		SpeechBudgetLimit:   cloneBudgetLimit(audit.SpeechBudgetLimit),
		ExecutionPolicy:     audit.ExecutionPolicy,
	}, nil
}

func cloneBudgetLimit(limit *controlplane.BudgetLimit) *controlplane.BudgetLimit {
	if limit == nil {
		return nil
	}
	cloned := *limit
	return &cloned
}

func sameRoute(left, right controlplane.ModelRouteSnapshot) bool {
	return left.CapabilityAlias == right.CapabilityAlias &&
		left.ProviderProfileID == right.ProviderProfileID &&
		left.Provider == right.Provider &&
		left.ModelID == right.ModelID &&
		left.EndpointID == right.EndpointID &&
		left.RouteVersion == right.RouteVersion &&
		left.CapabilityHash == right.CapabilityHash
}

func sameBudgetLimit(left *controlplane.BudgetLimit, right controlplane.BudgetLimit) bool {
	return left != nil &&
		left.AmountMicros == right.AmountMicros &&
		left.Currency == right.Currency
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func parseUUIDs(values []string) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		parsed, err := uuid.Parse(value)
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

func digestValue(value any) (string, error) {
	payload, err := controlplane.CanonicalJSON(value)
	if err != nil {
		return "", fmt.Errorf("encode canonical digest input: %w", err)
	}
	return fmt.Sprintf("%x", sha256Bytes(payload)), nil
}

func sha256Bytes(payload []byte) [32]byte {
	// Kept behind a helper to keep every content/idempotency hash path obvious in
	// code review and prevent accidental provider-specific hashing.
	return sha256.Sum256(payload)
}

func numericLimit(limits map[string]any, key string) (float64, bool) {
	value, ok := limits[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func saturatingMicros(units, price float64) int64 {
	if units <= 0 || price <= 0 {
		return 0
	}
	value := units * price
	if math.IsInf(value, 1) || value >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(math.Ceil(value))
}

func translateWriteError(operation string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "23505" {
		code := controlplane.CodeConflict
		if postgresError.ConstraintName == "ux_generation_runs_active_digest" {
			code = controlplane.CodeRunActive
		}
		return controlplane.NewConflictError(code, operation+" conflicts with an immutable or active record")
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func intPointer(value int) *int { return &value }

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func objectSlice(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return []map[string]any{}
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func stringSlice(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return []string{}
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func objectMap(value any) map[string]any {
	object, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return object
}
