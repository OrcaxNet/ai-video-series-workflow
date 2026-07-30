//go:build integration

package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgres_CreateSeriesIdempotencyAndAtomicEvidence(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewForPool(pool)

	profileID := uuid.New()
	profileGroupID := uuid.New()
	profileHash := strings.Repeat("b", 64)
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.generation_profiles
			(id, profile_id, revision, schema_version, status, stage, aspect_profile, episode_target_ms,
			 shot_min_ms, shot_max_ms, capability_routes, media_processing, render_defaults, qc_thresholds,
			 retry_policy, budget_policy, license_policy, content_hash, created_by)
		VALUES ($1, $2, 1, 'v1', 'ACTIVE', 'M0', '16:9_720P24', 60000, 4000, 6000,
		        '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
		        '{}'::jsonb, $3, 'integration-test')`,
		profileID, profileGroupID, profileHash,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		tracePattern := "integration-series-" + profileID.String() + "%"
		scopePattern := "integration-series:" + profileID.String() + "%"
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.outbox_events WHERE trace_id LIKE $1`, tracePattern)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.audit_events WHERE trace_id LIKE $1`, tracePattern)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.idempotency_records WHERE scope LIKE $1`, scopePattern)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.operation_requests WHERE trace_id LIKE $1`, tracePattern)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.series WHERE default_profile_id = $1`, profileID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.generation_profiles WHERE id = $1`, profileID)
	})

	command := controlplane.CreateSeriesCommand{
		SchemaVersion:               "v1",
		Title:                       "Concurrent immutable series",
		GenerationProfileRevisionID: profileID.String(),
		RightsDeclaration: controlplane.RightsDeclaration{
			Basis:                "licensed fixture",
			EvidenceArtifactHash: strings.Repeat("a", 64),
		},
		Actor: controlplane.Actor{ActorID: "integration-creator", Role: "CREATOR"},
	}
	key := uuid.NewString()
	requestHash, err := digestValue(command)
	if err != nil {
		t.Fatal(err)
	}
	idempotency := controlplane.Idempotency{
		Scope: "integration-series:" + profileID.String(), Key: key, RequestHash: requestHash,
	}

	const callers = 8
	results := make(chan controlplane.Stored[controlplane.Operation], callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := store.CreateSeries(ctx, command, idempotency, "integration-series-"+profileID.String())
			results <- result
			errs <- err
		}()
	}
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("CreateSeries() error = %v", err)
		}
	}
	var operationID, seriesID string
	replayed := 0
	for result := range results {
		if operationID == "" {
			operationID = result.Value.OperationID
			seriesID = result.Value.AggregateID
		}
		if result.Value.OperationID != operationID || result.Value.AggregateID != seriesID {
			t.Fatalf("idempotent result diverged: %#v", result)
		}
		if result.Replayed {
			replayed++
		}
	}
	if replayed != callers-1 {
		t.Fatalf("replayed = %d, want %d", replayed, callers-1)
	}
	var seriesCount, auditCount, outboxCount int
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM video_pipeline.series WHERE id = $1),
			(SELECT COUNT(*) FROM video_pipeline.audit_events WHERE aggregate_id = $1),
			(SELECT COUNT(*) FROM video_pipeline.outbox_events WHERE aggregate_id = $1)`,
		seriesID,
	).Scan(&seriesCount, &auditCount, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if seriesCount != 1 || auditCount != 1 || outboxCount != 1 {
		t.Fatalf("atomic evidence counts = series:%d audit:%d outbox:%d", seriesCount, auditCount, outboxCount)
	}

	conflict := command
	conflict.Title = "Different request"
	conflictHash, err := digestValue(conflict)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CreateSeries(ctx, conflict, controlplane.Idempotency{
		Scope: idempotency.Scope, Key: key, RequestHash: conflictHash,
	}, "integration-series-conflict")
	var domain *controlplane.DomainError
	if !errors.As(err, &domain) || domain.Code != controlplane.CodeConflict {
		t.Fatalf("conflicting idempotency error = %v", err)
	}
}

func TestPostgres_WorkflowStepJournalReplaysAtomicResult(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewForPool(pool)
	suffix := uuid.NewString()
	step := orchestration.WorkflowStep{
		WorkflowID:   "integration-workflow-" + suffix,
		ActivityID:   "activity-1",
		ActivityType: orchestration.ActivityCompilePrompt,
		TraceID:      "integration-workflow-" + suffix,
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.outbox_events WHERE trace_id = $1`, step.TraceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.audit_events WHERE trace_id = $1`, step.TraceID)
		_, _ = pool.Exec(cleanupCtx,
			`DELETE FROM video_pipeline.idempotency_records WHERE scope = $1`,
			"temporal-workflow:"+step.WorkflowID,
		)
	})

	inputHash := strings.Repeat("1", 64)
	output := []byte(`{"digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","id":"prompt-1"}`)
	replay, completed, err := store.BeginWorkflowStep(ctx, step, inputHash)
	if err != nil || completed || len(replay) != 0 {
		t.Fatalf("first BeginWorkflowStep() = %s, %t, %v", replay, completed, err)
	}
	if err := store.CompleteWorkflowStep(ctx, step, inputHash, output); err != nil {
		t.Fatalf("CompleteWorkflowStep() error = %v", err)
	}
	replay, completed, err = store.BeginWorkflowStep(ctx, step, inputHash)
	if err != nil || !completed {
		t.Fatalf("replay BeginWorkflowStep() = %s, %t, %v", replay, completed, err)
	}
	if string(replay) == "" {
		t.Fatal("replayed result is empty")
	}
	var auditCount, outboxCount int
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM video_pipeline.audit_events WHERE trace_id = $1),
			(SELECT COUNT(*) FROM video_pipeline.outbox_events WHERE trace_id = $1)`,
		step.TraceID,
	).Scan(&auditCount, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 || outboxCount != 1 {
		t.Fatalf("workflow evidence counts = audit:%d outbox:%d", auditCount, outboxCount)
	}
	_, _, err = store.BeginWorkflowStep(ctx, step, strings.Repeat("2", 64))
	var domain *controlplane.DomainError
	if !errors.As(err, &domain) || domain.Code != controlplane.CodeConflict {
		t.Fatalf("changed workflow input error = %v", err)
	}
}

func TestPostgres_CreateSeriesRollsBackIdempotencyOnPolicyFailure(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewForPool(pool)
	command := controlplane.CreateSeriesCommand{
		SchemaVersion: "v1", Title: "must roll back",
		GenerationProfileRevisionID: uuid.NewString(),
		RightsDeclaration: controlplane.RightsDeclaration{
			Basis: "fixture", EvidenceArtifactHash: strings.Repeat("a", 64),
		},
		Actor: controlplane.Actor{ActorID: "integration-creator", Role: "CREATOR"},
	}
	hash, err := digestValue(command)
	if err != nil {
		t.Fatal(err)
	}
	idempotency := controlplane.Idempotency{
		Scope: "integration-series:rollback:" + uuid.NewString(), Key: uuid.NewString(), RequestHash: hash,
	}
	_, err = store.CreateSeries(ctx, command, idempotency, "integration-series-rollback")
	if err == nil {
		t.Fatal("CreateSeries() error = nil")
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM video_pipeline.idempotency_records WHERE scope = $1 AND idempotency_key = $2`,
		idempotency.Scope, idempotency.Key,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rolled-back idempotency rows = %d, want 0", count)
	}
}

func TestPostgres_MigrationV2EnforcesImmutableAndRetentionGuards(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	auditID := uuid.New()
	aggregateID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.audit_events
			(id, actor_id, actor_role, action, aggregate_type, aggregate_id, trace_id, payload)
		VALUES ($1, 'integration', 'OPERATOR', 'guard.tested', 'TEST', $2, 'integration-guard', '{"stable":true}')`,
		auditID, aggregateID,
	); err != nil {
		t.Fatal(err)
	}
	expectGuardFailure(t, ctx, tx,
		`UPDATE video_pipeline.audit_events SET payload = '{"stable":false}' WHERE id = $1`,
		auditID,
	)

	artifactID := uuid.New()
	contentHash := strings.Repeat("c", 64)
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, status)
		VALUES ($1, $2, $3, 'video/mp4', 1, 'ACTIVE')`,
		artifactID, contentHash, "cas://sha256/"+contentHash,
	); err != nil {
		t.Fatal(err)
	}
	expectGuardFailure(t, ctx, tx, `DELETE FROM video_pipeline.artifacts WHERE id = $1`, artifactID)

	if _, err := tx.Exec(ctx, `SAVEPOINT manifest_secret_guard`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO video_pipeline.generation_manifests
			(id, scope_type, scope_revision_id, schema_version, payload, manifest_hash, artifact_id)
		VALUES ($1, 'EPISODE', $2, 'v1', '{"api_key":"forbidden"}', $3, $4)`,
		uuid.New(), uuid.New(), strings.Repeat("d", 64), artifactID,
	)
	if err == nil {
		t.Fatal("secret-bearing manifest insert error = nil")
	}
	if _, rollbackErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT manifest_secret_guard`); rollbackErr != nil {
		t.Fatalf("rollback manifest savepoint: %v", rollbackErr)
	}

	seriesID := uuid.New()
	decisionID := uuid.New()
	manifestID := uuid.New()
	manifestHash := strings.Repeat("e", 64)
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.series
			(id, title, status, rights_declaration, created_by)
		VALUES ($1, 'manifest guard fixture', 'DRAFT', '{}'::jsonb, 'integration')`,
		seriesID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.approval_decisions
			(id, series_id, gate, decision, reason_code, actor_id, actor_role, trace_id)
		VALUES ($1, $2, 'G3', 'APPROVED', 'integration', 'integration', 'DIRECTOR', 'integration-guard')`,
		decisionID, seriesID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.generation_manifests
			(id, scope_type, scope_revision_id, schema_version, payload, manifest_hash, artifact_id)
		VALUES ($1, 'EPISODE', $2, 'v1', '{"stable":true}', $3, $4)`,
		manifestID, uuid.New(), manifestHash, artifactID,
	); err != nil {
		t.Fatal(err)
	}
	expectGuardFailure(t, ctx, tx,
		`UPDATE video_pipeline.generation_manifests SET gate_decision_id = $2 WHERE id = $1`,
		manifestID, decisionID,
	)
	if _, err := tx.Exec(ctx, `
		UPDATE video_pipeline.generation_manifests
		SET gate_decision_id = $2, locked_at = now()
		WHERE id = $1`,
		manifestID, decisionID,
	); err != nil {
		t.Fatalf("lock manifest atomically: %v", err)
	}
	expectGuardFailure(t, ctx, tx,
		`UPDATE video_pipeline.generation_manifests SET locked_at = locked_at + interval '1 second' WHERE id = $1`,
		manifestID,
	)

	orphanArtifactID := uuid.New()
	orphanHash := strings.Repeat("f", 64)
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, status)
		VALUES ($1, $2, $3, 'video/mp4', 1, 'ACTIVE')`,
		orphanArtifactID, orphanHash, "cas://sha256/"+orphanHash,
	); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if _, err := tx.Exec(ctx, `
		UPDATE video_pipeline.artifacts
		SET status = 'ORPHAN_CANDIDATE', orphaned_at = $2, retention_until = $2
		WHERE id = $1`,
		orphanArtifactID, past,
	); err != nil {
		t.Fatal(err)
	}
	if tag, err := tx.Exec(ctx, `DELETE FROM video_pipeline.artifacts WHERE id = $1`, orphanArtifactID); err != nil {
		t.Fatalf("delete expired orphan: %v", err)
	} else if tag.RowsAffected() != 1 {
		t.Fatalf("deleted expired orphan rows = %d", tag.RowsAffected())
	}
}

func expectGuardFailure(t *testing.T, ctx context.Context, tx pgx.Tx, query string, arguments ...any) {
	t.Helper()
	if _, err := tx.Exec(ctx, `SAVEPOINT immutable_guard`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, query, arguments...); err == nil {
		t.Fatalf("guarded statement %q error = nil", query)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT immutable_guard`); err != nil {
		t.Fatalf("rollback guard savepoint: %v", err)
	}
}
