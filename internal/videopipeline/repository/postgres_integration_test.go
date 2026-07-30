//go:build integration

package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/temporalcontrol"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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

func TestPostgres_WorkflowProjectionClosesQ1AndManifestLineage(t *testing.T) {
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

	profileID, profileGroupID := uuid.New(), uuid.New()
	seriesID, episodeID, episodeRevisionID := uuid.New(), uuid.New(), uuid.New()
	sceneID, shotID := uuid.New(), uuid.New()
	scriptID, storyboardID, shotRevisionID := uuid.New(), uuid.New(), uuid.New()
	gate2ID, budgetID := uuid.New(), uuid.New()
	providerProfileID, capabilityID := uuid.New(), uuid.New()
	safetyEvidenceArtifactID := uuid.New()
	contextIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	episodeHash := strings.Repeat("1", 64)
	shotHash := strings.Repeat("2", 64)
	safetyEvidenceHash := strings.Repeat(
		strings.ReplaceAll(safetyEvidenceArtifactID.String(), "-", ""),
		2,
	)
	effectiveHash := strings.Repeat("3", 64)
	profileHash := strings.Repeat("4", 64)
	capabilityHash := strings.Repeat("5", 64)

	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.generation_profiles
			(id, profile_id, revision, schema_version, status, stage, aspect_profile,
			 episode_target_ms, shot_min_ms, shot_max_ms, capability_routes,
			 media_processing, render_defaults, qc_thresholds, retry_policy,
			 budget_policy, license_policy, content_hash, created_by)
		VALUES ($1, $2, 1, 'v1', 'ACTIVE', 'M0', '16:9_720P24',
		        60000, 4000, 6000, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
		        '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $3, 'integration')`,
		profileID, profileGroupID, profileHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.series
			(id, title, status, default_profile_id, rights_declaration, created_by)
		VALUES ($1, 'workflow projection fixture', 'ACTIVE', $2, '{}'::jsonb, 'integration')`,
		seriesID, profileID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.episodes (id, series_id, ordinal, title)
		VALUES ($1, $2, 1, 'episode')`,
		episodeID, seriesID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.episode_revisions
			(id, episode_id, revision, status, target_duration_ms, content_hash, created_by)
		VALUES ($1, $2, 1, 'G2_APPROVED', 5000, $3, 'integration')`,
		episodeRevisionID, episodeID, episodeHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.scenes (id, episode_id, ordinal)
		VALUES ($1, $2, 1)`,
		sceneID, episodeID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.shots (id, scene_id, ordinal)
		VALUES ($1, $2, 1)`,
		shotID, sceneID,
	)
	contextScopes := []struct {
		scope string
		id    uuid.UUID
	}{
		{"SERIES", seriesID},
		{"EPISODE", episodeID},
		{"SCENE", sceneID},
		{"SHOT", shotID},
	}
	for index, scoped := range contextScopes {
		mustExec(t, ctx, pool, `
			INSERT INTO video_pipeline.context_revisions
				(id, series_id, scope_type, scope_id, revision, status, schema_version,
				 resolver_version, payload, content_hash, created_by)
			VALUES ($1, $2, $3, $4, 1, 'APPROVED', 'v1',
			        'integration-resolver', $5, $6, 'integration')`,
			contextIDs[index], seriesID, scoped.scope, scoped.id,
			map[string]any{"scope": scoped.scope}, strings.Repeat(string(rune('6'+index)), 64),
		)
	}
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.approval_decisions
			(id, series_id, episode_id, gate, decision, reason_code,
			 actor_id, actor_role, trace_id)
		VALUES ($1, $2, $3, 'G2', 'APPROVED', 'integration',
		        'director', 'DIRECTOR', 'workflow-projection')`,
		gate2ID, seriesID, episodeID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.episode_script_revisions
			(id, episode_id, revision, status, schema_version, payload, content_hash, created_by)
		VALUES ($1, $2, 1, 'APPROVED', 'v1', '{}'::jsonb, $3, 'integration')`,
		scriptID, episodeID, strings.Repeat("a", 64),
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.storyboard_revisions
			(id, episode_id, script_revision_id, revision, status, content_hash, created_by)
		VALUES ($1, $2, $3, 1, 'APPROVED', $4, 'integration')`,
		storyboardID, episodeID, scriptID, strings.Repeat("b", 64),
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.shot_spec_revisions
			(id, shot_id, storyboard_revision_id, revision, lifecycle_state, freshness,
			 duration_ms, aspect_profile, fps, width, height, cast_count,
			 primary_action_count, narrative, asset_version_refs, context_revision_ids,
			 effective_context_hash, continuity, cinematography, generation_profile_id,
			 gate2_decision_id, content_hash, created_by)
		VALUES ($1, $2, $3, 1, 'READY', 'FRESH',
		        5000, '16:9_720P24', 24, 1280, 720, 1,
		        1, '{"action":"walk"}', '{}', $4,
		        $5, '{}'::jsonb, '{}'::jsonb, $6, $7, $8, 'integration')`,
		shotRevisionID, shotID, storyboardID, contextIDs,
		effectiveHash, profileID, gate2ID, shotHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.approval_bindings
			(decision_id, object_type, revision_id, content_hash)
		VALUES
			($1, 'EPISODE_REVISION', $2, $3),
			($1, 'SHOT_SPEC_REVISION', $4, $5)`,
		gate2ID, episodeRevisionID, episodeHash, shotRevisionID, shotHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.review_tasks
			(id, series_id, episode_id, review_type, state, assigned_role, decided_at)
		VALUES ($1, $2, $3, 'BUDGET', 'APPROVED', 'PRODUCER', now())`,
		budgetID, seriesID, episodeID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.provider_profiles
			(id, provider, display_name, base_url_ref, enabled, mode, health, config_hash)
		VALUES ($1, 'MOCK', $2, 'runtime://mock', true, 'MOCK', 'READY', $3)`,
		providerProfileID, "integration mock "+providerProfileID.String(), strings.Repeat("c", 64),
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.provider_capability_snapshots
			(id, provider_profile_id, capability_alias, model_id, route_version,
			 supported_inputs, limits, pricing_rule_version, capability_hash,
			 status, effective_at)
		VALUES ($1, $2, 'video.primary', 'fixture-video-v1', 'route-v1',
		        ARRAY['text'], '{"unitPriceMicros":10,"remainingCalls":100,"allowedTerritories":["CN"],"productForms":["INTERNAL_PREVIEW","COMMERCIAL_RELEASE"],"contentSafetyPolicyVersions":["safety-v1"]}', 'pricing-v1', $3,
		        'ACTIVE', now())`,
		capabilityID, providerProfileID, capabilityHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, status)
		VALUES ($1, $2, $3, 'application/json', 1, 'ACTIVE')`,
		safetyEvidenceArtifactID, safetyEvidenceHash, "cas://sha256/"+safetyEvidenceHash,
	)

	createSafetyDecision := func(
		name string,
		policyVersion string,
		validUntil time.Time,
		actor controlplane.Actor,
	) string {
		t.Helper()
		command := controlplane.CreateApprovalDecisionCommand{
			SchemaVersion: "v1", SeriesID: seriesID.String(), EpisodeID: episodeID.String(),
			Gate: "SAFETY", Decision: "APPROVED", ReasonCode: "CONTENT_SAFETY_APPROVED",
			PolicyVersion: policyVersion, EvidenceHash: safetyEvidenceHash, ValidUntil: &validUntil,
			Bindings: []controlplane.ApprovalBinding{
				{ObjectType: "EPISODE_REVISION", RevisionID: episodeRevisionID.String(), ContentHash: episodeHash},
				{ObjectType: "SHOT_SPEC_REVISION", RevisionID: shotRevisionID.String(), ContentHash: shotHash},
				{ObjectType: "ARTIFACT", RevisionID: safetyEvidenceArtifactID.String(), ContentHash: safetyEvidenceHash},
			},
			Actor: actor,
		}
		digest, err := digestValue(command)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := store.CreateApprovalDecision(ctx, command, controlplane.Idempotency{
			Scope: "workflow-projection-safety:" + name,
			Key:   uuid.NewString(), RequestHash: digest,
		}, "workflow-projection-safety-"+name)
		if err != nil {
			t.Fatal(err)
		}
		return stored.Value.DecisionID
	}
	safetyDecisionID := createSafetyDecision(
		"valid", "safety-v1", time.Now().UTC().Add(time.Hour),
		controlplane.Actor{ActorID: "safety-reviewer", Role: "SAFETY_REVIEWER"},
	)
	expiredSafetyDecisionID := createSafetyDecision(
		"expired", "safety-v1", time.Now().UTC().Add(-time.Minute),
		controlplane.Actor{ActorID: "safety-reviewer", Role: "SAFETY_REVIEWER"},
	)
	versionMismatchSafetyDecisionID := createSafetyDecision(
		"version-mismatch", "safety-v2", time.Now().UTC().Add(time.Hour),
		controlplane.Actor{ActorID: "safety-reviewer", Role: "SAFETY_REVIEWER"},
	)
	unauthorizedSafetyDecisionID := createSafetyDecision(
		"unauthorized", "safety-v1", time.Now().UTC().Add(time.Hour),
		controlplane.Actor{ActorID: "producer", Role: "PRODUCER"},
	)

	route := controlplane.ModelRouteSnapshot{
		CapabilityAlias: "video.primary", ProviderProfileID: providerProfileID.String(),
		Provider: "MOCK", ModelID: "fixture-video-v1", RouteVersion: "route-v1",
		CapabilityHash: capabilityHash,
	}
	executionPolicy := controlplane.ExecutionPolicy{
		TargetTerritory: "CN", ProductForm: "INTERNAL_PREVIEW",
		ContentSafetyPolicyVersion: "safety-v1", ContentSafetyDecisionID: safetyDecisionID,
	}
	planCommand := controlplane.CreateGenerationPlanCommand{
		SchemaVersion: "v1", SeriesID: seriesID.String(),
		EpisodeRevisionID:   episodeRevisionID.String(),
		ShotSpecRevisionIDs: []string{shotRevisionID.String()},
		CandidatesPerShot:   1, RouteSnapshot: route,
		BudgetLimit:     controlplane.BudgetLimit{AmountMicros: 1_000, Currency: "CNY"},
		ExecutionPolicy: executionPolicy,
		Actor:           controlplane.Actor{ActorID: "producer", Role: "PRODUCER"},
	}
	planDigest, err := digestValue(planCommand)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := store.CreateGenerationPlan(ctx, planCommand, controlplane.Idempotency{
		Scope: "workflow-projection-plan:" + seriesID.String(),
		Key:   uuid.NewString(), RequestHash: planDigest,
	}, "workflow-projection")
	if err != nil {
		t.Fatal(err)
	}
	blockedPlans := []struct {
		name    string
		command controlplane.CreateGenerationPlanCommand
		code    controlplane.ErrorCode
	}{
		{
			name: "territory",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.ExecutionPolicy.TargetTerritory = "US"
				return value
			}(),
			code: controlplane.CodeRegionUnavailable,
		},
		{
			name: "missing content safety decision",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.ExecutionPolicy.ContentSafetyDecisionID = uuid.NewString()
				return value
			}(),
			code: controlplane.CodeContentBlocked,
		},
		{
			name: "expired content safety decision",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.ExecutionPolicy.ContentSafetyDecisionID = expiredSafetyDecisionID
				return value
			}(),
			code: controlplane.CodeContentBlocked,
		},
		{
			name: "version mismatch content safety decision",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.ExecutionPolicy.ContentSafetyDecisionID = versionMismatchSafetyDecisionID
				return value
			}(),
			code: controlplane.CodeContentBlocked,
		},
		{
			name: "unauthorized content safety decision",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.ExecutionPolicy.ContentSafetyDecisionID = unauthorizedSafetyDecisionID
				return value
			}(),
			code: controlplane.CodeContentBlocked,
		},
		{
			name: "quota",
			command: func() controlplane.CreateGenerationPlanCommand {
				value := planCommand
				value.CandidatesPerShot = 101
				return value
			}(),
			code: controlplane.CodeQuotaExceeded,
		},
	}
	for _, blocked := range blockedPlans {
		blocked := blocked
		t.Run("prequeue policy "+blocked.name, func(t *testing.T) {
			digest, err := digestValue(blocked.command)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.CreateGenerationPlan(ctx, blocked.command, controlplane.Idempotency{
				Scope: "workflow-projection-blocked-plan:" + blocked.name,
				Key:   uuid.NewString(), RequestHash: digest,
			}, "workflow-projection")
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != blocked.code {
				t.Fatalf("blocked plan error = %#v, want %s", err, blocked.code)
			}
			var providerJobs int
			if err := pool.QueryRow(ctx,
				`SELECT COUNT(*) FROM video_pipeline.provider_jobs WHERE provider_profile_id = $1`,
				providerProfileID,
			).Scan(&providerJobs); err != nil {
				t.Fatal(err)
			}
			if providerJobs != 0 {
				t.Fatalf("provider jobs after prequeue block = %d, want 0", providerJobs)
			}
			var planCount int
			if err := pool.QueryRow(ctx,
				`SELECT COUNT(*)
				 FROM video_pipeline.operation_requests
				 WHERE aggregate_type = 'SERIES'
				   AND aggregate_id = $1
				   AND operation_type = 'CREATE_GENERATION_PLAN'`,
				seriesID,
			).Scan(&planCount); err != nil {
				t.Fatal(err)
			}
			if planCount != 1 {
				t.Fatalf("persisted plans after prequeue block = %d, want 1", planCount)
			}
		})
	}

	step := orchestration.WorkflowStep{
		WorkflowID: "workflow-" + uuid.NewString(),
		ActivityID: "compile", ActivityType: orchestration.ActivityCompilePrompt,
		TraceID: "workflow-projection",
	}
	prompt, err := store.CompilePromptSnapshot(ctx, step, orchestration.CompilePromptInput{
		ShotSpecRevisionID:   shotRevisionID.String(),
		GenerationProfileRef: profileID.String(),
		PersistProductTruth:  true,
		TraceID:              step.TraceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	model := providercontract.ModelSnapshot{
		CapabilityAlias: "video.primary", Provider: "MOCK", ModelID: "fixture-video-v1",
		RouteVersion: "route-v1", CapabilityHash: capabilityHash, Verification: "integration",
	}
	step.ActivityID, step.ActivityType = "create-run", orchestration.ActivityCreateRun
	run, err := store.CreateWorkflowRun(ctx, step, orchestration.CreateRunInput{
		ShotSpecRevisionID: shotRevisionID.String(), PromptSnapshot: prompt,
		GenerationProfileRef: profileID.String(), Route: model,
		GenerationPlanID: plan.Value.GenerationPlanID, BudgetApprovalID: budgetID.String(),
		ProviderProfileID: providerProfileID.String(),
		CreativeAttempt:   1, TraceID: step.TraceID, PersistProductTruth: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatch := orchestration.ExecuteProviderJobInput{
		Run: run, Prompt: prompt, Route: model,
		BudgetApprovalID: budgetID.String(), BudgetMaximumMicros: 1_000,
		BudgetCurrency: "CNY", ProviderProfileID: providerProfileID.String(),
		TraceID: step.TraceID, PersistProductTruth: true,
	}
	step.ActivityID, step.ActivityType = "provider", orchestration.ActivityExecuteProviderJob
	if err := store.PrepareProviderJob(ctx, step, dispatch); err != nil {
		t.Fatal(err)
	}
	qaPauseActor := controlplane.Actor{ActorID: "qa-operator", Role: "OPERATOR"}
	qaPauseDigest, _ := digestValue(map[string]any{"runId": run.RunID, "reason": "QA_PAUSE"})
	if _, err := store.RequestRunPause(
		ctx, run.RunID, 1, qaPauseActor, "QA_PAUSE",
		controlplane.Idempotency{
			Scope: "workflow-projection-qa-pause:" + run.RunID,
			Key:   uuid.NewString(), RequestHash: qaPauseDigest,
		},
		step.TraceID,
	); err != nil {
		t.Fatal(err)
	}
	qaPausedRun, err := store.GetGenerationRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if qaPausedRun.State != "PAUSED" || qaPausedRun.FailureCode != "QA_PAUSE" {
		t.Fatalf("QA paused run = %#v", qaPausedRun)
	}
	qaResumeDigest, _ := digestValue(map[string]any{"runId": run.RunID, "mode": "RESUME_PAUSED"})
	if _, err := store.RequestRunResume(
		ctx, run.RunID, 1, qaPauseActor, "RESUME_PAUSED",
		controlplane.Idempotency{
			Scope: "workflow-projection-qa-resume:" + run.RunID,
			Key:   uuid.NewString(), RequestHash: qaResumeDigest,
		},
		step.TraceID,
	); err != nil {
		t.Fatal(err)
	}
	qaResumedRun, err := store.GetGenerationRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if qaResumedRun.State != "RUNNING" ||
		qaResumedRun.FailureClass != "" || qaResumedRun.FailureCode != "" {
		t.Fatalf("QA resumed run retained pause failure = %#v", qaResumedRun)
	}
	cas, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	videoArtifact, err := cas.Put(ctx, bytes.NewReader([]byte("integration video bytes")))
	if err != nil {
		t.Fatal(err)
	}
	actualCost := int64(750)
	providerResult := orchestration.ProviderResult{
		UpstreamTaskID: "task-1", RequestID: "request-1",
		ArtifactDigest: videoArtifact.Digest, ArtifactURI: videoArtifact.URI,
		MediaType: "video/mp4", ArtifactSize: videoArtifact.Size,
		Width: 1280, Height: 720, DurationMillis: 5_000,
		Model: model,
		Usage: providercontract.Usage{InputUnits: 10, OutputUnits: 20, Unit: "mock-units"},
		Cost: providercontract.Cost{
			EstimatedMicros: 800, ActualMicros: &actualCost, Currency: "CNY",
			PricingVersion: "pricing-v1", Verified: true,
		},
	}
	if err := store.CompleteProviderJob(ctx, step, dispatch, providerResult); err != nil {
		t.Fatal(err)
	}
	completedAfterPause, err := store.GetGenerationRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if completedAfterPause.State != "SUCCEEDED" ||
		completedAfterPause.FailureClass != "" || completedAfterPause.FailureCode != "" {
		t.Fatalf("provider success retained pause failure = %#v", completedAfterPause)
	}
	step.ActivityID, step.ActivityType = "qc", orchestration.ActivityRunAutomaticQC
	qcInput := orchestration.RunQCInput{
		Run: run, Provider: providerResult, TraceID: step.TraceID, PersistProductTruth: true,
	}
	if err := store.RecordAutomaticQC(ctx, step, qcInput, orchestration.QCResult{Passed: true}); err != nil {
		t.Fatal(err)
	}
	step.ActivityID, step.ActivityType = "review", orchestration.ActivityCreateShotReview
	if err := store.OpenShotReview(ctx, step, orchestration.CreateReviewInput{
		ShotSpecRevisionID: shotRevisionID.String(), RunID: run.RunID,
		ArtifactDigest: videoArtifact.Digest, TraceID: step.TraceID, PersistProductTruth: true,
	}); err != nil {
		t.Fatal(err)
	}
	q1Command := controlplane.CreateApprovalDecisionCommand{
		SchemaVersion: "v1", SeriesID: seriesID.String(), EpisodeID: episodeID.String(),
		Gate: "Q1", Decision: "APPROVED", ReasonCode: "integration",
		Bindings: []controlplane.ApprovalBinding{
			{ObjectType: "SHOT_SPEC_REVISION", RevisionID: shotRevisionID.String(), ContentHash: shotHash},
			{ObjectType: "GENERATION_RUN", RevisionID: run.RunID, ContentHash: run.RunSpecDigest},
		},
		Actor: controlplane.Actor{ActorID: "reviewer", Role: "REVIEWER"},
	}
	q1Digest, err := digestValue(q1Command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApprovalDecision(ctx, q1Command, controlplane.Idempotency{
		Scope: "workflow-projection-q1:" + run.RunID,
		Key:   uuid.NewString(), RequestHash: q1Digest,
	}, step.TraceID); err != nil {
		t.Fatal(err)
	}
	var q1State string
	if err := pool.QueryRow(ctx, `
		SELECT state
		FROM video_pipeline.review_tasks
		WHERE generation_run_id = $1 AND review_type = 'Q1'`,
		run.RunID,
	).Scan(&q1State); err != nil {
		t.Fatal(err)
	}
	if q1State != "APPROVED" {
		t.Fatalf("Q1 review state = %q, want APPROVED", q1State)
	}
	approvedAfterPause, err := store.GetGenerationRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if approvedAfterPause.State != "SUCCEEDED" ||
		approvedAfterPause.FailureClass != "" || approvedAfterPause.FailureCode != "" {
		t.Fatalf("Q1-approved run retained pause failure = %#v", approvedAfterPause)
	}
	var storedSize int64
	var storedWidth, storedHeight int
	if err := pool.QueryRow(ctx, `
		SELECT size_bytes,
		       (media_spec->>'width')::int,
		       (media_spec->>'height')::int
		FROM video_pipeline.artifacts
		WHERE content_hash = $1`,
		videoArtifact.Digest,
	).Scan(&storedSize, &storedWidth, &storedHeight); err != nil {
		t.Fatal(err)
	}
	if storedSize != videoArtifact.Size || storedWidth != 1280 || storedHeight != 720 {
		t.Fatalf("stored artifact spec = size:%d %dx%d", storedSize, storedWidth, storedHeight)
	}

	step.ActivityID, step.ActivityType = "manifest", orchestration.ActivityCreateGate3
	gate3Input := orchestration.CreateGate3Input{
		EpisodeRevisionID: episodeRevisionID.String(), RunIDs: []string{run.RunID},
		TraceID: step.TraceID, PersistProductTruth: true,
	}
	manifestPayload, err := store.BuildEpisodeManifest(ctx, step, gate3Input)
	if err != nil {
		t.Fatal(err)
	}
	manifestArtifact, err := cas.Put(ctx, bytes.NewReader(manifestPayload))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitEpisodeManifest(ctx, step, gate3Input, manifestPayload, manifestArtifact); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.GetManifest(ctx, "EPISODE", episodeRevisionID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.ProviderExecutions) != 1 || len(manifest.Outputs) != 1 || manifest.LockedAt != nil {
		t.Fatalf("unlocked manifest projection = %#v", manifest)
	}
	g3Command := controlplane.CreateApprovalDecisionCommand{
		SchemaVersion: "v1", SeriesID: seriesID.String(), EpisodeID: episodeID.String(),
		Gate: "G3", Decision: "APPROVED", ReasonCode: "integration",
		Bindings: []controlplane.ApprovalBinding{
			{ObjectType: "EPISODE_REVISION", RevisionID: episodeRevisionID.String(), ContentHash: episodeHash},
			{ObjectType: "MANIFEST", RevisionID: manifest.ManifestID, ContentHash: manifest.ManifestHash},
		},
		Actor: controlplane.Actor{ActorID: "director", Role: "DIRECTOR"},
	}
	g3Digest, err := digestValue(g3Command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApprovalDecision(ctx, g3Command, controlplane.Idempotency{
		Scope: "workflow-projection-g3:" + episodeID.String(),
		Key:   uuid.NewString(), RequestHash: g3Digest,
	}, step.TraceID); err != nil {
		t.Fatal(err)
	}
	locked, err := store.GetManifest(ctx, "EPISODE", episodeRevisionID.String())
	if err != nil {
		t.Fatal(err)
	}
	if locked.LockedAt == nil {
		t.Fatal("G3 manifest remained unlocked")
	}
	var g3State string
	if err := pool.QueryRow(ctx, `
		SELECT state
		FROM video_pipeline.review_tasks
		WHERE episode_id = $1 AND review_type = 'G3'`,
		episodeID,
	).Scan(&g3State); err != nil {
		t.Fatal(err)
	}
	if g3State != "APPROVED" {
		t.Fatalf("G3 review state = %q, want APPROVED", g3State)
	}

	publicCommand := controlplane.CreateGenerationRunCommand{
		SchemaVersion: "v1", ShotSpecRevisionID: shotRevisionID.String(),
		PromptSnapshotID: prompt.ID, GenerationProfileRevisionID: profileID.String(),
		GenerationPlanID: plan.Value.GenerationPlanID, RouteSnapshot: route,
		BudgetApprovalID: budgetID.String(), ExecutionPolicy: executionPolicy,
		CreativeAttempt: 2,
		Actor:           controlplane.Actor{ActorID: "operator", Role: "OPERATOR"},
	}
	publicDigest, err := digestValue(publicCommand)
	if err != nil {
		t.Fatal(err)
	}
	if temporalAddress := os.Getenv("VIDEO_TEST_TEMPORAL_ADDRESS"); temporalAddress != "" {
		testHTTPTemporalImmediateCancellation(
			t,
			ctx,
			pool,
			store,
			temporalAddress,
			os.Getenv("VIDEO_TEST_PROVIDER_URL"),
			shotID.String(),
			publicCommand,
		)
		return
	}
	publicOperation, err := store.CreateGenerationRun(
		ctx, shotID.String(), 1, publicCommand,
		controlplane.Idempotency{
			Scope: "public-shot-run:" + shotID.String(),
			Key:   uuid.NewString(), RequestHash: publicDigest,
		},
		"public-shot-run",
	)
	if err != nil {
		t.Fatal(err)
	}
	publicRecord, err := store.GetShotWorkflowRecord(ctx, publicOperation.Value.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	if publicRecord.PromptHash != prompt.Digest ||
		publicRecord.RouteSnapshot.ProviderProfileID != providerProfileID.String() ||
		publicRecord.BudgetApprovalID != budgetID.String() ||
		publicRecord.BudgetLimit.AmountMicros != 1_000 {
		t.Fatalf("public shot workflow record = %#v", publicRecord)
	}
	if err := store.MarkOperationStarted(
		ctx, publicOperation.Value.OperationID,
		publicOperation.Value.TemporalWorkflowID, "temporal-run-public",
	); err != nil {
		t.Fatal(err)
	}
	publicRun, err := store.GetGenerationRun(ctx, publicRecord.Run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if publicRun.State != "QUEUED" {
		t.Fatalf("public run state = %q, want QUEUED", publicRun.State)
	}
	pauseCommand := map[string]any{"runId": publicRun.RunID, "reason": "integration"}
	pauseDigest, _ := digestValue(pauseCommand)
	_, err = store.RequestRunPause(
		ctx, publicRun.RunID, 2, publicCommand.Actor, "INTEGRATION_PAUSE",
		controlplane.Idempotency{
			Scope: "public-shot-pause:" + publicRun.RunID,
			Key:   uuid.NewString(), RequestHash: pauseDigest,
		},
		"public-shot-run",
	)
	if err != nil {
		t.Fatal(err)
	}
	pausedRun, err := store.GetGenerationRun(ctx, publicRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if pausedRun.State != "PAUSED" || pausedRun.FailureCode != "INTEGRATION_PAUSE" {
		t.Fatalf("paused public run = %#v", pausedRun)
	}
	resumeDigest, _ := digestValue(map[string]any{"runId": publicRun.RunID, "mode": "RESUME_PAUSED"})
	if _, err := store.RequestRunResume(
		ctx, publicRun.RunID, 2, publicCommand.Actor, "RESUME_PAUSED",
		controlplane.Idempotency{
			Scope: "public-shot-resume:" + publicRun.RunID,
			Key:   uuid.NewString(), RequestHash: resumeDigest,
		},
		"public-shot-run",
	); err != nil {
		t.Fatal(err)
	}
	resumedRun, err := store.GetGenerationRun(ctx, publicRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if resumedRun.State != "RUNNING" || resumedRun.FailureClass != "" || resumedRun.FailureCode != "" {
		t.Fatalf("resumed public run retained pause failure = %#v", resumedRun)
	}
	cancelDigest, _ := digestValue(map[string]any{"runId": publicRun.RunID, "reason": "INTEGRATION_CANCEL"})
	cancelOperation, err := store.RequestRunCancellation(
		ctx, publicRun.RunID, 2, publicCommand.Actor, "INTEGRATION_CANCEL",
		controlplane.Idempotency{
			Scope: "public-shot-cancel:" + publicRun.RunID,
			Key:   uuid.NewString(), RequestHash: cancelDigest,
		},
		"public-shot-run",
	)
	if err != nil {
		t.Fatal(err)
	}
	step.ActivityID, step.ActivityType = "cancel-public", orchestration.ActivityCancelProviderJob
	if err := store.RecordProviderCancellation(
		ctx, step,
		orchestration.CancelProviderJobInput{
			OperationID: cancelOperation.Value.OperationID,
			Dispatch: orchestration.ExecuteProviderJobInput{
				Run: orchestration.GenerationRunRef{
					RunID: publicRun.RunID, RunSpecDigest: publicRun.RunSpecDigest, Attempt: 2,
				},
			},
			ReasonCode: "INTEGRATION_CANCEL", TraceID: "public-shot-run",
		},
		orchestration.CancelProviderResult{State: "UNKNOWN", ErrorCode: "CANCEL_NOT_CONFIRMED"},
	); err != nil {
		t.Fatal(err)
	}
	cancelledRun, err := store.GetGenerationRun(ctx, publicRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelledRun.State != "CANCELLED" ||
		cancelledRun.FailureClass != "" || cancelledRun.FailureCode != "" {
		t.Fatalf("cancelled public run = %#v", cancelledRun)
	}
	var providerJobCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = $1`,
		publicRun.RunID,
	).Scan(&providerJobCount); err != nil {
		t.Fatal(err)
	}
	if providerJobCount != 0 {
		t.Fatalf("provider jobs for immediately cancelled run = %d, want 0", providerJobCount)
	}
	var createOperationState, cancelOperationState string
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT state FROM video_pipeline.operation_requests WHERE id = $1),
			(SELECT state FROM video_pipeline.operation_requests WHERE id = $2)`,
		publicOperation.Value.OperationID, cancelOperation.Value.OperationID,
	).Scan(&createOperationState, &cancelOperationState); err != nil {
		t.Fatal(err)
	}
	if createOperationState != "CANCELLED" || cancelOperationState != "SUCCEEDED" {
		t.Fatalf(
			"immediate cancellation operations = create:%s cancel:%s",
			createOperationState, cancelOperationState,
		)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE video_pipeline.generation_runs
		SET state = 'SUCCEEDED', failure_class = 'INFRASTRUCTURE', failure_code = 'INTEGRATION_CANCEL'
		WHERE id = $1`,
		publicRun.RunID,
	); err != nil {
		t.Fatal(err)
	}
	step.ActivityID = "cancel-public-race"
	if err := store.RecordProviderCancellation(
		ctx, step,
		orchestration.CancelProviderJobInput{
			OperationID: cancelOperation.Value.OperationID,
			Dispatch: orchestration.ExecuteProviderJobInput{
				Run: orchestration.GenerationRunRef{
					RunID: publicRun.RunID, RunSpecDigest: publicRun.RunSpecDigest, Attempt: 2,
				},
			},
			ReasonCode: "INTEGRATION_CANCEL", TraceID: "public-shot-run",
		},
		orchestration.CancelProviderResult{State: "SUCCEEDED"},
	); err != nil {
		t.Fatal(err)
	}
	racedRun, err := store.GetGenerationRun(ctx, publicRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if racedRun.State != "SUCCEEDED" || racedRun.FailureClass != "" || racedRun.FailureCode != "" {
		t.Fatalf("terminal-success cancellation race = %#v", racedRun)
	}
}

func testHTTPTemporalImmediateCancellation(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *Postgres,
	temporalAddress string,
	providerURL string,
	shotID string,
	command controlplane.CreateGenerationRunCommand,
) {
	t.Helper()
	if providerURL == "" {
		providerURL = "http://127.0.0.1:8090"
	}
	temporalClient, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalAddress,
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("connect Temporal integration service: %v", err)
	}
	defer temporalClient.Close()
	taskQueue := "video-http-cancel-" + uuid.NewString()
	controller, err := temporalcontrol.New(temporalClient, taskQueue, store)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	newWorker := func(identity string) worker.Worker {
		temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{Identity: identity})
		temporalWorker.RegisterWorkflowWithOptions(
			orchestration.ShotProductionWorkflow,
			workflow.RegisterOptions{Name: orchestration.ShotWorkflowName},
		)
		activities := orchestration.NewProductionActivities(providerURL, store, store, artifacts)
		temporalWorker.RegisterActivityWithOptions(
			activities.ExecuteProviderJob,
			activity.RegisterOptions{Name: orchestration.ActivityExecuteProviderJob},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.CancelProviderJob,
			activity.RegisterOptions{Name: orchestration.ActivityCancelProviderJob},
		)
		return temporalWorker
	}

	// Establish a prior worker process, then stop it. HTTP create/cancel below is
	// accepted by Temporal while no worker is polling; the replacement worker
	// must replay the already-recorded cancellation without provider submission.
	previousWorker := newWorker("prior-http-cancel-worker-" + uuid.NewString())
	if err := previousWorker.Start(); err != nil {
		t.Fatalf("start prior Temporal worker: %v", err)
	}
	previousWorker.Stop()

	api := httptest.NewServer(
		controlplane.NewWithRuntime(
			runtimeconfig.ControlPlane{},
			nil,
			store,
			controller,
			nil,
		).Handler(),
	)
	defer api.Close()
	createBody, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	createResponse := executeIntegrationRequest(
		t,
		http.MethodPost,
		api.URL+controlplane.APIBase+"/shots/"+shotID+"/runs",
		createBody,
		map[string]string{
			"Idempotency-Key": uuid.NewString(),
			"If-Match":        `"1"`,
		},
	)
	if createResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", createResponse.StatusCode, createResponse.Body)
	}
	var createOperation controlplane.Operation
	if err := json.Unmarshal(createResponse.Body, &createOperation); err != nil {
		t.Fatalf("decode create operation: %v body=%s", err, createResponse.Body)
	}
	if createOperation.State != "RUNNING" || createOperation.TemporalWorkflowID == "" {
		t.Fatalf("create operation = %#v", createOperation)
	}

	cancelBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"reasonCode":"IMMEDIATE_CANCEL"}`,
	)
	cancelKey := uuid.NewString()
	var cancelOperation controlplane.Operation
	for attempt := 1; attempt <= 2; attempt++ {
		response := executeIntegrationRequest(
			t,
			http.MethodPost,
			api.URL+controlplane.APIBase+"/runs/"+createOperation.AggregateID+"/cancel",
			cancelBody,
			map[string]string{
				"Idempotency-Key": cancelKey,
				"If-Match":        `"2"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf(
				"cancel attempt %d status=%d body=%s",
				attempt,
				response.StatusCode,
				response.Body,
			)
		}
		if attempt == 1 {
			// Discarding the first response models a client-visible response
			// loss; the same idempotency key must recover the operation.
			continue
		}
		var replayed controlplane.Operation
		if err := json.Unmarshal(response.Body, &replayed); err != nil {
			t.Fatalf("decode cancel attempt %d: %v", attempt, err)
		}
		cancelOperation = replayed
	}
	var cancelOperationCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.operation_requests
		WHERE aggregate_type = 'GENERATION_RUN'
		  AND aggregate_id = $1
		  AND operation_type = 'CANCEL_GENERATION_RUN'`,
		createOperation.AggregateID,
	).Scan(&cancelOperationCount); err != nil {
		t.Fatal(err)
	}
	if cancelOperationCount != 1 || cancelOperation.OperationID == "" {
		t.Fatalf(
			"duplicate cancellation persisted %d operations; replay=%#v",
			cancelOperationCount,
			cancelOperation,
		)
	}

	replacementWorker := newWorker("replacement-http-cancel-worker-" + uuid.NewString())
	if err := replacementWorker.Start(); err != nil {
		t.Fatalf("start replacement Temporal worker: %v", err)
	}
	defer replacementWorker.Stop()
	workflowRun := temporalClient.GetWorkflow(ctx, createOperation.TemporalWorkflowID, "")
	if err := workflowRun.Get(ctx, nil); err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("workflow completion error = %v, want Canceled", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		run, err := store.GetGenerationRun(ctx, createOperation.AggregateID)
		if err != nil {
			t.Fatal(err)
		}
		createState, cancelState := "", ""
		if err := pool.QueryRow(ctx, `
			SELECT
				(SELECT state FROM video_pipeline.operation_requests WHERE id = $1),
				(SELECT state FROM video_pipeline.operation_requests WHERE id = $2)`,
			createOperation.OperationID,
			cancelOperation.OperationID,
		).Scan(&createState, &cancelState); err != nil {
			t.Fatal(err)
		}
		if run.State == "CANCELLED" &&
			createState == "CANCELLED" &&
			cancelState == "SUCCEEDED" {
			if run.FailureClass != "" || run.FailureCode != "" {
				t.Fatalf("cancelled run retained failure = %#v", run)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"cancellation did not converge: run=%#v create=%s cancel=%s",
				run,
				createState,
				cancelState,
			)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var providerJobs int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = $1`,
		createOperation.AggregateID,
	).Scan(&providerJobs); err != nil {
		t.Fatal(err)
	}
	if providerJobs != 0 {
		t.Fatalf("provider jobs after immediate cancellation = %d, want 0", providerJobs)
	}
}

type integrationHTTPResponse struct {
	StatusCode int
	Body       []byte
}

func executeIntegrationRequest(
	t *testing.T,
	method string,
	url string,
	body []byte,
	headers map[string]string,
) integrationHTTPResponse {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return integrationHTTPResponse{StatusCode: response.StatusCode, Body: responseBody}
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

func mustExec(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	query string,
	arguments ...any,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, arguments...); err != nil {
		t.Fatal(err)
	}
}
