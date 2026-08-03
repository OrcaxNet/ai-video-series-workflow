//go:build integration

package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/temporalcontrol"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type cancelRequestedProviderFailureTransport struct {
	base    http.RoundTripper
	enabled *atomic.Bool
	started chan struct{}
	once    sync.Once
}

func (t *cancelRequestedProviderFailureTransport) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	if request.Method == http.MethodGet &&
		strings.HasPrefix(request.URL.Path, "/v1/jobs/") &&
		t.enabled.Load() {
		t.once.Do(func() { close(t.started) })
		<-request.Context().Done()
		return nil, &net.DNSError{
			Err: "no such host", Name: "mock-provider", IsNotFound: true,
		}
	}
	return t.base.RoundTrip(request)
}

func TestPostgres_CreateSeriesIdempotencyAndAtomicEvidence(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewForPool(pool)

	profileID, profileGroupID := uuid.New(), uuid.New()
	sourceRevisionID := uuid.New()
	seriesID, episodeID, episodeRevisionID := uuid.New(), uuid.New(), uuid.New()
	sceneID, shotID := uuid.New(), uuid.New()
	scriptID, storyboardID, shotRevisionID := uuid.New(), uuid.New(), uuid.New()
	gate1ID, gate2ID, budgetID, speechBudgetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	providerProfileID, capabilityID, textCapabilityID := uuid.New(), uuid.New(), uuid.New()
	safetyEvidenceArtifactID := uuid.New()
	voiceLicenseID, musicLicenseID, consentID := uuid.New(), uuid.New(), uuid.New()
	voiceAssetID, voiceAssetVersionID := uuid.New(), uuid.New()
	musicAssetID, musicAssetVersionID := uuid.New(), uuid.New()
	voiceArtifactID, musicArtifactID := uuid.New(), uuid.New()
	contextIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	episodeHash := strings.Repeat("1", 64)
	sourceHash := strings.Repeat("0", 64)
	shotHash := strings.Repeat("2", 64)
	safetyEvidenceHash := strings.Repeat(
		strings.ReplaceAll(safetyEvidenceArtifactID.String(), "-", ""),
		2,
	)
	effectiveHash := strings.Repeat("3", 64)
	profileHash := strings.Repeat("4", 64)
	capabilityHash := strings.Repeat("5", 64)
	textCapabilityHash := strings.Repeat("8", 64)
	voiceContentHash := strings.Repeat(
		strings.ReplaceAll(voiceArtifactID.String(), "-", ""), 2,
	)
	musicContentHash := strings.Repeat(
		strings.ReplaceAll(musicArtifactID.String(), "-", ""), 2,
	)

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
		INSERT INTO video_pipeline.source_revisions
			(id, series_id, revision, status, content_hash, artifact_uri,
			 language, rights_snapshot, created_by)
		VALUES ($1, $2, 1, 'APPROVED', $3, $4, 'zh-CN',
		        '{"basis":"integration"}'::jsonb, 'integration')`,
		sourceRevisionID, seriesID, sourceHash, "cas://sha256/"+sourceHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.episodes (id, series_id, ordinal, title)
		VALUES ($1, $2, 1, 'episode')`,
		episodeID, seriesID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.license_snapshots
			(id, subject_type, subject_ref, license_id, license_hash,
			 policy_status, territories, commercial_use, expires_at, source_uri)
		VALUES
			($1, 'VOICE', $2, 'voice-license', $3, 'ALLOWED', ARRAY['CN'], true,
			 now() + interval '1 hour', 'license://integration/voice'),
			($4, 'MUSIC', $5, 'music-license', $6, 'ALLOWED', ARRAY['CN'], true,
			 now() + interval '1 hour', 'license://integration/music')`,
		voiceLicenseID, voiceAssetVersionID.String(), strings.Repeat("d", 64),
		musicLicenseID, musicAssetVersionID.String(), strings.Repeat("e", 64),
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.consent_assets
			(id, subject_ref, consent_type, content_hash, artifact_uri,
			 territories, expires_at, status)
		VALUES ($1, $2, 'VOICE_CLONE', $3, $4, ARRAY['CN'],
		        now() + interval '1 hour', 'ACTIVE')`,
		consentID, voiceAssetVersionID.String(), strings.Repeat("9", 64),
		"cas://sha256/"+strings.Repeat("9", 64),
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.assets
			(id, series_id, asset_type, scope_type, scope_id)
		VALUES
			($1, $2, 'VOICE', 'SERIES', $2),
			($3, $2, 'MUSIC', 'SERIES', $2)`,
		voiceAssetID, seriesID, musicAssetID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.asset_versions
			(id, asset_id, revision, status, content_hash, artifact_uri, media_type,
			 source_ref, license_snapshot_id, consent_asset_id, created_by)
		VALUES
			($1, $2, 1, 'APPROVED', $3, $4, 'audio/wav',
			 'integration-voice', $5, $6, 'integration'),
			($7, $8, 1, 'APPROVED', $9, $10, 'audio/wav',
			 'integration-music', $11, NULL, 'integration')`,
		voiceAssetVersionID, voiceAssetID, voiceContentHash,
		"cas://sha256/"+voiceContentHash, voiceLicenseID, consentID,
		musicAssetVersionID, musicAssetID, musicContentHash,
		"cas://sha256/"+musicContentHash, musicLicenseID,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES
			($1, $2, $3, 'audio/wav', 1024, '{"kind":"voice_fixture"}', 'ACTIVE'),
			($4, $5, $6, 'audio/wav', 2048, '{"kind":"music_fixture"}', 'ACTIVE')`,
		voiceArtifactID, voiceContentHash, "cas://sha256/"+voiceContentHash,
		musicArtifactID, musicContentHash, "cas://sha256/"+musicContentHash,
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
		INSERT INTO video_pipeline.approval_decisions
			(id, series_id, episode_id, gate, decision, reason_code,
			 actor_id, actor_role, trace_id)
		VALUES ($1, $2, $3, 'G1', 'APPROVED', 'integration',
		        'art-director', 'ART_DIRECTOR', 'workflow-projection')`,
		gate1ID, seriesID, episodeID,
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
		        1, '{"action":"walk","dialogue":[{"id":"line-1","speaker":"A","text":"Hello fixture","startMillis":500,"endMillis":2000}]}', ARRAY[$9::uuid], $4,
		        $5, '{}'::jsonb, '{}'::jsonb, $6, $7, $8, 'integration')`,
		shotRevisionID, shotID, storyboardID, contextIDs,
		effectiveHash, profileID, gate2ID, shotHash, voiceAssetVersionID,
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
		INSERT INTO video_pipeline.approval_bindings
			(decision_id, object_type, revision_id, content_hash)
		VALUES ($1, 'EPISODE_REVISION', $2, $3)`,
		gate1ID, episodeRevisionID, episodeHash,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.review_tasks
			(id, series_id, episode_id, review_type, state, assigned_role, decided_at)
		VALUES
			($1, $3, $4, 'BUDGET', 'APPROVED', 'PRODUCER', now()),
			($2, $3, $4, 'BUDGET', 'APPROVED', 'PRODUCER', now())`,
		budgetID, speechBudgetID, seriesID, episodeID,
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
		VALUES
			($1, $2, 'video.primary', 'fixture-video-v1', 'route-v1',
			 ARRAY['text'], '{"unitPriceMicros":10,"remainingCalls":100,"allowedTerritories":["CN"],"productForms":["INTERNAL_PREVIEW","COMMERCIAL_RELEASE"],"contentSafetyPolicyVersions":["safety-v1"]}', 'pricing-v1', $3,
			 'ACTIVE', now()),
			($4, $2, 'text.primary', 'fixture-text-v1', 'route-v1',
			 ARRAY['text'], '{"unitPriceMicros":1,"remainingCalls":100,"allowedTerritories":["CN"],"productForms":["INTERNAL_PREVIEW"],"contentSafetyPolicyVersions":["safety-v1"]}', 'text-pricing-v1', $5,
			 'ACTIVE', now())`,
		capabilityID, providerProfileID, capabilityHash,
		textCapabilityID, textCapabilityHash,
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
	textRoute := controlplane.ModelRouteSnapshot{
		CapabilityAlias: "text.primary", ProviderProfileID: providerProfileID.String(),
		Provider: "MOCK", ModelID: "fixture-text-v1", RouteVersion: "route-v1",
		CapabilityHash: textCapabilityHash,
	}
	compilationCommand := controlplane.StartContentCompilationCommand{
		SchemaVersion: "v1", SourceHash: sourceHash,
		Stages:            []string{"STRUCTURE", "EPISODES", "SCENES", "SHOTS"},
		TextRouteSnapshot: textRoute,
		Actor:             controlplane.Actor{ActorID: "producer", Role: "PRODUCER"},
	}
	compilationDigest, err := digestValue(compilationCommand)
	if err != nil {
		t.Fatal(err)
	}
	compilationIdempotency := controlplane.Idempotency{
		Scope: "workflow-projection-compilation:" + sourceRevisionID.String(),
		Key:   uuid.NewString(), RequestHash: compilationDigest,
	}
	compilation, err := store.StartContentCompilation(
		ctx, sourceRevisionID.String(), compilationCommand,
		compilationIdempotency, "workflow-projection-compilation",
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedCompilation, err := store.StartContentCompilation(
		ctx, sourceRevisionID.String(), compilationCommand,
		compilationIdempotency, "workflow-projection-compilation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if compilation.Value.State != "ACCEPTED" ||
		!replayedCompilation.Replayed ||
		replayedCompilation.Value.OperationID != compilation.Value.OperationID {
		t.Fatalf(
			"content compilation idempotency = first:%#v replay:%#v",
			compilation, replayedCompilation,
		)
	}
	var compilationRuns int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.content_compilation_runs
		WHERE source_revision_id = $1 AND state = 'VALIDATED'`,
		sourceRevisionID,
	).Scan(&compilationRuns); err != nil {
		t.Fatal(err)
	}
	if compilationRuns != 4 {
		t.Fatalf("content compilation runs = %d, want 4", compilationRuns)
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
		BudgetLimit:       controlplane.BudgetLimit{AmountMicros: 1_000, Currency: "CNY"},
		SpeechBudgetLimit: &controlplane.BudgetLimit{AmountMicros: 1_000, Currency: "CNY"},
		ExecutionPolicy:   executionPolicy,
		Actor:             controlplane.Actor{ActorID: "producer", Role: "PRODUCER"},
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
	if plan.Value.SpeechBudgetLimit == nil ||
		plan.Value.SpeechBudgetLimit.AmountMicros != 1_000 ||
		plan.Value.SpeechBudgetLimit.Currency != "CNY" {
		t.Fatalf("generation plan speech budget = %#v", plan.Value.SpeechBudgetLimit)
	}
	storedPlan, err := store.GetGenerationPlan(ctx, plan.Value.GenerationPlanID)
	if err != nil {
		t.Fatal(err)
	}
	if storedPlan.SpeechBudgetLimit == nil ||
		storedPlan.SpeechBudgetLimit.AmountMicros != 1_000 ||
		storedPlan.SpeechBudgetLimit.Currency != "CNY" {
		t.Fatalf("stored generation plan speech budget = %#v", storedPlan.SpeechBudgetLimit)
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.review_tasks
		SET generation_plan_id = $3,
		    budget_scope = CASE id WHEN $1 THEN 'VIDEO' ELSE 'SPEECH' END,
		    budget_limit_micros = 1000,
		    budget_currency = 'CNY'
		WHERE id IN ($1, $2)`,
		budgetID, speechBudgetID, plan.Value.GenerationPlanID,
	)
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
	alternateVideoPlanCommand := planCommand
	alternateVideoPlanCommand.CandidatesPerShot = 2
	alternateVideoPlanDigest, err := digestValue(alternateVideoPlanCommand)
	if err != nil {
		t.Fatal(err)
	}
	alternateVideoPlan, err := store.CreateGenerationPlan(
		ctx,
		alternateVideoPlanCommand,
		controlplane.Idempotency{
			Scope:       "workflow-projection-alternate-video-plan:" + seriesID.String(),
			Key:         uuid.NewString(),
			RequestHash: alternateVideoPlanDigest,
		},
		"workflow-projection-alternate-video-plan",
	)
	if err != nil {
		t.Fatal(err)
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
	if len(prompt.Assets) != 1 ||
		prompt.Assets[0].Revision != voiceAssetVersionID.String() ||
		prompt.Assets[0].SizeBytes != 1024 {
		t.Fatalf("prompt ACTIVE CAS size evidence = %#v, want voice size 1024", prompt.Assets)
	}
	mustExec(t, ctx, pool, `UPDATE video_pipeline.artifacts SET status='DISABLED' WHERE id=$1`, voiceArtifactID)
	if _, err := store.ResolvePromptSnapshot(ctx, prompt.ID); err == nil {
		t.Fatal("prompt resolution accepted a DISABLED CAS artifact")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeLicenseBlocked {
			t.Fatalf("DISABLED prompt artifact error = %#v", err)
		}
	}
	mustExec(t, ctx, pool, `UPDATE video_pipeline.artifacts SET status='ACTIVE' WHERE id=$1`, voiceArtifactID)
	resolvedPrompt, err := store.ResolvePromptSnapshot(ctx, prompt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolvedPrompt.Assets) != 1 || resolvedPrompt.Assets[0].SizeBytes != 1024 {
		t.Fatalf("restored ACTIVE CAS size evidence = %#v", resolvedPrompt.Assets)
	}
	var promptInputCount, promptAssetCount int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM video_pipeline.prompt_snapshot_inputs
		   WHERE prompt_snapshot_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.prompt_snapshot_assets
		   WHERE prompt_snapshot_id = $1)`,
		prompt.ID,
	).Scan(&promptInputCount, &promptAssetCount); err != nil {
		t.Fatal(err)
	}
	if promptInputCount != 6 || promptAssetCount != 1 {
		t.Fatalf(
			"Prompt v6 lineage counts = inputs:%d assets:%d, want 6/1",
			promptInputCount, promptAssetCount,
		)
	}
	publicCommand := controlplane.CreateGenerationRunCommand{
		SchemaVersion: "v1", ShotSpecRevisionID: shotRevisionID.String(),
		PromptSnapshotID: prompt.ID, GenerationProfileRevisionID: profileID.String(),
		GenerationPlanID: plan.Value.GenerationPlanID, RouteSnapshot: route,
		BudgetApprovalID: budgetID.String(), ExecutionPolicy: executionPolicy,
		CreativeAttempt: 2,
		Actor:           controlplane.Actor{ActorID: "operator", Role: "OPERATOR"},
	}
	var temporalPauseShotID string
	var temporalPauseCommand controlplane.CreateGenerationRunCommand
	var composeOutageShotID string
	var composeOutageCommand controlplane.CreateGenerationRunCommand
	if os.Getenv("VIDEO_TEST_TEMPORAL_ADDRESS") != "" {
		temporalPauseShotID, temporalPauseCommand = cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		if os.Getenv("VIDEO_TEST_PROVIDER_CONTAINER") != "" {
			composeOutageShotID, composeOutageCommand = cloneIntegrationShotCommand(
				t, ctx, pool, store, shotID.String(), publicCommand,
			)
		}
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
	attemptDriftCases := []struct {
		name     string
		wantCode controlplane.ErrorCode
		mutate   func(t *testing.T)
	}{
		{
			name:     "failed attempt",
			wantCode: controlplane.CodeConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'FAILED'
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
		{
			name:     "cancelled attempt",
			wantCode: controlplane.CodeConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'CANCELLED'
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
		{
			name:     "succeeded attempt",
			wantCode: controlplane.CodeConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'SUCCEEDED'
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
		{
			name:     "unknown attempt without a prepared job",
			wantCode: controlplane.CodeConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'UNKNOWN'
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
		{
			name:     "input hash drift",
			wantCode: controlplane.CodeRevisionConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET input_hash = $2
					WHERE generation_run_id = $1`, run.RunID, strings.Repeat("f", 64))
			},
		},
		{
			name:     "attempt kind drift",
			wantCode: controlplane.CodeRevisionConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET attempt_kind = 'CREATIVE_REVISION'
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
		{
			name:     "attempt sequence drift",
			wantCode: controlplane.CodeRevisionConflict,
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.generation_attempts
					SET sequence = 2
					WHERE generation_run_id = $1`, run.RunID)
			},
		},
	}
	for _, test := range attemptDriftCases {
		t.Run("PrepareProviderJob rejects "+test.name, func(t *testing.T) {
			mustExec(t, ctx, pool, `
				UPDATE video_pipeline.generation_attempts
				SET sequence = 1, attempt_kind = 'PROVIDER_REQUEST', state = 'VALIDATED',
				    input_hash = $2, model_snapshot = $3
				WHERE generation_run_id = $1`, run.RunID, run.RunSpecDigest, model)
			test.mutate(t)
			if _, err := store.PrepareProviderJob(ctx, step, dispatch); err == nil {
				t.Fatal("PrepareProviderJob accepted a drifted generation attempt")
			} else {
				var domain *controlplane.DomainError
				if !errors.As(err, &domain) || domain.Code != test.wantCode {
					t.Fatalf("drifted generation attempt error = %#v, want %s", err, test.wantCode)
				}
			}
			var reservations, jobs, costs int
			if err := pool.QueryRow(ctx, `
				SELECT
				  (SELECT count(*) FROM video_pipeline.budget_reservations
				   WHERE generation_run_id = $1),
				  (SELECT count(*) FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1),
				  (SELECT count(*) FROM video_pipeline.cost_ledger cl
				   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`, run.RunID,
			).Scan(&reservations, &jobs, &costs); err != nil {
				t.Fatal(err)
			}
			if reservations != 0 || jobs != 0 || costs != 0 {
				t.Fatalf(
					"drift rejection side effects = reservations:%d jobs:%d costs:%d",
					reservations, jobs, costs,
				)
			}
		})
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.generation_attempts
		SET sequence = 1, attempt_kind = 'PROVIDER_REQUEST', state = 'VALIDATED',
		    input_hash = $2, model_snapshot = $3
		WHERE generation_run_id = $1`, run.RunID, run.RunSpecDigest, model)
	atomicMismatch := dispatch
	atomicMismatch.ExpectedProductTruth = &orchestration.PreparedProductTruth{
		ShotSpecRevisionID:  shotRevisionID.String(),
		Run:                 run,
		PromptSnapshotID:    prompt.ID,
		PromptSnapshotHash:  prompt.Digest,
		GenerationPlanID:    uuid.NewString(),
		BudgetApprovalID:    budgetID.String(),
		BudgetMaximumMicros: 1_000,
		BudgetCurrency:      "CNY",
		ProviderProfileID:   providerProfileID.String(),
		Route:               model,
	}
	if _, err := store.PrepareProviderJob(ctx, step, atomicMismatch); err == nil {
		t.Fatal("PrepareProviderJob accepted a frozen package / PostgreSQL truth mismatch")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("frozen package atomic mismatch error = %#v", err)
		}
	}
	shotMismatch := dispatch
	shotMismatch.ExpectedProductTruth = &orchestration.PreparedProductTruth{
		ShotSpecRevisionID:  uuid.NewString(),
		Run:                 run,
		PromptSnapshotID:    prompt.ID,
		PromptSnapshotHash:  prompt.Digest,
		GenerationPlanID:    plan.Value.GenerationPlanID,
		BudgetApprovalID:    budgetID.String(),
		BudgetMaximumMicros: 1_000,
		BudgetCurrency:      "CNY",
		ProviderProfileID:   providerProfileID.String(),
		Route:               model,
	}
	if _, err := store.PrepareProviderJob(ctx, step, shotMismatch); err == nil {
		t.Fatal("PrepareProviderJob accepted a frozen ShotSpec / PostgreSQL truth mismatch")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("frozen ShotSpec atomic mismatch error = %#v", err)
		}
	}
	var atomicReservations, atomicJobs, atomicCosts int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM video_pipeline.budget_reservations
		   WHERE generation_run_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
		   JOIN video_pipeline.generation_attempts ga
		     ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.cost_ledger cl
		   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
		   JOIN video_pipeline.generation_attempts ga
		     ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1)`,
		run.RunID,
	).Scan(&atomicReservations, &atomicJobs, &atomicCosts); err != nil {
		t.Fatal(err)
	}
	if atomicReservations != 0 || atomicJobs != 0 || atomicCosts != 0 {
		t.Fatalf(
			"frozen package atomic rejection side effects = reservations:%d jobs:%d costs:%d",
			atomicReservations, atomicJobs, atomicCosts,
		)
	}
	lineageShotID, lineageCommand := cloneIntegrationShotCommand(
		t, ctx, pool, store, shotID.String(), publicCommand,
	)
	lineagePrompt, err := store.ResolvePromptSnapshot(
		ctx, lineageCommand.PromptSnapshotID,
	)
	if err != nil {
		t.Fatal(err)
	}
	wrongPromptDispatch := dispatch
	wrongPromptDispatch.Prompt = lineagePrompt
	if _, err := store.PrepareProviderJob(ctx, step, wrongPromptDispatch); err == nil {
		t.Fatal("Run A accepted Prompt B")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) ||
			domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("Run A / Prompt B error = %#v", err)
		}
	}
	wrongRouteDispatch := dispatch
	wrongRouteDispatch.Route.Verification = "different-route-evidence"
	if _, err := store.PrepareProviderJob(ctx, step, wrongRouteDispatch); err == nil {
		t.Fatal("Run A accepted Route B")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) ||
			domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("Run A / Route B error = %#v", err)
		}
	}
	var rejectedBindingReservations, rejectedBindingJobs int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM video_pipeline.budget_reservations
		   WHERE generation_run_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
		   JOIN video_pipeline.generation_attempts ga
		     ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1)`,
		run.RunID,
	).Scan(&rejectedBindingReservations, &rejectedBindingJobs); err != nil {
		t.Fatal(err)
	}
	if rejectedBindingReservations != 0 || rejectedBindingJobs != 0 {
		t.Fatalf(
			"Run binding rejection left paid side effects = reservations:%d jobs:%d",
			rejectedBindingReservations, rejectedBindingJobs,
		)
	}

	const raceProviderCallCount = 10
	const raceFanout = raceProviderCallCount * 2
	raceBaseCommands := make([]controlplane.CreateGenerationRunCommand, raceProviderCallCount)
	raceShotRevisionIDs := make([]string, raceProviderCallCount)
	raceBaseCommands[0] = lineageCommand
	raceShotRevisionIDs[0] = lineageCommand.ShotSpecRevisionID
	for index := 1; index < raceProviderCallCount; index++ {
		_, raceBaseCommands[index] = cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		raceShotRevisionIDs[index] = raceBaseCommands[index].ShotSpecRevisionID
	}
	raceShotIDs, err := parseUUIDs(raceShotRevisionIDs)
	if err != nil {
		t.Fatal(err)
	}
	raceSafetyBindings := []controlplane.ApprovalBinding{
		{
			ObjectType: "EPISODE_REVISION", RevisionID: episodeRevisionID.String(),
			ContentHash: episodeHash,
		},
		{
			ObjectType: "ARTIFACT", RevisionID: safetyEvidenceArtifactID.String(),
			ContentHash: safetyEvidenceHash,
		},
	}
	raceSafetyRows, err := pool.Query(ctx, `
		SELECT id, content_hash
		FROM video_pipeline.shot_spec_revisions
		WHERE id = ANY($1::uuid[])
		ORDER BY id`,
		raceShotIDs,
	)
	if err != nil {
		t.Fatal(err)
	}
	for raceSafetyRows.Next() {
		var revisionID uuid.UUID
		var contentHash string
		if err := raceSafetyRows.Scan(&revisionID, &contentHash); err != nil {
			raceSafetyRows.Close()
			t.Fatal(err)
		}
		raceSafetyBindings = append(raceSafetyBindings, controlplane.ApprovalBinding{
			ObjectType: "SHOT_SPEC_REVISION", RevisionID: revisionID.String(),
			ContentHash: contentHash,
		})
	}
	if err := raceSafetyRows.Err(); err != nil {
		raceSafetyRows.Close()
		t.Fatal(err)
	}
	raceSafetyRows.Close()
	if len(raceSafetyBindings) != raceProviderCallCount+2 {
		t.Fatalf(
			"race safety bindings = %d, want %d",
			len(raceSafetyBindings), raceProviderCallCount+2,
		)
	}
	raceSafetyValidUntil := time.Now().UTC().Add(time.Hour)
	raceSafetyCommand := controlplane.CreateApprovalDecisionCommand{
		SchemaVersion: "v1", SeriesID: seriesID.String(), EpisodeID: episodeID.String(),
		Gate: "SAFETY", Decision: "APPROVED", ReasonCode: "CONTENT_SAFETY_APPROVED",
		PolicyVersion: "safety-v1", EvidenceHash: safetyEvidenceHash,
		ValidUntil: &raceSafetyValidUntil, Bindings: raceSafetyBindings,
		Actor: controlplane.Actor{ActorID: "safety-reviewer", Role: "SAFETY_REVIEWER"},
	}
	raceSafetyDigest, err := digestValue(raceSafetyCommand)
	if err != nil {
		t.Fatal(err)
	}
	raceSafety, err := store.CreateApprovalDecision(
		ctx, raceSafetyCommand,
		controlplane.Idempotency{
			Scope: "workflow-projection-race-safety:" + lineageShotID,
			Key:   uuid.NewString(), RequestHash: raceSafetyDigest,
		},
		"workflow-projection-race-safety",
	)
	if err != nil {
		t.Fatal(err)
	}
	racePlanCommand := planCommand
	racePlanCommand.ShotSpecRevisionIDs = raceShotRevisionIDs
	racePlanCommand.BudgetLimit = controlplane.BudgetLimit{
		AmountMicros: 500, Currency: "CNY",
	}
	racePlanCommand.ExecutionPolicy = lineageCommand.ExecutionPolicy
	racePlanCommand.ExecutionPolicy.ContentSafetyDecisionID = raceSafety.Value.DecisionID
	racePlanDigest, err := digestValue(racePlanCommand)
	if err != nil {
		t.Fatal(err)
	}
	racePlan, err := store.CreateGenerationPlan(
		ctx, racePlanCommand,
		controlplane.Idempotency{
			Scope: "workflow-projection-race-plan:" + lineageShotID,
			Key:   uuid.NewString(), RequestHash: racePlanDigest,
		},
		"workflow-projection-race-plan",
	)
	if err != nil {
		t.Fatal(err)
	}
	raceBudgetID := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.review_tasks
			(id, series_id, episode_id, review_type, state, assigned_role,
			 decided_at, generation_plan_id, budget_scope,
			 budget_limit_micros, budget_currency)
		VALUES ($1, $2, $3, 'BUDGET', 'APPROVED', 'PRODUCER',
		        now(), $4, 'VIDEO', 500, 'CNY')`,
		raceBudgetID, seriesID, episodeID, racePlan.Value.GenerationPlanID,
	)
	raceCommands := make([]controlplane.CreateGenerationRunCommand, raceFanout)
	for index := range raceCommands {
		raceCommands[index] = raceBaseCommands[index%raceProviderCallCount]
		raceCommands[index].CreativeAttempt = index/raceProviderCallCount + 1
		raceCommands[index].GenerationPlanID = racePlan.Value.GenerationPlanID
		raceCommands[index].BudgetApprovalID = raceBudgetID.String()
	}
	raceSteps := make([]orchestration.WorkflowStep, raceFanout)
	raceRuns := make([]orchestration.GenerationRunRef, raceFanout)
	raceDispatches := make([]orchestration.ExecuteProviderJobInput, raceFanout)
	for index := range raceCommands {
		raceSteps[index], raceRuns[index], raceDispatches[index] =
			createIntegrationWorkflowRun(
				t, ctx, store, fmt.Sprintf("budget-race-%d", index+1),
				raceCommands[index],
			)
		// Semantically identical UUID spellings must share one cumulative
		// approval bucket, including under concurrent reservation pressure.
		switch index % 3 {
		case 1:
			raceDispatches[index].BudgetApprovalID = strings.ToUpper(raceBudgetID.String())
		case 2:
			raceDispatches[index].BudgetApprovalID = strings.ReplaceAll(raceBudgetID.String(), "-", "")
		}
	}
	raceErrors := make([]error, raceFanout)
	var raceWait sync.WaitGroup
	var raceReady sync.WaitGroup
	raceReady.Add(raceFanout)
	raceStart := make(chan struct{})
	for index := range raceDispatches {
		index := index
		raceWait.Add(1)
		go func() {
			defer raceWait.Done()
			raceReady.Done()
			<-raceStart
			_, raceErrors[index] = store.PrepareProviderJob(
				ctx, raceSteps[index], raceDispatches[index],
			)
		}()
	}
	raceReady.Wait()
	close(raceStart)
	raceWait.Wait()
	winners := make([]int, 0, raceProviderCallCount)
	losers := make([]int, 0, raceFanout-raceProviderCallCount)
	for index, raceErr := range raceErrors {
		if raceErr == nil {
			winners = append(winners, index)
			continue
		}
		var domain *controlplane.DomainError
		if !errors.As(raceErr, &domain) ||
			domain.Code != controlplane.CodeBudgetExceeded || domain.Retryable {
			t.Fatalf("concurrent cumulative reservation %d error = %#v", index, raceErr)
		}
		losers = append(losers, index)
	}
	if len(winners) != raceProviderCallCount ||
		len(losers) != raceFanout-raceProviderCallCount {
		t.Fatalf(
			"cumulative reservation race = %#v, want %d successes/%d budget rejections",
			raceErrors, raceProviderCallCount, raceFanout-raceProviderCallCount,
		)
	}
	winner := winners[0]
	loser := losers[0]
	var raceReservations int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.budget_reservations br
		JOIN video_pipeline.generation_runs gr ON gr.id = br.generation_run_id
		WHERE gr.budget_approval_id = $1 AND br.status = 'RESERVED'`,
		raceBudgetID.String(),
	).Scan(&raceReservations); err != nil {
		t.Fatal(err)
	}
	if raceReservations != raceProviderCallCount {
		t.Fatalf(
			"concurrent cumulative RESERVED rows = %d, want %d",
			raceReservations, raceProviderCallCount,
		)
	}
	var loserReservations, loserJobs, loserLedger int
	for _, loserIndex := range losers {
		if err := pool.QueryRow(ctx, `
			SELECT
			  (SELECT COUNT(*)
			   FROM video_pipeline.budget_reservations
			   WHERE generation_run_id = $1),
			  (SELECT COUNT(*)
			   FROM video_pipeline.provider_jobs pj
			   JOIN video_pipeline.generation_attempts ga
			     ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $1),
			  (SELECT COUNT(*)
			   FROM video_pipeline.cost_ledger cl
			   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
			   JOIN video_pipeline.generation_attempts ga
			     ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $1)`,
			raceRuns[loserIndex].RunID,
		).Scan(&loserReservations, &loserJobs, &loserLedger); err != nil {
			t.Fatal(err)
		}
		if loserReservations != 0 || loserJobs != 0 || loserLedger != 0 {
			t.Fatalf(
				"concurrent budget loser %d side effects = reservations:%d jobs:%d ledger:%d",
				loserIndex, loserReservations, loserJobs, loserLedger,
			)
		}
	}
	if err := store.RecordProviderCancellation(
		ctx, raceSteps[winner],
		orchestration.CancelProviderJobInput{
			Dispatch:   raceDispatches[winner],
			ReasonCode: "INTEGRATION_RELEASE",
			TraceID:    raceSteps[winner].TraceID,
		},
		orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
	); err != nil {
		t.Fatal(err)
	}
	winnerRunID := uuid.MustParse(raceRuns[winner].RunID)
	winnerJobID := uuid.NewSHA1(winnerRunID, []byte("provider-job"))
	winnerReservationID := uuid.NewSHA1(winnerRunID, []byte("budget-reservation"))
	winnerReleaseID := uuid.NewSHA1(
		winnerJobID, []byte("release:"+providerCancelledReleaseReason),
	)
	t.Run("exact cancellation release replay", func(t *testing.T) {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := releaseRunBudgetReservation(
			ctx, tx, winnerRunID, providerCancelledReleaseReason,
		); err != nil {
			t.Fatalf("exact cancellation release replay: %v", err)
		}
	})
	t.Run("released reservation rejects a different reason replay", func(t *testing.T) {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		err = releaseRunBudgetReservation(ctx, tx, winnerRunID, "different-release-reason")
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("different release reason replay error = %#v", err)
		}
	})
	if _, err := store.PrepareProviderJob(
		ctx, raceSteps[loser], raceDispatches[loser],
	); err != nil {
		t.Fatalf("reservation after release: %v", err)
	}
	replacementRunID := uuid.MustParse(raceRuns[loser].RunID)
	replacementJobID := uuid.NewSHA1(replacementRunID, []byte("provider-job"))
	replacementReservationID := uuid.NewSHA1(
		replacementRunID, []byte("budget-reservation"),
	)
	releaseProbe := losers[1]
	type releaseDriftCase struct {
		name    string
		mutate  func(t *testing.T)
		restore func(t *testing.T)
	}
	wrongReleaseID := uuid.New()
	extraReleaseID := uuid.New()
	extraAdjustmentID := uuid.New()
	releaseDrifts := []releaseDriftCase{
		{
			name: "missing RELEASE",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					INSERT INTO video_pipeline.cost_ledger
						(id, provider_job_id, budget_reservation_id, entry_type,
						 amount_micros, currency, pricing_rule_version, verified)
					VALUES ($1, $2, $3, 'RELEASE', 50, 'CNY', 'pricing-v1', true)`,
					winnerReleaseID, winnerJobID, winnerReservationID,
				)
			},
		},
		{
			name: "RELEASE deterministic ID",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET id = $2 WHERE id = $1`, winnerReleaseID, wrongReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET id = $2 WHERE id = $1`, wrongReleaseID, winnerReleaseID)
			},
		},
		{
			name: "RELEASE provider job",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET provider_job_id = $2 WHERE id = $1`, winnerReleaseID, replacementJobID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET provider_job_id = $2 WHERE id = $1`, winnerReleaseID, winnerJobID)
			},
		},
		{
			name: "RELEASE reservation",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET budget_reservation_id = $2 WHERE id = $1`, winnerReleaseID, replacementReservationID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET budget_reservation_id = $2 WHERE id = $1`, winnerReleaseID, winnerReservationID)
			},
		},
		{
			name: "RELEASE entry type",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET entry_type = 'ACTUAL' WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET entry_type = 'RELEASE' WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE amount",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 49 WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 50 WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE currency",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = 'USD' WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = 'CNY' WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE pricing",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET pricing_rule_version = 'drift-v1' WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET pricing_rule_version = 'pricing-v1' WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE verified",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = false WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = true WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE units",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET units = 1 WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET units = NULL WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "RELEASE unit name",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET unit_name = 'drift-units' WHERE id = $1`, winnerReleaseID)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET unit_name = NULL WHERE id = $1`, winnerReleaseID)
			},
		},
		{
			name: "duplicate RELEASE",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					INSERT INTO video_pipeline.cost_ledger
						(id, provider_job_id, budget_reservation_id, entry_type,
						 amount_micros, currency, pricing_rule_version, verified)
					VALUES ($1, $2, $3, 'RELEASE', 50, 'CNY', 'pricing-v1', true)`,
					extraReleaseID, winnerJobID, winnerReservationID,
				)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, extraReleaseID)
			},
		},
		{
			name: "unexpected ADJUSTMENT",
			mutate: func(t *testing.T) {
				mustExec(t, ctx, pool, `
					INSERT INTO video_pipeline.cost_ledger
						(id, provider_job_id, budget_reservation_id, entry_type,
						 amount_micros, currency, pricing_rule_version, verified)
					VALUES ($1, $2, $3, 'ADJUSTMENT', 1, 'CNY', 'pricing-v1', true)`,
					extraAdjustmentID, winnerJobID, winnerReservationID,
				)
			},
			restore: func(t *testing.T) {
				mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, extraAdjustmentID)
			},
		},
	}
	for _, releaseDrift := range releaseDrifts {
		releaseDrift := releaseDrift
		t.Run("cancelled RELEASE rejects "+releaseDrift.name+" drift", func(t *testing.T) {
			releaseDrift.mutate(t)
			t.Cleanup(func() { releaseDrift.restore(t) })
			replayErr := store.RecordProviderCancellation(
				ctx, raceSteps[winner],
				orchestration.CancelProviderJobInput{
					Dispatch:   raceDispatches[winner],
					ReasonCode: "INTEGRATION_RELEASE",
					TraceID:    raceSteps[winner].TraceID,
				},
				orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
			)
			var domain *controlplane.DomainError
			if !errors.As(replayErr, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("cancelled RELEASE %s replay error = %#v", releaseDrift.name, replayErr)
			}
			_, prepareErr := store.PrepareProviderJob(
				ctx, raceSteps[releaseProbe], raceDispatches[releaseProbe],
			)
			if !errors.As(prepareErr, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("cancelled RELEASE %s cumulative error = %#v", releaseDrift.name, prepareErr)
			}
			var reservations, jobs, ledger int
			if err := pool.QueryRow(ctx, `
				SELECT
				  (SELECT COUNT(*) FROM video_pipeline.budget_reservations WHERE generation_run_id = $1),
				  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1),
				  (SELECT COUNT(*) FROM video_pipeline.cost_ledger cl
				   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
				   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`,
				raceRuns[releaseProbe].RunID,
			).Scan(&reservations, &jobs, &ledger); err != nil {
				t.Fatal(err)
			}
			if reservations != 0 || jobs != 0 || ledger != 0 {
				t.Fatalf(
					"cancelled RELEASE %s candidate side effects = reservations:%d jobs:%d ledger:%d",
					releaseDrift.name, reservations, jobs, ledger,
				)
			}
		})
	}
	if _, err := store.PrepareProviderJob(
		ctx, raceSteps[releaseProbe], raceDispatches[releaseProbe],
	); err == nil {
		t.Fatal("exact released allocation permitted an over-budget candidate")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeBudgetExceeded || domain.Retryable {
			t.Fatalf("exact released allocation cumulative error = %#v", err)
		}
	}
	workflowFailedWinner := winners[1]
	if err := store.FinalizeShotRun(
		ctx, raceSteps[workflowFailedWinner],
		orchestration.FinalizeShotRunInput{
			OperationID:  uuid.NewString(),
			RunID:        raceRuns[workflowFailedWinner].RunID,
			State:        "FAILED",
			FailureClass: "INFRASTRUCTURE",
			FailureCode:  "INTEGRATION_WORKFLOW_FAILED",
			TraceID:      raceSteps[workflowFailedWinner].TraceID,
		},
	); err != nil {
		t.Fatalf("workflow-failed deterministic release: %v", err)
	}
	workflowFailedRunID := uuid.MustParse(raceRuns[workflowFailedWinner].RunID)
	workflowFailedJobID := uuid.NewSHA1(workflowFailedRunID, []byte("provider-job"))
	workflowFailedReleaseID := uuid.NewSHA1(
		workflowFailedJobID, []byte("release:"+workflowFailedReleaseReason),
	)
	var failedRunState, failedAttemptState, failedJobState, failedReservationState string
	var failedReleaseCount int
	if err := pool.QueryRow(ctx, `
		SELECT gr.state, ga.state, pj.state, br.status,
		       (SELECT COUNT(*)
		        FROM video_pipeline.cost_ledger
		        WHERE id = $2 AND provider_job_id = pj.id
		          AND budget_reservation_id = br.id
		          AND entry_type = 'RELEASE'
		          AND amount_micros = br.amount_micros
		          AND currency = br.currency
		          AND pricing_rule_version = br.pricing_rule_version
		          AND verified = true
		          AND units IS NULL AND unit_name IS NULL)
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
		JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
		WHERE gr.id = $1`,
		workflowFailedRunID, workflowFailedReleaseID,
	).Scan(
		&failedRunState, &failedAttemptState, &failedJobState,
		&failedReservationState, &failedReleaseCount,
	); err != nil {
		t.Fatal(err)
	}
	if failedRunState != "FAILED" || failedAttemptState != "FAILED" ||
		failedJobState != "FAILED" || failedReservationState != "RELEASED" ||
		failedReleaseCount != 1 {
		t.Fatalf(
			"workflow-failed release projection = run:%s attempt:%s job:%s reservation:%s release:%d",
			failedRunState, failedAttemptState, failedJobState,
			failedReservationState, failedReleaseCount,
		)
	}
	t.Run("exact workflow-failed release replay", func(t *testing.T) {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := releaseRunBudgetReservation(
			ctx, tx, workflowFailedRunID, workflowFailedReleaseReason,
		); err != nil {
			t.Fatalf("exact workflow-failed release replay: %v", err)
		}
	})
	if _, err := store.PrepareProviderJob(
		ctx, raceSteps[releaseProbe], raceDispatches[releaseProbe],
	); err != nil {
		t.Fatalf("reservation after workflow-failed release: %v", err)
	}
	afterWorkflowReleaseProbe := losers[2]
	if _, err := store.PrepareProviderJob(
		ctx, raceSteps[afterWorkflowReleaseProbe], raceDispatches[afterWorkflowReleaseProbe],
	); err == nil {
		t.Fatal("workflow-failed release permitted an over-budget candidate")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeBudgetExceeded || domain.Retryable {
			t.Fatalf("workflow-failed release cumulative error = %#v", err)
		}
	}
	overBudgetDigest := strings.Repeat(
		strings.ReplaceAll(uuid.NewString(), "-", ""),
		2,
	)
	overBudgetActual := int64(51)
	overBudgetResult := orchestration.ProviderResult{
		UpstreamTaskID: "over-budget-task", RequestID: "over-budget-request",
		ArtifactDigest: overBudgetDigest,
		ArtifactURI:    "cas://sha256/" + overBudgetDigest,
		MediaType:      "video/mp4", ArtifactSize: 1,
		Width: 1280, Height: 720, DurationMillis: 5_000,
		Model: raceDispatches[loser].Route,
		Cost: providercontract.Cost{
			EstimatedMicros: 50, ActualMicros: &overBudgetActual,
			Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
		},
	}
	if err := store.CompleteProviderJob(
		ctx, raceSteps[loser], raceDispatches[loser], overBudgetResult,
	); err == nil {
		t.Fatal("over-reservation Provider actual was accepted")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) ||
			domain.Code != controlplane.CodeBudgetExceeded {
			t.Fatalf("over-reservation completion error = %#v", err)
		}
	}
	var overBudgetRunState, overBudgetAttemptState, overBudgetJobState string
	var overBudgetArtifacts int
	if err := pool.QueryRow(ctx, `
		SELECT gr.state, ga.state, pj.state,
		       (SELECT COUNT(*) FROM video_pipeline.run_artifacts
		        WHERE generation_run_id = gr.id)
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
		WHERE gr.id = $1`,
		raceRuns[loser].RunID,
	).Scan(
		&overBudgetRunState, &overBudgetAttemptState,
		&overBudgetJobState, &overBudgetArtifacts,
	); err != nil {
		t.Fatal(err)
	}
	if overBudgetRunState != "FAILED" ||
		overBudgetAttemptState != "FAILED" ||
		overBudgetJobState != "FAILED" ||
		overBudgetArtifacts != 0 {
		t.Fatalf(
			"over-budget terminal projection = run:%s attempt:%s job:%s artifacts:%d",
			overBudgetRunState, overBudgetAttemptState,
			overBudgetJobState, overBudgetArtifacts,
		)
	}

	costPointer := func(value int64) *int64 {
		return &value
	}
	billingDriftCases := []struct {
		name               string
		cost               providercontract.Cost
		wantActual         bool
		wantActualVerified bool
		wantReleaseMicros  int64
		wantSecondRejected bool
	}{
		{
			name: "estimated exceeds reservation",
			cost: providercontract.Cost{
				EstimatedMicros: 51, ActualMicros: costPointer(40),
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
			wantActual:         true,
			wantActualVerified: true,
			wantReleaseMicros:  10,
		},
		{
			name: "actual exceeds reservation",
			cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: costPointer(51),
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
			wantActual:         true,
			wantActualVerified: true,
			wantSecondRejected: true,
		},
		{
			name: "actual is missing",
			cost: providercontract.Cost{
				EstimatedMicros: 40, ActualMicros: nil,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
			wantSecondRejected: true,
		},
		{
			name: "cost is unverified",
			cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: costPointer(40),
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: false,
			},
			wantActual:         true,
			wantSecondRejected: true,
		},
		{
			name: "currency differs from reservation",
			cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: costPointer(40),
				Currency: "USD", PricingVersion: "pricing-v1", Verified: true,
			},
			wantActual:         true,
			wantSecondRejected: true,
		},
		{
			name: "pricing version differs from reservation",
			cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: costPointer(40),
				Currency: "CNY", PricingVersion: "pricing-v2", Verified: true,
			},
			wantActual:         true,
			wantSecondRejected: true,
		},
	}
	for _, drift := range billingDriftCases {
		drift := drift
		t.Run("provider completion billing drift "+drift.name, func(t *testing.T) {
			_, driftCommand := cloneIntegrationShotCommand(
				t, ctx, pool, store, shotID.String(), publicCommand,
			)
			driftPlanCommand := planCommand
			driftPlanCommand.ShotSpecRevisionIDs = []string{
				driftCommand.ShotSpecRevisionID,
			}
			driftPlanCommand.BudgetLimit = controlplane.BudgetLimit{
				AmountMicros: 90, Currency: "CNY",
			}
			driftPlanCommand.ExecutionPolicy = driftCommand.ExecutionPolicy
			driftPlanDigest, err := digestValue(driftPlanCommand)
			if err != nil {
				t.Fatal(err)
			}
			driftPlan, err := store.CreateGenerationPlan(
				ctx, driftPlanCommand,
				controlplane.Idempotency{
					Scope: "completion-billing-drift-plan:" + uuid.NewString(),
					Key:   uuid.NewString(), RequestHash: driftPlanDigest,
				},
				"completion-billing-drift-plan",
			)
			if err != nil {
				t.Fatal(err)
			}
			driftBudgetID := uuid.New()
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.review_tasks
					(id, series_id, episode_id, review_type, state, assigned_role,
					 decided_at, generation_plan_id, budget_scope,
					 budget_limit_micros, budget_currency)
				VALUES ($1, $2, $3, 'BUDGET', 'APPROVED', 'PRODUCER',
				        now(), $4, 'VIDEO', 90, 'CNY')`,
				driftBudgetID, seriesID, episodeID, driftPlan.Value.GenerationPlanID,
			)
			driftCommand.GenerationPlanID = driftPlan.Value.GenerationPlanID
			driftCommand.BudgetApprovalID = driftBudgetID.String()
			_, driftRun, driftDispatch := createIntegrationWorkflowRun(
				t, ctx, store,
				"completion-billing-drift-"+strings.ReplaceAll(drift.name, " ", "-"),
				driftCommand,
			)
			driftCAS, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			driftArtifact, err := driftCAS.Put(
				ctx,
				bytes.NewReader([]byte("billing drift artifact: "+drift.name)),
			)
			if err != nil {
				t.Fatal(err)
			}
			var providerCalls atomic.Int32
			driftProvider := httptest.NewServer(http.HandlerFunc(func(
				response http.ResponseWriter,
				request *http.Request,
			) {
				providerCalls.Add(1)
				if request.Method != http.MethodPost ||
					request.URL.Path != "/v1/jobs" {
					http.NotFound(response, request)
					return
				}
				var job providercontract.JobRequest
				if err := json.NewDecoder(request.Body).Decode(&job); err != nil {
					http.Error(response, "invalid fixture request", http.StatusBadRequest)
					return
				}
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(response).Encode(providercontract.JobResponse{
					JobID: job.JobID, RunID: job.RunID,
					UpstreamTaskID: "billing-drift-task-" + driftArtifact.Digest[:12],
					RequestID:      "billing-drift-request-" + driftArtifact.Digest[:12],
					State:          providercontract.StatusSucceeded,
					Progress:       100,
					Model:          job.Model,
					Artifacts: []providercontract.AssetRef{{
						ID:       "billing-drift-asset-" + driftArtifact.Digest[:12],
						Revision: driftArtifact.Digest, Kind: providercontract.ModalityVideo,
						Role: providercontract.AssetRoleOutput,
						URI:  driftArtifact.URI, SHA256: driftArtifact.Digest,
						LicenseReference: "integration-fixture-license",
						MediaType:        "video/mp4", SizeBytes: driftArtifact.Size,
						Width: 1280, Height: 720, DurationMillis: 5_000,
					}},
					Usage: providercontract.Usage{
						InputUnits: 10, OutputUnits: 20, Unit: "mock-units",
					},
					Cost: drift.cost,
				})
			}))
			defer driftProvider.Close()

			activities := orchestration.NewProductionActivities(
				driftProvider.URL, nil, store, driftCAS,
			)
			var activitySuite testsuite.WorkflowTestSuite
			activityEnvironment := activitySuite.NewTestActivityEnvironment()
			activityEnvironment.RegisterActivity(activities.ExecuteProviderJob)
			_, activityErr := activityEnvironment.ExecuteActivity(
				activities.ExecuteProviderJob, driftDispatch,
			)
			var applicationErr *temporal.ApplicationError
			if !errors.As(activityErr, &applicationErr) ||
				applicationErr.Type() != string(controlplane.CodeBudgetExceeded) ||
				!applicationErr.NonRetryable() {
				t.Fatalf(
					"billing drift Activity error = %#v, want non-retryable %s",
					activityErr, controlplane.CodeBudgetExceeded,
				)
			}
			if providerCalls.Load() != 1 {
				t.Fatalf("billing drift Provider calls = %d, want 1", providerCalls.Load())
			}

			var (
				runState, runFailureClass, runFailureCode string
				attemptState, attemptFailureCode          string
				jobState, jobErrorCode                    string
				reservationState                          string
				reservedMicros                            int64
				actualCount, releaseCount                 int
				actualMicros, releaseMicros               int64
				actualCurrency, actualPricing             string
				actualVerified                            bool
				artifactCount, runArtifactCount           int
				qcCount, manifestCount, lockCount         int
			)
			if err := pool.QueryRow(ctx, `
				SELECT
				  gr.state, gr.failure_class, gr.failure_code,
				  ga.state, ga.failure_code,
				  pj.state, pj.error_code,
				  br.status, br.amount_micros,
				  COUNT(*) FILTER (WHERE cl.entry_type = 'ACTUAL'),
				  COALESCE(SUM(cl.amount_micros)
				    FILTER (WHERE cl.entry_type = 'ACTUAL'), 0),
				  COALESCE(MAX(cl.currency)
				    FILTER (WHERE cl.entry_type = 'ACTUAL'), ''),
				  COALESCE(MAX(cl.pricing_rule_version)
				    FILTER (WHERE cl.entry_type = 'ACTUAL'), ''),
				  COALESCE(BOOL_AND(cl.verified)
				    FILTER (WHERE cl.entry_type = 'ACTUAL'), false),
				  COUNT(*) FILTER (WHERE cl.entry_type = 'RELEASE'),
				  COALESCE(SUM(cl.amount_micros)
				    FILTER (WHERE cl.entry_type = 'RELEASE'), 0),
				  (SELECT COUNT(*) FROM video_pipeline.artifacts
				   WHERE content_hash = $2),
				  (SELECT COUNT(*) FROM video_pipeline.run_artifacts
				   WHERE generation_run_id = gr.id),
				  (SELECT COUNT(*) FROM video_pipeline.qc_reports
				   WHERE generation_run_id = gr.id),
				  (SELECT COUNT(*) FROM video_pipeline.generation_manifests
				   WHERE scope_revision_id = gr.shot_spec_revision_id),
				  (SELECT COUNT(*) FROM video_pipeline.publication_locks
				   WHERE generation_run_id = gr.id)
				FROM video_pipeline.generation_runs gr
				JOIN video_pipeline.generation_attempts ga
				  ON ga.generation_run_id = gr.id
				JOIN video_pipeline.provider_jobs pj
				  ON pj.generation_attempt_id = ga.id
				JOIN video_pipeline.budget_reservations br
				  ON br.id = pj.budget_reservation_id
				LEFT JOIN video_pipeline.cost_ledger cl
				  ON cl.budget_reservation_id = br.id
				WHERE gr.id = $1
				GROUP BY gr.id, ga.id, pj.id, br.id`,
				driftRun.RunID, driftArtifact.Digest,
			).Scan(
				&runState, &runFailureClass, &runFailureCode,
				&attemptState, &attemptFailureCode,
				&jobState, &jobErrorCode,
				&reservationState, &reservedMicros,
				&actualCount, &actualMicros, &actualCurrency,
				&actualPricing, &actualVerified,
				&releaseCount, &releaseMicros,
				&artifactCount, &runArtifactCount,
				&qcCount, &manifestCount, &lockCount,
			); err != nil {
				t.Fatal(err)
			}
			if runState != "FAILED" ||
				runFailureClass != "BUDGET" ||
				runFailureCode != "BUDGET_EXCEEDED" ||
				attemptState != "FAILED" ||
				attemptFailureCode != "BUDGET_EXCEEDED" ||
				jobState != "FAILED" ||
				jobErrorCode != "BUDGET_EXCEEDED" {
				t.Fatalf(
					"billing drift terminal projection = run:%s/%s/%s attempt:%s/%s job:%s/%s",
					runState, runFailureClass, runFailureCode,
					attemptState, attemptFailureCode, jobState, jobErrorCode,
				)
			}
			if reservationState != "SETTLED" || reservedMicros != 50 {
				t.Fatalf(
					"billing drift reservation = %s/%d, want SETTLED/50",
					reservationState, reservedMicros,
				)
			}
			var wantActual int64
			wantActualCount := 0
			wantActualCurrency := ""
			wantActualPricing := ""
			wantActualVerified := false
			if drift.wantActual {
				wantActualCount = 1
				wantActual = *drift.cost.ActualMicros
				wantActualCurrency = drift.cost.Currency
				wantActualPricing = drift.cost.PricingVersion
				wantActualVerified = drift.wantActualVerified
			}
			wantReleaseCount := 0
			if drift.wantReleaseMicros > 0 {
				wantReleaseCount = 1
			}
			if actualCount != wantActualCount ||
				actualMicros != wantActual ||
				actualCurrency != wantActualCurrency ||
				actualPricing != wantActualPricing ||
				actualVerified != wantActualVerified ||
				releaseCount != wantReleaseCount ||
				releaseMicros != drift.wantReleaseMicros {
				t.Fatalf(
					"billing drift ledger = actual:%d/%d/%s/%s/%t release:%d/%d, want actual:%d/%d/%s/%s/%t release:%d/%d",
					actualCount, actualMicros, actualCurrency, actualPricing,
					actualVerified, releaseCount, releaseMicros,
					wantActualCount, wantActual, wantActualCurrency,
					wantActualPricing, wantActualVerified,
					wantReleaseCount, drift.wantReleaseMicros,
				)
			}
			if artifactCount != 0 || runArtifactCount != 0 ||
				qcCount != 0 || manifestCount != 0 || lockCount != 0 {
				t.Fatalf(
					"billing drift downstream truth = artifact:%d run-artifact:%d qc:%d manifest:%d lock:%d",
					artifactCount, runArtifactCount, qcCount, manifestCount, lockCount,
				)
			}

			secondCommand := driftCommand
			secondCommand.CreativeAttempt = 2
			secondStep, secondRun, secondDispatch := createIntegrationWorkflowRun(
				t, ctx, store,
				"completion-billing-drift-second-"+
					strings.ReplaceAll(drift.name, " ", "-"),
				secondCommand,
			)
			if drift.name == "cost is unverified" {
				failedRunID := uuid.MustParse(driftRun.RunID)
				failedJobID := uuid.NewSHA1(failedRunID, []byte("provider-job"))
				failedActualID := uuid.NewSHA1(failedJobID, []byte("actual-cost"))
				usageDrifts := []struct {
					name    string
					mutate  func(*testing.T)
					restore func(*testing.T)
				}{
					{
						name: "units",
						mutate: func(t *testing.T) {
							mustExec(t, ctx, pool, `
								UPDATE video_pipeline.cost_ledger
								SET units = units + 1
								WHERE id = $1`,
								failedActualID,
							)
						},
						restore: func(t *testing.T) {
							mustExec(t, ctx, pool, `
								UPDATE video_pipeline.cost_ledger
								SET units = 30
								WHERE id = $1`,
								failedActualID,
							)
						},
					},
					{
						name: "unit name",
						mutate: func(t *testing.T) {
							mustExec(t, ctx, pool, `
								UPDATE video_pipeline.cost_ledger
								SET unit_name = 'mock-units-drift'
								WHERE id = $1`,
								failedActualID,
							)
						},
						restore: func(t *testing.T) {
							mustExec(t, ctx, pool, `
								UPDATE video_pipeline.cost_ledger
								SET unit_name = 'mock-units'
								WHERE id = $1`,
								failedActualID,
							)
						},
					},
				}
				for _, usageDrift := range usageDrifts {
					usageDrift := usageDrift
					t.Run("failed ACTUAL rejects "+usageDrift.name+" drift", func(t *testing.T) {
						usageDrift.mutate(t)
						t.Cleanup(func() { usageDrift.restore(t) })
						_, prepareErr := store.PrepareProviderJob(
							ctx, secondStep, secondDispatch,
						)
						var domain *controlplane.DomainError
						if !errors.As(prepareErr, &domain) ||
							domain.Code != controlplane.CodeRevisionConflict {
							t.Fatalf(
								"failed ACTUAL %s drift error = %#v, want %s",
								usageDrift.name, prepareErr, controlplane.CodeRevisionConflict,
							)
						}
						var reservations, jobs, ledger int
						if err := pool.QueryRow(ctx, `
							SELECT
							  (SELECT COUNT(*)
							   FROM video_pipeline.budget_reservations
							   WHERE generation_run_id = $1),
							  (SELECT COUNT(*)
							   FROM video_pipeline.provider_jobs pj
							   JOIN video_pipeline.generation_attempts ga
							     ON ga.id = pj.generation_attempt_id
							   WHERE ga.generation_run_id = $1),
							  (SELECT COUNT(*)
							   FROM video_pipeline.cost_ledger cl
							   JOIN video_pipeline.provider_jobs pj
							     ON pj.id = cl.provider_job_id
							   JOIN video_pipeline.generation_attempts ga
							     ON ga.id = pj.generation_attempt_id
							   WHERE ga.generation_run_id = $1)`,
							secondRun.RunID,
						).Scan(&reservations, &jobs, &ledger); err != nil {
							t.Fatal(err)
						}
						if reservations != 0 || jobs != 0 || ledger != 0 {
							t.Fatalf(
								"failed ACTUAL %s drift side effects = reservations:%d jobs:%d ledger:%d",
								usageDrift.name, reservations, jobs, ledger,
							)
						}
					})
				}
			}
			_, secondErr := store.PrepareProviderJob(
				ctx, secondStep, secondDispatch,
			)
			if drift.wantSecondRejected {
				var domain *controlplane.DomainError
				if !errors.As(secondErr, &domain) ||
					domain.Code != controlplane.CodeBudgetExceeded {
					t.Fatalf(
						"secondary allocation after %s = %#v, want %s",
						drift.name, secondErr, controlplane.CodeBudgetExceeded,
					)
				}
				var secondReservations, secondJobs int
				if err := pool.QueryRow(ctx, `
					SELECT
					  (SELECT COUNT(*)
					   FROM video_pipeline.budget_reservations
					   WHERE generation_run_id = $1),
					  (SELECT COUNT(*)
					   FROM video_pipeline.provider_jobs pj
					   JOIN video_pipeline.generation_attempts ga
					     ON ga.id = pj.generation_attempt_id
					   WHERE ga.generation_run_id = $1)`,
					secondRun.RunID,
				).Scan(&secondReservations, &secondJobs); err != nil {
					t.Fatal(err)
				}
				if secondReservations != 0 || secondJobs != 0 {
					t.Fatalf(
						"rejected secondary allocation side effects = reservations:%d jobs:%d",
						secondReservations, secondJobs,
					)
				}
			} else {
				if secondErr != nil {
					t.Fatalf(
						"trusted secondary allocation after %s: %v",
						drift.name, secondErr,
					)
				}
				if err := store.RecordProviderCancellation(
					ctx, secondStep,
					orchestration.CancelProviderJobInput{
						Dispatch:   secondDispatch,
						ReasonCode: "INTEGRATION_DRIFT_SECONDARY_CLEANUP",
						TraceID:    secondStep.TraceID,
					},
					orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
				); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	t.Run("public run persists canonical budget approval UUID", func(t *testing.T) {
		clonedShotID, command := cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		canonicalApproval := command.BudgetApprovalID
		command.BudgetApprovalID = strings.ReplaceAll(
			strings.ToUpper(command.BudgetApprovalID), "-", "",
		)
		requestHash, err := digestValue(command)
		if err != nil {
			t.Fatal(err)
		}
		created, err := store.CreateGenerationRun(
			ctx, clonedShotID, 1, command,
			controlplane.Idempotency{
				Scope: "canonical-public-run:" + clonedShotID,
				Key:   uuid.NewString(), RequestHash: requestHash,
			},
			"canonical-public-run",
		)
		if err != nil {
			t.Fatal(err)
		}
		var persistedApproval string
		if err := pool.QueryRow(ctx, `
			SELECT budget_approval_id
			FROM video_pipeline.generation_runs
			WHERE id = $1`,
			created.Value.AggregateID,
		).Scan(&persistedApproval); err != nil {
			t.Fatal(err)
		}
		if persistedApproval != canonicalApproval {
			t.Fatalf("persisted approval = %q, want %q", persistedApproval, canonicalApproval)
		}
	})

	t.Run("confirmed cancellation settles actual cost and releases only unused budget", func(t *testing.T) {
		_, command := cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		cancelStep, cancelRun, cancelDispatch := createIntegrationWorkflowRun(
			t, ctx, store, "cancel-with-actual-cost", command,
		)
		if _, err := store.PrepareProviderJob(ctx, cancelStep, cancelDispatch); err != nil {
			t.Fatal(err)
		}
		actualMicros := int64(20)
		cancelResult := orchestration.CancelProviderResult{
			State: "CANCELLED", UpstreamTaskID: "cancel-cost-task",
			RequestID: "cancel-cost-request",
			Usage: providercontract.Usage{
				InputUnits: 2, OutputUnits: 3, Unit: "mock-units",
			},
			Cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: &actualMicros,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
		}
		cancelInput := orchestration.CancelProviderJobInput{
			Dispatch: cancelDispatch, ReasonCode: "INTEGRATION_CANCEL_WITH_COST",
			TraceID: cancelStep.TraceID,
		}
		if err := store.RecordProviderJobObservation(
			ctx, cancelStep, cancelDispatch,
			orchestration.ProviderJobObservation{
				State: "RUNNING", UpstreamTaskID: cancelResult.UpstreamTaskID,
				RequestID: cancelResult.RequestID,
			},
		); err != nil {
			t.Fatal(err)
		}
		identityDrift := cancelResult
		identityDrift.UpstreamTaskID = "different-cancel-cost-task"
		if err := store.RecordProviderCancellation(
			ctx, cancelStep, cancelInput, identityDrift,
		); err == nil {
			t.Fatal("costed cancellation accepted a different upstream task")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("costed cancellation identity drift = %#v", err)
			}
		}
		var preRunState, preAttemptState, preJobState, preReservationState string
		var preTerminalLedger int
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, ga.state, pj.state, br.status,
			       COUNT(*) FILTER (WHERE cl.entry_type IN ('ACTUAL', 'RELEASE'))
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl ON cl.provider_job_id = pj.id
			WHERE gr.id = $1
			GROUP BY gr.state, ga.state, pj.state, br.status`,
			cancelRun.RunID,
		).Scan(
			&preRunState, &preAttemptState, &preJobState, &preReservationState,
			&preTerminalLedger,
		); err != nil {
			t.Fatal(err)
		}
		if preRunState != "RUNNING" || preAttemptState != "RUNNING" ||
			preJobState != "RUNNING" || preReservationState != "RESERVED" ||
			preTerminalLedger != 0 {
			t.Fatalf(
				"cancellation identity drift side effects = run:%s attempt:%s job:%s reservation:%s terminalLedger:%d",
				preRunState, preAttemptState, preJobState, preReservationState,
				preTerminalLedger,
			)
		}
		if err := store.RecordProviderCancellation(
			ctx, cancelStep, cancelInput, cancelResult,
		); err != nil {
			t.Fatal(err)
		}
		var runState, attemptState, jobState, reservationState string
		var actualCount, releaseCount int
		var actualAmount, releaseAmount int64
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, ga.state, pj.state, br.status,
			       COUNT(*) FILTER (WHERE cl.entry_type = 'ACTUAL'),
			       COUNT(*) FILTER (WHERE cl.entry_type = 'RELEASE'),
			       COALESCE(SUM(cl.amount_micros) FILTER (WHERE cl.entry_type = 'ACTUAL'), 0),
			       COALESCE(SUM(cl.amount_micros) FILTER (WHERE cl.entry_type = 'RELEASE'), 0)
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl
			  ON cl.provider_job_id = pj.id AND cl.entry_type IN ('ACTUAL', 'RELEASE')
			WHERE gr.id = $1
			GROUP BY gr.state, ga.state, pj.state, br.status`,
			cancelRun.RunID,
		).Scan(
			&runState, &attemptState, &jobState, &reservationState,
			&actualCount, &releaseCount, &actualAmount, &releaseAmount,
		); err != nil {
			t.Fatal(err)
		}
		if runState != "CANCELLED" || attemptState != "CANCELLED" ||
			jobState != "CANCELLED" || reservationState != "SETTLED" ||
			actualCount != 1 || releaseCount != 1 ||
			actualAmount != 20 || releaseAmount != 30 {
			t.Fatalf(
				"costed cancellation = run:%s attempt:%s job:%s reservation:%s actual:%d/%d release:%d/%d",
				runState, attemptState, jobState, reservationState,
				actualCount, actualAmount, releaseCount, releaseAmount,
			)
		}
		if err := store.RecordProviderCancellation(
			ctx, cancelStep, cancelInput, cancelResult,
		); err != nil {
			t.Fatalf("exact costed cancellation replay: %v", err)
		}
		missingIdentity := cancelResult
		missingIdentity.UpstreamTaskID = ""
		if err := store.RecordProviderCancellation(
			ctx, cancelStep, cancelInput, missingIdentity,
		); err == nil {
			t.Fatal("costed cancellation replay accepted a missing upstream task")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("missing cancellation identity replay = %#v", err)
			}
		}
		drifted := cancelResult
		driftedActual := int64(19)
		drifted.Cost.ActualMicros = &driftedActual
		if err := store.RecordProviderCancellation(
			ctx, cancelStep, cancelInput, drifted,
		); err == nil {
			t.Fatal("costed cancellation replay accepted changed actual cost")
		}
	})

	t.Run("confirmed Provider failure settles cost without release escape", func(t *testing.T) {
		_, command := cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		failureStep, failureRun, failureDispatch := createIntegrationWorkflowRun(
			t, ctx, store, "provider-failed-with-cost", command,
		)
		if _, err := store.PrepareProviderJob(ctx, failureStep, failureDispatch); err != nil {
			t.Fatal(err)
		}
		actualMicros := int64(12)
		failureResult := orchestration.CancelProviderResult{
			State: "FAILED", UpstreamTaskID: "failed-cost-task",
			RequestID: "failed-cost-request", ErrorCode: "model_unavailable",
			Usage: providercontract.Usage{
				InputUnits: 1, OutputUnits: 2, Unit: "mock-units",
			},
			Cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: &actualMicros,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
		}
		failureInput := orchestration.CancelProviderJobInput{
			Dispatch: failureDispatch, ReasonCode: "RECONCILE_HISTORY",
			TraceID: failureStep.TraceID,
		}
		if err := store.RecordProviderJobObservation(
			ctx, failureStep, failureDispatch,
			orchestration.ProviderJobObservation{
				State: "RUNNING", UpstreamTaskID: failureResult.UpstreamTaskID,
				RequestID: failureResult.RequestID,
			},
		); err != nil {
			t.Fatal(err)
		}
		identityDrift := failureResult
		identityDrift.RequestID = "different-failed-cost-request"
		if err := store.RecordProviderCancellation(
			ctx, failureStep, failureInput, identityDrift,
		); err == nil {
			t.Fatal("Provider failure accepted a different upstream request")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("Provider failure identity drift = %#v", err)
			}
		}
		var preRunState, preAttemptState, preJobState, preReservationState string
		var preTerminalLedger int
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, ga.state, pj.state, br.status,
			       COUNT(*) FILTER (WHERE cl.entry_type IN ('ACTUAL', 'RELEASE'))
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl ON cl.provider_job_id = pj.id
			WHERE gr.id = $1
			GROUP BY gr.state, ga.state, pj.state, br.status`,
			failureRun.RunID,
		).Scan(
			&preRunState, &preAttemptState, &preJobState, &preReservationState,
			&preTerminalLedger,
		); err != nil {
			t.Fatal(err)
		}
		if preRunState != "RUNNING" || preAttemptState != "RUNNING" ||
			preJobState != "RUNNING" || preReservationState != "RESERVED" ||
			preTerminalLedger != 0 {
			t.Fatalf(
				"failure identity drift side effects = run:%s attempt:%s job:%s reservation:%s terminalLedger:%d",
				preRunState, preAttemptState, preJobState, preReservationState,
				preTerminalLedger,
			)
		}
		if err := store.RecordProviderCancellation(
			ctx, failureStep, failureInput, failureResult,
		); err != nil {
			t.Fatal(err)
		}
		var runState, failureClass, jobState, reservationState string
		var actualAmount, releaseAmount int64
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, gr.failure_class, pj.state, br.status,
			       COALESCE(SUM(cl.amount_micros) FILTER (WHERE cl.entry_type = 'ACTUAL'), 0),
			       COALESCE(SUM(cl.amount_micros) FILTER (WHERE cl.entry_type = 'RELEASE'), 0)
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl
			  ON cl.provider_job_id = pj.id AND cl.entry_type IN ('ACTUAL', 'RELEASE')
			WHERE gr.id = $1
			GROUP BY gr.state, gr.failure_class, pj.state, br.status`,
			failureRun.RunID,
		).Scan(
			&runState, &failureClass, &jobState, &reservationState,
			&actualAmount, &releaseAmount,
		); err != nil {
			t.Fatal(err)
		}
		if runState != "FAILED" || failureClass != "TRANSIENT" ||
			jobState != "FAILED" || reservationState != "SETTLED" ||
			actualAmount != 12 || releaseAmount != 38 {
			t.Fatalf(
				"failed Provider settlement = run:%s class:%s job:%s reservation:%s actual:%d release:%d",
				runState, failureClass, jobState, reservationState,
				actualAmount, releaseAmount,
			)
		}
		if err := store.RecordProviderCancellation(
			ctx, failureStep, failureInput, failureResult,
		); err != nil {
			t.Fatalf("exact Provider failure replay: %v", err)
		}
		missingIdentity := failureResult
		missingIdentity.RequestID = ""
		if err := store.RecordProviderCancellation(
			ctx, failureStep, failureInput, missingIdentity,
		); err == nil {
			t.Fatal("Provider failure replay accepted a missing upstream request")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("missing Provider failure identity replay = %#v", err)
			}
		}
	})

	t.Run("ambiguous submit persists recover-only state and stable identities", func(t *testing.T) {
		_, command := cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		observationStep, observationRun, observationDispatch := createIntegrationWorkflowRun(
			t, ctx, store, "ambiguous-submit-observation", command,
		)
		prepared, err := store.PrepareProviderJob(
			ctx, observationStep, observationDispatch,
		)
		if err != nil {
			t.Fatal(err)
		}
		if prepared.ReconcileOnly {
			t.Fatal("new Provider allocation started recover-only")
		}
		observationDispatch.BudgetApprovalID = strings.ToUpper(
			observationDispatch.BudgetApprovalID,
		)
		if err := store.RecordProviderJobObservation(
			ctx, observationStep, observationDispatch,
			orchestration.ProviderJobObservation{
				State: "UNKNOWN", ErrorCode: "PROVIDER_SUBMISSION_PENDING",
			},
		); err != nil {
			t.Fatal(err)
		}
		replayed, err := store.PrepareProviderJob(
			ctx, observationStep, observationDispatch,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !replayed.ReconcileOnly {
			t.Fatal("ambiguous paid boundary allowed another Provider submit")
		}
		if err := store.RecordProviderJobObservation(
			ctx, observationStep, observationDispatch,
			orchestration.ProviderJobObservation{
				State: "RUNNING", UpstreamTaskID: "stable-observation-task",
				RequestID: "stable-observation-request",
			},
		); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordProviderJobObservation(
			ctx, observationStep, observationDispatch,
			orchestration.ProviderJobObservation{
				State: "UNKNOWN", ErrorCode: "PROVIDER_POLL_UNKNOWN",
			},
		); err != nil {
			t.Fatal(err)
		}
		var runState, attemptState, jobState, reservationState string
		var upstreamTaskID, requestID string
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, ga.state, pj.state, br.status,
			       pj.upstream_task_id, pj.upstream_request_id
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			WHERE gr.id = $1`,
			observationRun.RunID,
		).Scan(
			&runState, &attemptState, &jobState, &reservationState,
			&upstreamTaskID, &requestID,
		); err != nil {
			t.Fatal(err)
		}
		if runState != "RECONCILING" || attemptState != "RECONCILING" ||
			jobState != "UNKNOWN" || reservationState != "RESERVED" ||
			upstreamTaskID != "stable-observation-task" ||
			requestID != "stable-observation-request" {
			t.Fatalf(
				"ambiguous observation = run:%s attempt:%s job:%s reservation:%s task:%s request:%s",
				runState, attemptState, jobState, reservationState,
				upstreamTaskID, requestID,
			)
		}
		if err := store.RecordProviderCancellation(
			ctx, observationStep,
			orchestration.CancelProviderJobInput{
				Dispatch:   observationDispatch,
				ReasonCode: "RECONCILE_HISTORY",
				TraceID:    observationStep.TraceID,
			},
			orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
		); err == nil {
			t.Fatal("adapter absence released a durable upstream task")
		}
		var protectedRunState, protectedAttemptState, protectedJobState string
		var protectedReservationState, protectedTaskID, protectedRequestID string
		var protectedTerminalLedger int
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, ga.state, pj.state, br.status,
			       pj.upstream_task_id, pj.upstream_request_id,
			       COUNT(*) FILTER (WHERE cl.entry_type IN ('ACTUAL', 'RELEASE'))
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl ON cl.provider_job_id = pj.id
			WHERE gr.id = $1
			GROUP BY gr.state, ga.state, pj.state, br.status,
			         pj.upstream_task_id, pj.upstream_request_id`,
			observationRun.RunID,
		).Scan(
			&protectedRunState, &protectedAttemptState, &protectedJobState,
			&protectedReservationState, &protectedTaskID, &protectedRequestID,
			&protectedTerminalLedger,
		); err != nil {
			t.Fatal(err)
		}
		if protectedRunState != "RECONCILING" || protectedAttemptState != "UNKNOWN" ||
			protectedJobState != "UNKNOWN" || protectedReservationState != "RESERVED" ||
			protectedTaskID != "stable-observation-task" ||
			protectedRequestID != "stable-observation-request" ||
			protectedTerminalLedger != 0 {
			t.Fatalf(
				"known upstream after adapter absence = run:%s attempt:%s job:%s reservation:%s task:%s request:%s terminalLedger:%d",
				protectedRunState, protectedAttemptState, protectedJobState,
				protectedReservationState, protectedTaskID, protectedRequestID,
				protectedTerminalLedger,
			)
		}
		reconcileReplay, err := store.PrepareProviderJob(
			ctx, observationStep, observationDispatch,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !reconcileReplay.ReconcileOnly {
			t.Fatal("adapter absence reopened paid Provider submission")
		}
	})

	_, cancelRaceCommand := cloneIntegrationShotCommand(
		t, ctx, pool, store, shotID.String(), publicCommand,
	)
	cancelRaceStep, cancelRaceRun, cancelRaceDispatch :=
		createIntegrationWorkflowRun(
			t, ctx, store, "cancel-first", cancelRaceCommand,
		)
	if _, err := store.PrepareProviderJob(
		ctx, cancelRaceStep, cancelRaceDispatch,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProviderCancellation(
		ctx, cancelRaceStep,
		orchestration.CancelProviderJobInput{
			Dispatch:   cancelRaceDispatch,
			ReasonCode: "INTEGRATION_CANCEL_FIRST",
			TraceID:    cancelRaceStep.TraceID,
		},
		orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
	); err != nil {
		t.Fatal(err)
	}
	cancelRaceDigest := strings.Repeat(
		strings.ReplaceAll(uuid.NewString(), "-", ""),
		2,
	)
	cancelRaceActual := int64(40)
	cancelRaceResult := orchestration.ProviderResult{
		UpstreamTaskID: "cancel-race-task", RequestID: "cancel-race-request",
		ArtifactDigest: cancelRaceDigest,
		ArtifactURI:    "cas://sha256/" + cancelRaceDigest,
		MediaType:      "video/mp4", ArtifactSize: 1,
		Width: 1280, Height: 720, DurationMillis: 5_000,
		Model: cancelRaceDispatch.Route,
		Cost: providercontract.Cost{
			EstimatedMicros: 50, ActualMicros: &cancelRaceActual,
			Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
		},
	}
	if err := store.CompleteProviderJob(
		ctx, cancelRaceStep, cancelRaceDispatch, cancelRaceResult,
	); err == nil {
		t.Fatal("cancel-first run accepted a late Provider success")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeConflict {
			t.Fatalf("cancel-first late success error = %#v", err)
		}
	}
	var cancelRunState, cancelAttemptState, cancelJobState, cancelReservationState string
	var cancelArtifacts int
	if err := pool.QueryRow(ctx, `
		SELECT gr.state, ga.state, pj.state, br.status,
		       (SELECT COUNT(*) FROM video_pipeline.run_artifacts
		        WHERE generation_run_id = gr.id)
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
		JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
		WHERE gr.id = $1`,
		cancelRaceRun.RunID,
	).Scan(
		&cancelRunState, &cancelAttemptState, &cancelJobState,
		&cancelReservationState, &cancelArtifacts,
	); err != nil {
		t.Fatal(err)
	}
	if cancelRunState != "CANCELLED" ||
		cancelAttemptState != "CANCELLED" ||
		cancelJobState != "CANCELLED" ||
		cancelReservationState != "RELEASED" ||
		cancelArtifacts != 0 {
		t.Fatalf(
			"cancel-first terminal projection = run:%s attempt:%s job:%s reservation:%s artifacts:%d",
			cancelRunState, cancelAttemptState, cancelJobState,
			cancelReservationState, cancelArtifacts,
		)
	}

	_, artifactConflictCommand := cloneIntegrationShotCommand(
		t, ctx, pool, store, shotID.String(), publicCommand,
	)
	artifactConflictStep, artifactConflictRun, artifactConflictDispatch :=
		createIntegrationWorkflowRun(
			t, ctx, store, "artifact-conflict", artifactConflictCommand,
		)
	if _, err := store.PrepareProviderJob(
		ctx, artifactConflictStep, artifactConflictDispatch,
	); err != nil {
		t.Fatal(err)
	}
	artifactConflictDigest := strings.Repeat(
		strings.ReplaceAll(uuid.NewString(), "-", ""),
		2,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES ($1, $2, $3, 'application/octet-stream', 99, '{}', 'ACTIVE')`,
		uuid.New(), artifactConflictDigest,
		"cas://sha256/"+artifactConflictDigest,
	)
	artifactConflictActual := int64(40)
	artifactConflictResult := orchestration.ProviderResult{
		UpstreamTaskID: "artifact-conflict-task",
		RequestID:      "artifact-conflict-request",
		ArtifactDigest: artifactConflictDigest,
		ArtifactURI:    "cas://sha256/" + artifactConflictDigest,
		MediaType:      "video/mp4", ArtifactSize: 1,
		Width: 1280, Height: 720, DurationMillis: 5_000,
		Model: artifactConflictDispatch.Route,
		Cost: providercontract.Cost{
			EstimatedMicros: 50, ActualMicros: &artifactConflictActual,
			Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
		},
	}
	if err := store.CompleteProviderJob(
		ctx, artifactConflictStep, artifactConflictDispatch, artifactConflictResult,
	); err == nil {
		t.Fatal("incompatible artifact metadata was reused")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeConflict {
			t.Fatalf("artifact metadata conflict error = %#v", err)
		}
	}
	var conflictRunState, conflictReservationState string
	var conflictRunArtifacts int
	if err := pool.QueryRow(ctx, `
		SELECT gr.state, br.status,
		       (SELECT COUNT(*) FROM video_pipeline.run_artifacts
		        WHERE generation_run_id = gr.id)
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id = gr.id
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
		JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
		WHERE gr.id = $1`,
		artifactConflictRun.RunID,
	).Scan(
		&conflictRunState, &conflictReservationState, &conflictRunArtifacts,
	); err != nil {
		t.Fatal(err)
	}
	if conflictRunState != "QUEUED" ||
		conflictReservationState != "RESERVED" ||
		conflictRunArtifacts != 0 {
		t.Fatalf(
			"artifact conflict projection = run:%s reservation:%s artifacts:%d",
			conflictRunState, conflictReservationState, conflictRunArtifacts,
		)
	}
	if err := store.RecordProviderCancellation(
		ctx, artifactConflictStep,
		orchestration.CancelProviderJobInput{
			Dispatch:   artifactConflictDispatch,
			ReasonCode: "INTEGRATION_CONFLICT_CLEANUP",
			TraceID:    artifactConflictStep.TraceID,
		},
		orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
	); err != nil {
		t.Fatal(err)
	}
	var videoProviderCalls atomic.Int32
	blockedProvider := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		_ *http.Request,
	) {
		videoProviderCalls.Add(1)
		http.Error(response, "VIDEO budget boundary called provider", http.StatusInternalServerError)
	}))
	defer blockedProvider.Close()
	videoActivities := orchestration.NewProductionActivities(
		blockedProvider.URL, nil, store, nil,
	)
	var videoActivitySuite testsuite.WorkflowTestSuite
	videoActivityEnvironment := videoActivitySuite.NewTestActivityEnvironment()
	videoActivityEnvironment.RegisterActivity(videoActivities.ExecuteProviderJob)
	restoreVideoApproval := func() {
		t.Helper()
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.review_tasks
			SET state = 'APPROVED',
			    decided_at = now(),
			    generation_plan_id = $2,
			    budget_scope = 'VIDEO',
			    budget_limit_micros = 1000,
			    budget_currency = 'CNY'
			WHERE id = $1`,
			budgetID, plan.Value.GenerationPlanID,
		)
	}
	restorePaidBoundaryTruth := func() {
		t.Helper()
		restoreVideoApproval()
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.consent_assets
			SET status = 'ACTIVE', expires_at = now() + interval '1 hour'
			WHERE id = $1`, consentID)
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.license_snapshots
			SET policy_status = 'ALLOWED', expires_at = now() + interval '1 hour'
			WHERE id = $1`, voiceLicenseID)
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.shot_spec_revisions
			SET freshness = 'FRESH'
			WHERE id = $1`, shotRevisionID)
	}
	paidTruthRegressions := []struct {
		name   string
		code   controlplane.ErrorCode
		mutate func()
	}{
		{
			name: "consent revoked after run creation", code: controlplane.CodeConsentRequired,
			mutate: func() {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.consent_assets SET status = 'REVOKED' WHERE id = $1`, consentID)
			},
		},
		{
			name: "license expired after run creation", code: controlplane.CodeLicenseBlocked,
			mutate: func() {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.license_snapshots SET expires_at = now() - interval '1 second' WHERE id = $1`, voiceLicenseID)
			},
		},
		{
			name: "shot freshness drift after run creation", code: controlplane.CodeGateRequired,
			mutate: func() {
				mustExec(t, ctx, pool, `UPDATE video_pipeline.shot_spec_revisions SET freshness = 'STALE' WHERE id = $1`, shotRevisionID)
			},
		},
	}
	for _, regression := range paidTruthRegressions {
		regression := regression
		t.Run("provider submit product truth "+regression.name, func(t *testing.T) {
			restorePaidBoundaryTruth()
			regression.mutate()
			_, activityErr := videoActivityEnvironment.ExecuteActivity(
				videoActivities.ExecuteProviderJob, dispatch,
			)
			var applicationErr *temporal.ApplicationError
			if !errors.As(activityErr, &applicationErr) ||
				applicationErr.Type() != string(regression.code) ||
				!applicationErr.NonRetryable() {
				t.Fatalf("paid-boundary Activity error = %#v, want non-retryable %s", activityErr, regression.code)
			}
			var reservations, jobs int
			if err := pool.QueryRow(ctx, `
				SELECT
				  (SELECT COUNT(*) FROM video_pipeline.budget_reservations WHERE generation_run_id = $1),
				  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`,
				run.RunID,
			).Scan(&reservations, &jobs); err != nil {
				t.Fatal(err)
			}
			if reservations != 0 || jobs != 0 || videoProviderCalls.Load() != 0 {
				t.Fatalf("blocked paid-boundary side effects = reservations:%d jobs:%d provider:%d", reservations, jobs, videoProviderCalls.Load())
			}
		})
	}
	restorePaidBoundaryTruth()
	videoBudgetRegressions := []struct {
		name        string
		mutate      func()
		mutateInput func(*orchestration.ExecuteProviderJobInput)
	}{
		{
			name: "legacy NULL approval after run creation",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET generation_plan_id = NULL, budget_scope = NULL,
					    budget_limit_micros = NULL, budget_currency = NULL
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "approval revoked after run creation",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET state = 'CANCELLED', decided_at = now()
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "approval rebound to different same-budget plan",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET generation_plan_id = $2
					WHERE id = $1`,
					budgetID, alternateVideoPlan.Value.GenerationPlanID,
				)
			},
		},
		{
			name: "approval scope changed",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_scope = 'SPEECH'
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "approval amount below plan",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_limit_micros = 999
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "approval amount above plan",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_limit_micros = 1001
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "approval currency changed",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_currency = 'USD'
					WHERE id = $1`,
					budgetID,
				)
			},
		},
		{
			name: "dispatch amount below plan",
			mutateInput: func(input *orchestration.ExecuteProviderJobInput) {
				input.BudgetMaximumMicros = 999
			},
		},
		{
			name: "dispatch amount above plan",
			mutateInput: func(input *orchestration.ExecuteProviderJobInput) {
				input.BudgetMaximumMicros = 1001
			},
		},
		{
			name: "dispatch currency differs from plan",
			mutateInput: func(input *orchestration.ExecuteProviderJobInput) {
				input.BudgetCurrency = "USD"
			},
		},
		{
			name: "dispatch approval differs from persisted run",
			mutateInput: func(input *orchestration.ExecuteProviderJobInput) {
				input.BudgetApprovalID = speechBudgetID.String()
			},
		},
	}
	for _, regression := range videoBudgetRegressions {
		regression := regression
		t.Run("provider submit budget "+regression.name, func(t *testing.T) {
			restoreVideoApproval()
			if regression.mutate != nil {
				regression.mutate()
			}
			blockedDispatch := dispatch
			if regression.mutateInput != nil {
				regression.mutateInput(&blockedDispatch)
			}
			_, activityErr := videoActivityEnvironment.ExecuteActivity(
				videoActivities.ExecuteProviderJob,
				blockedDispatch,
			)
			var applicationErr *temporal.ApplicationError
			if !errors.As(activityErr, &applicationErr) ||
				applicationErr.Type() != string(controlplane.CodeBudgetExceeded) ||
				!applicationErr.NonRetryable() {
				t.Fatalf(
					"VIDEO budget Activity error = %#v, want non-retryable %s",
					activityErr,
					controlplane.CodeBudgetExceeded,
				)
			}
			var reservations, jobs int
			if err := pool.QueryRow(ctx, `
				SELECT
				  (SELECT COUNT(*)
				   FROM video_pipeline.budget_reservations
				   WHERE generation_run_id = $1),
				  (SELECT COUNT(*)
				   FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`,
				run.RunID,
			).Scan(&reservations, &jobs); err != nil {
				t.Fatal(err)
			}
			if reservations != 0 || jobs != 0 || videoProviderCalls.Load() != 0 {
				t.Fatalf(
					"blocked VIDEO submit side effects = reservations:%d jobs:%d provider:%d",
					reservations,
					jobs,
					videoProviderCalls.Load(),
				)
			}
		})
	}
	restoreVideoApproval()
	mustExec(t, ctx, pool, `
		DELETE FROM video_pipeline.prompt_snapshot_inputs
		WHERE prompt_snapshot_id = $1
		  AND dependency_role = 'context:shot'`,
		prompt.ID,
	)
	if err := store.ValidateWorkerUpgradeReadiness(ctx); err == nil {
		t.Fatal("v6 worker accepted an active run with incomplete Prompt lineage")
	}
	if _, err := store.CompilePromptSnapshot(
		ctx,
		orchestration.WorkflowStep{
			WorkflowID: "workflow-upgrade-recompile-" + uuid.NewString(),
			ActivityID: "compile", ActivityType: orchestration.ActivityCompilePrompt,
			TraceID: "workflow-upgrade-recompile",
		},
		orchestration.CompilePromptInput{
			ShotSpecRevisionID:   shotRevisionID.String(),
			GenerationProfileRef: profileID.String(),
			TraceID:              "workflow-upgrade-recompile", PersistProductTruth: true,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateWorkerUpgradeReadiness(ctx); err != nil {
		t.Fatalf("v6 worker rejected restored Prompt lineage: %v", err)
	}
	step.ActivityID, step.ActivityType = "provider", orchestration.ActivityExecuteProviderJob
	preparedProvider, err := store.PrepareProviderJob(ctx, step, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if preparedProvider.Budget.MaxCostMicros != 50 ||
		preparedProvider.Budget.EstimatedCostMicros != 50 ||
		preparedProvider.BudgetReservation.AmountMicros != 50 ||
		preparedProvider.BudgetReservation.Currency != "CNY" ||
		preparedProvider.BudgetReservation.PricingVersion != "pricing-v1" {
		t.Fatalf("per-run durable Provider allocation = %#v", preparedProvider)
	}
	var preparedRequestSnapshot []byte
	if err := pool.QueryRow(ctx, `
		SELECT pj.request_snapshot
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = $1`,
		run.RunID,
	).Scan(&preparedRequestSnapshot); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.provider_jobs pj
		SET request_snapshot = '{"legacy":"v5"}'::jsonb
		FROM video_pipeline.generation_attempts ga
		WHERE pj.generation_attempt_id = ga.id
		  AND ga.generation_run_id = $1`,
		run.RunID,
	)
	if err := store.ValidateWorkerUpgradeReadiness(ctx); err == nil {
		t.Fatal("v6 worker accepted an active v5 Provider request snapshot")
	}
	if _, err := store.PrepareProviderJob(ctx, step, dispatch); err == nil {
		t.Fatal("Provider retry accepted a drifted prepared request snapshot")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) ||
			domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("drifted prepared request retry error = %#v", err)
		}
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.provider_jobs pj
		SET request_snapshot = $2
		FROM video_pipeline.generation_attempts ga
		WHERE pj.generation_attempt_id = ga.id
		  AND ga.generation_run_id = $1`,
		run.RunID, preparedRequestSnapshot,
	)
	if err := store.ValidateWorkerUpgradeReadiness(ctx); err != nil {
		t.Fatalf("v6 worker rejected restored Provider reservation lineage: %v", err)
	}
	replayedPreparedProvider, err := store.PrepareProviderJob(ctx, step, dispatch)
	if err != nil {
		t.Fatalf("exact prepared Provider job replay failed: %v", err)
	}
	if !reflect.DeepEqual(replayedPreparedProvider, preparedProvider) {
		t.Fatal("exact prepared Provider job replay changed the durable allocation")
	}
	t.Run("RESERVED allocation rejects unexpected ledger entry", func(t *testing.T) {
		runUUID := uuid.MustParse(run.RunID)
		jobID := uuid.NewSHA1(runUUID, []byte("provider-job"))
		reservationID := uuid.NewSHA1(runUUID, []byte("budget-reservation"))
		extraID := uuid.New()
		mustExec(t, ctx, pool, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type,
				 amount_micros, currency, pricing_rule_version, verified)
			VALUES ($1, $2, $3, 'ADJUSTMENT', 1, 'CNY', 'pricing-v1', true)`,
			extraID, jobID, reservationID,
		)
		if _, err := store.PrepareProviderJob(ctx, step, dispatch); err == nil {
			t.Fatal("RESERVED replay accepted an unexpected ADJUSTMENT")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("unexpected RESERVED ledger error = %#v", err)
			}
		}
		mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, extraID)
		if _, err := store.PrepareProviderJob(ctx, step, dispatch); err != nil {
			t.Fatalf("exact RESERVED replay after repair: %v", err)
		}
	})
	// A late Temporal Activity replay can arrive after completion has already
	// frozen both product records. It must recover the exact durable job rather
	// than allocate a second paid boundary or reject the workflow's history.
	var beforeReplayRunState, beforeReplayAttemptState string
	if err := pool.QueryRow(ctx, `
		SELECT gr.state, ga.state
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga
		  ON ga.generation_run_id = gr.id AND ga.sequence = 1
		WHERE gr.id = $1`, run.RunID,
	).Scan(&beforeReplayRunState, &beforeReplayAttemptState); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.generation_runs SET state = 'SUCCEEDED' WHERE id = $1`,
		run.RunID,
	)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.generation_attempts
		SET state = 'SUCCEEDED' WHERE generation_run_id = $1 AND sequence = 1`,
		run.RunID,
	)
	terminalReplay, err := store.PrepareProviderJob(ctx, step, dispatch)
	if err != nil {
		t.Fatalf("terminal exact prepared Provider job replay failed: %v", err)
	}
	if !reflect.DeepEqual(terminalReplay, preparedProvider) {
		t.Fatal("terminal exact Provider replay changed the durable allocation")
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.generation_runs SET state = $2 WHERE id = $1`,
		run.RunID, beforeReplayRunState,
	)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.generation_attempts
		SET state = $2 WHERE generation_run_id = $1 AND sequence = 1`,
		run.RunID, beforeReplayAttemptState,
	)
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
	actualCost := int64(40)
	providerResult := orchestration.ProviderResult{
		UpstreamTaskID: "task-1", RequestID: "request-1",
		ArtifactDigest: videoArtifact.Digest, ArtifactURI: videoArtifact.URI,
		MediaType: "video/mp4", ArtifactSize: videoArtifact.Size,
		Width: 1280, Height: 720, DurationMillis: 5_000,
		Model: model,
		Usage: providercontract.Usage{InputUnits: 10, OutputUnits: 20, Unit: "mock-units"},
		Cost: providercontract.Cost{
			EstimatedMicros: 50, ActualMicros: &actualCost, Currency: "CNY",
			PricingVersion: "pricing-v1", Verified: true,
		},
	}
	pauseDuringCompletionDigest, _ := digestValue(map[string]any{
		"runId": run.RunID, "reason": "PAUSE_DURING_PROVIDER_COMPLETION",
	})
	if _, err := store.RequestRunPause(
		ctx, run.RunID, 1, qaPauseActor, "PAUSE_DURING_PROVIDER_COMPLETION",
		controlplane.Idempotency{
			Scope: "workflow-projection-pause-during-completion:" + run.RunID,
			Key:   uuid.NewString(), RequestHash: pauseDuringCompletionDigest,
		},
		step.TraceID,
	); err != nil {
		t.Fatal(err)
	}
	// The core completion may commit immediately before the Stage 1 caller
	// records automatic QC, including while the run is PAUSED. The exact replay
	// must preserve the pause; after RESUME_PAUSED it repairs only the run/shot
	// and missing QC projections without changing the succeeded Provider result.
	if err := store.CompleteProviderJob(
		ctx, step, dispatch, providerResult,
	); err != nil {
		t.Fatalf("initial Provider completion: %v", err)
	}
	var qcBeforeRepair int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM video_pipeline.qc_reports
		WHERE generation_run_id = $1`, run.RunID,
	).Scan(&qcBeforeRepair); err != nil {
		t.Fatal(err)
	}
	if qcBeforeRepair != 0 {
		t.Fatalf("QC reports before prepared replay = %d, want 0", qcBeforeRepair)
	}
	if err := store.CompletePreparedProviderJob(
		ctx, step, run.RunID, providerResult,
	); err == nil {
		t.Fatal("prepared completion created QC while the succeeded run remained paused")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeConflict {
			t.Fatalf("paused prepared completion error = %#v", err)
		}
	}
	resumeAfterCompletionDigest, _ := digestValue(map[string]any{
		"runId": run.RunID, "mode": "RESUME_PAUSED_AFTER_PROVIDER_COMPLETION",
	})
	if _, err := store.RequestRunResume(
		ctx, run.RunID, 1, qaPauseActor, "RESUME_PAUSED",
		controlplane.Idempotency{
			Scope: "workflow-projection-resume-after-completion:" + run.RunID,
			Key:   uuid.NewString(), RequestHash: resumeAfterCompletionDigest,
		},
		step.TraceID,
	); err != nil {
		t.Fatal(err)
	}
	for replay := 1; replay <= 2; replay++ {
		if err := store.CompletePreparedProviderJob(
			ctx, step, run.RunID, providerResult,
		); err != nil {
			t.Fatalf("Stage 1 prepared completion replay %d: %v", replay, err)
		}
	}
	var stage1PassedQC int
	var stage1ShotState string
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*)
		   FROM video_pipeline.qc_reports
		   WHERE generation_run_id = $1 AND state = 'PASSED'),
		  (SELECT ssr.lifecycle_state
		   FROM video_pipeline.shot_spec_revisions ssr
		   JOIN video_pipeline.generation_runs gr
		     ON gr.shot_spec_revision_id = ssr.id
		   WHERE gr.id = $1)`,
		run.RunID,
	).Scan(&stage1PassedQC, &stage1ShotState); err != nil {
		t.Fatal(err)
	}
	if stage1PassedQC != 1 || stage1ShotState != "REVIEW" {
		t.Fatalf(
			"Stage 1 completion QC projection = reports:%d shot:%s, want 1/REVIEW",
			stage1PassedQC, stage1ShotState,
		)
	}
	completionDriftCases := []struct {
		name   string
		mutate func(*orchestration.ProviderResult)
	}{
		{
			name: "upstream task",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.UpstreamTaskID += "-drift"
			},
		},
		{
			name: "upstream request",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.RequestID += "-drift"
			},
		},
		{
			name: "artifact",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.ArtifactDigest = strings.Repeat("a", 64)
				if candidate.ArtifactDigest == providerResult.ArtifactDigest {
					candidate.ArtifactDigest = strings.Repeat("b", 64)
				}
				candidate.ArtifactURI = "cas://sha256/" + candidate.ArtifactDigest
			},
		},
		{
			name: "usage distribution",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.Usage.InputUnits++
				candidate.Usage.OutputUnits--
			},
		},
		{
			name: "estimated cost",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.Cost.EstimatedMicros--
			},
		},
		{
			name: "actual cost",
			mutate: func(candidate *orchestration.ProviderResult) {
				actual := *candidate.Cost.ActualMicros + 1
				candidate.Cost.ActualMicros = &actual
			},
		},
		{
			name: "model",
			mutate: func(candidate *orchestration.ProviderResult) {
				candidate.Model.RouteVersion += "-drift"
			},
		},
	}
	for _, test := range completionDriftCases {
		t.Run("terminal Provider completion rejects "+test.name+" drift", func(t *testing.T) {
			candidate := providerResult
			test.mutate(&candidate)
			err := store.CompletePreparedProviderJob(ctx, step, run.RunID, candidate)
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("completion drift error = %#v, want %s", err, controlplane.CodeRevisionConflict)
			}

			var (
				storedTaskID, storedRequestID      string
				outputCount, passedQC, actualCount int
				driftArtifactCount                 int
			)
			if err := pool.QueryRow(ctx, `
				SELECT COALESCE(pj.upstream_task_id, ''),
				       COALESCE(pj.upstream_request_id, ''),
				       (SELECT COUNT(*)
				        FROM video_pipeline.run_artifacts
				        WHERE generation_run_id = gr.id AND role = 'OUTPUT'),
				       (SELECT COUNT(*)
				        FROM video_pipeline.qc_reports
				        WHERE generation_run_id = gr.id AND state = 'PASSED'),
				       (SELECT COUNT(*)
				        FROM video_pipeline.cost_ledger
				        WHERE provider_job_id = pj.id AND entry_type = 'ACTUAL'),
				       (SELECT COUNT(*)
				        FROM video_pipeline.artifacts
				        WHERE content_hash = $2 AND $2 <> $3)
				FROM video_pipeline.provider_jobs pj
				JOIN video_pipeline.generation_attempts ga
				  ON ga.id = pj.generation_attempt_id
				JOIN video_pipeline.generation_runs gr
				  ON gr.id = ga.generation_run_id
				WHERE gr.id = $1`,
				run.RunID, candidate.ArtifactDigest, providerResult.ArtifactDigest,
			).Scan(
				&storedTaskID, &storedRequestID, &outputCount,
				&passedQC, &actualCount, &driftArtifactCount,
			); err != nil {
				t.Fatal(err)
			}
			if storedTaskID != providerResult.UpstreamTaskID ||
				storedRequestID != providerResult.RequestID ||
				outputCount != 1 || passedQC != 1 || actualCount != 1 ||
				driftArtifactCount != 0 {
				t.Fatalf(
					"completion drift side effects = task:%q request:%q output:%d qc:%d actual:%d artifact:%d",
					storedTaskID, storedRequestID, outputCount,
					passedQC, actualCount, driftArtifactCount,
				)
			}
		})
	}
	type terminalProjection struct {
		RunState, RunFinishedAt, ShotState        string
		JobState, TaskID, RequestID, JobUpdatedAt string
		JobTerminalAt, ArtifactStatus             string
		OutputCount, QCCount, JobLedgerCount      int
		ReservationLedgerCount                    int
	}
	providerRunID := uuid.MustParse(run.RunID)
	providerJobID := uuid.NewSHA1(providerRunID, []byte("provider-job"))
	providerArtifactID := uuid.NewSHA1(
		uuid.NameSpaceOID, []byte("artifact:"+providerResult.ArtifactDigest),
	)
	actualLedgerID := uuid.NewSHA1(providerJobID, []byte("actual-cost"))
	releaseLedgerID := uuid.NewSHA1(providerJobID, []byte("unused-reservation-release"))
	reservationLedgerID := uuid.NewSHA1(providerJobID, []byte("reservation-cost"))
	readTerminalProjection := func(
		t *testing.T,
		runID string,
		jobID uuid.UUID,
		artifactDigest string,
	) terminalProjection {
		t.Helper()
		var projection terminalProjection
		if err := pool.QueryRow(ctx, `
			SELECT gr.state, COALESCE(gr.finished_at::text, ''), ssr.lifecycle_state,
			       pj.state, COALESCE(pj.upstream_task_id, ''),
			       COALESCE(pj.upstream_request_id, ''), pj.updated_at::text,
			       COALESCE(pj.terminal_at::text, ''),
			       COALESCE((SELECT status FROM video_pipeline.artifacts
			                 WHERE content_hash = $3), 'MISSING'),
			       (SELECT COUNT(*) FROM video_pipeline.run_artifacts
			        WHERE generation_run_id = gr.id AND role = 'OUTPUT'),
			       (SELECT COUNT(*) FROM video_pipeline.qc_reports
			        WHERE generation_run_id = gr.id),
			       (SELECT COUNT(*) FROM video_pipeline.cost_ledger
			        WHERE provider_job_id = pj.id),
			       (SELECT COUNT(*) FROM video_pipeline.cost_ledger
			        WHERE budget_reservation_id = pj.budget_reservation_id)
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr
			  ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.generation_attempts ga
			  ON ga.generation_run_id = gr.id
			JOIN video_pipeline.provider_jobs pj
			  ON pj.generation_attempt_id = ga.id AND pj.id = $2
			WHERE gr.id = $1`,
			runID, jobID, artifactDigest,
		).Scan(
			&projection.RunState, &projection.RunFinishedAt, &projection.ShotState,
			&projection.JobState, &projection.TaskID, &projection.RequestID,
			&projection.JobUpdatedAt, &projection.JobTerminalAt,
			&projection.ArtifactStatus, &projection.OutputCount,
			&projection.QCCount, &projection.JobLedgerCount,
			&projection.ReservationLedgerCount,
		); err != nil {
			t.Fatal(err)
		}
		return projection
	}
	prepareTerminalRepairWindow := func(t *testing.T, runID string) {
		t.Helper()
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.generation_runs SET state = 'RUNNING' WHERE id = $1`,
			runID,
		)
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.shot_spec_revisions ssr
			SET lifecycle_state = 'RUNNING'
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1 AND ssr.id = gr.shot_spec_revision_id`,
			runID,
		)
	}
	restoreTerminalProjection := func(t *testing.T, runID string) {
		t.Helper()
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.generation_runs SET state = 'SUCCEEDED' WHERE id = $1`,
			runID,
		)
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.shot_spec_revisions ssr
			SET lifecycle_state = 'REVIEW'
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1 AND ssr.id = gr.shot_spec_revision_id`,
			runID,
		)
	}
	assertTerminalReplayConflictWithoutWrites := func(
		t *testing.T,
		runID string,
		jobID uuid.UUID,
		artifactDigest string,
		result orchestration.ProviderResult,
	) {
		t.Helper()
		before := readTerminalProjection(t, runID, jobID, artifactDigest)
		err := store.CompletePreparedProviderJob(ctx, step, runID, result)
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("terminal projection replay error = %#v, want %s", err, controlplane.CodeRevisionConflict)
		}
		after := readTerminalProjection(t, runID, jobID, artifactDigest)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("terminal projection changed on rejected replay:\nbefore=%#v\nafter=%#v", before, after)
		}
	}

	for _, artifactStatus := range []string{"ARCHIVED", "DISABLED", "ORPHAN_CANDIDATE"} {
		artifactStatus := artifactStatus
		t.Run("terminal Provider replay rejects "+artifactStatus+" shot output", func(t *testing.T) {
			t.Cleanup(func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.artifacts
					SET status = 'ACTIVE', orphaned_at = NULL, retention_until = NULL
					WHERE id = $1`,
					providerArtifactID,
				)
				restoreTerminalProjection(t, run.RunID)
			})
			mustExec(t, ctx, pool, `
				UPDATE video_pipeline.artifacts
				SET status = $2,
				    orphaned_at = CASE WHEN $2 = 'ORPHAN_CANDIDATE' THEN now() ELSE NULL END,
				    retention_until = CASE
				      WHEN $2 = 'ORPHAN_CANDIDATE' THEN now() + interval '1 hour'
				      ELSE NULL
				    END
				WHERE id = $1`,
				providerArtifactID, artifactStatus,
			)
			prepareTerminalRepairWindow(t, run.RunID)
			assertTerminalReplayConflictWithoutWrites(
				t, run.RunID, providerJobID, providerResult.ArtifactDigest, providerResult,
			)
		})
	}
	t.Run("terminal Provider replay rejects missing shot output", func(t *testing.T) {
		t.Cleanup(func() {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.run_artifacts
					(generation_run_id, artifact_id, role)
				VALUES ($1, $2, 'OUTPUT') ON CONFLICT DO NOTHING`,
				run.RunID, providerArtifactID,
			)
			restoreTerminalProjection(t, run.RunID)
		})
		mustExec(t, ctx, pool, `
			DELETE FROM video_pipeline.run_artifacts
			WHERE generation_run_id = $1 AND artifact_id = $2 AND role = 'OUTPUT'`,
			run.RunID, providerArtifactID,
		)
		prepareTerminalRepairWindow(t, run.RunID)
		assertTerminalReplayConflictWithoutWrites(
			t, run.RunID, providerJobID, providerResult.ArtifactDigest, providerResult,
		)
	})

	var providerReservationID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT budget_reservation_id FROM video_pipeline.provider_jobs WHERE id = $1`,
		providerJobID,
	).Scan(&providerReservationID); err != nil {
		t.Fatal(err)
	}
	var otherJobID, otherReservationID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT id, budget_reservation_id
		FROM video_pipeline.provider_jobs
		WHERE id <> $1
		ORDER BY created_at
		LIMIT 1`,
		providerJobID,
	).Scan(&otherJobID, &otherReservationID); err != nil {
		t.Fatal(err)
	}
	restoreSuccessfulCostProjection := func(t *testing.T) {
		t.Helper()
		mustExec(t, ctx, pool, `
			DELETE FROM video_pipeline.cost_ledger
			WHERE id IN ($1, $2)
			   OR ((provider_job_id = $3 OR budget_reservation_id = $4)
			       AND entry_type IN ('ACTUAL', 'RELEASE'))`,
			actualLedgerID, releaseLedgerID, providerJobID, providerReservationID,
		)
		mustExec(t, ctx, pool, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type,
				 amount_micros, currency, units, unit_name,
				 pricing_rule_version, verified)
			VALUES
				($1, $3, $4, 'ACTUAL', 40, 'CNY', 30, 'mock-units', 'pricing-v1', true),
				($2, $3, $4, 'RELEASE', 10, 'CNY', NULL, NULL, 'pricing-v1', true)`,
			actualLedgerID, releaseLedgerID, providerJobID, providerReservationID,
		)
	}
	var reservationEstimatePayload []byte
	if err := pool.QueryRow(ctx, `
		SELECT estimate_payload
		FROM video_pipeline.budget_reservations
		WHERE id = $1`,
		providerReservationID,
	).Scan(&reservationEstimatePayload); err != nil {
		t.Fatal(err)
	}
	var otherRunID uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT ga.generation_run_id
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga
		  ON ga.id = pj.generation_attempt_id
		WHERE pj.id = $1`,
		otherJobID,
	).Scan(&otherRunID); err != nil {
		t.Fatal(err)
	}
	restoreDurableReservationProjection := func(t *testing.T) {
		t.Helper()
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.provider_jobs
			SET budget_reservation_id = CASE
			  WHEN id = $1 THEN $2::uuid
			  WHEN id = $3 THEN $4::uuid
			  ELSE budget_reservation_id
			END
			WHERE id IN ($1, $3)`,
			providerJobID, providerReservationID, otherJobID, otherReservationID,
		)
		mustExec(t, ctx, pool, `
			DELETE FROM video_pipeline.budget_reservations
			WHERE generation_run_id = $1 AND id <> $2`,
			providerRunID, providerReservationID,
		)
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.budget_reservations
			SET generation_run_id = $2, amount_micros = 50,
			    currency = 'CNY', pricing_rule_version = 'pricing-v1',
			    estimate_payload = $3, status = 'SETTLED',
			    confirmed_by = $4, confirmed_at = created_at
			WHERE id = $1`,
			providerReservationID, providerRunID,
			reservationEstimatePayload, dispatch.BudgetApprovalID,
		)
		mustExec(t, ctx, pool, `
			DELETE FROM video_pipeline.cost_ledger
			WHERE id = $1
			   OR ((provider_job_id = $2 OR budget_reservation_id = $3)
			       AND entry_type = 'RESERVATION')`,
			reservationLedgerID, providerJobID, providerReservationID,
		)
		mustExec(t, ctx, pool, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type,
				 amount_micros, currency, pricing_rule_version, verified)
			VALUES ($1, $2, $3, 'RESERVATION', 50, 'CNY', 'pricing-v1', true)`,
			reservationLedgerID, providerJobID, providerReservationID,
		)
	}
	readDurablePaidProjection := func(t *testing.T) string {
		t.Helper()
		var projection string
		if err := pool.QueryRow(ctx, `
			SELECT jsonb_build_object(
			  'reservations', COALESCE((
			    SELECT jsonb_agg(to_jsonb(br) ORDER BY br.id)
			    FROM video_pipeline.budget_reservations br
			    WHERE br.id = $1 OR br.generation_run_id = $2
			  ), '[]'::jsonb),
			  'jobs', COALESCE((
			    SELECT jsonb_agg(to_jsonb(pj) ORDER BY pj.id)
			    FROM video_pipeline.provider_jobs pj
			    WHERE pj.id = $3 OR pj.budget_reservation_id = $1
			  ), '[]'::jsonb),
			  'ledger', COALESCE((
			    SELECT jsonb_agg(to_jsonb(cl) ORDER BY cl.id)
			    FROM video_pipeline.cost_ledger cl
			    WHERE cl.provider_job_id = $3 OR cl.budget_reservation_id = $1
			       OR cl.id IN ($4, $5, $6)
			  ), '[]'::jsonb)
			)::text`,
			providerReservationID, providerRunID, providerJobID,
			reservationLedgerID, actualLedgerID, releaseLedgerID,
		).Scan(&projection); err != nil {
			t.Fatal(err)
		}
		return projection
	}
	assertReservationReplayConflictWithoutWrites := func(t *testing.T) {
		t.Helper()
		beforeTerminal := readTerminalProjection(
			t, run.RunID, providerJobID, providerResult.ArtifactDigest,
		)
		beforePaid := readDurablePaidProjection(t)
		if _, err := store.PrepareProviderJob(ctx, step, dispatch); err == nil {
			t.Fatal("prepared Provider replay accepted a drifted durable reservation")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("prepared reservation replay error = %#v", err)
			}
		}
		if err := store.CompletePreparedProviderJob(
			ctx, step, run.RunID, providerResult,
		); err == nil {
			t.Fatal("terminal Provider replay accepted a drifted durable reservation")
		} else {
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
				t.Fatalf("terminal reservation replay error = %#v", err)
			}
		}
		afterTerminal := readTerminalProjection(
			t, run.RunID, providerJobID, providerResult.ArtifactDigest,
		)
		afterPaid := readDurablePaidProjection(t)
		if !reflect.DeepEqual(afterTerminal, beforeTerminal) || afterPaid != beforePaid {
			t.Fatalf(
				"durable reservation changed on rejected replay:\nterminal before=%#v\nterminal after=%#v\npaid before=%s\npaid after=%s",
				beforeTerminal, afterTerminal, beforePaid, afterPaid,
			)
		}
	}
	reservationProjectionDrifts := []struct {
		name   string
		mutate func(*testing.T)
	}{
		{name: "reservation run", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET generation_run_id = $2 WHERE id = $1`, providerReservationID, otherRunID)
		}},
		{name: "reservation amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET amount_micros = 49 WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET currency = 'USD' WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation pricing", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET pricing_rule_version = 'drift-v1' WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation estimate payload", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET estimate_payload = jsonb_set(estimate_payload, '{estimatedMicros}', '49') WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation confirmer", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET confirmed_by = 'drift-approval' WHERE id = $1`, providerReservationID)
		}},
		{name: "missing reservation confirmer", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET confirmed_by = NULL WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation confirmation time", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET confirmed_at = confirmed_at + interval '1 second' WHERE id = $1`, providerReservationID)
		}},
		{name: "missing reservation confirmation time", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET confirmed_at = NULL WHERE id = $1`, providerReservationID)
		}},
		{name: "reservation status", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.budget_reservations SET status = 'RESERVED' WHERE id = $1`, providerReservationID)
		}},
		{name: "extra run reservation", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.budget_reservations
					(id, generation_run_id, amount_micros, currency,
					 pricing_rule_version, estimate_payload, status,
					 confirmed_by, confirmed_at)
				VALUES ($1, $2, 50, 'CNY', 'pricing-v1', $3,
				        'SETTLED', $4, now())`,
				uuid.New(), providerRunID, reservationEstimatePayload, dispatch.BudgetApprovalID,
			)
		}},
		{name: "extra job reservation binding", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.provider_jobs SET budget_reservation_id = $2 WHERE id = $1`, otherJobID, providerReservationID)
		}},
		{name: "missing RESERVATION ledger", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, reservationLedgerID)
		}},
		{name: "non-deterministic RESERVATION ledger id", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET id = $2 WHERE id = $1`, reservationLedgerID, uuid.New())
		}},
		{name: "RESERVATION ledger job", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET provider_job_id = $2 WHERE id = $1`, reservationLedgerID, otherJobID)
		}},
		{name: "RESERVATION ledger reservation", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET budget_reservation_id = $2 WHERE id = $1`, reservationLedgerID, otherReservationID)
		}},
		{name: "RESERVATION ledger type", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET entry_type = 'ADJUSTMENT' WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 49 WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = 'USD' WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger units", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET units = 1 WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger unit name", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET unit_name = 'drift-units' WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger pricing", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET pricing_rule_version = 'drift-v1' WHERE id = $1`, reservationLedgerID)
		}},
		{name: "RESERVATION ledger verified", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = false WHERE id = $1`, reservationLedgerID)
		}},
		{name: "duplicate RESERVATION ledger", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'RESERVATION', 50, 'CNY', 'pricing-v1', true)`,
				uuid.New(), providerJobID, providerReservationID,
			)
		}},
		{name: "unexpected ESTIMATE ledger", mutate: func(t *testing.T) {
			extraID := uuid.New()
			t.Cleanup(func() {
				mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, extraID)
			})
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'ESTIMATE', 1, 'CNY', 'pricing-v1', true)`,
				extraID, providerJobID, providerReservationID,
			)
		}},
	}
	for _, test := range reservationProjectionDrifts {
		test := test
		t.Run("Provider replay rejects "+test.name+" drift", func(t *testing.T) {
			t.Cleanup(func() {
				restoreDurableReservationProjection(t)
				restoreSuccessfulCostProjection(t)
				restoreTerminalProjection(t, run.RunID)
			})
			test.mutate(t)
			prepareTerminalRepairWindow(t, run.RunID)
			assertReservationReplayConflictWithoutWrites(t)
		})
	}

	cumulativeCommand := publicCommand
	cumulativeCommand.CreativeAttempt = 2
	_, cumulativeRun, cumulativeDispatch := createIntegrationWorkflowRun(
		t, ctx, store, "cumulative-binding-probe", cumulativeCommand,
	)
	cumulativeCostDrifts := []struct {
		name   string
		mutate func(*testing.T)
	}{
		{name: "other job borrows reservation", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, units, unit_name,
					 pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'ACTUAL', 0, 'CNY', 0, 'mock-units', 'pricing-v1', true)`,
				uuid.New(), otherJobID, providerReservationID,
			)
		}},
		{name: "non-deterministic ACTUAL id", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET id = $2 WHERE id = $1`, actualLedgerID, uuid.New())
		}},
		{name: "missing ACTUAL", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, actualLedgerID)
		}},
		{name: "duplicate ACTUAL", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, units, unit_name,
					 pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'ACTUAL', 40, 'CNY', 30, 'mock-units', 'pricing-v1', true)`,
				uuid.New(), providerJobID, providerReservationID,
			)
		}},
		{name: "ACTUAL amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 0 WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL verified", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = false WHERE id = $1`, actualLedgerID)
		}},
	}
	for _, test := range cumulativeCostDrifts {
		test := test
		t.Run("cumulative allocation rejects "+test.name, func(t *testing.T) {
			t.Cleanup(func() {
				restoreSuccessfulCostProjection(t)
			})
			test.mutate(t)
			providerCallsBefore := videoProviderCalls.Load()
			_, activityErr := videoActivityEnvironment.ExecuteActivity(
				videoActivities.ExecuteProviderJob, cumulativeDispatch,
			)
			var applicationErr *temporal.ApplicationError
			if !errors.As(activityErr, &applicationErr) ||
				applicationErr.Type() != string(controlplane.CodeRevisionConflict) ||
				!applicationErr.NonRetryable() {
				t.Fatalf("cumulative allocation Activity error = %#v", activityErr)
			}
			var reservations, jobs, ledger int
			if err := pool.QueryRow(ctx, `
				SELECT
				  (SELECT COUNT(*) FROM video_pipeline.budget_reservations
				   WHERE generation_run_id = $1),
				  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1),
				  (SELECT COUNT(*) FROM video_pipeline.cost_ledger cl
				   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`,
				cumulativeRun.RunID,
			).Scan(&reservations, &jobs, &ledger); err != nil {
				t.Fatal(err)
			}
			if reservations != 0 || jobs != 0 || ledger != 0 ||
				videoProviderCalls.Load() != providerCallsBefore {
				t.Fatalf(
					"rejected cumulative allocation side effects = reservations:%d jobs:%d ledger:%d provider:%d→%d",
					reservations, jobs, ledger,
					providerCallsBefore, videoProviderCalls.Load(),
				)
			}
		})
	}
	costProjectionDrifts := []struct {
		name   string
		mutate func(*testing.T)
	}{
		{name: "missing ACTUAL", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL provider job", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET provider_job_id = $2 WHERE id = $1`, actualLedgerID, otherJobID)
		}},
		{name: "ACTUAL reservation", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET budget_reservation_id = $2 WHERE id = $1`, actualLedgerID, otherReservationID)
		}},
		{name: "ACTUAL entry type", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET entry_type = 'ADJUSTMENT' WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 41 WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL null amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = NULL WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = 'USD' WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL null currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = NULL WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL units", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET units = 31 WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL unit name", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET unit_name = 'drift-units' WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL pricing", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET pricing_rule_version = 'drift-v1' WHERE id = $1`, actualLedgerID)
		}},
		{name: "ACTUAL verified", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = false WHERE id = $1`, actualLedgerID)
		}},
		{name: "extra ACTUAL", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, units, unit_name,
					 pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'ACTUAL', 40, 'CNY', 30, 'mock-units', 'pricing-v1', true)`,
				uuid.New(), providerJobID, providerReservationID,
			)
		}},
		{name: "missing RELEASE", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE provider job", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET provider_job_id = $2 WHERE id = $1`, releaseLedgerID, otherJobID)
		}},
		{name: "RELEASE reservation", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET budget_reservation_id = $2 WHERE id = $1`, releaseLedgerID, otherReservationID)
		}},
		{name: "RELEASE entry type", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET entry_type = 'ADJUSTMENT' WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = 11 WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE null amount", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET amount_micros = NULL WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = 'USD' WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE null currency", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET currency = NULL WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE units", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET units = 1 WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE unit name", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET unit_name = 'drift-units' WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE pricing", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET pricing_rule_version = 'drift-v1' WHERE id = $1`, releaseLedgerID)
		}},
		{name: "RELEASE verified", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `UPDATE video_pipeline.cost_ledger SET verified = false WHERE id = $1`, releaseLedgerID)
		}},
		{name: "extra RELEASE", mutate: func(t *testing.T) {
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'RELEASE', 10, 'CNY', 'pricing-v1', true)`,
				uuid.New(), providerJobID, providerReservationID,
			)
		}},
	}
	for _, test := range costProjectionDrifts {
		test := test
		t.Run("terminal Provider replay rejects "+test.name+" drift", func(t *testing.T) {
			t.Cleanup(func() {
				restoreSuccessfulCostProjection(t)
				restoreTerminalProjection(t, run.RunID)
			})
			test.mutate(t)
			prepareTerminalRepairWindow(t, run.RunID)
			assertTerminalReplayConflictWithoutWrites(
				t, run.RunID, providerJobID, providerResult.ArtifactDigest, providerResult,
			)
		})
	}
	t.Run("terminal Provider replay rejects unexpected RELEASE at full reservation", func(t *testing.T) {
		_, fullReservationCommand := cloneIntegrationShotCommand(
			t, ctx, pool, store, shotID.String(), publicCommand,
		)
		fullStep, fullRun, fullDispatch := createIntegrationWorkflowRun(
			t, ctx, store, "full-reservation-terminal-replay", fullReservationCommand,
		)
		if _, err := store.PrepareProviderJob(ctx, fullStep, fullDispatch); err != nil {
			t.Fatal(err)
		}
		fullArtifact, err := cas.Put(
			ctx, bytes.NewReader([]byte("full reservation terminal replay artifact")),
		)
		if err != nil {
			t.Fatal(err)
		}
		fullActual := int64(50)
		fullResult := providerResult
		fullResult.UpstreamTaskID = "full-reservation-task"
		fullResult.RequestID = "full-reservation-request"
		fullResult.ArtifactDigest = fullArtifact.Digest
		fullResult.ArtifactURI = fullArtifact.URI
		fullResult.ArtifactSize = fullArtifact.Size
		fullResult.Model = fullDispatch.Route
		fullResult.Cost.ActualMicros = &fullActual
		if err := store.CompletePreparedProviderJob(
			ctx, fullStep, fullRun.RunID, fullResult,
		); err != nil {
			t.Fatal(err)
		}
		fullRunUUID := uuid.MustParse(fullRun.RunID)
		fullJobID := uuid.NewSHA1(fullRunUUID, []byte("provider-job"))
		var fullReservationID uuid.UUID
		var fullReleaseCount int
		if err := pool.QueryRow(ctx, `
			SELECT pj.budget_reservation_id,
			       (SELECT COUNT(*) FROM video_pipeline.cost_ledger
			        WHERE provider_job_id = pj.id AND entry_type = 'RELEASE')
			FROM video_pipeline.provider_jobs pj
			WHERE pj.id = $1`,
			fullJobID,
		).Scan(&fullReservationID, &fullReleaseCount); err != nil {
			t.Fatal(err)
		}
		if fullReleaseCount != 0 {
			t.Fatalf("full-reservation completion RELEASE rows = %d, want 0", fullReleaseCount)
		}
		unexpectedReleaseID := uuid.NewSHA1(
			fullJobID, []byte("unused-reservation-release"),
		)
		t.Cleanup(func() {
			mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, unexpectedReleaseID)
			restoreTerminalProjection(t, fullRun.RunID)
		})
		mustExec(t, ctx, pool, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type,
				 amount_micros, currency, pricing_rule_version, verified)
			VALUES ($1, $2, $3, 'RELEASE', 1, 'CNY', 'pricing-v1', true)`,
			unexpectedReleaseID, fullJobID, fullReservationID,
		)
		prepareTerminalRepairWindow(t, fullRun.RunID)
		assertTerminalReplayConflictWithoutWrites(
			t, fullRun.RunID, fullJobID, fullResult.ArtifactDigest, fullResult,
		)
		mustExec(t, ctx, pool, `DELETE FROM video_pipeline.cost_ledger WHERE id = $1`, unexpectedReleaseID)
		restoreTerminalProjection(t, fullRun.RunID)
		if err := store.CompletePreparedProviderJob(
			ctx, fullStep, fullRun.RunID, fullResult,
		); err != nil {
			t.Fatalf("full-reservation exact terminal replay: %v", err)
		}
	})
	if err := store.RecordProviderCancellation(
		ctx, step,
		orchestration.CancelProviderJobInput{
			Dispatch:   dispatch,
			ReasonCode: "INTEGRATION_SUCCESS_FIRST",
			TraceID:    step.TraceID,
		},
		orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true},
	); err != nil {
		t.Fatal(err)
	}
	completedAfterPause, err := store.GetGenerationRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if completedAfterPause.State != "SUCCEEDED" ||
		completedAfterPause.FailureClass != "" || completedAfterPause.FailureCode != "" {
		t.Fatalf("success-first cancellation race projection = %#v", completedAfterPause)
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

	step.ActivityID, step.ActivityType = "post-production", orchestration.ActivityFinalizeEpisode
	finalizeInput := orchestration.FinalizeEpisodeInput{
		EpisodeRevisionID: episodeRevisionID.String(),
		RunIDs:            []string{run.RunID},
		GenerationPlanID:  plan.Value.GenerationPlanID,
		Config: orchestration.PostProductionConfig{
			Enabled:  true,
			Evidence: postproduction.EvidenceMockOnly,
			SpeechRoute: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "MOCK",
				ModelID:         "fixture-speech-v1",
				RouteVersion:    "route-v1",
				CapabilityHash:  capabilityHash,
				Verification:    "mock_only",
			},
			SpeechProviderProfileID:       providerProfileID.String(),
			SpeechBudgetApprovalID:        speechBudgetID.String(),
			SpeechBudgetMaximumMicros:     1_000,
			SpeechBudgetCurrency:          "CNY",
			SubtitleLanguage:              "en",
			BackgroundAudioAssetVersionID: musicAssetVersionID.String(),
		},
		TraceID: step.TraceID,
	}
	postRequest, err := store.PrepareEpisodePostProduction(ctx, step, finalizeInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(postRequest.Clips) != 1 || len(postRequest.Subtitle.Cues) != 1 {
		t.Fatalf("prepared post-production request = %#v", postRequest)
	}
	if postRequest.Subtitle.Cues[0].StartMillis != 500 ||
		postRequest.Subtitle.Cues[0].EndMillis != 2_000 {
		t.Fatalf("prepared cue timing = %#v", postRequest.Subtitle.Cues[0])
	}
	assertPolicyCode := func(
		test *testing.T,
		stage string,
		err error,
		code controlplane.ErrorCode,
	) {
		test.Helper()
		var policy *controlplane.DomainError
		if !errors.As(err, &policy) || policy.Code != code {
			test.Fatalf("%s error = %#v, want %s", stage, err, code)
		}
	}
	alternatePlan, err := store.CreateGenerationPlan(
		ctx,
		planCommand,
		controlplane.Idempotency{
			Scope:       "workflow-projection-alternate-speech-plan:" + seriesID.String(),
			Key:         uuid.NewString(),
			RequestHash: planDigest,
		},
		"workflow-projection",
	)
	if err != nil {
		t.Fatal(err)
	}
	budgetRegressions := []struct {
		name   string
		mutate func()
	}{
		{
			name: "legacy old approval",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET generation_plan_id = NULL, budget_scope = NULL,
					    budget_limit_micros = NULL, budget_currency = NULL
					WHERE id = $1`,
					speechBudgetID,
				)
			},
		},
		{
			name: "different generation plan",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET generation_plan_id = $2
					WHERE id = $1`,
					speechBudgetID, alternatePlan.Value.GenerationPlanID,
				)
			},
		},
		{
			name: "wrong spend scope",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_scope = 'VIDEO'
					WHERE id = $1`,
					speechBudgetID,
				)
			},
		},
		{
			name: "insufficient amount",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_limit_micros = $2
					WHERE id = $1`,
					speechBudgetID, 999,
				)
			},
		},
		{
			name: "currency mismatch",
			mutate: func() {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.review_tasks
					SET budget_currency = 'USD'
					WHERE id = $1`,
					speechBudgetID,
				)
			},
		},
	}
	for _, regression := range budgetRegressions {
		regression := regression
		t.Run("paid speech budget "+regression.name, func(t *testing.T) {
			regression.mutate()
			err := store.AuthorizeEpisodePostProduction(ctx, step, finalizeInput)
			assertPolicyCode(t, regression.name, err, controlplane.CodeBudgetExceeded)
			mustExec(t, ctx, pool, `
				UPDATE video_pipeline.review_tasks
				SET generation_plan_id = $2, budget_scope = 'SPEECH',
				    budget_limit_micros = 1000, budget_currency = 'CNY'
				WHERE id = $1`,
				speechBudgetID, plan.Value.GenerationPlanID,
			)
		})
	}
	for _, mismatch := range []struct {
		name     string
		amount   int64
		currency string
	}{
		{name: "amount differs from plan", amount: 1_001, currency: "CNY"},
		{name: "currency differs from plan", amount: 1_000, currency: "USD"},
	} {
		mismatch := mismatch
		t.Run("paid speech plan "+mismatch.name, func(t *testing.T) {
			input := finalizeInput
			input.Config.SpeechBudgetMaximumMicros = mismatch.amount
			input.Config.SpeechBudgetCurrency = mismatch.currency
			err := store.AuthorizeEpisodePostProduction(ctx, step, input)
			assertPolicyCode(t, mismatch.name, err, controlplane.CodeBudgetExceeded)
		})
	}
	if err := store.AuthorizeEpisodePostProduction(ctx, step, finalizeInput); err != nil {
		t.Fatalf("current exact speech budget authorization: %v", err)
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'REVOKED' WHERE id = $1`,
		consentID,
	)
	err = store.AuthorizeEpisodePostProduction(ctx, step, finalizeInput)
	assertPolicyCode(t, "paid submit after consent revocation", err, controlplane.CodeConsentRequired)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'ACTIVE' WHERE id = $1`,
		consentID,
	)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() - interval '1 second'
		WHERE id = $1`,
		voiceLicenseID,
	)
	err = store.AuthorizeEpisodePostProduction(ctx, step, finalizeInput)
	assertPolicyCode(t, "paid submit after license expiry", err, controlplane.CodeLicenseBlocked)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() + interval '1 hour'
		WHERE id = $1`,
		voiceLicenseID,
	)
	liveInput := finalizeInput
	liveInput.Config.Evidence = postproduction.EvidenceLive
	liveInput.Config.SpeechRoute.Verification = "live_provider_call"
	_, err = store.PrepareEpisodePostProduction(ctx, step, liveInput)
	var voicePolicy *controlplane.DomainError
	if !errors.As(err, &voicePolicy) || voicePolicy.Code != controlplane.CodeConsentRequired {
		t.Fatalf("live unbound voice error = %#v, want %s", err, controlplane.CodeConsentRequired)
	}
	putPostArtifact := func(
		kind, mediaType, payload string,
		duration int64,
		width, height, fps int,
	) postproduction.Artifact {
		t.Helper()
		committed, err := cas.Put(ctx, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		return postproduction.Artifact{
			Kind: kind, Digest: committed.Digest, URI: committed.URI,
			MediaType: mediaType, SizeBytes: committed.Size,
			DurationMillis: duration, Width: width, Height: height, FPS: fps,
		}
	}
	postManifest := putPostArtifact(
		"postproduction_manifest",
		"application/vnd.video-series.postproduction-manifest+json",
		fmt.Sprintf(
			`{"schemaVersion":"v1","evidence":"mock_only","episodeRevisionId":%q}`,
			episodeRevisionID.String(),
		),
		0, 0, 0, 0,
	)
	serviceBOM := putPostArtifact(
		"service_bom",
		"application/vnd.video-series.service-bom+json",
		fmt.Sprintf(
			`{"schemaVersion":"v1","evidence":"mock_only","episodeRevisionId":%q,"components":[]}`,
			episodeRevisionID.String(),
		),
		0, 0, 0, 0,
	)
	postResult := postproduction.Result{
		SchemaVersion:     postproduction.SchemaVersion,
		Evidence:          postproduction.EvidenceMockOnly,
		EpisodeRevisionID: episodeRevisionID.String(),
		Subtitle: putPostArtifact(
			"subtitle_srt", "application/x-subrip; charset=utf-8",
			"1\n00:00:00,500 --> 00:00:02,000\nHello fixture "+
				episodeRevisionID.String()+"\n\n",
			5_000, 0, 0, 0,
		),
		Dialogue: putPostArtifact(
			"dialogue_audio", "audio/wav", "fixture dialogue "+episodeRevisionID.String(),
			5_000, 0, 0, 0,
		),
		FinalVideo: putPostArtifact(
			"final_video", "video/mp4", "fixture final video "+episodeRevisionID.String(),
			5_000, 1280, 720, 24,
		),
		Manifest:        postManifest,
		ServiceBOM:      serviceBOM,
		CommandPlanHash: strings.Repeat("d", 64),
		ManifestHash:    postManifest.Digest,
		ServiceBOMHash:  serviceBOM.Digest,
		QC: postproduction.QCReport{
			State: "STRUCTURAL_PASSED", ActualDurationMillis: 5_000,
			ManualTimingRequired: true, MeasurementEvidence: postproduction.EvidenceMockOnly,
		},
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'REVOKED' WHERE id = $1`,
		consentID,
	)
	err = store.CommitEpisodePostProduction(ctx, step, finalizeInput, postResult)
	assertPolicyCode(t, "post-production commit after consent revocation", err, controlplane.CodeConsentRequired)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'ACTIVE' WHERE id = $1`,
		consentID,
	)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() - interval '1 second'
		WHERE id = $1`,
		musicLicenseID,
	)
	err = store.CommitEpisodePostProduction(ctx, step, finalizeInput, postResult)
	assertPolicyCode(t, "post-production commit after license expiry", err, controlplane.CodeLicenseBlocked)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() + interval '1 hour'
		WHERE id = $1`,
		musicLicenseID,
	)
	fixtureDigest := func(label string) string {
		t.Helper()
		digest, err := digestValue(map[string]string{
			"label": label,
			"nonce": uuid.NewString(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return digest
	}
	inactiveArtifactRegressions := []struct {
		name   string
		status string
		digest string
	}{
		{name: "orphan candidate", status: "ORPHAN_CANDIDATE", digest: fixtureDigest("orphan")},
		{name: "archived", status: "ARCHIVED", digest: fixtureDigest("archived")},
		{name: "disabled", status: "DISABLED", digest: fixtureDigest("disabled")},
	}
	for _, regression := range inactiveArtifactRegressions {
		regression := regression
		t.Run("post-production CAS conflict "+regression.name, func(t *testing.T) {
			conflictingResult := postResult
			conflictingResult.FinalVideo.Digest = regression.digest
			conflictingResult.FinalVideo.URI = "cas://sha256/" + regression.digest
			inactiveArtifactID := uuid.New()
			mustExec(t, ctx, pool, `
				INSERT INTO video_pipeline.artifacts
					(id, content_hash, artifact_uri, media_type, size_bytes,
					 media_spec, status, orphaned_at, retention_until)
				VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now() + interval '1 hour')`,
				inactiveArtifactID,
				conflictingResult.FinalVideo.Digest,
				conflictingResult.FinalVideo.URI,
				conflictingResult.FinalVideo.MediaType,
				conflictingResult.FinalVideo.SizeBytes,
				map[string]any{
					"kind":                       "final_video",
					"postProductionManifestHash": conflictingResult.ManifestHash,
				},
				regression.status,
			)
			err := store.CommitEpisodePostProduction(ctx, step, finalizeInput, conflictingResult)
			assertPolicyCode(t, regression.name, err, controlplane.CodeConflict)
			var links int
			if err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM video_pipeline.run_artifacts
				WHERE generation_run_id = $1 AND artifact_id = $2`,
				run.RunID, inactiveArtifactID,
			).Scan(&links); err != nil {
				t.Fatal(err)
			}
			if links != 0 {
				t.Fatalf("non-ACTIVE artifact links = %d, want 0", links)
			}
		})
	}
	var rejectedPostArtifacts int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.artifacts
		WHERE content_hash IN ($1, $2, $3, $4, $5)`,
		postResult.FinalVideo.Digest,
		postResult.Dialogue.Digest,
		postResult.Subtitle.Digest,
		postResult.Manifest.Digest,
		postResult.ServiceBOM.Digest,
	).Scan(&rejectedPostArtifacts); err != nil {
		t.Fatal(err)
	}
	if rejectedPostArtifacts != 0 {
		t.Fatalf("rejected post-production persisted %d artifacts", rejectedPostArtifacts)
	}
	if err := store.CommitEpisodePostProduction(ctx, step, finalizeInput, postResult); err != nil {
		t.Fatal(err)
	}
	var linkedPostArtifacts int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.run_artifacts ra
		JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
		WHERE ra.generation_run_id = $1
		  AND a.media_spec->>'postProductionManifestHash' = $2`,
		run.RunID, postResult.ManifestHash,
	).Scan(&linkedPostArtifacts); err != nil {
		t.Fatal(err)
	}
	if linkedPostArtifacts != 5 {
		t.Fatalf("linked post-production artifacts = %d, want 5", linkedPostArtifacts)
	}
	// final_video is intentionally another OUTPUT binding for every source run.
	// Terminal Provider replay must compare only the immutable shot_video output,
	// so a legitimate post-production projection cannot make an exact replay fail.
	if err := store.CompletePreparedProviderJob(
		ctx, step, run.RunID, providerResult,
	); err != nil {
		t.Fatalf("exact Provider completion replay after post-production: %v", err)
	}
	var passedQCAfterPostProduction int
	var shotStateAfterPostProductionReplay string
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*)
		   FROM video_pipeline.qc_reports
		   WHERE generation_run_id = $1 AND state = 'PASSED'),
		  (SELECT ssr.lifecycle_state
		   FROM video_pipeline.shot_spec_revisions ssr
		   JOIN video_pipeline.generation_runs gr
		     ON gr.shot_spec_revision_id = ssr.id
		   WHERE gr.id = $1)`,
		run.RunID,
	).Scan(
		&passedQCAfterPostProduction, &shotStateAfterPostProductionReplay,
	); err != nil {
		t.Fatal(err)
	}
	if passedQCAfterPostProduction != 1 || shotStateAfterPostProductionReplay != "APPROVED" {
		t.Fatalf(
			"Provider completion replay after post-production = QC:%d shot:%s, want 1/APPROVED",
			passedQCAfterPostProduction, shotStateAfterPostProductionReplay,
		)
	}
	// A corrected post-production revision commonly reuses the exact UTF-8 SRT
	// bytes while producing new dialogue/video/manifest bytes. The CAS artifact
	// must remain valid for both historical manifest hashes, and the new
	// Generation Manifest projection must expose the selected current binding.
	revisedPostResult := postResult
	revisedPostResult.Dialogue = putPostArtifact(
		"dialogue_audio", "audio/wav", "revised fixture dialogue "+episodeRevisionID.String(),
		5_000, 0, 0, 0,
	)
	revisedPostResult.FinalVideo = putPostArtifact(
		"final_video", "video/mp4", "revised fixture final video "+episodeRevisionID.String(),
		5_000, 1280, 720, 24,
	)
	revisedPostResult.Manifest = putPostArtifact(
		"postproduction_manifest",
		"application/vnd.video-series.postproduction-manifest+json",
		fmt.Sprintf(
			`{"schemaVersion":"v1","evidence":"mock_only","episodeRevisionId":%q,"revision":2}`,
			episodeRevisionID.String(),
		),
		0, 0, 0, 0,
	)
	revisedPostResult.ServiceBOM = putPostArtifact(
		"service_bom",
		"application/vnd.video-series.service-bom+json",
		fmt.Sprintf(
			`{"schemaVersion":"v1","evidence":"mock_only","episodeRevisionId":%q,"revision":2,"components":[]}`,
			episodeRevisionID.String(),
		),
		0, 0, 0, 0,
	)
	revisedPostResult.ManifestHash = revisedPostResult.Manifest.Digest
	revisedPostResult.ServiceBOMHash = revisedPostResult.ServiceBOM.Digest
	revisedPostResult.CommandPlanHash = strings.Repeat("e", 64)
	if err := store.CommitEpisodePostProduction(ctx, step, finalizeInput, revisedPostResult); err != nil {
		t.Fatalf("commit corrected post-production revision with reused SRT: %v", err)
	}
	revisedManifestPayload, err := store.BuildEpisodeManifest(ctx, step, orchestration.CreateGate3Input{
		EpisodeRevisionID: episodeRevisionID.String(), RunIDs: []string{run.RunID},
		GenerationPlanID:              plan.Value.GenerationPlanID,
		PostProductionManifestHash:    revisedPostResult.ManifestHash,
		BackgroundAudioAssetVersionID: musicAssetVersionID.String(),
		TraceID:                       step.TraceID,
	})
	if err != nil {
		t.Fatalf("build corrected Generation Manifest with reused SRT: %v", err)
	}
	var revisedManifest struct {
		ProviderExecutions []struct {
			Artifacts []struct {
				ContentHash string `json:"content_hash"`
				MediaSpec   struct {
					Kind                       string `json:"kind"`
					PostProductionManifestHash string `json:"postProductionManifestHash"`
				} `json:"media_spec"`
			} `json:"artifacts"`
		} `json:"providerExecutions"`
	}
	if err := json.Unmarshal(revisedManifestPayload, &revisedManifest); err != nil {
		t.Fatal(err)
	}
	foundReusedSubtitle := false
	for _, execution := range revisedManifest.ProviderExecutions {
		for _, artifact := range execution.Artifacts {
			if artifact.ContentHash == postResult.Subtitle.Digest &&
				artifact.MediaSpec.Kind == "subtitle_srt" &&
				artifact.MediaSpec.PostProductionManifestHash == revisedPostResult.ManifestHash {
				foundReusedSubtitle = true
			}
		}
	}
	if !foundReusedSubtitle {
		t.Fatal("corrected Generation Manifest omitted the reused SRT current binding")
	}
	oldPostManifestHash := fixtureDigest("old post-production manifest")
	oldFinalVideoHash := fixtureDigest("old final video")
	oldSubtitleHash := fixtureDigest("old subtitle")
	oldPostManifestID, oldFinalVideoID, oldSubtitleID := uuid.New(), uuid.New(), uuid.New()
	oldPostManifestURI := "cas://sha256/" + oldPostManifestHash
	oldFinalVideoURI := "cas://sha256/" + oldFinalVideoHash
	oldSubtitleURI := "cas://sha256/" + oldSubtitleHash
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES
			($1, $2, $3, 'application/json', 1,
			 jsonb_build_object(
			   'kind', 'postproduction_manifest',
			   'postProductionManifestHash', $7::text
			 ), 'ACTIVE'),
			($4, $5, $6, 'video/mp4', 1,
			 jsonb_build_object(
			   'kind', 'final_video',
			   'postProductionManifestHash', $7::text
			 ), 'ACTIVE'),
			($8, $9, $10, 'application/x-subrip', 1,
			 jsonb_build_object(
			   'kind', 'subtitle_srt',
			   'postProductionManifestHash', $7::text
			 ), 'ACTIVE')`,
		oldPostManifestID, oldPostManifestHash, oldPostManifestURI,
		oldFinalVideoID, oldFinalVideoHash, oldFinalVideoURI, oldPostManifestHash,
		oldSubtitleID, oldSubtitleHash, oldSubtitleURI,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.run_artifacts
			(generation_run_id, artifact_id, role)
		VALUES
			($1, $2, 'MANIFEST'),
			($1, $3, 'OUTPUT'),
			($1, $4, 'SUBTITLE')`,
		run.RunID, oldPostManifestID, oldFinalVideoID, oldSubtitleID,
	)
	extraHashlessArtifactID := uuid.New()
	extraHashlessArtifactHash := fixtureDigest("hashless provider auxiliary")
	extraHashlessArtifactURI := "cas://sha256/" + extraHashlessArtifactHash
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES ($1, $2, $3, 'application/octet-stream', 1,
		        '{"kind":"provider_auxiliary"}', 'ACTIVE')`,
		extraHashlessArtifactID,
		extraHashlessArtifactHash,
		extraHashlessArtifactURI,
	)
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.run_artifacts
			(generation_run_id, artifact_id, role)
		VALUES ($1, $2, 'PROXY')`,
		run.RunID,
		extraHashlessArtifactID,
	)

	step.ActivityID, step.ActivityType = "manifest", orchestration.ActivityCreateGate3
	gate3Input := orchestration.CreateGate3Input{
		EpisodeRevisionID: episodeRevisionID.String(), RunIDs: []string{run.RunID},
		GenerationPlanID:              plan.Value.GenerationPlanID,
		PostProductionManifestHash:    postResult.ManifestHash,
		BackgroundAudioAssetVersionID: musicAssetVersionID.String(),
		TraceID:                       step.TraceID,
		PersistProductTruth:           true,
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'REVOKED' WHERE id = $1`,
		consentID,
	)
	_, err = store.BuildEpisodeManifest(ctx, step, gate3Input)
	assertPolicyCode(t, "G3 build after consent revocation", err, controlplane.CodeConsentRequired)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.consent_assets SET status = 'ACTIVE' WHERE id = $1`,
		consentID,
	)
	manifestPayload, err := store.BuildEpisodeManifest(ctx, step, gate3Input)
	if err != nil {
		t.Fatal(err)
	}
	var builtManifest struct {
		PostProductionManifestHash string   `json:"postProductionManifestHash"`
		Outputs                    []string `json:"outputs"`
		ProviderExecutions         []struct {
			Artifacts []struct {
				ID          string `json:"id"`
				ContentHash string `json:"content_hash"`
				ArtifactURI string `json:"artifact_uri"`
				Role        string `json:"role"`
				MediaSpec   struct {
					Kind                       string `json:"kind"`
					PostProductionManifestHash string `json:"postProductionManifestHash"`
				} `json:"media_spec"`
			} `json:"artifacts"`
		} `json:"providerExecutions"`
	}
	if err := json.Unmarshal(manifestPayload, &builtManifest); err != nil {
		t.Fatal(err)
	}
	if builtManifest.PostProductionManifestHash != postResult.ManifestHash ||
		len(builtManifest.Outputs) != 1 ||
		builtManifest.Outputs[0] != postResult.FinalVideo.URI {
		t.Fatalf("G3 current post-production binding = %#v", builtManifest)
	}
	currentPostProductionKinds := make(map[string]int)
	for _, execution := range builtManifest.ProviderExecutions {
		for _, artifact := range execution.Artifacts {
			if artifact.MediaSpec.PostProductionManifestHash == postResult.ManifestHash {
				currentPostProductionKinds[artifact.MediaSpec.Kind]++
			}
		}
	}
	for _, requiredKind := range []string{
		"final_video",
		"subtitle_srt",
		"dialogue_audio",
		"postproduction_manifest",
		"service_bom",
	} {
		if currentPostProductionKinds[requiredKind] != 1 {
			t.Fatalf(
				"G3 current post-production kind %s count = %d, want 1",
				requiredKind,
				currentPostProductionKinds[requiredKind],
			)
		}
	}
	if bytes.Contains(manifestPayload, []byte(oldPostManifestURI)) ||
		bytes.Contains(manifestPayload, []byte(oldFinalVideoURI)) ||
		bytes.Contains(manifestPayload, []byte(oldSubtitleURI)) {
		t.Fatal("G3 manifest leaked an old post-production revision")
	}
	frozenHashlessArtifacts := map[string]struct {
		id, uri, kind, role string
	}{
		providerResult.ArtifactDigest: {
			id: uuid.NewSHA1(
				uuid.NameSpaceOID,
				[]byte("artifact:"+providerResult.ArtifactDigest),
			).String(),
			uri: providerResult.ArtifactURI, kind: "shot_video", role: "OUTPUT",
		},
		extraHashlessArtifactHash: {
			id:  extraHashlessArtifactID.String(),
			uri: extraHashlessArtifactURI, kind: "provider_auxiliary", role: "PROXY",
		},
	}
	for _, execution := range builtManifest.ProviderExecutions {
		for _, artifact := range execution.Artifacts {
			expected, ok := frozenHashlessArtifacts[artifact.ContentHash]
			if !ok {
				continue
			}
			if artifact.ID != expected.id ||
				artifact.ArtifactURI != expected.uri ||
				artifact.Role != expected.role ||
				artifact.MediaSpec.Kind != expected.kind ||
				artifact.MediaSpec.PostProductionManifestHash != "" {
				t.Fatalf("G3 frozen hashless artifact = %#v, want %#v", artifact, expected)
			}
			delete(frozenHashlessArtifacts, artifact.ContentHash)
		}
	}
	if len(frozenHashlessArtifacts) != 0 {
		t.Fatalf("G3 omitted frozen hashless artifacts = %#v", frozenHashlessArtifacts)
	}
	manifestArtifact, err := cas.Put(ctx, bytes.NewReader(manifestPayload))
	if err != nil {
		t.Fatal(err)
	}
	assertNoG3CommitSideEffects := func(test *testing.T, name string) {
		test.Helper()
		var generationManifests, g3Reviews, generationManifestLinks int
		if err := pool.QueryRow(ctx, `
			SELECT
			  (SELECT COUNT(*)
			   FROM video_pipeline.generation_manifests
			   WHERE scope_type = 'EPISODE' AND scope_revision_id = $1),
			  (SELECT COUNT(*)
			   FROM video_pipeline.review_tasks
			   WHERE episode_id = $2 AND review_type = 'G3'),
			  (SELECT COUNT(*)
			   FROM video_pipeline.run_artifacts ra
			   JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
			   WHERE ra.generation_run_id = $3
			     AND a.media_spec->>'kind' = 'generation-manifest')`,
			episodeRevisionID, episodeID, run.RunID,
		).Scan(
			&generationManifests,
			&g3Reviews,
			&generationManifestLinks,
		); err != nil {
			test.Fatal(err)
		}
		if generationManifests != 0 || g3Reviews != 0 || generationManifestLinks != 0 {
			test.Fatalf(
				"%s persisted generation manifests=%d G3=%d new run links=%d",
				name,
				generationManifests,
				g3Reviews,
				generationManifestLinks,
			)
		}
	}
	postProductionDigests := map[string]string{
		"subtitle_srt":            postResult.Subtitle.Digest,
		"dialogue_audio":          postResult.Dialogue.Digest,
		"postproduction_manifest": postResult.Manifest.Digest,
		"service_bom":             postResult.ServiceBOM.Digest,
	}
	for kind, digest := range postProductionDigests {
		kind, digest := kind, digest
		for _, inactiveStatus := range []string{
			"ORPHAN_CANDIDATE",
			"ARCHIVED",
			"DISABLED",
		} {
			inactiveStatus := inactiveStatus
			t.Run(
				"G3 commit rejects "+kind+" "+inactiveStatus,
				func(t *testing.T) {
					mustExec(t, ctx, pool, `
						UPDATE video_pipeline.artifacts
						SET status = $2,
						    orphaned_at = CASE
						      WHEN $2::text = 'ORPHAN_CANDIDATE' THEN now()
						      ELSE orphaned_at
						    END,
						    retention_until = CASE
						      WHEN $2::text = 'ORPHAN_CANDIDATE' THEN now() + interval '1 hour'
						      ELSE retention_until
						    END
						WHERE content_hash = $1`,
						digest, inactiveStatus,
					)
					err := store.CommitEpisodeManifest(
						ctx,
						step,
						gate3Input,
						manifestPayload,
						manifestArtifact,
					)
					assertPolicyCode(
						t,
						"G3 commit "+kind+" "+inactiveStatus,
						err,
						controlplane.CodeGateRequired,
					)
					assertNoG3CommitSideEffects(t, kind+" "+inactiveStatus)
					mustExec(t, ctx, pool, `
						UPDATE video_pipeline.artifacts
						SET status = 'ACTIVE'
						WHERE content_hash = $1`,
						digest,
					)
				},
			)
		}
	}
	for _, inactiveStatus := range []string{
		"ORPHAN_CANDIDATE",
		"ARCHIVED",
		"DISABLED",
	} {
		inactiveStatus := inactiveStatus
		t.Run(
			"G3 commit rejects raw provider output "+inactiveStatus,
			func(t *testing.T) {
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.artifacts
					SET status = $2,
					    orphaned_at = CASE
					      WHEN $2::text = 'ORPHAN_CANDIDATE' THEN now()
					      ELSE orphaned_at
					    END,
					    retention_until = CASE
					      WHEN $2::text = 'ORPHAN_CANDIDATE' THEN now() + interval '1 hour'
					      ELSE retention_until
					    END
					WHERE content_hash = $1`,
					providerResult.ArtifactDigest,
					inactiveStatus,
				)
				err := store.CommitEpisodeManifest(
					ctx,
					step,
					gate3Input,
					manifestPayload,
					manifestArtifact,
				)
				assertPolicyCode(
					t,
					"G3 raw provider output "+inactiveStatus,
					err,
					controlplane.CodeGateRequired,
				)
				assertNoG3CommitSideEffects(t, "raw provider output "+inactiveStatus)
				mustExec(t, ctx, pool, `
					UPDATE video_pipeline.artifacts
					SET status = 'ACTIVE'
					WHERE content_hash = $1`,
					providerResult.ArtifactDigest,
				)
			},
		)
	}
	t.Run("G3 commit rejects inactive hashless payload artifact", func(t *testing.T) {
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.artifacts
			SET status = 'DISABLED'
			WHERE id = $1`,
			extraHashlessArtifactID,
		)
		err := store.CommitEpisodeManifest(
			ctx,
			step,
			gate3Input,
			manifestPayload,
			manifestArtifact,
		)
		assertPolicyCode(
			t,
			"G3 inactive hashless payload artifact",
			err,
			controlplane.CodeGateRequired,
		)
		assertNoG3CommitSideEffects(t, "inactive hashless payload artifact")
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.artifacts
			SET status = 'ACTIVE'
			WHERE id = $1`,
			extraHashlessArtifactID,
		)
	})
	disabledManifestPayload := append(append([]byte(nil), manifestPayload...), '\n')
	disabledManifestArtifact, err := cas.Put(ctx, bytes.NewReader(disabledManifestPayload))
	if err != nil {
		t.Fatal(err)
	}
	disabledManifestArtifactID := uuid.New()
	mustExec(t, ctx, pool, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES ($1, $2, $3, 'application/json', $4,
		        '{"kind":"generation-manifest"}', 'DISABLED')`,
		disabledManifestArtifactID,
		disabledManifestArtifact.Digest,
		disabledManifestArtifact.URI,
		disabledManifestArtifact.Size,
	)
	err = store.CommitEpisodeManifest(
		ctx, step, gate3Input, disabledManifestPayload, disabledManifestArtifact,
	)
	assertPolicyCode(t, "G3 non-ACTIVE CAS conflict", err, controlplane.CodeConflict)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.artifacts
		SET status = 'ARCHIVED'
		WHERE content_hash = $1`,
		postResult.FinalVideo.Digest,
	)
	err = store.CommitEpisodeManifest(ctx, step, gate3Input, manifestPayload, manifestArtifact)
	assertPolicyCode(t, "G3 commit after final video archival", err, controlplane.CodeGateRequired)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.artifacts
		SET status = 'ACTIVE'
		WHERE content_hash = $1`,
		postResult.FinalVideo.Digest,
	)
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() - interval '1 second'
		WHERE id = $1`,
		musicLicenseID,
	)
	err = store.CommitEpisodeManifest(ctx, step, gate3Input, manifestPayload, manifestArtifact)
	assertPolicyCode(t, "G3 commit after license expiry", err, controlplane.CodeLicenseBlocked)
	var rejectedManifests, rejectedG3Reviews int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.generation_manifests
		WHERE scope_type = 'EPISODE' AND scope_revision_id = $1`,
		episodeRevisionID,
	).Scan(&rejectedManifests); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.review_tasks
		WHERE episode_id = $1 AND review_type = 'G3'`,
		episodeID,
	).Scan(&rejectedG3Reviews); err != nil {
		t.Fatal(err)
	}
	if rejectedManifests != 0 || rejectedG3Reviews != 0 {
		t.Fatalf(
			"rejected G3 persisted manifests=%d reviews=%d",
			rejectedManifests, rejectedG3Reviews,
		)
	}
	mustExec(t, ctx, pool, `
		UPDATE video_pipeline.license_snapshots
		SET expires_at = now() + interval '1 hour'
		WHERE id = $1`,
		musicLicenseID,
	)
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
	g3Decision, err := store.CreateApprovalDecision(ctx, g3Command, controlplane.Idempotency{
		Scope: "workflow-projection-g3:" + episodeID.String(),
		Key:   uuid.NewString(), RequestHash: g3Digest,
	}, step.TraceID)
	if err != nil {
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
	var qcReportID, qcReportHash string
	if err := pool.QueryRow(ctx, `
		SELECT id, report_hash
		FROM video_pipeline.qc_reports
		WHERE generation_run_id = $1 AND state = 'PASSED'`,
		run.RunID,
	).Scan(&qcReportID, &qcReportHash); err != nil {
		t.Fatal(err)
	}
	publicationCommand := controlplane.LockPublicationCommand{
		SchemaVersion: "v1",
		ManifestID:    manifest.ManifestID, ManifestHash: manifest.ManifestHash,
		QCReportID: qcReportID, QCReportHash: qcReportHash,
		Gate3DecisionID: g3Decision.Value.DecisionID,
		Actor:           controlplane.Actor{ActorID: "director", Role: "DIRECTOR"},
	}
	publicationDigest, err := digestValue(publicationCommand)
	if err != nil {
		t.Fatal(err)
	}
	publicationIdempotency := controlplane.Idempotency{
		Scope: "workflow-projection-publication:" + run.RunID,
		Key:   uuid.NewString(), RequestHash: publicationDigest,
	}
	publication, err := store.LockPublication(
		ctx, run.RunID, publicationCommand,
		publicationIdempotency, "workflow-projection-publication",
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedPublication, err := store.LockPublication(
		ctx, run.RunID, publicationCommand,
		publicationIdempotency, "workflow-projection-publication",
	)
	if err != nil {
		t.Fatal(err)
	}
	if publication.Value.RunID != run.RunID ||
		publication.Value.ManifestID != manifest.ManifestID ||
		publication.Value.QCReportID != qcReportID ||
		publication.Value.Gate3DecisionID != g3Decision.Value.DecisionID ||
		!replayedPublication.Replayed ||
		replayedPublication.Value.PublicationLockID !=
			publication.Value.PublicationLockID {
		t.Fatalf(
			"publication lock = first:%#v replay:%#v",
			publication, replayedPublication,
		)
	}
	stalePublicationCommand := publicationCommand
	stalePublicationCommand.QCReportHash = strings.Repeat("f", 64)
	stalePublicationDigest, _ := digestValue(stalePublicationCommand)
	if _, err := store.LockPublication(
		ctx, run.RunID, stalePublicationCommand,
		controlplane.Idempotency{
			Scope: "workflow-projection-publication-stale:" + run.RunID,
			Key:   uuid.NewString(), RequestHash: stalePublicationDigest,
		},
		"workflow-projection-publication-stale",
	); err == nil {
		t.Fatal("publication lock accepted a stale QC hash")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) ||
			domain.Code != controlplane.CodeGateRequired {
			t.Fatalf("stale publication lock error = %#v", err)
		}
	}

	publicDigest, err := digestValue(publicCommand)
	if err != nil {
		t.Fatal(err)
	}
	if temporalAddress := os.Getenv("VIDEO_TEST_TEMPORAL_ADDRESS"); temporalAddress != "" {
		// This monolithic integration scenario has already exercised G3 and
		// publication locking above. Reset only the fixture lifecycle state so
		// the independent pause/outage cases still model a pre-G3 paid submit;
		// PrepareProviderJob must reject a genuinely G3_LOCKED revision.
		mustExec(t, ctx, pool, `
			UPDATE video_pipeline.episode_revisions
			SET status = 'G2_APPROVED'
			WHERE id = $1`,
			episodeRevisionID,
		)
		testHTTPTemporalRecoveryAndPause(
			t,
			ctx,
			pool,
			store,
			temporalAddress,
			os.Getenv("VIDEO_TEST_PROVIDER_URL"),
			shotID.String(),
			publicCommand,
			temporalPauseShotID,
			temporalPauseCommand,
			composeOutageShotID,
			composeOutageCommand,
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

func testHTTPTemporalRecoveryAndPause(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *Postgres,
	temporalAddress string,
	_ string,
	shotID string,
	command controlplane.CreateGenerationRunCommand,
	pauseShotID string,
	pauseCommand controlplane.CreateGenerationRunCommand,
	composeOutageShotID string,
	composeOutageCommand controlplane.CreateGenerationRunCommand,
) {
	t.Helper()
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
	artifactRoot := t.TempDir()
	artifacts, err := artifactstore.New(artifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	provider := mockprovider.New(runtimeconfig.MockProvider{
		ProviderID:   "integration-recoverable-provider",
		Capabilities: []string{"video.primary"},
	}, artifacts)
	var providerUp atomic.Bool
	providerUp.Store(true)
	var holdFirstPoll atomic.Bool
	holdFirstPoll.Store(true)
	firstPollStarted := make(chan struct{})
	releaseFirstPoll := make(chan struct{})
	var firstPollOnce sync.Once
	var interruptOutagePoll atomic.Bool
	outagePollInterrupted := make(chan struct{})
	providerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !providerUp.Load() {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("provider response writer does not support connection interruption")
				return
			}
			connection, _, hijackErr := hijacker.Hijack()
			if hijackErr == nil {
				_ = connection.Close()
			}
			return
		}
		if r.Method == http.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/v1/jobs/") &&
			holdFirstPoll.Load() {
			firstPollOnce.Do(func() { close(firstPollStarted) })
			select {
			case <-releaseFirstPoll:
			case <-r.Context().Done():
				return
			}
		}
		provider.Handler().ServeHTTP(w, r)
	}))
	defer providerServer.Close()

	newWorker := func(identity string) worker.Worker {
		temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{Identity: identity})
		temporalWorker.RegisterWorkflowWithOptions(
			orchestration.ShotProductionWorkflow,
			workflow.RegisterOptions{Name: orchestration.ShotWorkflowName},
		)
		temporalWorker.RegisterWorkflowWithOptions(
			orchestration.ShotReconciliationWorkflow,
			workflow.RegisterOptions{Name: orchestration.ShotReconciliationWorkflowName},
		)
		activities := orchestration.NewProductionActivities(providerServer.URL, store, store, artifacts)
		baseTransport := activities.HTTPClient.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		activities.HTTPClient.Transport = &cancelRequestedProviderFailureTransport{
			base: baseTransport, enabled: &interruptOutagePoll, started: outagePollInterrupted,
		}
		temporalWorker.RegisterActivityWithOptions(
			activities.ExecuteProviderJob,
			activity.RegisterOptions{Name: orchestration.ActivityExecuteProviderJob},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.CancelProviderJob,
			activity.RegisterOptions{Name: orchestration.ActivityCancelProviderJob},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.RunAutomaticQC,
			activity.RegisterOptions{Name: orchestration.ActivityRunAutomaticQC},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.CreateShotReview,
			activity.RegisterOptions{Name: orchestration.ActivityCreateShotReview},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.EscalateShot,
			activity.RegisterOptions{Name: orchestration.ActivityEscalateShot},
		)
		temporalWorker.RegisterActivityWithOptions(
			activities.FinalizeShotRun,
			activity.RegisterOptions{Name: orchestration.ActivityFinalizeShotRun},
		)
		return temporalWorker
	}

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
	createRun := func(targetShotID string, createCommand controlplane.CreateGenerationRunCommand) controlplane.Operation {
		t.Helper()
		body, marshalErr := json.Marshal(createCommand)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		response := executeIntegrationRequest(
			t,
			http.MethodPost,
			api.URL+controlplane.APIBase+"/shots/"+targetShotID+"/runs",
			body,
			map[string]string{
				"Idempotency-Key": uuid.NewString(),
				"If-Match":        `"1"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("create status=%d body=%s", response.StatusCode, response.Body)
		}
		var operation controlplane.Operation
		if unmarshalErr := json.Unmarshal(response.Body, &operation); unmarshalErr != nil {
			t.Fatalf("decode create operation: %v body=%s", unmarshalErr, response.Body)
		}
		if operation.State != "RUNNING" || operation.TemporalWorkflowID == "" {
			t.Fatalf("create operation = %#v", operation)
		}
		return operation
	}

	firstWorker := newWorker("pause-and-outage-worker-" + uuid.NewString())
	if err := firstWorker.Start(); err != nil {
		t.Fatalf("start initial Temporal worker: %v", err)
	}
	firstWorkerStopped := false
	defer func() {
		if !firstWorkerStopped {
			firstWorker.Stop()
		}
	}()

	// Real HTTP + Temporal pause/resume: hold the provider's terminal poll,
	// persist and deliver PAUSE, then resume the same stable provider JobID.
	pauseCreate := createRun(pauseShotID, pauseCommand)
	select {
	case <-firstPollStarted:
	case <-ctx.Done():
		t.Fatalf("provider poll was not reached before pause: %v", ctx.Err())
	}
	pauseResponse := executeIntegrationRequest(
		t,
		http.MethodPost,
		api.URL+controlplane.APIBase+"/runs/"+pauseCreate.AggregateID+"/pause",
		[]byte(`{"actor":{"actorId":"operator","role":"OPERATOR"},"reasonCode":"QA_PAUSE"}`),
		map[string]string{
			"Idempotency-Key": uuid.NewString(),
			"If-Match":        `"1"`,
		},
	)
	if pauseResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("pause status=%d body=%s", pauseResponse.StatusCode, pauseResponse.Body)
	}
	pauseDeadline := time.Now().Add(10 * time.Second)
	for {
		var workflowStatus orchestration.WorkflowStatus
		value, queryErr := temporalClient.QueryWorkflow(
			ctx, pauseCreate.TemporalWorkflowID, "", orchestration.StatusQuery,
		)
		if queryErr == nil {
			queryErr = value.Get(&workflowStatus)
		}
		if queryErr == nil && workflowStatus.Paused {
			break
		}
		if time.Now().After(pauseDeadline) {
			t.Fatalf("Temporal did not observe PAUSE: status=%#v error=%v", workflowStatus, queryErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	holdFirstPoll.Store(false)
	close(releaseFirstPoll)
	resumeBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"recoveryMode":"RESUME_PAUSED"}`,
	)
	resumeKey := uuid.NewString()
	for attempt := 1; attempt <= 2; attempt++ {
		response := executeIntegrationRequest(
			t,
			http.MethodPost,
			api.URL+controlplane.APIBase+"/runs/"+pauseCreate.AggregateID+"/resume",
			resumeBody,
			map[string]string{
				"Idempotency-Key": resumeKey,
				"If-Match":        `"1"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("resume attempt %d status=%d body=%s", attempt, response.StatusCode, response.Body)
		}
	}
	q1Deadline := time.Now().Add(15 * time.Second)
	for {
		var runState string
		var providerJobs, passedQC, openQ1, resumeOperations int
		if err := pool.QueryRow(ctx, `
			SELECT
			  (SELECT state FROM video_pipeline.generation_runs WHERE id = $1),
			  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
			   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $1),
			  (SELECT COUNT(*) FROM video_pipeline.qc_reports
			   WHERE generation_run_id = $1 AND state = 'PASSED'),
			  (SELECT COUNT(*) FROM video_pipeline.review_tasks
			   WHERE generation_run_id = $1 AND review_type = 'Q1' AND state = 'OPEN'),
			  (SELECT COUNT(*) FROM video_pipeline.operation_requests
			   WHERE aggregate_id = $1 AND operation_type = 'RESUME_GENERATION_RUN')`,
			pauseCreate.AggregateID,
		).Scan(&runState, &providerJobs, &passedQC, &openQ1, &resumeOperations); err != nil {
			t.Fatal(err)
		}
		if runState == "SUCCEEDED" && providerJobs == 1 && passedQC == 1 &&
			openQ1 == 1 && resumeOperations == 1 {
			break
		}
		if time.Now().After(q1Deadline) {
			t.Fatalf(
				"pause/resume did not converge: run=%s providerJobs=%d qc=%d q1=%d resumeOps=%d",
				runState, providerJobs, passedQC, openQ1, resumeOperations,
			)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := temporalClient.CancelWorkflow(ctx, pauseCreate.TemporalWorkflowID, ""); err != nil {
		t.Fatal(err)
	}
	if err := temporalClient.GetWorkflow(ctx, pauseCreate.TemporalWorkflowID, "").Get(ctx, nil); err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("pause workflow cleanup error = %v, want Canceled", err)
	}

	// Submit the provider job while the adapter is healthy, then interrupt its
	// first poll and let Temporal observe the retryable network failure before
	// cancellation. This reproduces the production history shape where an
	// ActivityFailure with RetryState=CancelRequested carries the provider
	// network error instead of a CanceledError.
	interruptOutagePoll.Store(true)
	createOperation := createRun(shotID, command)
	select {
	case <-outagePollInterrupted:
	case <-ctx.Done():
		t.Fatalf("provider job was not submitted and polled before network cancellation: %v", ctx.Err())
	}
	// The submitted job is now blocked in its provider poll. Make every
	// compensation request fail as well; cancelling the Workflow releases the
	// poll with a DNS error, which Temporal records as ActivityFailure with
	// RetryState=CancelRequested rather than CanceledError.
	providerUp.Store(false)
	cancelBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"reasonCode":"NETWORK_CANCEL"}`,
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
			t.Fatalf("cancel attempt %d status=%d body=%s", attempt, response.StatusCode, response.Body)
		}
		if err := json.Unmarshal(response.Body, &cancelOperation); err != nil {
			t.Fatalf("decode cancel attempt %d: %v", attempt, err)
		}
	}
	workflowRun := temporalClient.GetWorkflow(ctx, createOperation.TemporalWorkflowID, "")
	if err := workflowRun.Get(ctx, nil); err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("workflow completion error = %v, want Canceled", err)
	}
	history := temporalClient.GetWorkflowHistory(
		ctx,
		createOperation.TemporalWorkflowID,
		"",
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	sawCancelRequestedActivityFailure := false
	for history.HasNext() {
		event, historyErr := history.Next()
		if historyErr != nil {
			t.Fatalf("read cancelled workflow history: %v", historyErr)
		}
		failed := event.GetActivityTaskFailedEventAttributes()
		if failed != nil && failed.RetryState == enumspb.RETRY_STATE_CANCEL_REQUESTED {
			sawCancelRequestedActivityFailure = true
			break
		}
	}
	if !sawCancelRequestedActivityFailure {
		t.Fatal("workflow history did not retain ActivityFailure with RetryState=CancelRequested")
	}
	unknownRun, err := store.GetGenerationRun(ctx, createOperation.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	if unknownRun.State != "UNKNOWN" ||
		unknownRun.FailureCode != "CANCEL_NOT_CONFIRMED" {
		t.Fatalf("network cancellation run = %#v", unknownRun)
	}
	// Restart only after Temporal has durably closed the original execution.
	// The replacement process owns the independently started reconciler and
	// replays its Activity journal without depending on prior worker memory.
	firstWorker.Stop()
	firstWorkerStopped = true
	replacementWorker := newWorker("reconciliation-worker-" + uuid.NewString())
	if err := replacementWorker.Start(); err != nil {
		t.Fatalf("start replacement Temporal worker: %v", err)
	}
	defer replacementWorker.Stop()
	interruptOutagePoll.Store(false)
	providerUp.Store(true)
	reconcileBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"recoveryMode":"RECONCILE_HISTORY"}`,
	)
	reconcileKey := uuid.NewString()
	var reconcileOperation controlplane.Operation
	for attempt := 1; attempt <= 2; attempt++ {
		response := executeIntegrationRequest(
			t,
			http.MethodPost,
			api.URL+controlplane.APIBase+"/runs/"+createOperation.AggregateID+"/resume",
			reconcileBody,
			map[string]string{
				"Idempotency-Key": reconcileKey,
				"If-Match":        `"2"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("reconcile attempt %d status=%d body=%s", attempt, response.StatusCode, response.Body)
		}
		if err := json.Unmarshal(response.Body, &reconcileOperation); err != nil {
			t.Fatalf("decode reconcile attempt %d: %v", attempt, err)
		}
	}
	recoveryDeadline := time.Now().Add(15 * time.Second)
	for {
		run, getErr := store.GetGenerationRun(ctx, createOperation.AggregateID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		createState, cancelState, recoveryState, providerState := "", "", "", ""
		if err := pool.QueryRow(ctx, `
			SELECT
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $1),
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $2),
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $3),
			  (SELECT pj.state FROM video_pipeline.provider_jobs pj
			   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $4)`,
			createOperation.OperationID,
			cancelOperation.OperationID,
			reconcileOperation.OperationID,
			createOperation.AggregateID,
		).Scan(&createState, &cancelState, &recoveryState, &providerState); err != nil {
			t.Fatal(err)
		}
		if run.State == "CANCELLED" &&
			createState == "CANCELLED" &&
			cancelState == "SUCCEEDED" &&
			recoveryState == "SUCCEEDED" &&
			providerState == "CANCELLED" {
			if run.FailureClass != "" || run.FailureCode != "" {
				t.Fatalf("reconciled run retained failure = %#v", run)
			}
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatalf(
				"recovery did not converge: run=%#v create=%s cancel=%s recovery=%s provider=%s",
				run, createState, cancelState, recoveryState, providerState,
			)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var providerJobs, recoveryOperations int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
		   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.operation_requests
		   WHERE aggregate_id = $1 AND operation_type = 'RESUME_GENERATION_RUN')`,
		createOperation.AggregateID,
	).Scan(&providerJobs, &recoveryOperations); err != nil {
		t.Fatal(err)
	}
	if providerJobs != 1 || recoveryOperations != 1 {
		t.Fatalf(
			"recovery duplicated durable facts: providerJobs=%d recoveryOperations=%d",
			providerJobs, recoveryOperations,
		)
	}
	if os.Getenv("VIDEO_TEST_PROVIDER_CONTAINER") != "" {
		testComposeProviderPollingCancellation(
			t,
			ctx,
			pool,
			store,
			temporalClient,
			composeOutageShotID,
			composeOutageCommand,
		)
	}
}

func testComposeProviderPollingCancellation(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *Postgres,
	temporalClient temporalclient.Client,
	composeShotID string,
	composeCommand controlplane.CreateGenerationRunCommand,
) {
	t.Helper()
	providerContainer := os.Getenv("VIDEO_TEST_PROVIDER_CONTAINER")
	workerContainer := os.Getenv("VIDEO_TEST_WORKER_CONTAINER")
	taskQueue := os.Getenv("VIDEO_TEST_COMPOSE_TASK_QUEUE")
	if taskQueue == "" {
		taskQueue = "video-production-v1"
	}
	startProvider := func(commandContext context.Context) error {
		output, err := exec.CommandContext(
			commandContext, "docker", "start", providerContainer,
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("docker start %s: %w: %s", providerContainer, err, output)
		}
		return nil
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := startProvider(cleanupContext); err != nil {
			t.Errorf("restore Compose provider: %v", err)
		}
	})
	if err := startProvider(ctx); err != nil {
		t.Fatal(err)
	}

	controller, err := temporalcontrol.New(temporalClient, taskQueue, store)
	if err != nil {
		t.Fatal(err)
	}
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
	createBody, err := json.Marshal(composeCommand)
	if err != nil {
		t.Fatal(err)
	}
	createResponse := executeIntegrationRequest(
		t,
		http.MethodPost,
		api.URL+controlplane.APIBase+"/shots/"+composeShotID+"/runs",
		createBody,
		map[string]string{
			"Idempotency-Key": uuid.NewString(),
			"If-Match":        `"1"`,
		},
	)
	if createResponse.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"Compose create status=%d body=%s",
			createResponse.StatusCode,
			createResponse.Body,
		)
	}
	var createOperation controlplane.Operation
	if err := json.Unmarshal(createResponse.Body, &createOperation); err != nil {
		t.Fatalf("decode Compose create operation: %v", err)
	}
	providerJobDeadline := time.Now().Add(15 * time.Second)
	durableTaskID, durableRequestID := "", ""
	for {
		var providerJobs int
		if err := pool.QueryRow(ctx, `
			SELECT COUNT(*),
			       COALESCE(MAX(pj.upstream_task_id), ''),
			       COALESCE(MAX(pj.upstream_request_id), '')
			FROM video_pipeline.provider_jobs pj
			JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			WHERE ga.generation_run_id = $1`,
			createOperation.AggregateID,
		).Scan(&providerJobs, &durableTaskID, &durableRequestID); err != nil {
			t.Fatal(err)
		}
		if providerJobs == 1 && durableTaskID != "" && durableRequestID != "" {
			break
		}
		if time.Now().After(providerJobDeadline) {
			t.Fatalf(
				"Compose worker did not durably observe the provider identity: jobs=%d task=%q request=%q",
				providerJobs, durableTaskID, durableRequestID,
			)
		}
		time.Sleep(25 * time.Millisecond)
	}
	// PrepareProviderJob precedes the provider POST. The mock POST is local and
	// immediate; stopping after this short window leaves the Activity in its
	// mandatory 500 ms poll delay with an already-submitted stable JobID.
	time.Sleep(150 * time.Millisecond)
	stopOutput, err := exec.CommandContext(
		ctx, "docker", "stop", "--time", "1", providerContainer,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("stop Compose provider: %v: %s", err, stopOutput)
	}
	// Let the worker enter the failed poll/retry path before cancellation.
	time.Sleep(750 * time.Millisecond)

	cancelBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"reasonCode":"COMPOSE_DNS_CANCEL"}`,
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
				"If-Match":        `"1"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf(
				"Compose cancel attempt %d status=%d body=%s",
				attempt,
				response.StatusCode,
				response.Body,
			)
		}
		if err := json.Unmarshal(response.Body, &cancelOperation); err != nil {
			t.Fatalf("decode Compose cancel operation: %v", err)
		}
	}
	if err := temporalClient.GetWorkflow(
		ctx, createOperation.TemporalWorkflowID, "",
	).Get(ctx, nil); err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("Compose outage workflow completion = %v, want Canceled", err)
	}
	unknownRun, err := store.GetGenerationRun(ctx, createOperation.AggregateID)
	if err != nil {
		t.Fatal(err)
	}
	if unknownRun.State != "UNKNOWN" ||
		unknownRun.FailureCode != "CANCEL_NOT_CONFIRMED" {
		t.Fatalf("Compose outage cancellation run = %#v", unknownRun)
	}

	if workerContainer != "" {
		restartOutput, restartErr := exec.CommandContext(
			ctx, "docker", "restart", workerContainer,
		).CombinedOutput()
		if restartErr != nil {
			t.Fatalf("restart Compose worker: %v: %s", restartErr, restartOutput)
		}
	}
	if err := startProvider(ctx); err != nil {
		t.Fatal(err)
	}
	reconcileBody := []byte(
		`{"actor":{"actorId":"operator","role":"OPERATOR"},"recoveryMode":"RECONCILE_HISTORY"}`,
	)
	reconcileKey := uuid.NewString()
	var reconcileOperation controlplane.Operation
	for attempt := 1; attempt <= 2; attempt++ {
		response := executeIntegrationRequest(
			t,
			http.MethodPost,
			api.URL+controlplane.APIBase+"/runs/"+createOperation.AggregateID+"/resume",
			reconcileBody,
			map[string]string{
				"Idempotency-Key": reconcileKey,
				"If-Match":        `"1"`,
			},
		)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf(
				"Compose reconcile attempt %d status=%d body=%s",
				attempt,
				response.StatusCode,
				response.Body,
			)
		}
		if err := json.Unmarshal(response.Body, &reconcileOperation); err != nil {
			t.Fatalf("decode Compose reconcile operation: %v", err)
		}
	}
	// The restarted mock adapter intentionally lost its in-memory registry and
	// returns 404 for the stable JobID. PostgreSQL still proves that a paid
	// upstream task/request existed, so reconciliation must remain retryable and
	// retain the full reservation; adapter absence is not terminal evidence.
	recoveryDeadline := time.Now().Add(15 * time.Second)
	retryingCancellationObserved := false
	for {
		run, getErr := store.GetGenerationRun(ctx, createOperation.AggregateID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		createState, cancelState, recoveryState := "", "", ""
		attemptState, providerState, reservationState := "", "", ""
		storedTaskID, storedRequestID := "", ""
		terminalLedger := 0
		if err := pool.QueryRow(ctx, `
			SELECT
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $1),
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $2),
			  (SELECT state FROM video_pipeline.operation_requests WHERE id = $3),
			  ga.state, pj.state, br.status,
			  COALESCE(pj.upstream_task_id, ''),
			  COALESCE(pj.upstream_request_id, ''),
			  COUNT(*) FILTER (WHERE cl.entry_type IN ('ACTUAL', 'RELEASE'))
			FROM video_pipeline.generation_attempts ga
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id = ga.id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			LEFT JOIN video_pipeline.cost_ledger cl ON cl.provider_job_id = pj.id
			WHERE ga.generation_run_id = $4
			GROUP BY ga.state, pj.state, br.status,
			         pj.upstream_task_id, pj.upstream_request_id`,
			createOperation.OperationID,
			cancelOperation.OperationID,
			reconcileOperation.OperationID,
			createOperation.AggregateID,
		).Scan(
			&createState, &cancelState, &recoveryState,
			&attemptState, &providerState, &reservationState,
			&storedTaskID, &storedRequestID, &terminalLedger,
		); err != nil {
			t.Fatal(err)
		}
		description, describeErr := temporalClient.DescribeWorkflowExecution(
			ctx, reconcileOperation.TemporalWorkflowID, "",
		)
		if describeErr != nil {
			t.Fatalf("describe Compose reconciliation: %v", describeErr)
		}
		for _, pending := range description.GetPendingActivities() {
			if pending.GetActivityType().GetName() == orchestration.ActivityCancelProviderJob &&
				pending.GetLastFailure() != nil {
				retryingCancellationObserved = true
				break
			}
		}
		if run.State == "RECONCILING" && attemptState == "UNKNOWN" &&
			providerState == "UNKNOWN" && reservationState == "RESERVED" &&
			storedTaskID == durableTaskID && storedRequestID == durableRequestID &&
			terminalLedger == 0 && retryingCancellationObserved {
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatalf(
				"Compose 404 safety did not hold: run=%#v create=%s cancel=%s recovery=%s attempt=%s provider=%s reservation=%s task=%q request=%q terminalLedger=%d retryingCancellation=%t",
				run,
				createState,
				cancelState,
				recoveryState,
				attemptState,
				providerState,
				reservationState,
				storedTaskID,
				storedRequestID,
				terminalLedger,
				retryingCancellationObserved,
			)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := temporalClient.CancelWorkflow(
		ctx, reconcileOperation.TemporalWorkflowID, "",
	); err != nil {
		t.Fatalf("cancel Compose reconciliation fixture: %v", err)
	}
	if err := temporalClient.GetWorkflow(
		ctx, reconcileOperation.TemporalWorkflowID, "",
	).Get(ctx, nil); err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("Compose reconciliation cleanup = %v, want Canceled", err)
	}
	recoveryHistory := temporalClient.GetWorkflowHistory(
		ctx,
		reconcileOperation.TemporalWorkflowID,
		"",
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	for recoveryHistory.HasNext() {
		event, historyErr := recoveryHistory.Next()
		if historyErr != nil {
			t.Fatalf("read Compose reconciliation history: %v", historyErr)
		}
		scheduled := event.GetActivityTaskScheduledEventAttributes()
		if scheduled != nil && scheduled.ActivityType.GetName() == orchestration.ActivityExecuteProviderJob {
			t.Fatal("Compose reconciliation scheduled a second paid provider POST")
		}
	}
	var providerJobs, cancelOperations, recoveryOperations int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM video_pipeline.provider_jobs pj
		   JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1),
		  (SELECT COUNT(*) FROM video_pipeline.operation_requests
		   WHERE aggregate_id = $1 AND operation_type = 'CANCEL_GENERATION_RUN'),
		  (SELECT COUNT(*) FROM video_pipeline.operation_requests
		   WHERE aggregate_id = $1 AND operation_type = 'RESUME_GENERATION_RUN')`,
		createOperation.AggregateID,
	).Scan(&providerJobs, &cancelOperations, &recoveryOperations); err != nil {
		t.Fatal(err)
	}
	if providerJobs != 1 || cancelOperations != 1 || recoveryOperations != 1 {
		t.Fatalf(
			"Compose 404 safety duplicated durable facts: providerJobs=%d cancelOperations=%d recoveryOperations=%d",
			providerJobs,
			cancelOperations,
			recoveryOperations,
		)
	}
}

func cloneIntegrationShotCommand(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *Postgres,
	sourceShotID string,
	source controlplane.CreateGenerationRunCommand,
) (string, controlplane.CreateGenerationRunCommand) {
	t.Helper()
	newShotID := uuid.New()
	newShotRevisionID := uuid.New()
	newGate2ID := uuid.New()
	newShotHash := strings.Repeat(
		strings.ReplaceAll(uuid.NewString(), "-", ""),
		2,
	)
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.shots (id, scene_id, ordinal)
		SELECT $2, source.scene_id,
		       (SELECT MAX(ordinal) + 1 FROM video_pipeline.shots WHERE scene_id = source.scene_id)
		FROM video_pipeline.shots source
		WHERE source.id = $1`,
		sourceShotID, newShotID,
	); err != nil {
		t.Fatalf("clone integration shot: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.approval_decisions
		  (id, series_id, episode_id, gate, decision, reason_code,
		   actor_id, actor_role, trace_id)
		SELECT $3, source.series_id, source.episode_id, 'G2', 'APPROVED',
		       'integration-clone', 'integration-director', 'DIRECTOR',
		       'integration-clone-' || $2::text
		FROM video_pipeline.shot_spec_revisions ssr
		JOIN video_pipeline.approval_decisions source
		  ON source.id = ssr.gate2_decision_id
		WHERE ssr.id = $1`,
		source.ShotSpecRevisionID, newShotID, newGate2ID,
	); err != nil {
		t.Fatalf("clone integration G2 decision: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.shot_spec_revisions
		  (id, shot_id, storyboard_revision_id, revision, parent_revision_id,
		   lifecycle_state, freshness, duration_ms, aspect_profile, fps, width, height,
		   cast_count, primary_action_count, narrative, asset_version_refs,
		   context_revision_ids, effective_context_hash, continuity, cinematography,
		   generation_profile_id, gate2_decision_id, content_hash, created_by)
		SELECT $2, $3, storyboard_revision_id, 1, NULL,
		       'READY', 'FRESH', duration_ms, aspect_profile, fps, width, height,
		       cast_count, primary_action_count, narrative, asset_version_refs,
		       context_revision_ids, $4, continuity, cinematography,
		       generation_profile_id, $5, $4, 'integration-clone'
		FROM video_pipeline.shot_spec_revisions
		WHERE id = $1`,
		source.ShotSpecRevisionID, newShotRevisionID, newShotID, newShotHash, newGate2ID,
	); err != nil {
		t.Fatalf("clone integration shot revision: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.approval_bindings
		  (decision_id, object_type, revision_id, content_hash)
		SELECT $1::uuid, original.object_type, original.revision_id, original.content_hash
		FROM video_pipeline.shot_spec_revisions ssr
		JOIN video_pipeline.approval_bindings original
		  ON original.decision_id = ssr.gate2_decision_id
		 AND original.object_type = 'EPISODE_REVISION'
		WHERE ssr.id = $2
		UNION ALL
		SELECT $1::uuid, 'SHOT_SPEC_REVISION', $3::uuid, $4`,
		newGate2ID, source.ShotSpecRevisionID, newShotRevisionID, newShotHash,
	); err != nil {
		t.Fatalf("clone integration G2 bindings: %v", err)
	}
	clonedPrompt, err := store.CompilePromptSnapshot(
		ctx,
		orchestration.WorkflowStep{
			WorkflowID:   "integration-clone-" + newShotID.String(),
			ActivityID:   "compile-prompt",
			ActivityType: orchestration.ActivityCompilePrompt,
			TraceID:      "integration-clone-" + newShotID.String(),
		},
		orchestration.CompilePromptInput{
			ShotSpecRevisionID:   newShotRevisionID.String(),
			GenerationProfileRef: source.GenerationProfileRevisionID,
			TraceID:              "integration-clone-" + newShotID.String(),
			PersistProductTruth:  true,
		},
	)
	if err != nil {
		t.Fatalf("compile cloned integration prompt: %v", err)
	}

	planRecord, err := store.GetGenerationPlan(ctx, source.GenerationPlanID)
	if err != nil {
		t.Fatal(err)
	}
	var episodeID, episodeHash string
	if err := pool.QueryRow(ctx, `
		SELECT episode_id, content_hash
		FROM video_pipeline.episode_revisions
		WHERE id = $1`,
		planRecord.EpisodeRevisionID,
	).Scan(&episodeID, &episodeHash); err != nil {
		t.Fatal(err)
	}
	bindings := []controlplane.ApprovalBinding{
		{
			ObjectType:  "EPISODE_REVISION",
			RevisionID:  planRecord.EpisodeRevisionID,
			ContentHash: episodeHash,
		},
		{
			ObjectType:  "SHOT_SPEC_REVISION",
			RevisionID:  newShotRevisionID.String(),
			ContentHash: newShotHash,
		},
	}
	rows, err := pool.Query(ctx, `
		SELECT revision_id, content_hash
		FROM video_pipeline.approval_bindings
		WHERE decision_id = $1 AND object_type = 'ARTIFACT'
		ORDER BY revision_id`,
		source.ExecutionPolicy.ContentSafetyDecisionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	var evidenceHash string
	for rows.Next() {
		var artifactID, artifactHash string
		if err := rows.Scan(&artifactID, &artifactHash); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if evidenceHash == "" {
			evidenceHash = artifactHash
		}
		bindings = append(bindings, controlplane.ApprovalBinding{
			ObjectType: "ARTIFACT", RevisionID: artifactID, ContentHash: artifactHash,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	validUntil := time.Now().UTC().Add(time.Hour)
	safetyCommand := controlplane.CreateApprovalDecisionCommand{
		SchemaVersion: "v1",
		SeriesID:      planRecord.SeriesID,
		EpisodeID:     episodeID,
		Gate:          "SAFETY",
		Decision:      "APPROVED",
		ReasonCode:    "CONTENT_SAFETY_APPROVED",
		PolicyVersion: source.ExecutionPolicy.ContentSafetyPolicyVersion,
		EvidenceHash:  evidenceHash,
		ValidUntil:    &validUntil,
		Bindings:      bindings,
		Actor: controlplane.Actor{
			ActorID: "integration-safety-reviewer",
			Role:    "SAFETY_REVIEWER",
		},
	}
	safetyDigest, err := digestValue(safetyCommand)
	if err != nil {
		t.Fatal(err)
	}
	safety, err := store.CreateApprovalDecision(
		ctx,
		safetyCommand,
		controlplane.Idempotency{
			Scope:       "integration-clone-safety:" + newShotID.String(),
			Key:         uuid.NewString(),
			RequestHash: safetyDigest,
		},
		"integration-clone-safety",
	)
	if err != nil {
		t.Fatal(err)
	}
	executionPolicy := source.ExecutionPolicy
	executionPolicy.ContentSafetyDecisionID = safety.Value.DecisionID
	planCommand := controlplane.CreateGenerationPlanCommand{
		SchemaVersion:       "v1",
		SeriesID:            planRecord.SeriesID,
		EpisodeRevisionID:   planRecord.EpisodeRevisionID,
		ShotSpecRevisionIDs: []string{newShotRevisionID.String()},
		CandidatesPerShot:   1,
		RouteSnapshot:       source.RouteSnapshot,
		BudgetLimit:         planRecord.BudgetLimit,
		ExecutionPolicy:     executionPolicy,
		Actor: controlplane.Actor{
			ActorID: "integration-producer",
			Role:    "PRODUCER",
		},
	}
	planDigest, err := digestValue(planCommand)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := store.CreateGenerationPlan(
		ctx,
		planCommand,
		controlplane.Idempotency{
			Scope:       "integration-clone-plan:" + newShotID.String(),
			Key:         uuid.NewString(),
			RequestHash: planDigest,
		},
		"integration-clone-plan",
	)
	if err != nil {
		t.Fatal(err)
	}
	clonedBudgetID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO video_pipeline.review_tasks
			(id, series_id, episode_id, review_type, state, assigned_role,
			 decided_at, generation_plan_id, budget_scope,
			 budget_limit_micros, budget_currency)
		VALUES ($1, $2, $3, 'BUDGET', 'APPROVED', 'PRODUCER',
		        now(), $4, 'VIDEO', $5, $6)`,
		clonedBudgetID,
		planRecord.SeriesID,
		episodeID,
		plan.Value.GenerationPlanID,
		planRecord.BudgetLimit.AmountMicros,
		planRecord.BudgetLimit.Currency,
	); err != nil {
		t.Fatal(err)
	}
	cloned := source
	cloned.ShotSpecRevisionID = newShotRevisionID.String()
	cloned.PromptSnapshotID = clonedPrompt.ID
	cloned.GenerationPlanID = plan.Value.GenerationPlanID
	cloned.BudgetApprovalID = clonedBudgetID.String()
	cloned.ExecutionPolicy = executionPolicy
	cloned.CreativeAttempt = 1
	return newShotID.String(), cloned
}

func createIntegrationWorkflowRun(
	t *testing.T,
	ctx context.Context,
	store *Postgres,
	label string,
	command controlplane.CreateGenerationRunCommand,
) (
	orchestration.WorkflowStep,
	orchestration.GenerationRunRef,
	orchestration.ExecuteProviderJobInput,
) {
	t.Helper()
	prompt, err := store.ResolvePromptSnapshot(ctx, command.PromptSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := store.GetGenerationPlan(ctx, command.GenerationPlanID)
	if err != nil {
		t.Fatal(err)
	}
	model := providercontract.ModelSnapshot{
		CapabilityAlias: command.RouteSnapshot.CapabilityAlias,
		Provider:        command.RouteSnapshot.Provider,
		ModelID:         command.RouteSnapshot.ModelID,
		EndpointID:      command.RouteSnapshot.EndpointID,
		RouteVersion:    command.RouteSnapshot.RouteVersion,
		CapabilityHash:  command.RouteSnapshot.CapabilityHash,
		Verification:    "integration",
	}
	step := orchestration.WorkflowStep{
		WorkflowID:   "integration-workflow-" + label + "-" + uuid.NewString(),
		ActivityID:   "create-run",
		ActivityType: orchestration.ActivityCreateRun,
		TraceID:      "integration-" + label,
	}
	run, err := store.CreateWorkflowRun(ctx, step, orchestration.CreateRunInput{
		ShotSpecRevisionID:   command.ShotSpecRevisionID,
		PromptSnapshot:       prompt,
		GenerationProfileRef: command.GenerationProfileRevisionID,
		Route:                model,
		GenerationPlanID:     command.GenerationPlanID,
		BudgetApprovalID:     command.BudgetApprovalID,
		ProviderProfileID:    command.RouteSnapshot.ProviderProfileID,
		CreativeAttempt:      command.CreativeAttempt,
		TraceID:              step.TraceID,
		PersistProductTruth:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	step.ActivityID = "provider"
	step.ActivityType = orchestration.ActivityExecuteProviderJob
	dispatch := orchestration.ExecuteProviderJobInput{
		Run: run, Prompt: prompt, Route: model,
		BudgetApprovalID:    command.BudgetApprovalID,
		BudgetMaximumMicros: plan.BudgetLimit.AmountMicros,
		BudgetCurrency:      plan.BudgetLimit.Currency,
		ProviderProfileID:   command.RouteSnapshot.ProviderProfileID,
		TraceID:             step.TraceID,
		PersistProductTruth: true,
	}
	return step, run, dispatch
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

func TestPostgres_CreatorLiveShotIdempotencyQuotaAndManifest(t *testing.T) {
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
	t.Cleanup(pool.Close)
	store := NewForPool(pool)
	actor := controlplane.Actor{ActorID: "creator-integration-" + uuid.NewString(), Role: "CREATOR"}
	primaryActor := actor
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_idempotency WHERE scope LIKE $1`, "creator-integration:"+primaryActor.ActorID+"%")
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_runs WHERE actor_id=$1`, primaryActor.ActorID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_plans WHERE actor_id=$1`, primaryActor.ActorID)
	})
	capability := providercontract.CapabilitySnapshot{
		Alias: providercontract.CapabilityVideo,
		Capability: providercontract.Capability{Provider: "volcengine_ark", ModelFamily: "doubao-seedance-2.0", OutputModality: providercontract.ModalityVideo,
			Resolutions: []string{"720p"}, AspectRatios: []string{"16:9", "9:16"}, MinDurationMillis: 4000, MaxDurationMillis: 15000, NativeFPS: []int{24}},
		Configured: true, Enabled: true, Mode: "live", RouteVersion: "agent-plan-large-v1", SnapshotHash: strings.Repeat("a", 64),
		Limits:          map[string]any{"billingMode": "subscription", "maximumConcurrency": float64(1)},
		SupportedInputs: []string{"text"},
	}
	createPlan := func(index int) (controlplane.CreatorLiveShotPlan, controlplane.Idempotency) {
		command := controlplane.CreatorLiveShotPlanCommand{Title: fmt.Sprintf("shot-%d", index), SceneText: "雨夜车站", AspectRatio: "16:9", RightsAccepted: true, SourceArtifactHash: strings.Repeat(fmt.Sprint(index%9+1), 64), SourceArtifactURI: "cas://sha256/" + strings.Repeat(fmt.Sprint(index%9+1), 64), Route: capability, Actor: actor}
		idem := controlplane.Idempotency{Scope: "creator-integration:" + actor.ActorID + ":plan", Key: uuid.NewString(), RequestHash: strings.Repeat(fmt.Sprint((index+3)%9+1), 64)}
		stored, err := store.CreateCreatorLiveShotPlan(ctx, command, idem, "creator-live-integration")
		if err != nil {
			t.Fatal(err)
		}
		return stored.Value, idem
	}
	plan1, idem1 := createPlan(1)
	if plan1.State != "AWAITING_CONFIRMATION" || !plan1.Confirmable || plan1.ProviderCallCount != 1 || plan1.ProviderSubmitCount != 0 || plan1.BudgetApprovalID == "" || plan1.SafetyDecisionID == "" {
		t.Fatalf("creator plan contract=%#v", plan1)
	}
	project, err := store.ListCreatorLiveShots(ctx, plan1.SeriesID, actor)
	if err != nil || project.Plan.PlanID != plan1.PlanID || len(project.Runs) != 0 {
		t.Fatalf("pre-confirm recovery projection=%#v err=%v", project, err)
	}
	var normalizedBindings int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM video_pipeline.operation_requests WHERE id=$1 AND operation_type='CREATE_CREATOR_LIVE_SHOT_PLAN') +
		       (SELECT count(*) FROM video_pipeline.review_tasks WHERE id=$2 AND state='APPROVED' AND generation_plan_id=$1) +
		       (SELECT count(*) FROM video_pipeline.prompt_snapshot_inputs WHERE prompt_snapshot_id=$3)`,
		plan1.PlanID, plan1.BudgetApprovalID, plan1.PromptSnapshotID).Scan(&normalizedBindings); err != nil || normalizedBindings != 11 {
		t.Fatalf("normalized creator bindings=%d err=%v", normalizedBindings, err)
	}
	replayed, err := store.CreateCreatorLiveShotPlan(ctx, controlplane.CreatorLiveShotPlanCommand{Title: "shot-1", SceneText: "雨夜车站", AspectRatio: "16:9", RightsAccepted: true, SourceArtifactHash: strings.Repeat("2", 64), SourceArtifactURI: "cas://sha256/" + strings.Repeat("2", 64), Route: capability, Actor: actor}, idem1, "creator-live-integration")
	if err != nil || !replayed.Replayed || replayed.Value.PlanID != plan1.PlanID {
		t.Fatalf("plan replay=%#v err=%v", replayed, err)
	}
	rejectConfirm := func(command controlplane.ConfirmCreatorLiveShotCommand) error {
		_, err := store.ConfirmCreatorLiveShotPlan(ctx, plan1.PlanID, command, controlplane.Idempotency{Scope: "creator-integration:" + actor.ActorID + ":confirm:" + plan1.PlanID, Key: uuid.NewString(), RequestHash: strings.Repeat("e", 64)}, "creator-live-integration")
		return err
	}
	if err := rejectConfirm(controlplane.ConfirmCreatorLiveShotCommand{Confirmed: false, PlanHash: plan1.PlanHash, LiveCallsEnabled: true, Route: capability, Actor: actor}); asCode(err) != controlplane.CodeValidation {
		t.Fatalf("unconfirmed error=%v", err)
	}
	if err := rejectConfirm(controlplane.ConfirmCreatorLiveShotCommand{Confirmed: true, PlanHash: strings.Repeat("f", 64), LiveCallsEnabled: true, Route: capability, Actor: actor}); asCode(err) != controlplane.CodePlanHashMismatch {
		t.Fatalf("plan hash drift error=%v", err)
	}
	paygo := capability
	paygo.Limits = map[string]any{"billingMode": "paygo", "maximumConcurrency": float64(1)}
	if err := rejectConfirm(controlplane.ConfirmCreatorLiveShotCommand{Confirmed: true, PlanHash: plan1.PlanHash, LiveCallsEnabled: true, Route: paygo, Actor: actor}); asCode(err) != controlplane.CodeSubscriptionRouteRequired {
		t.Fatalf("paygo error=%v", err)
	}
	confirm := func(plan controlplane.CreatorLiveShotPlan, key string) (controlplane.Stored[controlplane.CreatorLiveShotRun], error) {
		return store.ConfirmCreatorLiveShotPlan(ctx, plan.PlanID, controlplane.ConfirmCreatorLiveShotCommand{Confirmed: true, PlanHash: plan.PlanHash, LiveCallsEnabled: true, Route: capability, Actor: actor}, controlplane.Idempotency{Scope: "creator-integration:" + actor.ActorID + ":confirm:" + plan.PlanID, Key: key, RequestHash: strings.Repeat("c", 64)}, "creator-live-integration")
	}
	key1 := uuid.NewString()
	run1, err := confirm(plan1, key1)
	if err != nil {
		t.Fatal(err)
	}
	runReplay, err := confirm(plan1, key1)
	if err != nil || !runReplay.Replayed || runReplay.Value.RunID != run1.Value.RunID {
		t.Fatalf("confirm replay=%#v err=%v", runReplay, err)
	}
	plan2, _ := createPlan(2)
	if _, err := confirm(plan2, uuid.NewString()); err == nil || asCode(err) != controlplane.CodeConcurrencyLimit {
		t.Fatalf("concurrency error=%v", err)
	}

	var prepared creatorPreparedRequest
	var reservationID string
	if err := pool.QueryRow(ctx, `SELECT request_snapshot,reservation_id FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, run1.Value.RunID).Scan(&prepared, &reservationID); err != nil {
		t.Fatal(err)
	}
	workflowRecord, err := store.GetShotWorkflowRecord(ctx, run1.Value.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if workflowRecord.PromptSnapshotID != prepared.Input.Prompt.ID ||
		workflowRecord.PromptHash != prepared.Input.Prompt.Digest ||
		!reflect.DeepEqual(workflowRecord.Prompt, workflowPromptSnapshot(prepared.Input.Prompt)) {
		t.Fatalf("creator Temporal dispatch lost confirmed prompt: record=%#v confirmed=%#v", workflowRecord.Prompt, prepared.Input.Prompt)
	}
	if workflowRecord.Prompt.PositivePrompt == "" || workflowRecord.Prompt.Output.DurationMillis != 5000 ||
		workflowRecord.Prompt.Context.ShotSnapshotID == "" || len(workflowRecord.Prompt.InputRevisionHashes) == 0 {
		t.Fatalf("creator Temporal dispatch prompt is partial: %#v", workflowRecord.Prompt)
	}
	projectedInput := orchestration.ExecuteProviderJobInput{
		Run: orchestration.GenerationRunRef{
			RunID: workflowRecord.Run.RunID, RunSpecDigest: workflowRecord.Run.RunSpecDigest,
			Attempt: workflowRecord.Run.CreativeAttempt,
		},
		Prompt: orchestration.PromptSnapshotRef{
			ID: workflowRecord.Prompt.ID, Digest: workflowRecord.Prompt.Digest,
			PositivePrompt: workflowRecord.Prompt.PositivePrompt,
			NegativePrompt: workflowRecord.Prompt.NegativePrompt,
			Context:        workflowRecord.Prompt.Context, Assets: workflowRecord.Prompt.Assets,
			Output:              workflowRecord.Prompt.Output,
			InputRevisionHashes: workflowRecord.Prompt.InputRevisionHashes,
		},
		Route: providercontract.ModelSnapshot{
			CapabilityAlias: workflowRecord.RouteSnapshot.CapabilityAlias,
			Provider:        workflowRecord.RouteSnapshot.Provider,
			ModelID:         workflowRecord.RouteSnapshot.ModelID,
			EndpointID:      workflowRecord.RouteSnapshot.EndpointID,
			RouteVersion:    workflowRecord.RouteSnapshot.RouteVersion,
			CapabilityHash:  workflowRecord.RouteSnapshot.CapabilityHash,
			Verification:    "control_plane_capability_snapshot",
		},
		ProviderProfileID:   workflowRecord.RouteSnapshot.ProviderProfileID,
		BudgetApprovalID:    workflowRecord.BudgetApprovalID,
		BudgetMaximumMicros: workflowRecord.BudgetLimit.AmountMicros,
		BudgetCurrency:      workflowRecord.BudgetLimit.Currency,
		TraceID:             workflowRecord.Run.TraceID,
		PersistProductTruth: true,
	}
	if projectedInput.BudgetApprovalID != plan1.BudgetApprovalID ||
		projectedInput.BudgetApprovalID == reservationID {
		t.Fatalf("creator budget identities collapsed: approval=%q reservation=%q", projectedInput.BudgetApprovalID, reservationID)
	}
	if !reflect.DeepEqual(projectedInput, prepared.Input) {
		t.Fatalf("creator Temporal input differs from confirmed provider intent: projected=%#v confirmed=%#v", projectedInput, prepared.Input)
	}

	creatorCAS, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	creatorArtifact, err := creatorCAS.Put(ctx, bytes.NewReader([]byte("creator live-shot provider fixture")))
	if err != nil {
		t.Fatal(err)
	}
	var providerSubmits atomic.Int32
	creatorProvider := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/jobs" {
			http.NotFound(response, request)
			return
		}
		providerSubmits.Add(1)
		var job providercontract.JobRequest
		if err := json.NewDecoder(request.Body).Decode(&job); err != nil {
			http.Error(response, "invalid fixture request", http.StatusBadRequest)
			return
		}
		if request.Header.Get("Idempotency-Key") != job.JobID ||
			job.BudgetReservation.ReservationID != reservationID ||
			job.BudgetReservation.ConfirmedBy != plan1.BudgetApprovalID {
			http.Error(response, "creator budget binding mismatch", http.StatusUnprocessableEntity)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(response).Encode(providercontract.JobResponse{
			JobID: job.JobID, RunID: job.RunID,
			UpstreamTaskID: "creator-task-" + creatorArtifact.Digest[:12],
			RequestID:      "creator-request-" + creatorArtifact.Digest[:12],
			State:          providercontract.StatusSucceeded, Progress: 100,
			Model: job.Model, ProviderRegion: "cn-beijing",
			Artifacts: []providercontract.AssetRef{{
				ID: "creator-video-" + creatorArtifact.Digest[:12], Revision: creatorArtifact.Digest,
				Kind: providercontract.ModalityVideo, Role: providercontract.AssetRoleOutput,
				URI: creatorArtifact.URI, SHA256: creatorArtifact.Digest,
				LicenseReference: "creator-integration-license", MediaType: "video/mp4",
				SizeBytes: creatorArtifact.Size, Width: 1280, Height: 720, DurationMillis: 5000,
			}},
			Usage: providercontract.Usage{VideoTokens: 250000, GeneratedMillis: 5000},
			Cost: providercontract.Cost{
				PricingVersion: "agent-plan-video-token-v1", BillingMode: "subscription",
				Verified: false,
			},
		})
	}))
	defer creatorProvider.Close()
	activities := orchestration.NewProductionActivities(creatorProvider.URL, store, store, creatorCAS)
	var activitySuite testsuite.WorkflowTestSuite
	activityEnvironment := activitySuite.NewTestActivityEnvironment()
	activityEnvironment.RegisterActivity(activities.ExecuteProviderJob)
	encodedResult, err := activityEnvironment.ExecuteActivity(activities.ExecuteProviderJob, projectedInput)
	if err != nil {
		t.Fatalf("confirmed creator dispatch failed before provider submit: %v", err)
	}
	var result orchestration.ProviderResult
	if err := encodedResult.Get(&result); err != nil {
		t.Fatal(err)
	}
	if providerSubmits.Load() != 1 || result.UpstreamTaskID == "" || result.RequestID == "" {
		t.Fatalf("creator provider result=%#v submits=%d", result, providerSubmits.Load())
	}
	completed, err := store.GetCreatorLiveShotRun(ctx, run1.Value.RunID, actor)
	if err != nil || completed.State != "SUCCEEDED" || completed.ManifestHash == "" || completed.Manifest == nil || completed.SubmitCount != 1 || completed.Progress == nil || *completed.Progress != 100 || completed.CashCost.AmountMicros != nil || completed.CashCost.Verified {
		t.Fatalf("completed=%#v err=%v", completed, err)
	}
	manifest, err := store.GetCreatorLiveShotManifest(ctx, run1.Value.RunID, actor)
	if err != nil || manifest.Evidence != "live_provider_call" || manifest.OutputHash != result.ArtifactDigest || manifest.ProviderRegion == nil || *manifest.ProviderRegion != "cn-beijing" || manifest.ProviderJobID == "" || manifest.Budget.ReservedTasks != 1 || manifest.Budget.ReservedVideoTokens != creatorVideoTokenLimit || manifest.Budget.SettledVideoTokens == nil || *manifest.Budget.SettledVideoTokens != 250000 || manifest.CashCost.AmountMicros != nil {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
	var succeededBefore []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, run1.Value.RunID).Scan(&succeededBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteProviderJob(ctx, orchestration.WorkflowStep{}, projectedInput, result); err != nil {
		t.Fatalf("exact creator completion replay: %v", err)
	}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, orchestration.CancelProviderJobInput{Dispatch: projectedInput}, orchestration.CancelProviderResult{State: "SUCCEEDED", UpstreamTaskID: result.UpstreamTaskID, RequestID: result.RequestID}); err != nil {
		t.Fatalf("terminal-success cancellation race: %v", err)
	}
	var succeededAfter []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, run1.Value.RunID).Scan(&succeededAfter); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(succeededBefore, succeededAfter) {
		t.Fatalf("exact completion/cancellation replay mutated succeeded evidence\nbefore=%s\nafter=%s", succeededBefore, succeededAfter)
	}
	var succeededOperation string
	if err := pool.QueryRow(ctx, `SELECT state FROM video_pipeline.operation_requests WHERE id=$1`, run1.Value.OperationID).Scan(&succeededOperation); err != nil || succeededOperation != "SUCCEEDED" {
		t.Fatalf("creator success operation=%q err=%v", succeededOperation, err)
	}
	driftedInput := projectedInput
	driftedInput.TraceID += "-drift"
	if err := store.CompleteProviderJob(ctx, orchestration.WorkflowStep{}, driftedInput, result); asCode(err) != controlplane.CodeRevisionConflict {
		t.Fatalf("drifted confirmed creator input error=%v", err)
	}
	driftedResult := result
	driftedResult.ArtifactSize++
	if err := store.CompleteProviderJob(ctx, orchestration.WorkflowStep{}, projectedInput, driftedResult); asCode(err) != controlplane.CodeRevisionConflict {
		t.Fatalf("drifted creator completion error=%v", err)
	}
	if _, err := confirm(plan2, uuid.NewString()); err == nil || asCode(err) != controlplane.CodePlanStale {
		t.Fatalf("plan created under prior concurrency snapshot error=%v", err)
	}
	plan3, _ := createPlan(3)
	third, err := confirm(plan3, uuid.NewString())
	if err != nil {
		t.Fatalf("fresh second confirm after terminal: %v", err)
	}
	thirdCancel := orchestration.CancelProviderResult{State: "CANCELLED", NoRemoteTask: true}
	thirdInput := orchestration.CancelProviderJobInput{Dispatch: orchestration.ExecuteProviderJobInput{Run: orchestration.GenerationRunRef{RunID: third.Value.RunID}}}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, thirdInput, thirdCancel); err != nil {
		t.Fatal(err)
	}
	var cancelledBefore []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, third.Value.RunID).Scan(&cancelledBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, thirdInput, thirdCancel); err != nil {
		t.Fatalf("exact creator cancellation replay: %v", err)
	}
	var cancelledAfter []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, third.Value.RunID).Scan(&cancelledAfter); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cancelledBefore, cancelledAfter) {
		t.Fatalf("exact cancellation replay mutated terminal evidence\nbefore=%s\nafter=%s", cancelledBefore, cancelledAfter)
	}
	var cancelledOperation string
	if err := pool.QueryRow(ctx, `SELECT state FROM video_pipeline.operation_requests WHERE id=$1`, third.Value.OperationID).Scan(&cancelledOperation); err != nil || cancelledOperation != "CANCELLED" {
		t.Fatalf("creator cancelled operation=%q err=%v", cancelledOperation, err)
	}
	currencyDrift := thirdCancel
	currencyDrift.Cost = providercontract.Cost{BillingMode: "subscription", Currency: "CNY"}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, thirdInput, currencyDrift); asCode(err) != controlplane.CodeRevisionConflict {
		t.Fatalf("creator cancellation cost drift error=%v", err)
	}
	illegalCash := int64(1)
	illegalCost := thirdCancel
	illegalCost.Cost = providercontract.Cost{ActualMicros: &illegalCash, BillingMode: "subscription"}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, thirdInput, illegalCost); asCode(err) != controlplane.CodeCashChargeNotAllowed {
		t.Fatalf("creator cancellation illegal cash replay error=%v", err)
	}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, thirdInput, orchestration.CancelProviderResult{State: "FAILED", ErrorCode: "PROVIDER_FAILED"}); asCode(err) != controlplane.CodeRevisionConflict {
		t.Fatalf("drifted creator cancellation error=%v", err)
	}
	plan4, _ := createPlan(4)
	fourth, err := confirm(plan4, uuid.NewString())
	if err != nil {
		t.Fatalf("third project task: %v", err)
	}
	fourthInput := orchestration.CancelProviderJobInput{Dispatch: orchestration.ExecuteProviderJobInput{Run: orchestration.GenerationRunRef{RunID: fourth.Value.RunID}}}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, fourthInput, orchestration.CancelProviderResult{State: "UNKNOWN", ErrorCode: "CANCEL_NOT_CONFIRMED"}); err == nil {
		t.Fatal("unconfirmed creator cancellation error = nil")
	}
	var fourthState string
	if err := pool.QueryRow(ctx, `SELECT state FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, fourth.Value.RunID).Scan(&fourthState); err != nil || fourthState != "RECONCILING" {
		t.Fatalf("unconfirmed creator cancellation state=%q err=%v", fourthState, err)
	}
	fourthFailure := orchestration.CancelProviderResult{State: "FAILED", UpstreamTaskID: "creator-failed-task", RequestID: "creator-failed-request", ErrorCode: "REMOTE_FAILED", Usage: providercontract.Usage{VideoTokens: 1234}, Cost: providercontract.Cost{BillingMode: "subscription", Currency: "VTC"}}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, fourthInput, fourthFailure); err != nil {
		t.Fatal(err)
	}
	var failedBefore []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, fourth.Value.RunID).Scan(&failedBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, fourthInput, fourthFailure); err != nil {
		t.Fatalf("exact creator failure cancellation replay: %v", err)
	}
	if err := store.FinalizeShotRun(ctx, orchestration.WorkflowStep{}, orchestration.FinalizeShotRunInput{OperationID: fourth.Value.OperationID, RunID: fourth.Value.RunID, State: "FAILED", FailureCode: "REMOTE_FAILED"}); err != nil {
		t.Fatalf("creator failure post-commit finalization replay: %v", err)
	}
	var failedAfter []byte
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM video_pipeline.creator_live_shot_runs r WHERE id=$1`, fourth.Value.RunID).Scan(&failedAfter); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(failedBefore, failedAfter) {
		t.Fatalf("exact failed replay mutated terminal evidence\nbefore=%s\nafter=%s", failedBefore, failedAfter)
	}
	plan5, _ := createPlan(5)
	if plan5.Confirmable || len(plan5.Blockers) != 1 || plan5.Blockers[0] != string(controlplane.CodeProjectBudgetExceeded) {
		t.Fatalf("exhausted plan=%#v", plan5)
	}
	if _, err := confirm(plan5, uuid.NewString()); asCode(err) != controlplane.CodeProjectBudgetExceeded {
		t.Fatalf("project budget error=%v", err)
	}
	store.now = func() time.Time { return plan5.ExpiresAt.Add(time.Second) }
	if _, err := confirm(plan5, uuid.NewString()); asCode(err) != controlplane.CodePlanExpired {
		t.Fatalf("expired creator plan error=%v", err)
	}
	var expiredState string
	if err := pool.QueryRow(ctx, `SELECT state FROM video_pipeline.creator_live_shot_plans WHERE id=$1`, plan5.PlanID).Scan(&expiredState); err != nil || expiredState != "EXPIRED" {
		t.Fatalf("expired creator plan durable state=%q err=%v", expiredState, err)
	}

	for _, race := range []struct {
		name            string
		completionFirst bool
		wantState       string
	}{
		{name: "completion wins row-lock queue", completionFirst: true, wantState: "SUCCEEDED"},
		{name: "cancellation wins row-lock queue", completionFirst: false, wantState: "CANCELLED"},
	} {
		t.Run(race.name, func(t *testing.T) {
			actor = controlplane.Actor{ActorID: "creator-race-" + uuid.NewString(), Role: "CREATOR"}
			raceActor := actor
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_idempotency WHERE scope LIKE $1`, "creator-integration:"+raceActor.ActorID+"%")
				_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_runs WHERE actor_id=$1`, raceActor.ActorID)
				_, _ = pool.Exec(cleanupCtx, `DELETE FROM video_pipeline.creator_live_shot_plans WHERE actor_id=$1`, raceActor.ActorID)
			})
			plan, _ := createPlan(6)
			run, err := confirm(plan, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			var prepared creatorPreparedRequest
			if err := pool.QueryRow(ctx, `SELECT request_snapshot FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, run.Value.RunID).Scan(&prepared); err != nil {
				t.Fatal(err)
			}
			digest := strings.Repeat("d", 64)
			completion := orchestration.ProviderResult{UpstreamTaskID: "race-task", RequestID: "race-request", Model: prepared.Input.Route, ArtifactURI: "cas://sha256/" + digest, ArtifactDigest: digest, MediaType: "video/mp4", ArtifactSize: 10, Width: 1280, Height: 720, DurationMillis: 5000, Usage: providercontract.Usage{VideoTokens: 1000}, Cost: providercontract.Cost{BillingMode: "subscription"}}
			cancellation := orchestration.CancelProviderResult{State: "CANCELLED", UpstreamTaskID: completion.UpstreamTaskID, RequestID: completion.RequestID, Cost: providercontract.Cost{BillingMode: "subscription"}}
			cancelInput := orchestration.CancelProviderJobInput{Dispatch: prepared.Input}

			lockTx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lockTx.Rollback(context.Background()) }()
			if _, err := lockTx.Exec(ctx, `SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1 FOR UPDATE`, run.Value.RunID); err != nil {
				_ = lockTx.Rollback(ctx)
				t.Fatal(err)
			}
			completeErr := make(chan error, 1)
			cancelErr := make(chan error, 1)
			startCompletion := func() {
				go func() {
					completeErr <- store.CompleteProviderJob(ctx, orchestration.WorkflowStep{}, prepared.Input, completion)
				}()
			}
			startCancellation := func() {
				go func() {
					cancelErr <- store.RecordProviderCancellation(ctx, orchestration.WorkflowStep{}, cancelInput, cancellation)
				}()
			}
			waitForBlocked := func(want int) {
				t.Helper()
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					var count int
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count >= want {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatalf("blocked creator writers=%d not observed", want)
			}
			if race.completionFirst {
				startCompletion()
				waitForBlocked(1)
				startCancellation()
			} else {
				startCancellation()
				waitForBlocked(1)
				startCompletion()
			}
			waitForBlocked(2)
			if err := lockTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			completionError, cancellationError := <-completeErr, <-cancelErr
			if race.completionFirst {
				if completionError != nil || cancellationError != nil {
					t.Fatalf("completion-first errors: completion=%v cancellation=%v", completionError, cancellationError)
				}
			} else {
				if cancellationError != nil || asCode(completionError) != controlplane.CodeRunTerminal {
					t.Fatalf("cancellation-first errors: completion=%v cancellation=%v", completionError, cancellationError)
				}
			}
			var runState, operationState string
			if err := pool.QueryRow(ctx, `
				SELECT r.state,o.state
				FROM video_pipeline.creator_live_shot_runs r
				JOIN video_pipeline.operation_requests o ON o.id=r.operation_id
				WHERE r.id=$1`, run.Value.RunID).Scan(&runState, &operationState); err != nil {
				t.Fatal(err)
			}
			if runState != race.wantState || operationState != race.wantState {
				t.Fatalf("concurrent creator truth run=%q operation=%q want=%q", runState, operationState, race.wantState)
			}
		})
	}
}

func asCode(err error) controlplane.ErrorCode {
	var domain *controlplane.DomainError
	if errors.As(err, &domain) {
		return domain.Code
	}
	return ""
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
