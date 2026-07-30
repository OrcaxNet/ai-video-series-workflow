package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CompilePromptSnapshot materializes the exact four-level context resolution
// and immutable prompt used by a workflow Activity.
func (p *Postgres) CompilePromptSnapshot(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CompilePromptInput,
) (orchestration.PromptSnapshotRef, error) {
	shotID, err := uuid.Parse(input.ShotSpecRevisionID)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, errors.New("shotSpecRevisionId must be a UUID")
	}
	profileID, err := uuid.Parse(input.GenerationProfileRef)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, errors.New("generationProfileRef must be a UUID")
	}
	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (orchestration.PromptSnapshotRef, error) {
		var contextIDs, assetIDs []uuid.UUID
		var effectiveHash, shotHash, freshness string
		var narrative []byte
		var storedProfileID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT context_revision_ids, asset_version_refs, effective_context_hash,
			       narrative, content_hash, generation_profile_id, freshness
			FROM video_pipeline.shot_spec_revisions
			WHERE id = $1
			FOR SHARE`,
			shotID,
		).Scan(&contextIDs, &assetIDs, &effectiveHash, &narrative, &shotHash, &storedProfileID, &freshness); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orchestration.PromptSnapshotRef{}, controlplane.NewNotFoundError("shot revision", input.ShotSpecRevisionID)
			}
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("read prompt inputs: %w", err)
		}
		if storedProfileID != profileID || freshness == "STALE" {
			return orchestration.PromptSnapshotRef{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"prompt inputs do not match the current immutable shot profile",
				"revalidate the shot and its generation profile",
			)
		}
		var contextCount, scopeCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(DISTINCT scope_type)
			FROM video_pipeline.context_revisions
			WHERE id = ANY($1::uuid[])
			  AND status = 'APPROVED'
			  AND scope_type IN ('SERIES', 'EPISODE', 'SCENE', 'SHOT')`,
			contextIDs,
		).Scan(&contextCount, &scopeCount); err != nil {
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("validate prompt contexts: %w", err)
		}
		if contextCount != len(contextIDs) || scopeCount != 4 {
			return orchestration.PromptSnapshotRef{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"effective context must bind approved SERIES, EPISODE, SCENE, and SHOT revisions",
				"resolve and approve the complete four-level context",
			)
		}

		normalized := map[string]any{
			"shotSpecRevisionId": input.ShotSpecRevisionID,
			"shotContentHash":    shotHash,
			"contextRevisionIds": uuidStrings(contextIDs),
			"assetVersionRefs":   uuidStrings(assetIDs),
		}
		effectiveID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("effective-context:"+input.ShotSpecRevisionID+":"+effectiveHash))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.effective_context_snapshots
				(id, shot_spec_revision_id, schema_version, resolver_version, context_revision_ids,
				 normalized_payload, content_hash)
			VALUES ($1, $2, 'v1', 'control-plane-resolver-v1', $3, $4, $5)
			ON CONFLICT (shot_spec_revision_id, content_hash) DO NOTHING`,
			effectiveID, shotID, contextIDs, normalized, effectiveHash,
		); err != nil {
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("insert effective context snapshot: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			SELECT id
			FROM video_pipeline.effective_context_snapshots
			WHERE shot_spec_revision_id = $1 AND content_hash = $2`,
			shotID, effectiveHash,
		).Scan(&effectiveID); err != nil {
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("resolve effective context snapshot: %w", err)
		}

		promptHash, err := digestValue(map[string]any{
			"schemaVersion":             "v1",
			"compilerVersion":           "control-plane-compiler-v1",
			"shotSpecRevisionId":        input.ShotSpecRevisionID,
			"generationProfileRevision": input.GenerationProfileRef,
			"effectiveContextHash":      effectiveHash,
			"assetVersionRefs":          uuidStrings(assetIDs),
			"narrative":                 json.RawMessage(narrative),
		})
		if err != nil {
			return orchestration.PromptSnapshotRef{}, err
		}
		promptID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("prompt:"+promptHash))
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.prompt_snapshots
				(id, shot_spec_revision_id, schema_version, compiler_version, prompt_template_ref,
				 effective_context_snapshot_id, asset_version_refs, positive_prompt, negative_prompt,
				 model_payload, normalized_input_hash, content_hash)
			VALUES ($1, $2, 'v1', 'control-plane-compiler-v1', 'video.prompt.v1',
			        $3, $4, $5, '', $6, $7, $7)
			ON CONFLICT (shot_spec_revision_id, content_hash) DO NOTHING`,
			promptID, shotID, effectiveID, assetIDs, string(narrative),
			map[string]any{"generationProfileRevisionId": input.GenerationProfileRef},
			promptHash,
		)
		if err != nil {
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("insert prompt snapshot: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			SELECT id
			FROM video_pipeline.prompt_snapshots
			WHERE shot_spec_revision_id = $1 AND content_hash = $2`,
			shotID, promptHash,
		).Scan(&promptID); err != nil {
			return orchestration.PromptSnapshotRef{}, fmt.Errorf("resolve prompt snapshot: %w", err)
		}
		if tag.RowsAffected() == 1 {
			for _, contextID := range contextIDs {
				if _, err := tx.Exec(ctx, `
					INSERT INTO video_pipeline.revision_dependencies
						(producer_type, producer_revision_id, consumer_type, consumer_revision_id,
						 dependency_role, producer_hash)
					SELECT 'CONTEXT_REVISION', id, 'PROMPT_SNAPSHOT', $2, 'EFFECTIVE_CONTEXT', content_hash
					FROM video_pipeline.context_revisions
					WHERE id = $1
					ON CONFLICT DO NOTHING`,
					contextID, promptID,
				); err != nil {
					return orchestration.PromptSnapshotRef{}, fmt.Errorf("insert prompt context dependency: %w", err)
				}
			}
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(promptID, []byte("audit")),
				uuid.NewSHA1(promptID, []byte("outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"prompt_snapshot.created", "PROMPT_SNAPSHOT", promptID,
				nil, nil, "", step.TraceID,
				map[string]any{
					"workflowId":               step.WorkflowID,
					"shotSpecRevisionId":       input.ShotSpecRevisionID,
					"effectiveContextSnapshot": effectiveID.String(),
					"contentHash":              promptHash,
				},
				p.now().UTC(),
			); err != nil {
				return orchestration.PromptSnapshotRef{}, err
			}
		}
		return orchestration.PromptSnapshotRef{ID: promptID.String(), Digest: promptHash}, nil
	})
}

// CreateWorkflowRun creates the same normalized Run/Attempt projection used by
// the public run endpoint, but binds it to the parent episode workflow.
func (p *Postgres) CreateWorkflowRun(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CreateRunInput,
) (orchestration.GenerationRunRef, error) {
	shotID, err := uuid.Parse(input.ShotSpecRevisionID)
	if err != nil {
		return orchestration.GenerationRunRef{}, errors.New("shotSpecRevisionId must be a UUID")
	}
	promptID, err := uuid.Parse(input.PromptSnapshot.ID)
	if err != nil {
		return orchestration.GenerationRunRef{}, errors.New("prompt snapshot ID must be a UUID")
	}
	profileID, err := uuid.Parse(input.GenerationProfileRef)
	if err != nil {
		return orchestration.GenerationRunRef{}, errors.New("generation profile ID must be a UUID")
	}
	if input.CreativeAttempt < 1 || input.CreativeAttempt > 2 {
		return orchestration.GenerationRunRef{}, errors.New("creativeAttempt must be 1 or 2")
	}
	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (orchestration.GenerationRunRef, error) {
		var seriesID, episodeID, shotProfileID uuid.UUID
		var freshness, lifecycle, promptHash string
		if err := tx.QueryRow(ctx, `
			SELECT ep.series_id, ep.id, ssr.generation_profile_id, ssr.freshness,
			       ssr.lifecycle_state, ps.content_hash
			FROM video_pipeline.shot_spec_revisions ssr
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			JOIN video_pipeline.prompt_snapshots ps
			  ON ps.id = $2 AND ps.shot_spec_revision_id = ssr.id
			WHERE ssr.id = $1
			FOR UPDATE OF ssr`,
			shotID, promptID,
		).Scan(&seriesID, &episodeID, &shotProfileID, &freshness, &lifecycle, &promptHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orchestration.GenerationRunRef{}, controlplane.NewNotFoundError("workflow run inputs", input.ShotSpecRevisionID)
			}
			return orchestration.GenerationRunRef{}, fmt.Errorf("lock workflow run inputs: %w", err)
		}
		if shotProfileID != profileID || freshness == "STALE" {
			return orchestration.GenerationRunRef{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"workflow run inputs differ from the immutable shot specification",
				"revalidate the shot and prompt before retrying",
			)
		}
		if lifecycle != "READY" && lifecycle != "REVIEW" && lifecycle != "APPROVED" && lifecycle != "RUNNING" {
			return orchestration.GenerationRunRef{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"shot is not in a generation-ready state",
				"complete G2 or resolve the current review",
			)
		}
		if err := requireActiveProfile(ctx, tx, input.GenerationProfileRef); err != nil {
			return orchestration.GenerationRunRef{}, err
		}
		plan, err := readPlan(ctx, tx, input.GenerationPlanID)
		if err != nil {
			return orchestration.GenerationRunRef{}, err
		}
		if plan.SeriesID != seriesID.String() ||
			!containsString(plan.ShotSpecRevisionIDs, input.ShotSpecRevisionID) ||
			plan.Plan.State == "BLOCKED" {
			return orchestration.GenerationRunRef{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"workflow run is outside the immutable generation plan",
				"use the exact approved plan and shot set",
			)
		}
		workflowRoute := controlplane.ModelRouteSnapshot{
			CapabilityAlias:   input.Route.CapabilityAlias,
			ProviderProfileID: input.ProviderProfileID,
			Provider:          input.Route.Provider,
			ModelID:           input.Route.ModelID,
			RouteVersion:      input.Route.RouteVersion,
			CapabilityHash:    input.Route.CapabilityHash,
		}
		if !sameRoute(plan.Plan.RouteSnapshot, workflowRoute) {
			return orchestration.GenerationRunRef{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"workflow route differs from the immutable generation plan",
			)
		}
		if err := requireBudgetApproval(ctx, tx, input.BudgetApprovalID, seriesID, episodeID); err != nil {
			return orchestration.GenerationRunRef{}, err
		}
		runDigest, err := digestValue(map[string]any{
			"shotSpecRevisionId": input.ShotSpecRevisionID,
			"promptSnapshotId":   input.PromptSnapshot.ID,
			"promptHash":         promptHash,
			"profileId":          input.GenerationProfileRef,
			"generationPlanId":   input.GenerationPlanID,
			"route":              input.Route,
			"creativeAttempt":    input.CreativeAttempt,
		})
		if err != nil {
			return orchestration.GenerationRunRef{}, err
		}
		runID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("generation-run:"+runDigest))
		attemptID := uuid.NewSHA1(runID, []byte("attempt:1"))
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.generation_runs
				(id, shot_spec_revision_id, prompt_snapshot_id, generation_profile_id,
				 temporal_workflow_id, temporal_run_id, run_spec_digest, creative_attempt,
				 state, dry_run, budget_approval_id, trace_id, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'VALIDATED', false, $9, $10, 'temporal-worker')
			ON CONFLICT (id) DO NOTHING`,
			runID, shotID, promptID, profileID, step.WorkflowID, temporalRunID(step),
			runDigest, input.CreativeAttempt, input.BudgetApprovalID, step.TraceID,
		)
		if err != nil {
			return orchestration.GenerationRunRef{}, translateWriteError("insert workflow generation run", err)
		}
		modelSnapshot, err := json.Marshal(input.Route)
		if err != nil {
			return orchestration.GenerationRunRef{}, fmt.Errorf("encode workflow model snapshot: %w", err)
		}
		attemptKind := "PROVIDER_REQUEST"
		if input.CreativeAttempt > 1 {
			attemptKind = "CREATIVE_REVISION"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.generation_attempts
				(id, generation_run_id, sequence, attempt_kind, state, input_hash, model_snapshot)
			VALUES ($1, $2, 1, $3, 'VALIDATED', $4, $5)
			ON CONFLICT (generation_run_id, sequence) DO NOTHING`,
			attemptID, runID, attemptKind, runDigest, modelSnapshot,
		); err != nil {
			return orchestration.GenerationRunRef{}, fmt.Errorf("insert workflow generation attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.shot_spec_revisions
			SET lifecycle_state = 'RUNNING'
			WHERE id = $1 AND lifecycle_state IN ('READY', 'REVIEW', 'APPROVED')`,
			shotID,
		); err != nil {
			return orchestration.GenerationRunRef{}, fmt.Errorf("advance workflow shot state: %w", err)
		}
		if tag.RowsAffected() == 1 {
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(runID, []byte("audit")),
				uuid.NewSHA1(runID, []byte("outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"generation_run.created", "GENERATION_RUN", runID,
				nil, nil, "", step.TraceID,
				map[string]any{
					"workflowId":         step.WorkflowID,
					"shotSpecRevisionId": input.ShotSpecRevisionID,
					"promptSnapshotId":   input.PromptSnapshot.ID,
					"runSpecDigest":      runDigest,
					"creativeAttempt":    input.CreativeAttempt,
				},
				p.now().UTC(),
			); err != nil {
				return orchestration.GenerationRunRef{}, err
			}
		}
		return orchestration.GenerationRunRef{
			RunID: runID.String(), RunSpecDigest: runDigest, Attempt: input.CreativeAttempt,
		}, nil
	})
}

// PrepareProviderJob commits the paid-attempt identity before any network call.
func (p *Postgres) PrepareProviderJob(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.ExecuteProviderJobInput,
) error {
	runID, err := uuid.Parse(input.Run.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	providerProfileID, err := uuid.Parse(input.ProviderProfileID)
	if err != nil {
		return errors.New("providerProfileId must be a UUID")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var attemptID uuid.UUID
		var runDigest, runState string
		if err := tx.QueryRow(ctx, `
			SELECT ga.id, gr.run_spec_digest, gr.state
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga
			  ON ga.generation_run_id = gr.id AND ga.sequence = 1
			WHERE gr.id = $1
			FOR UPDATE OF gr, ga`,
			runID,
		).Scan(&attemptID, &runDigest, &runState); err != nil {
			return struct{}{}, fmt.Errorf("lock provider run: %w", err)
		}
		if runDigest != input.Run.RunSpecDigest {
			return struct{}{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch digest differs from the persisted run",
			)
		}
		if runState == "SUCCEEDED" {
			return struct{}{}, nil
		}
		var capabilityID uuid.UUID
		var pricingVersion string
		if err := tx.QueryRow(ctx, `
			SELECT pcs.id, COALESCE(pcs.pricing_rule_version, 'unpriced')
			FROM video_pipeline.provider_capability_snapshots pcs
			JOIN video_pipeline.provider_profiles pp ON pp.id = pcs.provider_profile_id
			WHERE pp.id = $1
			  AND pp.enabled
			  AND pp.health = 'READY'
			  AND pcs.capability_alias = 'video.primary'
			  AND pcs.capability_hash = $2
			  AND pcs.status = 'ACTIVE'
			FOR SHARE`,
			providerProfileID, input.Route.CapabilityHash,
		).Scan(&capabilityID, &pricingVersion); err != nil {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeCapability,
				"the frozen provider capability is no longer dispatchable",
				"refresh the generation plan and provider route",
			)
		}
		reservationID := uuid.NewSHA1(runID, []byte("budget-reservation"))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.budget_reservations
				(id, generation_run_id, amount_micros, currency, pricing_rule_version,
				 estimate_payload, status, confirmed_by, confirmed_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'RESERVED', $7, now())
			ON CONFLICT (id) DO NOTHING`,
			reservationID, runID, input.BudgetMaximumMicros, input.BudgetCurrency,
			pricingVersion, map[string]any{"maximumMicros": input.BudgetMaximumMicros},
			input.BudgetApprovalID,
		); err != nil {
			return struct{}{}, fmt.Errorf("reserve workflow budget: %w", err)
		}
		jobID := uuid.NewSHA1(runID, []byte("provider-job"))
		jobKey := "provider-job-" + input.Run.RunID
		requestHash, err := digestValue(input)
		if err != nil {
			return struct{}{}, err
		}
		requestSnapshot, err := json.Marshal(input)
		if err != nil {
			return struct{}{}, fmt.Errorf("encode provider request snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.provider_jobs
				(id, generation_attempt_id, provider_profile_id, capability_snapshot_id,
				 budget_reservation_id, idempotency_key, request_hash, request_snapshot,
				 state, timeout_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'QUEUED', now() + interval '30 minutes')
			ON CONFLICT (provider_profile_id, idempotency_key) DO NOTHING`,
			jobID, attemptID, providerProfileID, capabilityID, reservationID,
			jobKey, requestHash, requestSnapshot,
		); err != nil {
			return struct{}{}, fmt.Errorf("insert provider job: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_runs
			SET state = 'QUEUED'
			WHERE id = $1 AND state IN ('VALIDATED', 'UNKNOWN', 'RECONCILING')`,
			runID,
		); err != nil {
			return struct{}{}, fmt.Errorf("queue generation run: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_attempts
			SET state = 'QUEUED'
			WHERE id = $1 AND state IN ('VALIDATED', 'UNKNOWN', 'RECONCILING')`,
			attemptID,
		); err != nil {
			return struct{}{}, fmt.Errorf("queue generation attempt: %w", err)
		}
		return struct{}{}, nil
	})
	return err
}

// CompleteProviderJob atomically commits provider provenance, CAS metadata,
// cost, and terminal run state.
func (p *Postgres) CompleteProviderJob(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.ExecuteProviderJobInput,
	result orchestration.ProviderResult,
) error {
	runID, err := uuid.Parse(input.Run.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	if result.ArtifactURI != "cas://sha256/"+result.ArtifactDigest {
		return errors.New("provider artifact URI does not match its content hash")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var jobID, attemptID, reservationID uuid.UUID
		var state string
		if err := tx.QueryRow(ctx, `
			SELECT pj.id, pj.generation_attempt_id, pj.budget_reservation_id, gr.state
			FROM video_pipeline.provider_jobs pj
			JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			JOIN video_pipeline.generation_runs gr ON gr.id = ga.generation_run_id
			WHERE gr.id = $1
			FOR UPDATE OF pj, ga, gr`,
			runID,
		).Scan(&jobID, &attemptID, &reservationID, &state); err != nil {
			return struct{}{}, fmt.Errorf("lock provider completion: %w", err)
		}
		artifactID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("artifact:"+result.ArtifactDigest))
		mediaType := result.MediaType
		if mediaType == "" {
			mediaType = "video/mp4"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.artifacts
				(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE')
			ON CONFLICT (content_hash) DO NOTHING`,
			artifactID, result.ArtifactDigest, result.ArtifactURI, mediaType, result.ArtifactSize,
			map[string]any{
				"width": result.Width, "height": result.Height,
				"durationMillis": result.DurationMillis,
				"modelSnapshot":  result.Model, "usage": result.Usage, "cost": result.Cost,
			},
		); err != nil {
			return struct{}{}, fmt.Errorf("insert provider artifact: %w", err)
		}
		if err := tx.QueryRow(ctx,
			`SELECT id FROM video_pipeline.artifacts WHERE content_hash = $1`,
			result.ArtifactDigest,
		).Scan(&artifactID); err != nil {
			return struct{}{}, fmt.Errorf("resolve provider artifact: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.run_artifacts
				(generation_run_id, artifact_id, role)
			VALUES ($1, $2, 'OUTPUT')
			ON CONFLICT DO NOTHING`,
			runID, artifactID,
		); err != nil {
			return struct{}{}, fmt.Errorf("link provider artifact: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.provider_jobs
			SET upstream_task_id = $2, upstream_request_id = $3, state = 'SUCCEEDED',
			    progress = 100, updated_at = now(), terminal_at = now(), error_code = NULL,
			    error_snapshot = NULL
			WHERE id = $1`,
			jobID, result.UpstreamTaskID, result.RequestID,
		); err != nil {
			return struct{}{}, fmt.Errorf("complete provider job: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_attempts
			SET state = 'SUCCEEDED', heartbeat_at = now(),
			    started_at = COALESCE(started_at, now()), finished_at = now()
			WHERE id = $1`,
			attemptID,
		); err != nil {
			return struct{}{}, fmt.Errorf("complete generation attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_runs
			SET state = CASE WHEN state = 'PAUSED' THEN 'PAUSED' ELSE 'SUCCEEDED' END,
			    failure_class = CASE WHEN state = 'PAUSED' THEN failure_class ELSE NULL END,
			    failure_code = CASE WHEN state = 'PAUSED' THEN failure_code ELSE NULL END,
			    started_at = COALESCE(started_at, now()), finished_at = now()
			WHERE id = $1`,
			runID,
		); err != nil {
			return struct{}{}, fmt.Errorf("complete generation run: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.shot_spec_revisions ssr
			SET lifecycle_state = 'QC_PENDING'
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1 AND ssr.id = gr.shot_spec_revision_id`,
			runID,
		); err != nil {
			return struct{}{}, fmt.Errorf("advance shot to QC: %w", err)
		}
		actualMicros := result.Cost.EstimatedMicros
		if result.Cost.ActualMicros != nil {
			actualMicros = *result.Cost.ActualMicros
		}
		ledgerID := uuid.NewSHA1(jobID, []byte("actual-cost"))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type, amount_micros,
				 currency, units, unit_name, pricing_rule_version, verified)
			VALUES ($1, $2, $3, 'ACTUAL', $4, $5, $6, $7, $8, $9)
			ON CONFLICT (id) DO NOTHING`,
			ledgerID, jobID, reservationID, actualMicros, result.Cost.Currency,
			result.Usage.InputUnits+result.Usage.OutputUnits, result.Usage.Unit,
			result.Cost.PricingVersion, result.Cost.Verified,
		); err != nil {
			return struct{}{}, fmt.Errorf("insert actual provider cost: %w", err)
		}
		if state != "SUCCEEDED" {
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(runID, []byte("provider-completed-audit")),
				uuid.NewSHA1(runID, []byte("provider-completed-outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"provider_job.completed", "GENERATION_RUN", runID,
				nil, nil, "", step.TraceID,
				map[string]any{
					"providerJobId":  jobID.String(),
					"upstreamTaskId": result.UpstreamTaskID,
					"artifactHash":   result.ArtifactDigest,
					"cost":           result.Cost,
				},
				p.now().UTC(),
			); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

func (p *Postgres) RecordAutomaticQC(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.RunQCInput,
	result orchestration.QCResult,
) error {
	runID, err := uuid.Parse(input.Run.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		reportHash, err := digestValue(map[string]any{
			"thresholdVersion": "structural-qc-v1",
			"runSpecDigest":    input.Run.RunSpecDigest,
			"artifactHash":     input.Provider.ArtifactDigest,
			"result":           result,
		})
		if err != nil {
			return struct{}{}, err
		}
		reportID := uuid.NewSHA1(runID, []byte("qc:"+reportHash))
		state := "PASSED"
		reasons := []string{}
		shotState := "REVIEW"
		if !result.Passed {
			state = "FAILED"
			shotState = "AUTO_QC_FAILED"
			if result.FailureCode != "" {
				reasons = append(reasons, result.FailureCode)
			}
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.qc_reports
				(id, generation_run_id, threshold_version, state, metrics, reason_codes, report_hash)
			VALUES ($1, $2, 'structural-qc-v1', $3, $4, $5, $6)
			ON CONFLICT (generation_run_id, report_hash) DO NOTHING`,
			reportID, runID, state,
			map[string]any{"artifactHash": input.Provider.ArtifactDigest, "structuralPassed": result.Passed},
			reasons, reportHash,
		)
		if err != nil {
			return struct{}{}, fmt.Errorf("insert QC report: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.shot_spec_revisions ssr
			SET lifecycle_state = $2
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1 AND ssr.id = gr.shot_spec_revision_id`,
			runID, shotState,
		); err != nil {
			return struct{}{}, fmt.Errorf("advance shot QC state: %w", err)
		}
		if tag.RowsAffected() == 1 {
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(reportID, []byte("audit")),
				uuid.NewSHA1(reportID, []byte("outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"qc_report.created", "QC_REPORT", reportID,
				nil, nil, strings.Join(reasons, ","), step.TraceID,
				map[string]any{"runId": input.Run.RunID, "state": state, "reportHash": reportHash},
				p.now().UTC(),
			); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

func (p *Postgres) OpenShotReview(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CreateReviewInput,
) error {
	runID, err := uuid.Parse(input.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	shotRevisionID, err := uuid.Parse(input.ShotSpecRevisionID)
	if err != nil {
		return errors.New("shotSpecRevisionId must be a UUID")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var seriesID, episodeID, shotID uuid.UUID
		var runState string
		var qcPassed bool
		if err := tx.QueryRow(ctx, `
			SELECT ep.series_id, ep.id, sh.id, gr.state,
			       EXISTS (
			         SELECT 1 FROM video_pipeline.qc_reports qr
			         WHERE qr.generation_run_id = gr.id AND qr.state = 'PASSED'
			       )
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			WHERE gr.id = $1 AND ssr.id = $2
			FOR SHARE OF gr, ssr`,
			runID, shotRevisionID,
		).Scan(&seriesID, &episodeID, &shotID, &runState, &qcPassed); err != nil {
			return struct{}{}, fmt.Errorf("validate Q1 review inputs: %w", err)
		}
		if runState != "SUCCEEDED" || !qcPassed {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"Q1 review requires a succeeded run with passing automatic QC",
				"complete provider execution and automatic QC",
			)
		}
		reviewID := uuid.NewSHA1(runID, []byte("review:Q1"))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.review_tasks
				(id, series_id, episode_id, shot_id, generation_run_id,
				 review_type, state, assigned_role)
			VALUES ($1, $2, $3, $4, $5, 'Q1', 'OPEN', 'REVIEWER')
			ON CONFLICT (id) DO NOTHING`,
			reviewID, seriesID, episodeID, shotID, runID,
		); err != nil {
			return struct{}{}, fmt.Errorf("open Q1 review: %w", err)
		}
		return struct{}{}, nil
	})
	return err
}

func (p *Postgres) RecordShotIntervention(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.EscalateShotInput,
) error {
	shotID, err := uuid.Parse(input.ShotSpecRevisionID)
	if err != nil {
		return errors.New("shotSpecRevisionId must be a UUID")
	}
	_, err = p.pool.Exec(ctx, `
		UPDATE video_pipeline.shot_spec_revisions
		SET lifecycle_state = 'AUTO_QC_FAILED'
		WHERE id = $1`,
		shotID,
	)
	return err
}

// BuildEpisodeManifest reads only committed product truth and returns canonical
// secret-free JSON. CAS commit happens before the database manifest reference.
func (p *Postgres) BuildEpisodeManifest(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CreateGate3Input,
) ([]byte, error) {
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return nil, errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return nil, errors.New("manifest run IDs must be UUIDs")
	}
	executions := make([]json.RawMessage, 0, len(runIDs))
	for _, runID := range runIDs {
		var execution []byte
		if err := p.pool.QueryRow(ctx, `
			SELECT jsonb_build_object(
			  'runId', gr.id,
			  'shotSpecRevisionId', gr.shot_spec_revision_id,
			  'runSpecDigest', gr.run_spec_digest,
			  'creativeAttempt', gr.creative_attempt,
			  'state', gr.state,
			  'temporalWorkflowId', gr.temporal_workflow_id,
			  'temporalRunId', gr.temporal_run_id,
			  'generationProfile', to_jsonb(gp),
			  'shotSpec', to_jsonb(ssr),
			  'promptSnapshot', to_jsonb(ps),
			  'effectiveContextSnapshot', to_jsonb(ecs),
			  'attempts', COALESCE((
			    SELECT jsonb_agg(to_jsonb(ga) ORDER BY ga.sequence)
			    FROM video_pipeline.generation_attempts ga
			    WHERE ga.generation_run_id = gr.id
			  ), '[]'::jsonb),
			  'providerJobs', COALESCE((
			    SELECT jsonb_agg(
			      to_jsonb(pj) || jsonb_build_object(
			        'providerProfile', to_jsonb(pp) - 'credential_ref' - 'credential_fingerprint',
			        'capabilitySnapshot', to_jsonb(pcs)
			      )
			      ORDER BY pj.created_at
			    )
			    FROM video_pipeline.provider_jobs pj
			    JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			    JOIN video_pipeline.provider_profiles pp ON pp.id = pj.provider_profile_id
			    JOIN video_pipeline.provider_capability_snapshots pcs ON pcs.id = pj.capability_snapshot_id
			    WHERE ga.generation_run_id = gr.id
			  ), '[]'::jsonb),
			  'artifacts', COALESCE((
			    SELECT jsonb_agg(to_jsonb(a) || jsonb_build_object('role', ra.role) ORDER BY ra.role, a.id)
			    FROM video_pipeline.run_artifacts ra
			    JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
			    WHERE ra.generation_run_id = gr.id
			  ), '[]'::jsonb),
			  'qcReports', COALESCE((
			    SELECT jsonb_agg(to_jsonb(qr) ORDER BY qr.created_at)
			    FROM video_pipeline.qc_reports qr
			    WHERE qr.generation_run_id = gr.id
			  ), '[]'::jsonb),
			  'costLedger', COALESCE((
			    SELECT jsonb_agg(to_jsonb(cl) ORDER BY cl.created_at)
			    FROM video_pipeline.cost_ledger cl
			    JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
			    JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			    WHERE ga.generation_run_id = gr.id
			  ), '[]'::jsonb),
			  'licenseBOM', COALESCE((
			    SELECT jsonb_agg(
			      jsonb_build_object(
			        'assetVersionId', av.id,
			        'assetHash', av.content_hash,
			        'licenseSnapshot', to_jsonb(ls),
			        'consent', CASE WHEN ca.id IS NULL THEN NULL ELSE to_jsonb(ca) END
			      )
			      ORDER BY av.id
			    )
			    FROM unnest(ssr.asset_version_refs) requested(id)
			    JOIN video_pipeline.asset_versions av ON av.id = requested.id
			    JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
			    LEFT JOIN video_pipeline.consent_assets ca ON ca.id = av.consent_asset_id
			  ), '[]'::jsonb)
			)
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			JOIN video_pipeline.episode_revisions er ON er.episode_id = ep.id
			JOIN video_pipeline.prompt_snapshots ps ON ps.id = gr.prompt_snapshot_id
			JOIN video_pipeline.effective_context_snapshots ecs ON ecs.id = ps.effective_context_snapshot_id
			JOIN video_pipeline.generation_profiles gp ON gp.id = gr.generation_profile_id
			WHERE gr.id = $1
			  AND er.id = $2
			  AND gr.state = 'SUCCEEDED'
			  AND EXISTS (
			    SELECT 1 FROM video_pipeline.qc_reports qr
			    WHERE qr.generation_run_id = gr.id AND qr.state = 'PASSED'
			  )`,
			runID, episodeRevisionID,
		).Scan(&execution); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"manifest contains a run outside the episode or without passing QC",
					"finish and approve every exact episode run",
				)
			}
			return nil, fmt.Errorf("assemble manifest run %s: %w", runID, err)
		}
		executions = append(executions, json.RawMessage(execution))
	}
	var approvals []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT COALESCE(jsonb_agg(
		  to_jsonb(ad) || jsonb_build_object(
		    'bindings', (
		      SELECT COALESCE(jsonb_agg(to_jsonb(ab) ORDER BY ab.object_type, ab.revision_id), '[]'::jsonb)
		      FROM video_pipeline.approval_bindings ab
		      WHERE ab.decision_id = ad.id
		    )
		  )
		  ORDER BY ad.decided_at
		), '[]'::jsonb)
		FROM video_pipeline.approval_decisions ad
		JOIN video_pipeline.episode_revisions er ON er.episode_id = ad.episode_id
		WHERE er.id = $1`,
		episodeRevisionID,
	).Scan(&approvals); err != nil {
		return nil, fmt.Errorf("assemble manifest approvals: %w", err)
	}
	var totalMicros int64
	var currency string
	if err := p.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(cl.amount_micros), 0),
		       COALESCE(MAX(cl.currency), '')
		FROM video_pipeline.cost_ledger cl
		JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
		JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = ANY($1::uuid[])
		  AND cl.entry_type = 'ACTUAL'`,
		runIDs,
	).Scan(&totalMicros, &currency); err != nil {
		return nil, fmt.Errorf("assemble manifest cost summary: %w", err)
	}
	rows, err := p.pool.Query(ctx, `
		SELECT a.artifact_uri
		FROM video_pipeline.run_artifacts ra
		JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
		WHERE ra.generation_run_id = ANY($1::uuid[])
		  AND ra.role = 'OUTPUT'
		ORDER BY ra.generation_run_id, a.content_hash`,
		runIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("assemble manifest outputs: %w", err)
	}
	defer rows.Close()
	outputs := make([]string, 0, len(runIDs))
	for rows.Next() {
		var output string
		if err := rows.Scan(&output); err != nil {
			return nil, fmt.Errorf("scan manifest output: %w", err)
		}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate manifest outputs: %w", err)
	}
	inputs := []string{"episode-revision:" + input.EpisodeRevisionID}
	for _, runID := range input.RunIDs {
		inputs = append(inputs, "generation-run:"+runID)
	}
	payload := map[string]any{
		"schemaVersion":      "v1",
		"scopeType":          "EPISODE",
		"episodeRevisionId":  input.EpisodeRevisionID,
		"workflowId":         step.WorkflowID,
		"providerExecutions": executions,
		"inputs":             inputs,
		"outputs":            outputs,
		"costSummary": map[string]any{
			"actualMicros": totalMicros,
			"currency":     currency,
		},
		"gateHistory": json.RawMessage(approvals),
	}
	return json.Marshal(payload)
}

func (p *Postgres) CommitEpisodeManifest(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CreateGate3Input,
	payload []byte,
	artifact artifactstore.Artifact,
) error {
	if !json.Valid(payload) || artifact.URI != "cas://sha256/"+artifact.Digest {
		return errors.New("manifest payload or CAS identity is invalid")
	}
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return errors.New("manifest run IDs must be UUIDs")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var seriesID, episodeID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT ep.series_id, ep.id
			FROM video_pipeline.episode_revisions er
			JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
			WHERE er.id = $1 AND er.status = 'G2_APPROVED'
			FOR SHARE`,
			episodeRevisionID,
		).Scan(&seriesID, &episodeID); err != nil {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"manifest episode is not G2_APPROVED",
				"complete G2 before creating the final manifest",
			)
		}
		artifactID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("artifact:"+artifact.Digest))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.artifacts
				(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
			VALUES ($1, $2, $3, 'application/json', $4, '{"kind":"generation-manifest"}', 'ACTIVE')
			ON CONFLICT (content_hash) DO NOTHING`,
			artifactID, artifact.Digest, artifact.URI, artifact.Size,
		); err != nil {
			return struct{}{}, fmt.Errorf("insert manifest artifact: %w", err)
		}
		if err := tx.QueryRow(ctx,
			`SELECT id FROM video_pipeline.artifacts WHERE content_hash = $1`,
			artifact.Digest,
		).Scan(&artifactID); err != nil {
			return struct{}{}, fmt.Errorf("resolve manifest artifact: %w", err)
		}
		manifestID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("manifest:"+artifact.Digest))
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.generation_manifests
				(id, scope_type, scope_revision_id, schema_version, payload,
				 manifest_hash, artifact_id)
			VALUES ($1, 'EPISODE', $2, 'v1', $3, $4, $5)
			ON CONFLICT (scope_type, scope_revision_id, manifest_hash) DO NOTHING`,
			manifestID, episodeRevisionID, payload, artifact.Digest, artifactID,
		)
		if err != nil {
			return struct{}{}, fmt.Errorf("insert generation manifest: %w", err)
		}
		for _, runID := range runIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.run_artifacts
					(generation_run_id, artifact_id, role)
				VALUES ($1, $2, 'MANIFEST')
				ON CONFLICT DO NOTHING`,
				runID, artifactID,
			); err != nil {
				return struct{}{}, fmt.Errorf("link manifest to run: %w", err)
			}
		}
		reviewID := uuid.NewSHA1(manifestID, []byte("review:G3"))
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.review_tasks
				(id, series_id, episode_id, review_type, state, assigned_role)
			VALUES ($1, $2, $3, 'G3', 'OPEN', 'DIRECTOR')
			ON CONFLICT (id) DO NOTHING`,
			reviewID, seriesID, episodeID,
		); err != nil {
			return struct{}{}, fmt.Errorf("open G3 review: %w", err)
		}
		if tag.RowsAffected() == 1 {
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(manifestID, []byte("audit")),
				uuid.NewSHA1(manifestID, []byte("outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"manifest.created", "MANIFEST", manifestID,
				nil, nil, "", step.TraceID,
				map[string]any{
					"episodeRevisionId": input.EpisodeRevisionID,
					"manifestHash":      artifact.Digest,
					"artifactUri":       artifact.URI,
					"runIds":            input.RunIDs,
				},
				p.now().UTC(),
			); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

func (p *Postgres) RecordProviderCancellation(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.CancelProviderJobInput,
	result orchestration.CancelProviderResult,
) error {
	runID, err := uuid.Parse(input.Dispatch.Run.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var currentState string
		if err := tx.QueryRow(ctx, `
			SELECT state
			FROM video_pipeline.generation_runs
			WHERE id = $1
			FOR UPDATE`,
			runID,
		).Scan(&currentState); err != nil {
			return struct{}{}, fmt.Errorf("lock cancelling generation run: %w", err)
		}
		if result.State == "UNKNOWN" {
			var prepared bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM video_pipeline.provider_jobs pj
					JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
					WHERE ga.generation_run_id = $1
				)`,
				runID,
			).Scan(&prepared); err != nil {
				return struct{}{}, fmt.Errorf("check cancellation provider job projection: %w", err)
			}
			if !prepared {
				result.State = "CANCELLED"
				result.ErrorCode = ""
			}
		}
		switch result.State {
		case "SUCCEEDED":
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_runs
				SET failure_class = NULL, failure_code = NULL
				WHERE id = $1 AND state = 'SUCCEEDED'`,
				runID,
			); err != nil {
				return struct{}{}, fmt.Errorf("clear terminal-success cancellation reason: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.operation_requests
				SET state = CASE
					WHEN operation_type = 'CANCEL_GENERATION_RUN' THEN 'SUCCEEDED'
					WHEN operation_type = 'CREATE_GENERATION_RUN' THEN 'SUCCEEDED'
					WHEN operation_type = 'RESUME_GENERATION_RUN' THEN 'SUCCEEDED'
					ELSE state
				END,
				updated_at = now()
				WHERE aggregate_type = 'GENERATION_RUN' AND aggregate_id = $1
				  AND state IN ('ACCEPTED', 'RUNNING', 'CANCEL_REQUESTED')`,
				runID,
			); err != nil {
				return struct{}{}, fmt.Errorf("finish terminal-success cancellation race: %w", err)
			}
		case "CANCELLED":
			if currentState != "SUCCEEDED" {
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.provider_jobs pj
					SET state = 'CANCELLED', terminal_at = now(), updated_at = now(),
					    error_code = NULL, error_snapshot = NULL
					FROM video_pipeline.generation_attempts ga
					WHERE pj.generation_attempt_id = ga.id AND ga.generation_run_id = $1
					  AND pj.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel provider job projection: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'CANCELLED', finished_at = now()
					WHERE generation_run_id = $1
					  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel generation attempt: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_runs
					SET state = 'CANCELLED', finished_at = now(),
					    failure_class = NULL, failure_code = NULL
					WHERE id = $1 AND state <> 'SUCCEEDED'`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel generation run: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.shot_spec_revisions ssr
					SET lifecycle_state = 'CANCELLED'
					FROM video_pipeline.generation_runs gr
					WHERE gr.id = $1 AND ssr.id = gr.shot_spec_revision_id`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel shot revision: %w", err)
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.operation_requests
				SET state = CASE
					WHEN operation_type = 'CANCEL_GENERATION_RUN' THEN 'SUCCEEDED'
					WHEN operation_type = 'CREATE_GENERATION_RUN' THEN 'CANCELLED'
					WHEN operation_type = 'RESUME_GENERATION_RUN' THEN 'SUCCEEDED'
					ELSE 'CANCELLED'
				END,
				updated_at = now()
				WHERE aggregate_type = 'GENERATION_RUN' AND aggregate_id = $1
				  AND state IN ('ACCEPTED', 'RUNNING', 'CANCEL_REQUESTED')`,
				runID,
			); err != nil {
				return struct{}{}, fmt.Errorf("finish cancellation operations: %w", err)
			}
		default:
			if currentState != "SUCCEEDED" {
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.provider_jobs pj
					SET state = 'UNKNOWN', error_code = 'CANCEL_NOT_CONFIRMED',
					    error_snapshot = '{"requiresReconciliation":true}'::jsonb,
					    next_poll_at = now(), updated_at = now()
					FROM video_pipeline.generation_attempts ga
					WHERE pj.generation_attempt_id = ga.id AND ga.generation_run_id = $1
					  AND pj.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("mark provider cancellation unknown: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'UNKNOWN', failure_code = 'CANCEL_NOT_CONFIRMED'
					WHERE generation_run_id = $1
					  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("mark attempt cancellation unknown: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_runs
					SET state = CASE
					      WHEN $2 = 'RECONCILE_HISTORY' THEN 'RECONCILING'
					      ELSE 'UNKNOWN'
					    END,
					    failure_class = 'INFRASTRUCTURE',
					    failure_code = 'CANCEL_NOT_CONFIRMED'
					WHERE id = $1 AND state <> 'SUCCEEDED'`,
					runID, input.ReasonCode,
				); err != nil {
					return struct{}{}, fmt.Errorf("mark run cancellation unknown: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.operation_requests
					SET state = 'RUNNING', updated_at = now()
					WHERE aggregate_type = 'GENERATION_RUN' AND aggregate_id = $1
					  AND operation_type = 'CANCEL_GENERATION_RUN'
					  AND state = 'ACCEPTED'`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("mark cancellation reconciliation active: %w", err)
				}
			}
		}
		if result.State == "UNKNOWN" &&
			(input.ReasonCode == "RECONCILE_HISTORY" || currentState == "UNKNOWN") {
			// Provider outages can produce many Activity retries. Preserve the
			// durable UNKNOWN/RECONCILING projection without duplicating
			// audit/outbox facts until a terminal result is observed.
			return struct{}{}, nil
		}
		return struct{}{}, insertAuditAndOutbox(
			ctx, tx,
			uuid.NewSHA1(runID, []byte("provider-cancellation-audit:"+step.ActivityID)),
			uuid.NewSHA1(runID, []byte("provider-cancellation-outbox:"+step.ActivityID)),
			controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
			"provider_job.cancellation_reconciled", "GENERATION_RUN", runID,
			nil, nil, input.ReasonCode, step.TraceID,
			map[string]any{
				"state": result.State, "errorCode": result.ErrorCode,
				"upstreamTaskId": result.UpstreamTaskID,
			},
			p.now().UTC(),
		)
	})
	return err
}

func (p *Postgres) ProviderJobPrepared(ctx context.Context, runIDRaw string) (bool, error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return false, errors.New("runId must be a UUID")
	}
	var prepared bool
	if err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM video_pipeline.provider_jobs pj
			JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			WHERE ga.generation_run_id = $1
		)`,
		runID,
	).Scan(&prepared); err != nil {
		return false, fmt.Errorf("check provider job projection: %w", err)
	}
	return prepared, nil
}

func (p *Postgres) FinalizeShotRun(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.FinalizeShotRunInput,
) error {
	runID, err := uuid.Parse(input.RunID)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	operationID, err := uuid.Parse(input.OperationID)
	if err != nil {
		return errors.New("operationId must be a UUID")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		var currentState string
		if err := tx.QueryRow(ctx,
			`SELECT state FROM video_pipeline.generation_runs WHERE id = $1 FOR UPDATE`,
			runID,
		).Scan(&currentState); err != nil {
			return struct{}{}, fmt.Errorf("lock finalizing shot run: %w", err)
		}
		operationState := "SUCCEEDED"
		if input.State == "FAILED" {
			operationState = "FAILED"
			if currentState != "SUCCEEDED" && currentState != "CANCELLED" {
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_runs
					SET state = 'FAILED', failure_class = NULLIF($2, ''),
					    failure_code = NULLIF($3, ''), finished_at = now()
					WHERE id = $1`,
					runID, input.FailureClass, input.FailureCode,
				); err != nil {
					return struct{}{}, fmt.Errorf("fail shot run: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.generation_attempts
					SET state = 'FAILED', failure_code = NULLIF($2, ''), finished_at = now()
					WHERE generation_run_id = $1
					  AND state NOT IN ('SUCCEEDED', 'CANCELLED')`,
					runID, input.FailureCode,
				); err != nil {
					return struct{}{}, fmt.Errorf("fail shot attempt: %w", err)
				}
			}
		} else if input.State == "SUCCEEDED" && currentState != "SUCCEEDED" {
			var providerSucceeded bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM video_pipeline.provider_jobs pj
					JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
					WHERE ga.generation_run_id = $1 AND pj.state = 'SUCCEEDED'
				)`,
				runID,
			).Scan(&providerSucceeded); err != nil {
				return struct{}{}, fmt.Errorf("verify provider success before finalizing run: %w", err)
			}
			if !providerSucceeded {
				return struct{}{}, controlplane.NewConflictError(
					controlplane.CodeConflict,
					"shot workflow cannot succeed before its provider job succeeds",
				)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_runs
				SET state = 'SUCCEEDED', failure_class = NULL, failure_code = NULL,
				    finished_at = COALESCE(finished_at, now())
				WHERE id = $1 AND state NOT IN ('CANCELLED', 'FAILED')`,
				runID,
			); err != nil {
				return struct{}{}, fmt.Errorf("finalize successful shot run: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.operation_requests
			SET state = $2, updated_at = now()
			WHERE id = $1 AND aggregate_type = 'GENERATION_RUN'
			  AND aggregate_id = $3 AND state IN ('ACCEPTED', 'RUNNING')`,
			operationID, operationState, runID,
		); err != nil {
			return struct{}{}, fmt.Errorf("finalize shot operation: %w", err)
		}
		return struct{}{}, insertAuditAndOutbox(
			ctx, tx,
			uuid.NewSHA1(runID, []byte("shot-finalized-audit:"+input.State)),
			uuid.NewSHA1(runID, []byte("shot-finalized-outbox:"+input.State)),
			controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
			"generation_run.workflow_finalized", "GENERATION_RUN", runID,
			nil, nil, input.FailureCode, step.TraceID,
			map[string]any{"state": input.State, "operationId": input.OperationID},
			p.now().UTC(),
		)
	})
	return err
}

func uuidStrings(values []uuid.UUID) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = value.String()
	}
	return result
}

func temporalRunID(step orchestration.WorkflowStep) any {
	// ActivityInfo exposes the workflow ID but not a stable SDK-independent
	// run ID through WorkflowStep. PostgreSQL keeps it nullable; the control
	// operation retains the Temporal run ID returned at workflow start.
	return nil
}
