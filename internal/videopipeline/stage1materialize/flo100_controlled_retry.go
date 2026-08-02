package stage1materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type controlledRetryTerminalEvidence struct {
	GenerationRunState string          `json:"generationRunState"`
	RunFailureClass    string          `json:"runFailureClass"`
	AttemptState       string          `json:"attemptState"`
	ProviderJobID      string          `json:"providerJobId"`
	ProviderJobState   string          `json:"providerJobState"`
	UpstreamTaskID     string          `json:"upstreamTaskId"`
	UpstreamRequestID  string          `json:"upstreamRequestId"`
	ErrorCode          string          `json:"errorCode"`
	ErrorSnapshot      json.RawMessage `json:"errorSnapshot"`
	TerminalAt         time.Time       `json:"terminalAt"`
	ReservationStatus  string          `json:"reservationStatus"`
	CostEvidenceCount  int64           `json:"costEvidenceCount"`
}

// BindFLO100LiveControlledRetry seals the only allowed +1 authority after an
// evidence-complete primary terminal failure. It creates the new creative Run
// and its sequence-one attempt in the same SERIALIZABLE transaction. Replays
// are accepted only when every immutable binding is identical.
func BindFLO100LiveControlledRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	plan stage1.Plan,
	primary stage1.ExecutionPackage,
	retry stage1.ControlledRetryPackage,
) error {
	if pool == nil {
		return errors.New("PostgreSQL pool is required")
	}
	if err := retry.Validate(plan, primary); err != nil {
		return err
	}
	if primary.LiveActivation == nil || primary.BatchID != formalBatchIDs[0] || retry.BatchID != formalBatchIDs[0] {
		return errors.New("controlled retry requires the FLO-100 Batch A live activation")
	}
	activationID, err := uuid.Parse(primary.LiveActivation.ActivationID)
	if err != nil {
		return errors.New("live activationId must be a UUID")
	}
	retryRunID, err := uuid.Parse(retry.Job.Run.RunID)
	if err != nil {
		return errors.New("controlled retry runId must be a UUID")
	}
	approvalID, err := uuid.Parse(retry.Approval.ApprovalID)
	if err != nil {
		return errors.New("controlled retry approvalId must be a UUID")
	}
	duplicateEvidenceID, err := uuid.Parse(retry.Approval.DuplicateTaskEvidenceID)
	if err != nil {
		return errors.New("controlled retry duplicateTaskEvidenceId must be a UUID")
	}
	original, ok := primary.Job(retry.Job.ShotID)
	if !ok || original.AttemptID != retry.Approval.OriginalAttemptID {
		return errors.New("controlled retry does not identify its exact primary attempt")
	}
	primaryRunID, err := uuid.Parse(original.Run.RunID)
	if err != nil {
		return errors.New("primary runId must be a UUID")
	}
	if retry.Job.PromptSnapshotID != original.PromptSnapshotID ||
		retry.Job.PromptSnapshotHash != original.PromptSnapshotHash ||
		retry.Job.EstimatedVideoTokens != original.EstimatedVideoTokens ||
		retry.Job.PredictedAFPMilli != original.PredictedAFPMilli ||
		retry.Job.EstimatedNonSubscriptionCashMicros != original.EstimatedNonSubscriptionCashMicros ||
		retry.Job.BillingMode != original.BillingMode {
		return errors.New("FLO-100 controlled retry must reuse the approved primary prompt and estimates")
	}

	packageJSON, err := json.Marshal(retry)
	if err != nil {
		return fmt.Errorf("encode controlled retry package: %w", err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var actorID, executionHash, sourceCommit string
	var generationPlanID, generationProfileID, videoBudgetID, videoProfileID uuid.UUID
	var authorization json.RawMessage
	var submitValidUntil time.Time
	if err := tx.QueryRow(ctx, `
		SELECT a.live_generation_plan_id,gr.generation_profile_id,a.video_budget_approval_id,
		       a.video_provider_profile_id,a.source_authorization,a.source_code_commit,
		       a.live_execution_package_hash,sa.actor_id,sa.valid_until
		FROM video_pipeline.stage1_live_activations a
		JOIN video_pipeline.stage1_live_activation_runs ar
		  ON ar.activation_id=a.id AND ar.run_id=$2
		JOIN video_pipeline.generation_runs gr ON gr.id=ar.run_id
		JOIN video_pipeline.stage1_live_projection_seals ps ON ps.activation_id=a.id
		JOIN video_pipeline.stage1_live_submit_authorizations sa
		  ON sa.activation_id=a.id AND sa.source_code_commit=a.source_code_commit
		 AND sa.execution_package_hash=a.live_execution_package_hash
		 AND sa.projection_hash=ps.projection_hash
		WHERE a.id=$1 AND a.batch_id='flo100-gold-a-v1'
		FOR UPDATE OF a`, activationID, primaryRunID).Scan(
		&generationPlanID, &generationProfileID, &videoBudgetID, &videoProfileID,
		&authorization, &sourceCommit, &executionHash, &actorID, &submitValidUntil,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("controlled retry has no exact Batch A primary live authority")
		}
		return fmt.Errorf("lock controlled retry activation: %w", err)
	}
	if executionHash != primary.ContentHash || sourceCommit != primary.LiveActivation.SourceCodeCommit ||
		!submitValidUntil.After(time.Now().UTC()) {
		return errors.New("controlled retry parent package, commit, or submit authorization drifted")
	}
	var envelope liveAuthorization
	if err := json.Unmarshal(authorization, &envelope); err != nil {
		return fmt.Errorf("decode controlled retry live authorization: %w", err)
	}
	if !envelope.Decision.BatchAProviderPostAuthorizedConditionally ||
		envelope.Decision.BatchBProviderPostAuthorized || envelope.Decision.BatchCProviderPostAuthorized ||
		envelope.Decision.Stage4Authorized || envelope.BatchABudget.VideoControlledRetriesMaximum != 1 ||
		envelope.BatchABudget.AutomaticRetryAllowed || envelope.BatchABudget.AutomaticProviderSwitchAllowed {
		return errors.New("controlled retry authority is not the exact manual Batch A +1 envelope")
	}

	var existingHash string
	existingErr := tx.QueryRow(ctx, `SELECT controlled_retry_package_hash
		FROM video_pipeline.stage1_live_controlled_retries WHERE activation_id=$1 FOR SHARE`, activationID).
		Scan(&existingHash)
	if existingErr == nil {
		if existingHash != retry.ContentHash {
			return errors.New("Batch A live activation is already bound to another controlled retry")
		}
		return verifyControlledRetryReplay(ctx, tx, activationID, retry, packageJSON)
	}
	if !errors.Is(existingErr, pgx.ErrNoRows) {
		return fmt.Errorf("read existing controlled retry authority: %w", existingErr)
	}

	var primaryAttemptID, primaryProviderJobID uuid.UUID
	var evidence controlledRetryTerminalEvidence
	if err := tx.QueryRow(ctx, `
		SELECT ga.id,gr.state,COALESCE(gr.failure_class,''),ga.state,pj.id,pj.state,
		       COALESCE(pj.upstream_task_id,''),COALESCE(pj.upstream_request_id,''),
		       COALESCE(pj.error_code,''),COALESCE(pj.error_snapshot,'{}'::jsonb),pj.terminal_at,
		       br.status,(SELECT COUNT(*) FROM video_pipeline.cost_ledger cl
		                  WHERE cl.provider_job_id=pj.id AND cl.budget_reservation_id=br.id)
		FROM video_pipeline.stage1_live_activation_runs ar
		JOIN video_pipeline.generation_runs gr ON gr.id=ar.run_id
		JOIN video_pipeline.generation_attempts ga
		  ON ga.generation_run_id=gr.id AND ga.sequence=1
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id=ga.id
		JOIN video_pipeline.budget_reservations br ON br.id=pj.budget_reservation_id
		WHERE ar.activation_id=$1 AND ar.run_id=$2
		FOR SHARE OF ar,gr,ga,pj`, activationID, primaryRunID).Scan(
		&primaryAttemptID, &evidence.GenerationRunState, &evidence.RunFailureClass,
		&evidence.AttemptState, &primaryProviderJobID, &evidence.ProviderJobState,
		&evidence.UpstreamTaskID, &evidence.UpstreamRequestID, &evidence.ErrorCode,
		&evidence.ErrorSnapshot, &evidence.TerminalAt,
		&evidence.ReservationStatus, &evidence.CostEvidenceCount,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("controlled retry primary has no durable Provider terminal evidence")
		}
		return fmt.Errorf("read controlled retry terminal evidence: %w", err)
	}
	evidence.ProviderJobID = primaryProviderJobID.String()
	if evidence.GenerationRunState != "FAILED" || evidence.RunFailureClass == "" ||
		evidence.AttemptState != "FAILED" || evidence.ProviderJobState != "FAILED" ||
		evidence.UpstreamTaskID == "" || evidence.UpstreamRequestID == "" ||
		evidence.ErrorCode == "" || evidence.ErrorCode != retry.Approval.FailureClass ||
		len(evidence.ErrorSnapshot) == 0 || evidence.TerminalAt.IsZero() ||
		evidence.ReservationStatus != "SETTLED" || evidence.CostEvidenceCount < 1 ||
		!terminalFailureSnapshotExact(evidence.ErrorSnapshot, evidence.ErrorCode) {
		return errors.New("controlled retry requires an evidence-complete primary terminal failure")
	}
	evidenceHash, err := digest(evidence)
	if err != nil {
		return err
	}
	wantDuplicateEvidenceID := uuid.NewSHA1(primaryProviderJobID, []byte("duplicate-task-evidence:"+evidenceHash))
	if duplicateEvidenceID != wantDuplicateEvidenceID {
		return errors.New("controlled retry duplicate-task evidence is not bound to the terminal Provider evidence")
	}

	wantDigest, err := repository.GenerationRunSpecDigest(
		retry.Job.ShotSpecRevisionID, retry.Job.PromptSnapshotID, retry.Job.PromptSnapshotHash,
		generationProfileID.String(), generationPlanID.String(), retry.Job.Route, retry.Job.Run.Attempt,
	)
	if err != nil {
		return err
	}
	if retry.Job.Run.Attempt != 2 || retry.Job.Run.RunSpecDigest != wantDigest ||
		retry.Job.GenerationPlanID != generationPlanID.String() ||
		retry.Job.BudgetApprovalID != videoBudgetID.String() ||
		retry.Job.ProviderProfileID != videoProfileID.String() {
		return errors.New("controlled retry Run, plan, approval, or route binding drifted")
	}
	retryAttemptID := uuid.NewSHA1(retryRunID, []byte("attempt:1"))
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.generation_runs
		(id,shot_spec_revision_id,prompt_snapshot_id,generation_profile_id,temporal_workflow_id,
		 run_spec_digest,creative_attempt,state,dry_run,budget_approval_id,trace_id,created_by)
		VALUES ($1,$2,$3,$4,$5,$6,2,'VALIDATED',false,$7,$8,$9)`,
		retryRunID, mustUUID(retry.Job.ShotSpecRevisionID), mustUUID(retry.Job.PromptSnapshotID),
		generationProfileID, retry.Job.WorkflowID, retry.Job.Run.RunSpecDigest,
		videoBudgetID.String(), retry.Job.TraceID, actorID); err != nil {
		return fmt.Errorf("insert controlled retry generation run: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.generation_attempts
		(id,generation_run_id,sequence,attempt_kind,state,input_hash,model_snapshot,parameter_diff)
		VALUES ($1,$2,1,'CREATIVE_REVISION','VALIDATED',$3,$4,$5)`,
		retryAttemptID, retryRunID, retry.Job.Run.RunSpecDigest, retry.Job.Route,
		map[string]any{"controlledRetryPackageHash": retry.ContentHash, "approvalId": approvalID,
			"duplicateTaskEvidenceId": duplicateEvidenceID, "primaryRunId": primaryRunID}); err != nil {
		return fmt.Errorf("insert controlled retry generation attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_controlled_retries
		(activation_id,primary_run_id,primary_attempt_id,primary_provider_job_id,primary_attempt_identity,
		 primary_failure_class,primary_failure_evidence_hash,retry_run_id,retry_attempt_id,retry_approval_id,
		 duplicate_task_evidence_id,controlled_retry_package_hash,controlled_retry_package,
		 source_execution_package_hash,created_by,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		activationID, primaryRunID, primaryAttemptID, primaryProviderJobID, original.AttemptID,
		retry.Approval.FailureClass, evidenceHash, retryRunID, retryAttemptID, approvalID,
		duplicateEvidenceID, retry.ContentHash, packageJSON, primary.ContentHash, actorID, time.Now().UTC()); err != nil {
		return fmt.Errorf("insert controlled retry authority: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','generation_run.created','GENERATION_RUN',$4,$5,$6,$7)`,
		uuid.NewSHA1(retryRunID, []byte("audit")), time.Now().UTC(), actorID, retryRunID,
		liveSourceReason, retry.Job.TraceID, map[string]any{
			"workflowId": retry.Job.WorkflowID, "shotSpecRevisionId": retry.Job.ShotSpecRevisionID,
			"promptSnapshotId": retry.Job.PromptSnapshotID, "runSpecDigest": retry.Job.Run.RunSpecDigest,
			"creativeAttempt": 2, "generationPlanId": generationPlanID,
			"controlledRetryPackageHash": retry.ContentHash, "primaryRunId": primaryRunID,
		}); err != nil {
		return fmt.Errorf("insert controlled retry Run audit: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','flo100.live_controlled_retry.bound','STAGE1_LIVE_ACTIVATION',$4,$5,$6,$7)`,
		uuid.NewSHA1(activationID, []byte("audit:controlled-retry:"+retry.ContentHash)), time.Now().UTC(),
		actorID, activationID, liveSourceReason, retry.Job.TraceID, map[string]any{
			"controlledRetryPackageHash": retry.ContentHash, "primaryRunId": primaryRunID,
			"retryRunId": retryRunID, "approvalId": approvalID,
			"duplicateTaskEvidenceId": duplicateEvidenceID, "failureEvidenceHash": evidenceHash,
		}); err != nil {
		return fmt.Errorf("insert controlled retry audit: %w", err)
	}
	return tx.Commit(ctx)
}

func terminalFailureSnapshotExact(snapshot json.RawMessage, errorCode string) bool {
	var evidence struct {
		Code          string         `json:"code"`
		ProviderCost  map[string]any `json:"providerCost"`
		ProviderUsage map[string]any `json:"providerUsage"`
	}
	return json.Unmarshal(snapshot, &evidence) == nil && evidence.Code == errorCode &&
		evidence.ProviderCost != nil && evidence.ProviderUsage != nil
}

func verifyControlledRetryReplay(
	ctx context.Context,
	tx pgx.Tx,
	activationID uuid.UUID,
	retry stage1.ControlledRetryPackage,
	packageJSON []byte,
) error {
	var runID uuid.UUID
	var packageHash, sourceHash string
	var storedPackage json.RawMessage
	if err := tx.QueryRow(ctx, `
		SELECT retry_run_id,controlled_retry_package_hash,controlled_retry_package,source_execution_package_hash
		FROM video_pipeline.stage1_live_controlled_retries WHERE activation_id=$1 FOR SHARE`, activationID).
		Scan(&runID, &packageHash, &storedPackage, &sourceHash); err != nil {
		return fmt.Errorf("verify controlled retry replay: %w", err)
	}
	var want, got any
	if err := json.Unmarshal(packageJSON, &want); err != nil {
		return err
	}
	if err := json.Unmarshal(storedPackage, &got); err != nil {
		return err
	}
	if runID.String() != retry.Job.Run.RunID || packageHash != retry.ContentHash ||
		sourceHash != retry.ParentExecutionPackageHash || !reflect.DeepEqual(got, want) {
		return errors.New("controlled retry replay differs from its immutable authority")
	}
	var exact bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id=gr.id AND ga.sequence=1
		WHERE gr.id=$1 AND gr.run_spec_digest=$2 AND gr.creative_attempt=2
		  AND ga.attempt_kind='CREATIVE_REVISION' AND ga.input_hash=$2
	)`, runID, retry.Job.Run.RunSpecDigest).Scan(&exact); err != nil {
		return err
	}
	if !exact || strings.TrimSpace(packageHash) == "" {
		return errors.New("controlled retry replay Run projection drifted")
	}
	return tx.Commit(ctx)
}
