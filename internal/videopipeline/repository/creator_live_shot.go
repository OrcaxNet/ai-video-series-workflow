package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	creatorTaskLimit              = 1
	creatorVideoTokenLimit        = int64(1_000_000)
	creatorProjectTaskLimit       = 3
	creatorProjectVideoTokenLimit = int64(3_000_000)
	creatorBillingMode            = "subscription"
)

type creatorPreparedRequest struct {
	Input    orchestration.ExecuteProviderJobInput `json:"input"`
	Prepared orchestration.PreparedProviderJob     `json:"prepared"`
}

func (p *Postgres) CreateCreatorLiveShotPlan(
	ctx context.Context,
	command controlplane.CreatorLiveShotPlanCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.CreatorLiveShotPlan], error) {
	now := p.now().UTC()
	planID := uuid.New()
	seriesID := uuid.NewSHA1(planID, []byte("series"))
	episodeEntityID := uuid.NewSHA1(planID, []byte("episode"))
	sceneEntityID := uuid.NewSHA1(planID, []byte("scene"))
	shotEntityID := uuid.NewSHA1(planID, []byte("shot"))
	sourceID := uuid.NewSHA1(planID, []byte("source-revision"))
	episodeID := uuid.NewSHA1(planID, []byte("episode-revision"))
	sceneID := uuid.NewSHA1(planID, []byte("scene-revision"))
	shotID := uuid.NewSHA1(planID, []byte("shot-spec-revision"))
	promptID := uuid.NewSHA1(planID, []byte("prompt-snapshot"))
	profileID := uuid.NewSHA1(planID, []byte("generation-profile-revision"))
	profileGroupID := uuid.NewSHA1(planID, []byte("generation-profile"))
	scriptID := uuid.NewSHA1(planID, []byte("script-revision"))
	storyboardID := uuid.NewSHA1(planID, []byte("storyboard-revision"))
	licenseID := uuid.NewSHA1(planID, []byte("source-license"))
	gate2ID := uuid.NewSHA1(planID, []byte("gate2-decision"))
	safetyID := uuid.NewSHA1(planID, []byte("safety-decision"))
	budgetApprovalID := uuid.NewSHA1(planID, []byte("budget-approval"))
	effectiveContextID := uuid.NewSHA1(planID, []byte("effective-context"))
	seriesContextID := uuid.NewSHA1(planID, []byte("context-series"))
	episodeContextID := uuid.NewSHA1(planID, []byte("context-episode"))
	sceneContextID := uuid.NewSHA1(planID, []byte("context-scene"))
	shotContextID := uuid.NewSHA1(planID, []byte("context-shot"))
	providerProfileID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("creator-provider:"+command.Route.Capability.Provider))
	capabilityID := uuid.NewSHA1(providerProfileID, []byte(command.Route.SnapshotHash))
	expiresAt := now.Add(15 * time.Minute)

	output := providercontract.OutputSpec{
		Resolution: "720p", AspectRatio: command.AspectRatio, FPS: 24,
		DurationMillis: 5_000, Format: "mp4", GenerateAudio: false,
	}
	if command.AspectRatio == "16:9" {
		output.Width, output.Height = 1280, 720
	} else {
		output.Width, output.Height = 720, 1280
	}
	route := providercontract.ModelSnapshot{
		CapabilityAlias: string(command.Route.Alias),
		Provider:        command.Route.Capability.Provider,
		ModelID:         command.Route.Capability.ModelFamily,
		RouteVersion:    command.Route.RouteVersion,
		CapabilityHash:  command.Route.SnapshotHash,
		Verification:    "control_plane_capability_snapshot",
	}
	contextSnapshot := map[string]any{
		"series":  map[string]any{"id": seriesContextID, "scopeId": seriesID, "style": "coherent cinematic AI series"},
		"episode": map[string]any{"id": episodeContextID, "scopeId": episodeEntityID, "durationMs": 5_000},
		"scene":   map[string]any{"id": sceneContextID, "scopeId": sceneEntityID, "textHash": command.SourceArtifactHash},
		"shot":    map[string]any{"id": shotContextID, "scopeId": shotEntityID, "ordinal": 1},
	}
	rights := map[string]any{
		"accepted": true, "basis": "USER_ATTESTED_OWNED_OR_AUTHORIZED", "evidenceHash": command.SourceArtifactHash,
	}
	safety := map[string]any{"decision": "ALLOWED", "policyVersion": "creator-live-shot-v1", "automated": true}
	executionPolicy := map[string]any{
		"providerSubmitLimit": 1, "automaticFallback": false, "infrastructureRetry": "RECONCILE_SAME_JOB_ONLY",
		"candidatesPerShot": 1, "liveCallsRequired": true, "subscriptionOnly": true,
	}
	profile := map[string]any{
		"id": profileID, "aspectRatio": command.AspectRatio, "durationMillis": 5_000,
		"resolution": "720p", "candidates": 1, "generateAudio": false,
	}
	materialization := creatorBindingMaterialization{
		PlanID: planID, SeriesID: seriesID, SourceID: sourceID,
		EpisodeID: episodeEntityID, EpisodeRevisionID: episodeID,
		SceneID: sceneEntityID, SceneRevisionID: sceneID,
		ShotID: shotEntityID, ShotSpecID: shotID, PromptID: promptID,
		ProfileID: profileID, ProfileGroupID: profileGroupID,
		ScriptID: scriptID, StoryboardID: storyboardID, LicenseID: licenseID,
		Gate2ID: gate2ID, SafetyID: safetyID, BudgetApprovalID: budgetApprovalID,
		EffectiveContextID: effectiveContextID,
		SeriesContextID:    seriesContextID, EpisodeContextID: episodeContextID,
		SceneContextID: sceneContextID, ShotContextID: shotContextID,
		ProviderProfileID: providerProfileID, CapabilityID: capabilityID,
		Command: command, Output: output, Route: route, Context: contextSnapshot,
		Rights: rights, Safety: safety, Profile: profile, ExecutionPolicy: executionPolicy,
		Now: now, ExpiresAt: expiresAt, TraceID: traceID,
	}
	hashes, err := creatorBindingHashes(materialization)
	if err != nil {
		return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, err
	}
	materialization.Hashes = hashes

	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (controlplane.Stored[controlplane.CreatorLiveShotPlan], error) {
		var replay controlplane.CreatorLiveShotPlan
		replayed, err := reserveCreatorIdempotency(ctx, tx, idempotency, &replay)
		if err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, err
		}
		if replayed {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{Value: replay, Replayed: true}, nil
		}
		if creatorCapabilityError(command.Route) != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, creatorCapabilityError(command.Route)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "creator-project:"+command.Actor.ActorID); err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, fmt.Errorf("lock creator project plan snapshot: %w", err)
		}
		var usedTasks, activeRuns int
		var usedTokens int64
		if err := tx.QueryRow(ctx, `
			SELECT count(*), COALESCE(sum(r.reserved_video_tokens),0),
			       count(*) FILTER (WHERE r.state IN ('QUEUED','RUNNING','UNKNOWN','RECONCILING','REQUIRES_ACTION'))
			FROM video_pipeline.creator_live_shot_runs r
			JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id
			WHERE p.actor_id=$1`, command.Actor.ActorID).Scan(&usedTasks, &usedTokens, &activeRuns); err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, fmt.Errorf("read creator project plan snapshot: %w", err)
		}
		projectSnapshot := map[string]any{"tasksUsed": usedTasks, "videoTokensUsed": usedTokens, "activeRuns": activeRuns, "taskLimit": creatorProjectTaskLimit, "videoTokenLimit": creatorProjectVideoTokenLimit}
		planHash, err := digestValue(map[string]any{
			"schemaVersion": "creator-live-shot-plan.v1", "seriesId": seriesID,
			"sourceRevisionId": sourceID, "episodeRevisionId": episodeID, "sceneRevisionId": sceneID,
			"shotSpecRevisionId": shotID, "promptSnapshotId": promptID, "generationProfileRevisionId": profileID,
			"generationPlanId": planID, "budgetApprovalId": budgetApprovalID, "safetyDecisionId": safetyID,
			"title": command.Title, "sceneTextHash": command.SourceArtifactHash, "sourceArtifactUri": command.SourceArtifactURI,
			"bindingHashes": hashes, "output": output, "context": contextSnapshot, "rights": rights,
			"safety": safety, "profile": profile, "executionPolicy": executionPolicy,
			"route": route, "capability": command.Route, "taskLimit": creatorTaskLimit,
			"videoTokenLimit": creatorVideoTokenLimit, "projectBudget": projectSnapshot, "expiresAt": expiresAt,
		})
		if err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, err
		}
		blockers := make([]string, 0, 2)
		if activeRuns > 0 {
			blockers = append(blockers, string(controlplane.CodeConcurrencyLimit))
		}
		if usedTasks+1 > creatorProjectTaskLimit || usedTokens+creatorVideoTokenLimit > creatorProjectVideoTokenLimit {
			blockers = append(blockers, string(controlplane.CodeProjectBudgetExceeded))
		}
		plan := controlplane.CreatorLiveShotPlan{
			SchemaVersion: "v1", PlanID: planID.String(), SeriesID: seriesID.String(), SourceRevisionID: sourceID.String(),
			EpisodeRevisionID: episodeID.String(), SceneRevisionID: sceneID.String(), ShotSpecRevisionID: shotID.String(),
			PromptSnapshotID: promptID.String(), GenerationProfileRevisionID: profileID.String(),
			BudgetApprovalID: budgetApprovalID.String(), SafetyDecisionID: safetyID.String(),
			State: "AWAITING_CONFIRMATION", Confirmable: len(blockers) == 0, Blockers: blockers, Title: command.Title,
			SceneTextHash: command.SourceArtifactHash, AspectRatio: command.AspectRatio, Output: output, Route: creatorRouteProjection(route),
			ProviderProfileID: providerProfileID.String(), BillingMode: creatorBillingMode, TaskLimit: creatorTaskLimit,
			VideoTokenLimit: creatorVideoTokenLimit, ProjectTaskLimit: creatorProjectTaskLimit,
			ProjectVideoTokenLimit: creatorProjectVideoTokenLimit, ProjectTasksUsed: usedTasks,
			ProjectVideoTokensUsed: usedTokens, ProjectActiveRuns: activeRuns, ProviderCallCount: 1, ProviderSubmitCount: 0,
			PlanHash: planHash, Spec: controlplane.CreatorLiveShotSpec{Candidates: 1, DurationSeconds: 5, Resolution: "720p", Audio: false, AspectRatio: command.AspectRatio},
			Budget:          controlplane.CreatorLiveShotBudget{MaxTasksThisConfirmation: 1, MaxVideoTokensThisConfirmation: creatorVideoTokenLimit, ProjectTaskLimit: creatorProjectTaskLimit, ProjectTokenLimit: creatorProjectVideoTokenLimit, ProjectTasksUsed: usedTasks, ProjectTokensUsed: usedTokens, CashAmountMaximum: nil, Verified: false},
			Bindings:        controlplane.CreatorLiveShotBindings{SourceRevisionID: sourceID.String(), EpisodeRevisionID: episodeID.String(), SceneRevisionID: sceneID.String(), ShotSpecRevisionID: shotID.String(), PromptSnapshotID: promptID.String(), GenerationProfileRevisionID: profileID.String(), GenerationPlanID: planID.String(), BudgetApprovalID: budgetApprovalID.String(), SafetyDecisionID: safetyID.String()},
			ExecutionPolicy: executionPolicy, TraceID: traceID, ExpiresAt: expiresAt, CreatedAt: now,
		}
		materialization.PlanHash = planHash
		materialization.ProjectTasksUsed = usedTasks
		materialization.ProjectVideoTokensUsed = usedTokens
		materialization.ProjectActiveRuns = activeRuns
		if err := materializeCreatorBindings(ctx, tx, materialization); err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO video_pipeline.creator_live_shot_plans (
				id, series_id, source_revision_id, episode_revision_id, scene_revision_id,
				shot_spec_revision_id, prompt_snapshot_id, generation_profile_revision_id,
				generation_plan_id,budget_approval_id,safety_decision_id,provider_profile_id, title, scene_text, scene_text_hash, source_artifact_uri,
				aspect_ratio, output_spec, context_snapshot, rights_snapshot, safety_snapshot,
				generation_profile,execution_policy, route_snapshot, capability_snapshot,
				project_tasks_used,project_video_tokens_used,project_active_runs,provider_call_count,provider_submit_count,plan_hash,projection,state,
				actor_id, expires_at, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,1,0,$29,$30,'READY',$31,$32,$33)`,
			planID, seriesID, sourceID, episodeID, sceneID, shotID, promptID, profileID,
			planID, budgetApprovalID, safetyID, providerProfileID, command.Title, command.SceneText, command.SourceArtifactHash,
			command.SourceArtifactURI, command.AspectRatio, output, contextSnapshot, rights, safety, profile, executionPolicy,
			route, command.Route, usedTasks, usedTokens, activeRuns, planHash, plan, command.Actor.ActorID, expiresAt, now,
		)
		if err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, fmt.Errorf("insert creator live-shot plan: %w", err)
		}
		if err := completeCreatorIdempotency(ctx, tx, idempotency, httpCreated, plan); err != nil {
			return controlplane.Stored[controlplane.CreatorLiveShotPlan]{}, err
		}
		_ = traceID
		return controlplane.Stored[controlplane.CreatorLiveShotPlan]{Value: plan}, nil
	})
}

const httpCreated = 201

type creatorBindingMaterialization struct {
	PlanID, SeriesID, SourceID, EpisodeID, EpisodeRevisionID uuid.UUID
	SceneID, SceneRevisionID, ShotID, ShotSpecID, PromptID   uuid.UUID
	ProfileID, ProfileGroupID, ScriptID, StoryboardID        uuid.UUID
	LicenseID, Gate2ID, SafetyID, BudgetApprovalID           uuid.UUID
	EffectiveContextID                                       uuid.UUID
	SeriesContextID, EpisodeContextID, SceneContextID        uuid.UUID
	ShotContextID, ProviderProfileID, CapabilityID           uuid.UUID
	Command                                                  controlplane.CreatorLiveShotPlanCommand
	Output                                                   providercontract.OutputSpec
	Route                                                    providercontract.ModelSnapshot
	Context, Rights, Safety, Profile, ExecutionPolicy        map[string]any
	Hashes                                                   creatorBindingDigestSet
	PlanHash                                                 string
	ProjectTasksUsed, ProjectActiveRuns                      int
	ProjectVideoTokensUsed                                   int64
	Now, ExpiresAt                                           time.Time
	TraceID                                                  string
}

type creatorBindingDigestSet struct {
	Profile, Episode, Scene, Script, Storyboard string
	SeriesContext, EpisodeContext, SceneContext string
	ShotContext, EffectiveContext, Shot, Prompt string
	License                                     string
}

func creatorBindingHashes(b creatorBindingMaterialization) (creatorBindingDigestSet, error) {
	hash := func(value any) (string, error) { return digestValue(value) }
	profileHash, err := hash(map[string]any{"profile": b.Profile, "route": b.Route, "output": b.Output})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	episodeHash, err := hash(map[string]any{"title": b.Command.Title, "sceneTextHash": b.Command.SourceArtifactHash, "durationMillis": 5000})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	sceneHash, err := hash(map[string]any{"sceneTextHash": b.Command.SourceArtifactHash, "ordinal": 1})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	scriptHash, err := hash(map[string]any{"sceneTextHash": b.Command.SourceArtifactHash, "shotCount": 1})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	storyboardHash, err := hash(map[string]any{"scriptHash": scriptHash, "shotCount": 1})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	seriesContextHash, err := hash(b.Context["series"])
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	episodeContextHash, err := hash(b.Context["episode"])
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	sceneContextHash, err := hash(b.Context["scene"])
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	shotContextHash, err := hash(b.Context["shot"])
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	contextIDs := []uuid.UUID{b.SeriesContextID, b.EpisodeContextID, b.SceneContextID, b.ShotContextID}
	contextHashes := map[string]string{"series": seriesContextHash, "episode": episodeContextHash, "scene": sceneContextHash, "shot": shotContextHash}
	effectiveHash, err := hash(map[string]any{"contextRevisionIds": contextIDs, "contextHashes": contextHashes})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	shotHash, err := hash(map[string]any{"sceneHash": sceneHash, "output": b.Output, "effectiveContextHash": effectiveHash, "profileHash": profileHash})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	promptHash, err := hash(map[string]any{"prompt": b.Command.SceneText, "output": b.Output, "shotSpecRevisionId": b.ShotSpecID})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	licenseHash, err := hash(map[string]any{"basis": "USER_ATTESTED_OWNED_OR_AUTHORIZED", "evidenceHash": b.Command.SourceArtifactHash, "actorId": b.Command.Actor.ActorID})
	if err != nil {
		return creatorBindingDigestSet{}, err
	}
	return creatorBindingDigestSet{Profile: profileHash, Episode: episodeHash, Scene: sceneHash, Script: scriptHash,
		Storyboard: storyboardHash, SeriesContext: seriesContextHash, EpisodeContext: episodeContextHash,
		SceneContext: sceneContextHash, ShotContext: shotContextHash, EffectiveContext: effectiveHash,
		Shot: shotHash, Prompt: promptHash, License: licenseHash}, nil
}

func materializeCreatorBindings(ctx context.Context, tx pgx.Tx, b creatorBindingMaterialization) error {
	profileHash, episodeHash, sceneHash := b.Hashes.Profile, b.Hashes.Episode, b.Hashes.Scene
	scriptHash, storyboardHash := b.Hashes.Script, b.Hashes.Storyboard
	seriesContextHash, episodeContextHash := b.Hashes.SeriesContext, b.Hashes.EpisodeContext
	sceneContextHash, shotContextHash := b.Hashes.SceneContext, b.Hashes.ShotContext
	effectiveHash, shotHash, promptHash, licenseHash := b.Hashes.EffectiveContext, b.Hashes.Shot, b.Hashes.Prompt, b.Hashes.License
	contextIDs := []uuid.UUID{b.SeriesContextID, b.EpisodeContextID, b.SceneContextID, b.ShotContextID}
	contextHashes := map[string]string{"series": seriesContextHash, "episode": episodeContextHash, "scene": sceneContextHash, "shot": shotContextHash}

	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.provider_profiles
			(id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash,created_at,updated_at)
		VALUES ($1,'VOLCENGINE','Studio Agent Plan video','internal://volcengine-provider','runtime-secret-store',true,'LIVE','READY',$2,$3,$3)
		ON CONFLICT (id) DO UPDATE SET enabled=true,mode='LIVE',health='READY',config_hash=EXCLUDED.config_hash,updated_at=EXCLUDED.updated_at`,
		b.ProviderProfileID, b.Command.Route.SnapshotHash, b.Now); err != nil {
		return fmt.Errorf("upsert creator provider profile: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.provider_capability_snapshots
			(id,provider_profile_id,capability_alias,model_id,route_version,supported_inputs,limits,pricing_rule_version,capability_hash,status,effective_at,expires_at)
		VALUES ($1,$2,'video.primary',$3,$4,$5,$6,'agent-plan-subscription-v1',$7,'ACTIVE',$8,$9)
		ON CONFLICT (provider_profile_id,capability_alias,capability_hash) DO NOTHING`,
		b.CapabilityID, b.ProviderProfileID, b.Route.ModelID, b.Route.RouteVersion, b.Command.Route.SupportedInputs, b.Command.Route.Limits, b.Route.CapabilityHash, b.Command.Route.EffectiveAt, b.ExpiresAt); err != nil {
		return fmt.Errorf("insert creator capability snapshot: %w", err)
	}
	aspectProfile := "16:9_720P24"
	if b.Command.AspectRatio == "9:16" {
		aspectProfile = "9:16_720P24"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.generation_profiles
			(id,profile_id,revision,schema_version,status,stage,aspect_profile,episode_target_ms,shot_min_ms,shot_max_ms,
			 capability_routes,media_processing,render_defaults,qc_thresholds,retry_policy,budget_policy,license_policy,content_hash,created_by,created_at)
		VALUES ($1,$2,1,'v1','ACTIVE','EXPERIMENT',$3,5000,5000,5000,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		b.ProfileID, b.ProfileGroupID, aspectProfile, map[string]any{"video": b.Route}, map[string]any{"audio": false}, b.Output,
		map[string]any{"structural": true}, map[string]any{"providerAttempts": 1}, map[string]any{"taskLimit": 1, "videoTokenLimit": creatorVideoTokenLimit},
		map[string]any{"rightsAccepted": true}, profileHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator generation profile: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.series (id,schema_version,title,status,default_profile_id,rights_declaration,created_by,created_at,updated_at) VALUES ($1,'v1',$2,'ACTIVE',$3,$4,$5,$6,$6)`, b.SeriesID, b.Command.Title, b.ProfileID, b.Rights, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator series: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.operation_requests (id,operation_type,aggregate_type,aggregate_id,state,trace_id,requested_by,created_at,updated_at) VALUES ($1,'CREATE_CREATOR_LIVE_SHOT_PLAN','SERIES',$2,'SUCCEEDED',$3,$4,$5,$5)`, b.PlanID, b.SeriesID, b.TraceID, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator generation plan operation: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.license_snapshots (id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,source_uri,reviewed_by,reviewed_at,created_at) VALUES ($1,'SOURCE',$2,'creator-declaration-v1',$3,'ALLOWED',ARRAY['CN'],false,$4,$5,$6,$6)`, b.LicenseID, b.SourceID.String(), licenseHash, b.Command.SourceArtifactURI, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator source rights: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.source_revisions (id,series_id,revision,status,content_hash,artifact_uri,language,rights_snapshot,created_by,created_at) VALUES ($1,$2,1,'APPROVED',$3,$4,'zh-CN',$5,$6,$7)`, b.SourceID, b.SeriesID, b.Command.SourceArtifactHash, b.Command.SourceArtifactURI, map[string]any{"licenseSnapshotId": b.LicenseID, "declaration": b.Rights}, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator source revision: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.episodes (id,series_id,ordinal,title,created_at) VALUES ($1,$2,1,$3,$4)`, b.EpisodeID, b.SeriesID, b.Command.Title, b.Now); err != nil {
		return fmt.Errorf("insert creator episode: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.review_tasks (id,series_id,episode_id,review_type,state,reason_codes,assigned_role,created_at,decided_at,generation_plan_id,budget_scope,budget_limit_micros,budget_currency) VALUES ($1,$2,$3,'BUDGET','APPROVED',ARRAY['AGENT_PLAN_SUBSCRIPTION_TOKEN_CAP'],'PRODUCER',$4,$4,$5,'VIDEO',$6,'VTC')`, b.BudgetApprovalID, b.SeriesID, b.EpisodeID, b.Now, b.PlanID, creatorVideoTokenLimit); err != nil {
		return fmt.Errorf("insert creator budget approval: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.episode_revisions (id,episode_id,revision,status,target_duration_ms,content_hash,created_by,created_at) VALUES ($1,$2,1,'G2_APPROVED',5000,$3,$4,$5)`, b.EpisodeRevisionID, b.EpisodeID, episodeHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator episode revision: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.scenes (id,episode_id,ordinal,created_at) VALUES ($1,$2,1,$3)`, b.SceneID, b.EpisodeID, b.Now); err != nil {
		return fmt.Errorf("insert creator scene: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.scene_revisions (id,scene_id,episode_revision_id,revision,status,content_hash,payload,created_by,created_at) VALUES ($1,$2,$3,1,'APPROVED',$4,$5,$6,$7)`, b.SceneRevisionID, b.SceneID, b.EpisodeRevisionID, sceneHash, map[string]any{"sceneText": b.Command.SceneText}, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator scene revision: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.episode_script_revisions (id,episode_id,revision,status,schema_version,payload,content_hash,created_by,created_at) VALUES ($1,$2,1,'APPROVED','v1',$3,$4,$5,$6)`, b.ScriptID, b.EpisodeID, map[string]any{"sceneText": b.Command.SceneText}, scriptHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator script revision: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.storyboard_revisions (id,episode_id,script_revision_id,revision,status,content_hash,created_by,created_at) VALUES ($1,$2,$3,1,'APPROVED',$4,$5,$6)`, b.StoryboardID, b.EpisodeID, b.ScriptID, storyboardHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator storyboard revision: %w", err)
	}

	contexts := []struct {
		id, scopeID uuid.UUID
		scope, hash string
		payload     any
	}{{b.SeriesContextID, b.SeriesID, "SERIES", seriesContextHash, b.Context["series"]}, {b.EpisodeContextID, b.EpisodeID, "EPISODE", episodeContextHash, b.Context["episode"]}, {b.SceneContextID, b.SceneID, "SCENE", sceneContextHash, b.Context["scene"]}}
	for _, item := range contexts {
		if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by,created_at) VALUES ($1,$2,$3,$4,1,'APPROVED','v1','creator-live-shot-v1',$5,$6,$7,$8)`, item.id, b.SeriesID, item.scope, item.scopeID, item.payload, item.hash, b.Command.Actor.ActorID, b.Now); err != nil {
			return fmt.Errorf("insert creator %s context: %w", item.scope, err)
		}
	}
	for _, decision := range []struct {
		id                 uuid.UUID
		gate, reason, role string
	}{{b.Gate2ID, "G2", "CREATOR_EXACT_SHOT", "PRODUCER"}, {b.SafetyID, "SAFETY", "CREATOR_POLICY_ALLOWED", "SAFETY_REVIEWER"}} {
		if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.approval_decisions (id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id) VALUES ($1,$2,$3,$4,'APPROVED',$5,'server-derived Studio v1 binding',$6,$7,$8,$9)`, decision.id, b.SeriesID, b.EpisodeID, decision.gate, decision.reason, b.Command.Actor.ActorID, decision.role, b.Now, b.TraceID); err != nil {
			return fmt.Errorf("insert creator %s decision: %w", decision.gate, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.shots (id,scene_id,ordinal,created_at) VALUES ($1,$2,1,$3)`, b.ShotID, b.SceneID, b.Now); err != nil {
		return fmt.Errorf("insert creator shot: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by,created_at) VALUES ($1,$2,'SHOT',$3,1,'APPROVED','v1','creator-live-shot-v1',$4,$5,$6,$7)`, b.ShotContextID, b.SeriesID, b.ShotID, b.Context["shot"], shotContextHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator shot context: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.shot_spec_revisions (id,shot_id,storyboard_revision_id,revision,lifecycle_state,freshness,duration_ms,aspect_profile,fps,width,height,cast_count,primary_action_count,narrative,asset_version_refs,context_revision_ids,effective_context_hash,continuity,cinematography,generation_profile_id,gate2_decision_id,content_hash,created_by,created_at) VALUES ($1,$2,$3,1,'READY','FRESH',5000,$4,24,$5,$6,0,1,$7,'{}'::uuid[],$8,$9,$10,$11,$12,$13,$14,$15,$16)`, b.ShotSpecID, b.ShotID, b.StoryboardID, aspectProfile, b.Output.Width, b.Output.Height, map[string]any{"sceneText": b.Command.SceneText}, contextIDs, effectiveHash, map[string]any{"singleShot": true}, map[string]any{"aspectRatio": b.Command.AspectRatio}, b.ProfileID, b.Gate2ID, shotHash, b.Command.Actor.ActorID, b.Now); err != nil {
		return fmt.Errorf("insert creator shot specification: %w", err)
	}
	for _, binding := range []struct {
		decision uuid.UUID
		typ      string
		revision uuid.UUID
		hash     string
	}{{b.Gate2ID, "SHOT_SPEC_REVISION", b.ShotSpecID, shotHash}, {b.SafetyID, "EPISODE_REVISION", b.EpisodeRevisionID, episodeHash}, {b.SafetyID, "SHOT_SPEC_REVISION", b.ShotSpecID, shotHash}} {
		if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.approval_bindings (decision_id,object_type,revision_id,content_hash) VALUES ($1,$2,$3,$4)`, binding.decision, binding.typ, binding.revision, binding.hash); err != nil {
			return fmt.Errorf("insert creator approval binding: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.effective_context_snapshots (id,shot_spec_revision_id,schema_version,resolver_version,context_revision_ids,normalized_payload,content_hash,created_at) VALUES ($1,$2,'v1','creator-live-shot-v1',$3,$4,$5,$6)`, b.EffectiveContextID, b.ShotSpecID, contextIDs, map[string]any{"contextHashes": contextHashes}, effectiveHash, b.Now); err != nil {
		return fmt.Errorf("insert creator effective context: %w", err)
	}
	inputHashes := map[string]string{"source": b.Command.SourceArtifactHash, "episode": episodeHash, "scene": sceneHash, "shotSpec": shotHash, "generationProfile": profileHash, "effectiveContext": effectiveHash}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.prompt_snapshots (id,shot_spec_revision_id,schema_version,compiler_version,prompt_template_ref,effective_context_snapshot_id,asset_version_refs,positive_prompt,negative_prompt,model_payload,normalized_input_hash,content_hash,output_spec,input_revision_hashes,created_at) VALUES ($1,$2,'v1','creator-live-shot-v1','creator-live-shot.v1',$3,'{}'::uuid[],$4,$5,$6,$7,$7,$8,$9,$10)`, b.PromptID, b.ShotSpecID, b.EffectiveContextID, b.Command.SceneText, "watermark, logo, subtitles, extra limbs, inconsistent identity", map[string]any{"route": b.Route, "planHash": b.PlanHash}, promptHash, b.Output, inputHashes, b.Now); err != nil {
		return fmt.Errorf("insert creator prompt snapshot: %w", err)
	}
	for _, input := range []struct {
		typ        string
		id         uuid.UUID
		hash, role string
	}{{"SOURCE", b.SourceID, b.Command.SourceArtifactHash, "source"}, {"EPISODE", b.EpisodeRevisionID, episodeHash, "episode"}, {"SCENE", b.SceneRevisionID, sceneHash, "scene"}, {"SHOT_SPEC", b.ShotSpecID, shotHash, "shot"}, {"GENERATION_PROFILE", b.ProfileID, profileHash, "profile"}, {"CONTEXT", b.SeriesContextID, seriesContextHash, "series-context"}, {"CONTEXT", b.EpisodeContextID, episodeContextHash, "episode-context"}, {"CONTEXT", b.SceneContextID, sceneContextHash, "scene-context"}, {"CONTEXT", b.ShotContextID, shotContextHash, "shot-context"}} {
		if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.prompt_snapshot_inputs (prompt_snapshot_id,input_type,input_revision_id,input_hash,dependency_role) VALUES ($1,$2,$3,$4,$5)`, b.PromptID, input.typ, input.id, input.hash, input.role); err != nil {
			return fmt.Errorf("insert creator prompt lineage: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,$4,'creator_live_shot.plan_created','CREATOR_LIVE_SHOT_PLAN',$5,'CREATOR_LIVE_SHOT_V1',$6,$7)`, uuid.NewSHA1(b.PlanID, []byte("plan-audit")), b.Now, b.Command.Actor.ActorID, b.Command.Actor.Role, b.PlanID, b.TraceID, map[string]any{"seriesId": b.SeriesID, "shotSpecRevisionId": b.ShotSpecID, "promptSnapshotId": b.PromptID, "profileId": b.ProfileID, "generationPlanId": b.PlanID, "budgetApprovalId": b.BudgetApprovalID, "safetyDecisionId": b.SafetyID, "capabilityHash": b.Route.CapabilityHash, "providerCallCount": 1, "providerSubmitCount": 0, "projectTasksUsed": b.ProjectTasksUsed, "projectVideoTokensUsed": b.ProjectVideoTokensUsed, "projectActiveRuns": b.ProjectActiveRuns, "bindingHashes": b.Hashes, "executionPolicy": b.ExecutionPolicy, "planHash": b.PlanHash}); err != nil {
		return fmt.Errorf("insert creator plan audit: %w", err)
	}
	return nil
}

func (p *Postgres) ConfirmCreatorLiveShotPlan(
	ctx context.Context,
	planIDRaw string,
	command controlplane.ConfirmCreatorLiveShotCommand,
	idempotency controlplane.Idempotency,
	traceID string,
) (controlplane.Stored[controlplane.CreatorLiveShotRun], error) {
	planID, err := uuid.Parse(planIDRaw)
	if err != nil {
		return controlplane.Stored[controlplane.CreatorLiveShotRun]{}, controlplane.NewNotFoundError("creator live-shot plan", planIDRaw)
	}
	now := p.now().UTC()
	runID := uuid.NewSHA1(planID, []byte("run"))
	operationID := uuid.NewSHA1(planID, []byte("confirm-operation"))
	providerJobID := uuid.NewSHA1(runID, []byte("provider-job"))
	reservationID := uuid.NewSHA1(runID, []byte("video-token-reservation"))
	workflowID := "creator-live-shot-" + runID.String()

	type confirmationResult struct {
		stored  controlplane.Stored[controlplane.CreatorLiveShotRun]
		expired bool
	}
	result, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (confirmationResult, error) {
		var replay controlplane.CreatorLiveShotRun
		replayed, err := reserveCreatorIdempotency(ctx, tx, idempotency, &replay)
		if err != nil {
			return confirmationResult{}, err
		}
		if replayed {
			return confirmationResult{stored: controlplane.Stored[controlplane.CreatorLiveShotRun]{Value: replay, Replayed: true}}, nil
		}

		var seriesID, episodeRevisionID, shotID, promptID, profileID, providerProfileID uuid.UUID
		var budgetApprovalID, safetyDecisionID uuid.UUID
		var planHash, state, actorID, sceneText string
		var expiresAt time.Time
		var plannedTasks, plannedActiveRuns int
		var plannedTokens int64
		var output providercontract.OutputSpec
		var route providercontract.ModelSnapshot
		var storedCapability providercontract.CapabilitySnapshot
		if err := tx.QueryRow(ctx, `
			SELECT series_id,episode_revision_id,shot_spec_revision_id,prompt_snapshot_id,
			       generation_profile_revision_id,provider_profile_id,budget_approval_id,safety_decision_id,
			       project_tasks_used,project_video_tokens_used,project_active_runs,plan_hash,
			       state, actor_id, scene_text, expires_at, output_spec,
			       route_snapshot, capability_snapshot
			FROM video_pipeline.creator_live_shot_plans
			WHERE id=$1 FOR UPDATE`, planID,
		).Scan(&seriesID, &episodeRevisionID, &shotID, &promptID, &profileID, &providerProfileID,
			&budgetApprovalID, &safetyDecisionID, &plannedTasks, &plannedTokens, &plannedActiveRuns,
			&planHash, &state, &actorID, &sceneText, &expiresAt, &output, &route, &storedCapability); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return confirmationResult{}, controlplane.NewNotFoundError("creator live-shot plan", planIDRaw)
			}
			return confirmationResult{}, fmt.Errorf("lock creator live-shot plan: %w", err)
		}
		if actorID != command.Actor.ActorID {
			return confirmationResult{}, controlplane.NewNotFoundError("creator live-shot plan", planIDRaw)
		}
		if !command.LiveCallsEnabled {
			return confirmationResult{}, controlplane.NewPolicyError(controlplane.CodeLiveCallsDisabled, "live Provider calls are not armed", "set VIDEO_LIVE_CALLS_ENABLED=true only in the authorized runtime")
		}
		if !command.Confirmed {
			return confirmationResult{}, controlplane.NewPolicyError(controlplane.CodeValidation, "confirmed must be true", "explicitly confirm the exact plan")
		}
		if command.PlanHash != planHash {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodePlanHashMismatch, "planHash differs from the immutable plan")
		}
		if !now.Before(expiresAt) {
			if _, err := tx.Exec(ctx, `UPDATE video_pipeline.creator_live_shot_plans SET state='EXPIRED' WHERE id=$1 AND state='READY'`, planID); err != nil {
				return confirmationResult{}, fmt.Errorf("expire creator live-shot plan: %w", err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM video_pipeline.creator_live_shot_idempotency WHERE scope=$1 AND idempotency_key=$2 AND response_body IS NULL`, idempotency.Scope, idempotency.Key); err != nil {
				return confirmationResult{}, fmt.Errorf("release expired creator confirmation idempotency: %w", err)
			}
			return confirmationResult{expired: true}, nil
		}
		if state != "READY" {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodePlanStale, "the live-shot plan is no longer confirmable")
		}
		if err := creatorCapabilityError(command.Route); err != nil {
			return confirmationResult{}, err
		}
		if !sameCreatorCapability(storedCapability, command.Route) || route.CapabilityHash != command.Route.SnapshotHash {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodePlanStale, "the live Adapter capability changed after planning")
		}

		// Project lock serializes cumulative quota and the conservative one-live-run guard.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "creator-project:"+actorID); err != nil {
			return confirmationResult{}, fmt.Errorf("lock creator project quota: %w", err)
		}
		var usedTasks int
		var usedTokens int64
		var active int
		if err := tx.QueryRow(ctx, `
			SELECT count(*), COALESCE(sum(r.reserved_video_tokens),0),
			       count(*) FILTER (WHERE r.state IN ('QUEUED','RUNNING','UNKNOWN','RECONCILING','REQUIRES_ACTION'))
			FROM video_pipeline.creator_live_shot_runs r
			JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id
			WHERE p.actor_id=$1`, actorID,
		).Scan(&usedTasks, &usedTokens, &active); err != nil {
			return confirmationResult{}, fmt.Errorf("read creator project quota: %w", err)
		}
		if usedTasks != plannedTasks || usedTokens != plannedTokens || active != plannedActiveRuns {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodePlanStale, "the project budget or concurrency snapshot changed after planning")
		}
		if active >= 1 {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodeConcurrencyLimit, "one live shot is already active for this project")
		}
		if usedTasks+1 > creatorProjectTaskLimit || usedTokens+creatorVideoTokenLimit > creatorProjectVideoTokenLimit {
			return confirmationResult{}, controlplane.NewPolicyError(controlplane.CodeProjectBudgetExceeded, "the project subscription task/token allowance is exhausted", "start a new project allowance before confirming another shot")
		}
		var bindingsValid bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM video_pipeline.review_tasks budget
				JOIN video_pipeline.approval_decisions safety ON safety.id=$2
				JOIN video_pipeline.approval_bindings safety_binding ON safety_binding.decision_id=safety.id
				JOIN video_pipeline.source_revisions source ON source.series_id=$3
				JOIN video_pipeline.license_snapshots license ON license.id=(source.rights_snapshot->>'licenseSnapshotId')::uuid
				WHERE budget.id=$1 AND budget.state='APPROVED' AND budget.review_type='BUDGET'
				  AND budget.generation_plan_id=$4 AND budget.budget_scope='VIDEO'
				  AND budget.budget_limit_micros=$5 AND budget.budget_currency='VTC'
				  AND safety.decision='APPROVED' AND safety.gate='SAFETY'
				  AND safety_binding.object_type='EPISODE_REVISION' AND safety_binding.revision_id=$6
				  AND license.policy_status='ALLOWED' AND license.license_id='creator-declaration-v1'
			)`, budgetApprovalID, safetyDecisionID, seriesID, planID, creatorVideoTokenLimit, episodeRevisionID).Scan(&bindingsValid); err != nil {
			return confirmationResult{}, fmt.Errorf("verify creator approval bindings: %w", err)
		}
		if !bindingsValid {
			return confirmationResult{}, controlplane.NewConflictError(controlplane.CodePlanStale, "rights, safety, or budget approval no longer matches the plan")
		}

		promptHash, err := digestValue(map[string]any{"prompt": sceneText, "output": output, "shotSpecRevisionId": shotID})
		if err != nil {
			return confirmationResult{}, err
		}
		runDigest, err := digestValue(map[string]any{
			"planId": planID, "planHash": planHash, "shotSpecRevisionId": shotID,
			"promptSnapshotId": promptID, "promptHash": promptHash, "profileId": profileID, "route": route,
		})
		if err != nil {
			return confirmationResult{}, err
		}
		contextRefs := providercontract.ContextRefs{
			SeriesSnapshotID:  uuid.NewSHA1(planID, []byte("context-series")).String(),
			EpisodeSnapshotID: uuid.NewSHA1(planID, []byte("context-episode")).String(),
			SceneSnapshotID:   uuid.NewSHA1(planID, []byte("context-scene")).String(),
			ShotSnapshotID:    uuid.NewSHA1(planID, []byte("context-shot")).String(),
		}
		input := orchestration.ExecuteProviderJobInput{
			Run: orchestration.GenerationRunRef{RunID: runID.String(), RunSpecDigest: runDigest, Attempt: 1},
			Prompt: orchestration.PromptSnapshotRef{
				ID: promptID.String(), Digest: promptHash, PositivePrompt: sceneText,
				NegativePrompt: "watermark, logo, subtitles, extra limbs, inconsistent identity",
				Context:        contextRefs, Output: output,
				InputRevisionHashes: map[string]string{"shotSpec": runDigest, "generationProfile": runDigest},
			},
			Route: route, BudgetApprovalID: budgetApprovalID.String(),
			BudgetMaximumMicros: creatorVideoTokenLimit, BudgetCurrency: "VTC",
			ProviderProfileID: providerProfileID.String(), TraceID: traceID, PersistProductTruth: true,
		}
		budget := providercontract.BudgetEnvelope{EstimatedCostMicros: 1, MaxCostMicros: creatorVideoTokenLimit, MaxAttempts: 1}
		reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
			ReservationID: reservationID.String(), Currency: "VTC", AmountMicros: creatorVideoTokenLimit,
			PricingVersion: "agent-plan-video-token-v1", ConfirmedBy: budgetApprovalID.String(),
		}, providercontract.BudgetBindingInput{RunID: runID.String(), InputHash: runDigest, Model: route, Budget: budget})
		if err != nil {
			return confirmationResult{}, fmt.Errorf("bind creator token reservation: %w", err)
		}
		prepared := orchestration.PreparedProviderJob{
			Budget: budget, BudgetReservation: reservation,
			ProductTruth: orchestration.PreparedProductTruth{
				ShotSpecRevisionID: shotID.String(), Run: input.Run,
				PromptSnapshotID: promptID.String(), PromptSnapshotHash: promptHash,
				GenerationPlanID: planID.String(), BudgetApprovalID: budgetApprovalID.String(),
				BudgetMaximumMicros: creatorVideoTokenLimit, BudgetCurrency: "VTC",
				ProviderProfileID: providerProfileID.String(), Route: route,
			},
		}
		request, err := orchestration.BuildProviderJobRequest(input, prepared)
		if err != nil {
			return confirmationResult{}, err
		}
		requestHash, err := digestValue(request)
		if err != nil {
			return confirmationResult{}, err
		}
		snapshot := creatorPreparedRequest{Input: input, Prepared: prepared}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.operation_requests
				(id,operation_type,aggregate_type,aggregate_id,state,temporal_workflow_id,trace_id,requested_by,created_at,updated_at)
			VALUES ($1,'CONFIRM_CREATOR_LIVE_SHOT','GENERATION_RUN',$2,'ACCEPTED',$3,$4,$5,$6,$6)`,
			operationID, runID, workflowID, traceID, actorID, now); err != nil {
			return confirmationResult{}, fmt.Errorf("insert creator confirmation operation: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO video_pipeline.creator_live_shot_runs (
				id,plan_id,operation_id,provider_job_id,workflow_id,run_spec_digest,
				request_hash,request_snapshot,reservation_id,reserved_tasks,reserved_video_tokens,
				state,trace_id,actor_id,created_at,updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,$10,'QUEUED',$11,$12,$13,$13)`,
			runID, planID, operationID, providerJobID, workflowID, runDigest,
			requestHash, snapshot, reservationID, creatorVideoTokenLimit, traceID, actorID, now,
		)
		if err != nil {
			return confirmationResult{}, fmt.Errorf("insert creator provider intent: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE video_pipeline.creator_live_shot_plans SET state='CONFIRMED', confirmed_at=$2 WHERE id=$1`, planID, now); err != nil {
			return confirmationResult{}, fmt.Errorf("confirm creator live-shot plan: %w", err)
		}
		run := creatorRunValue(runID, planID, seriesID, operationID, providerJobID, "QUEUED", nil, planHash, route, "", "", 0, "", nil, providercontract.Usage{}, "", traceID, now, now)
		if err := completeCreatorIdempotency(ctx, tx, idempotency, 202, run); err != nil {
			return confirmationResult{}, err
		}
		return confirmationResult{stored: controlplane.Stored[controlplane.CreatorLiveShotRun]{Value: run}}, nil
	})
	if err != nil {
		return controlplane.Stored[controlplane.CreatorLiveShotRun]{}, err
	}
	if result.expired {
		return controlplane.Stored[controlplane.CreatorLiveShotRun]{}, controlplane.NewConflictError(controlplane.CodePlanExpired, "the live-shot plan expired")
	}
	return result.stored, nil
}

func reserveCreatorIdempotency[T any](ctx context.Context, tx pgx.Tx, idem controlplane.Idempotency, replay *T) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.creator_live_shot_idempotency (scope,idempotency_key,request_hash)
		VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, idem.Scope, idem.Key, idem.RequestHash)
	if err != nil {
		return false, fmt.Errorf("reserve creator idempotency: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return false, nil
	}
	var requestHash string
	var body []byte
	if err := tx.QueryRow(ctx, `
		SELECT request_hash,response_body FROM video_pipeline.creator_live_shot_idempotency
		WHERE scope=$1 AND idempotency_key=$2 FOR UPDATE`, idem.Scope, idem.Key).Scan(&requestHash, &body); err != nil {
		return false, fmt.Errorf("read creator idempotency: %w", err)
	}
	if requestHash != idem.RequestHash {
		return false, controlplane.NewConflictError(controlplane.CodeIdempotencyConflict, "Idempotency-Key was already used for a different request")
	}
	if len(body) == 0 {
		return false, controlplane.NewConflictError(controlplane.CodeRecoveryActive, "the matching request is still being committed")
	}
	if err := json.Unmarshal(body, replay); err != nil {
		return false, fmt.Errorf("decode creator idempotency response: %w", err)
	}
	return true, nil
}

func completeCreatorIdempotency(ctx context.Context, tx pgx.Tx, idem controlplane.Idempotency, status int, response any) error {
	body, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode creator idempotency response: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE video_pipeline.creator_live_shot_idempotency SET response_status=$3,response_body=$4 WHERE scope=$1 AND idempotency_key=$2`, idem.Scope, idem.Key, status, body)
	return err
}

func creatorCapabilityError(snapshot providercontract.CapabilitySnapshot) error {
	billing, _ := snapshot.Limits["billingMode"].(string)
	if snapshot.Alias != providercontract.CapabilityVideo || !snapshot.Configured || !snapshot.Enabled || snapshot.Mode != "live" {
		return controlplane.NewPolicyError(controlplane.CodeCapability, "video.primary is not an enabled live Adapter capability", "restore and verify the Agent Plan Adapter")
	}
	if billing != creatorBillingMode {
		return controlplane.NewPolicyError(controlplane.CodeSubscriptionRouteRequired, "video.primary is not a subscription route", "select the verified Agent Plan subscription route")
	}
	for _, key := range []string{"cashAmountMaximum", "unitPriceMicros"} {
		if value, exists := snapshot.Limits[key]; exists && value != nil && !zeroCreatorCashValue(value) {
			return controlplane.NewPolicyError(controlplane.CodeCashChargeNotAllowed, "the route carries a positive or unverifiable cash estimate", "select a subscription route with null or exact-zero cash maximum")
		}
	}
	if snapshot.SnapshotHash == "" || snapshot.RouteVersion == "" || snapshot.Capability.Provider == "" || snapshot.Capability.ModelFamily == "" {
		return controlplane.NewPolicyError(controlplane.CodeCapability, "video.primary capability evidence is incomplete", "refresh the Adapter capability snapshot")
	}
	capability := snapshot.Capability
	if capability.OutputModality != providercontract.ModalityVideo || capability.MinDurationMillis > 5_000 ||
		capability.MaxDurationMillis < 5_000 || !containsValue(capability.Resolutions, "720p") ||
		!containsValue(capability.AspectRatios, "16:9") || !containsValue(capability.AspectRatios, "9:16") ||
		!containsInt(capability.NativeFPS, 24) || !containsValue(snapshot.SupportedInputs, "text") {
		return controlplane.NewPolicyError(controlplane.CodeCapability, "video.primary does not support the fixed Studio output contract", "select a capability supporting 5s 720p 24fps in 16:9 and 9:16")
	}
	return nil
}

func containsValue(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
func containsInt(values []int, expected int) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func zeroCreatorCashValue(value any) bool {
	switch typed := value.(type) {
	case int:
		return typed == 0
	case int64:
		return typed == 0
	case float64:
		return typed == 0
	case json.Number:
		parsed, err := typed.Int64()
		return err == nil && parsed == 0
	default:
		return false
	}
}

func sameCreatorCapability(a, b providercontract.CapabilitySnapshot) bool {
	return a.Alias == b.Alias && a.Configured == b.Configured && a.Enabled == b.Enabled &&
		a.Mode == b.Mode && a.RouteVersion == b.RouteVersion && a.SnapshotHash == b.SnapshotHash &&
		reflect.DeepEqual(a.Capability, b.Capability) && reflect.DeepEqual(a.Limits, b.Limits) &&
		reflect.DeepEqual(a.SupportedInputs, b.SupportedInputs)
}

func (p *Postgres) ListCreatorLiveShots(ctx context.Context, seriesIDRaw string, actor controlplane.Actor) (controlplane.CreatorLiveShotProject, error) {
	seriesID, err := uuid.Parse(seriesIDRaw)
	if err != nil {
		return controlplane.CreatorLiveShotProject{}, controlplane.NewNotFoundError("series", seriesIDRaw)
	}
	project := controlplane.CreatorLiveShotProject{SchemaVersion: "v1", SeriesID: seriesID.String(), Runs: []controlplane.CreatorLiveShotRun{}}
	if err := p.pool.QueryRow(ctx, `SELECT projection FROM video_pipeline.creator_live_shot_plans WHERE series_id=$1 AND actor_id=$2 ORDER BY created_at DESC LIMIT 1`, seriesID, actor.ActorID).Scan(&project.Plan); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.CreatorLiveShotProject{}, controlplane.NewNotFoundError("creator live-shot project", seriesIDRaw)
		}
		return controlplane.CreatorLiveShotProject{}, fmt.Errorf("read creator live-shot plan projection: %w", err)
	}
	rows, err := p.pool.Query(ctx, creatorRunSelect+` WHERE p.series_id=$1 AND p.actor_id=$2 ORDER BY r.created_at DESC`, seriesID, actor.ActorID)
	if err != nil {
		return controlplane.CreatorLiveShotProject{}, fmt.Errorf("list creator live shots: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		run, err := scanCreatorRun(rows)
		if err != nil {
			return controlplane.CreatorLiveShotProject{}, err
		}
		project.Runs = append(project.Runs, run)
	}
	return project, rows.Err()
}

func (p *Postgres) GetCreatorLiveShotRun(ctx context.Context, runIDRaw string, actor controlplane.Actor) (controlplane.CreatorLiveShotRun, error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.CreatorLiveShotRun{}, controlplane.NewNotFoundError("creator live-shot run", runIDRaw)
	}
	run, err := scanCreatorRun(p.pool.QueryRow(ctx, creatorRunSelect+` WHERE r.id=$1 AND p.actor_id=$2`, runID, actor.ActorID))
	if errors.Is(err, pgx.ErrNoRows) {
		return controlplane.CreatorLiveShotRun{}, controlplane.NewNotFoundError("creator live-shot run", runIDRaw)
	}
	return run, err
}

const creatorRunSelect = `
	SELECT r.id,r.plan_id,p.series_id,r.operation_id,r.provider_job_id,r.state,r.progress,p.plan_hash,p.route_snapshot,
	       COALESCE(r.upstream_task_id,''),COALESCE(r.upstream_request_id,''),r.submit_count,COALESCE(r.error_code,''),r.output_hash,
	       r.output_media_type,r.output_size_bytes,r.output_width,r.output_height,
	       r.output_duration_ms,r.usage,r.manifest_hash,r.trace_id,r.created_at,r.updated_at
	FROM video_pipeline.creator_live_shot_runs r
	JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id`

type rowScanner interface{ Scan(...any) error }

func scanCreatorRun(row rowScanner) (controlplane.CreatorLiveShotRun, error) {
	var runID, planID, seriesID, operationID, providerJobID uuid.UUID
	var state, planHash, upstream, requestID, errorCode, traceID string
	var route providercontract.ModelSnapshot
	var outputHash, mediaType, manifestHash *string
	var size, duration *int64
	var width, height, progress *int
	var submitCount int
	var usage providercontract.Usage
	var createdAt, updatedAt time.Time
	if err := row.Scan(&runID, &planID, &seriesID, &operationID, &providerJobID, &state, &progress, &planHash, &route, &upstream, &requestID, &submitCount, &errorCode,
		&outputHash, &mediaType, &size, &width, &height, &duration, &usage, &manifestHash, &traceID, &createdAt, &updatedAt); err != nil {
		return controlplane.CreatorLiveShotRun{}, err
	}
	var artifact *controlplane.CreatorLiveShotArtifact
	if outputHash != nil {
		artifact = &controlplane.CreatorLiveShotArtifact{
			Digest: *outputHash, MediaType: stringValue(mediaType), SizeBytes: int64Value(size),
			Width: intValue(width), Height: intValue(height), DurationMillis: int64Value(duration),
			DownloadURL: controlplane.APIBase + "/creator/live-shot-runs/" + runID.String() + "/artifact",
		}
	}
	return creatorRunValue(runID, planID, seriesID, operationID, providerJobID, state, progress, planHash, route, upstream, requestID, submitCount, errorCode, artifact, usage, stringValue(manifestHash), traceID, createdAt, updatedAt), nil
}

func creatorRunValue(runID, planID, seriesID, operationID, providerJobID uuid.UUID, state string, progress *int, planHash string, route providercontract.ModelSnapshot, upstream, requestID string, submitCount int, errorCode string, artifact *controlplane.CreatorLiveShotArtifact, usage providercontract.Usage, manifestHash, traceID string, createdAt, updatedAt time.Time) controlplane.CreatorLiveShotRun {
	run := controlplane.CreatorLiveShotRun{
		SchemaVersion: "v1", RunID: runID.String(), PlanID: planID.String(), SeriesID: seriesID.String(),
		OperationID: operationID.String(), ProviderJobID: providerJobID.String(), State: state, Progress: progress,
		PlanHash: planHash, Route: creatorRouteProjection(route), UpstreamTaskID: nonemptyString(upstream), ProviderRequestID: nonemptyString(requestID), SubmitCount: submitCount, ErrorCode: errorCode,
		Artifact: artifact, Usage: creatorUsageProjection(usage), CashCost: controlplane.CreatorCashCost{AmountMicros: nil, Verified: false, BillingMode: creatorBillingMode},
		ManifestHash: manifestHash, TraceID: traceID, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if errorCode != "" {
		run.Failure = creatorFailure(errorCode)
	}
	if manifestHash != "" {
		run.Manifest = &controlplane.CreatorManifestSummary{ID: uuid.NewSHA1(runID, []byte("manifest")).String(), Hash: manifestHash, URL: controlplane.APIBase + "/creator/live-shot-runs/" + runID.String() + "/manifest", Evidence: "live_provider_call"}
	}
	return run
}

func creatorRouteProjection(route providercontract.ModelSnapshot) controlplane.CreatorRoute {
	return controlplane.CreatorRoute{CapabilityAlias: route.CapabilityAlias, Provider: route.Provider, ModelID: route.ModelID,
		EndpointID: route.EndpointID, RouteVersion: route.RouteVersion, CapabilityHash: route.CapabilityHash,
		Verification: route.Verification, BillingMode: creatorBillingMode}
}

func creatorUsageProjection(usage providercontract.Usage) controlplane.CreatorUsage {
	return controlplane.CreatorUsage{PromptVideoTokens: nonzeroInt64(usage.InputTokens), CompletionVideoTokens: nonzeroInt64(usage.OutputTokens), TotalVideoTokens: nonzeroInt64(usage.VideoTokens), GeneratedDurationMS: nonzeroInt64(usage.GeneratedMillis)}
}

func nonzeroInt64(value int64) *int64 {
	if value == 0 {
		return nil
	}
	return &value
}

func nonemptyString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func creatorFailure(code string) *controlplane.CreatorFailure {
	retryable := code == string(providercontract.CodeRateLimited) || code == string(providercontract.CodeUnavailable) || code == string(providercontract.CodeTimeout)
	action := "inspect the trace and Provider account, then continue polling the same run"
	if retryable {
		action = "continue polling; recovery reuses the same Provider task"
	}
	return &controlplane.CreatorFailure{ErrorCode: code, Retryable: retryable, SuggestedAction: action}
}

func (p *Postgres) GetCreatorLiveShotArtifact(ctx context.Context, runIDRaw string, actor controlplane.Actor) (controlplane.CreatorArtifactRecord, error) {
	run, err := p.GetCreatorLiveShotRun(ctx, runIDRaw, actor)
	if err != nil {
		return controlplane.CreatorArtifactRecord{}, err
	}
	if run.State != "SUCCEEDED" || run.Artifact == nil {
		return controlplane.CreatorArtifactRecord{}, controlplane.NewConflictError(controlplane.CodeArtifactCommitFailed, "the run has no committed MP4 artifact")
	}
	return controlplane.CreatorArtifactRecord{Digest: run.Artifact.Digest, MediaType: run.Artifact.MediaType, SizeBytes: run.Artifact.SizeBytes}, nil
}

func (p *Postgres) GetCreatorLiveShotManifest(ctx context.Context, runIDRaw string, actor controlplane.Actor) (controlplane.CreatorLiveShotManifest, error) {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return controlplane.CreatorLiveShotManifest{}, controlplane.NewNotFoundError("creator live-shot run", runIDRaw)
	}
	var manifest controlplane.CreatorLiveShotManifest
	if err := p.pool.QueryRow(ctx, `SELECT r.manifest FROM video_pipeline.creator_live_shot_runs r JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id WHERE r.id=$1 AND p.actor_id=$2 AND r.state='SUCCEEDED'`, runID, actor.ActorID).Scan(&manifest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return controlplane.CreatorLiveShotManifest{}, controlplane.NewNotFoundError("creator live-shot manifest", runIDRaw)
		}
		return controlplane.CreatorLiveShotManifest{}, err
	}
	return manifest, nil
}

func (p *Postgres) creatorShotWorkflowRecord(ctx context.Context, runID uuid.UUID) (controlplane.ShotWorkflowRecord, error) {
	var record controlplane.ShotWorkflowRecord
	var planID uuid.UUID
	var route providercontract.ModelSnapshot
	var createdAt time.Time
	if err := p.pool.QueryRow(ctx, `
		SELECT r.id,p.shot_spec_revision_id,r.run_spec_digest,r.state,r.trace_id,r.created_at,
		       p.prompt_snapshot_id,(r.request_snapshot->'input'->'prompt'->>'digest'),
		       p.route_snapshot,p.budget_approval_id,r.workflow_id,p.id
		FROM video_pipeline.creator_live_shot_runs r JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id
		WHERE r.id=$1`, runID).Scan(&record.Run.RunID, &record.Run.ShotSpecRevisionID, &record.Run.RunSpecDigest,
		&record.Run.State, &record.Run.TraceID, &createdAt, &record.PromptSnapshotID, &record.PromptHash,
		&route, &record.BudgetApprovalID, &record.Run.TemporalWorkflowID, &planID); err != nil {
		return controlplane.ShotWorkflowRecord{}, err
	}
	record.Run.CreativeAttempt = 1
	record.Run.CreatedAt = createdAt
	record.RouteSnapshot = controlplane.ModelRouteSnapshot{
		CapabilityAlias: route.CapabilityAlias, ProviderProfileID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("creator-provider:"+route.Provider)).String(),
		Provider: route.Provider, ModelID: route.ModelID, EndpointID: route.EndpointID, RouteVersion: route.RouteVersion, CapabilityHash: route.CapabilityHash,
	}
	record.BudgetLimit = controlplane.BudgetLimit{AmountMicros: creatorVideoTokenLimit, Currency: "VTC"}
	prompt, err := p.creatorPromptSnapshot(ctx, uuid.MustParse(record.PromptSnapshotID))
	if err != nil {
		return controlplane.ShotWorkflowRecord{}, fmt.Errorf("read creator workflow prompt snapshot: %w", err)
	}
	record.Prompt = workflowPromptSnapshot(prompt)
	_ = planID
	return record, nil
}

func (p *Postgres) creatorPromptSnapshot(ctx context.Context, promptID uuid.UUID) (orchestration.PromptSnapshotRef, error) {
	var snapshot creatorPreparedRequest
	if err := p.pool.QueryRow(ctx, `SELECT r.request_snapshot FROM video_pipeline.creator_live_shot_runs r JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id WHERE p.prompt_snapshot_id=$1`, promptID).Scan(&snapshot); err != nil {
		return orchestration.PromptSnapshotRef{}, err
	}
	return snapshot.Input.Prompt, nil
}

func (p *Postgres) prepareCreatorProviderJob(ctx context.Context, runID uuid.UUID, input orchestration.ExecuteProviderJobInput) (orchestration.PreparedProviderJob, bool, error) {
	var snapshot creatorPreparedRequest
	var state string
	var upstream *string
	err := p.pool.QueryRow(ctx, `SELECT request_snapshot,state,upstream_task_id FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, runID).Scan(&snapshot, &state, &upstream)
	if errors.Is(err, pgx.ErrNoRows) {
		return orchestration.PreparedProviderJob{}, false, nil
	}
	if err != nil {
		return orchestration.PreparedProviderJob{}, true, err
	}
	if !reflect.DeepEqual(snapshot.Input, input) {
		return orchestration.PreparedProviderJob{}, true, controlplane.NewConflictError(controlplane.CodePlanStale, "workflow input differs from confirmed provider intent")
	}
	if state == "FAILED" || state == "CANCELLED" || state == "SUCCEEDED" {
		return orchestration.PreparedProviderJob{}, true, controlplane.NewConflictError(controlplane.CodeRunTerminal, "a terminal creator run cannot submit a provider job")
	}
	snapshot.Prepared.ReconcileOnly = state != "QUEUED" || upstream != nil
	if snapshot.Prepared.ReconcileOnly && state != "REQUIRES_ACTION" {
		if _, err := p.pool.Exec(ctx, `UPDATE video_pipeline.creator_live_shot_runs SET state='RECONCILING',updated_at=now() WHERE id=$1 AND state IN ('RUNNING','UNKNOWN')`, runID); err != nil {
			return orchestration.PreparedProviderJob{}, true, err
		}
	}
	return snapshot.Prepared, true, nil
}

func (p *Postgres) recordCreatorProviderObservation(ctx context.Context, runID uuid.UUID, observation orchestration.ProviderJobObservation) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1)`, runID).Scan(&exists); err != nil || !exists {
		return exists, err
	}
	state := observation.State
	if state == "RUNNING" && observation.UpstreamTaskID == "" {
		state = "UNKNOWN"
	}
	_, err := p.pool.Exec(ctx, `
		WITH changed AS (
			UPDATE video_pipeline.creator_live_shot_runs
			SET state=$2,upstream_task_id=COALESCE(NULLIF($3,''),upstream_task_id),
			    upstream_request_id=COALESCE(NULLIF($4,''),upstream_request_id),progress=COALESCE($5,progress),
			    submit_count=GREATEST(submit_count,CASE WHEN $6='PROVIDER_SUBMISSION_PENDING' OR $3<>'' THEN 1 ELSE 0 END),
			    error_code=NULLIF($6,''),updated_at=now()
			WHERE id=$1 AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')
			RETURNING operation_id
		)
		UPDATE video_pipeline.operation_requests SET state='RUNNING',updated_at=now()
		WHERE id IN (SELECT operation_id FROM changed) AND state='ACCEPTED'`,
		runID, state, observation.UpstreamTaskID, observation.RequestID, observation.Progress, observation.ErrorCode)
	return true, err
}

func (p *Postgres) completeCreatorProviderJob(ctx context.Context, runID uuid.UUID, input orchestration.ExecuteProviderJobInput, result orchestration.ProviderResult) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1)`, runID).Scan(&exists); err != nil || !exists {
		return exists, err
	}
	if result.ArtifactDigest == "" || result.ArtifactURI != "cas://sha256/"+result.ArtifactDigest {
		return true, controlplane.NewPolicyError(controlplane.CodeArtifactCommitFailed, "provider output is not an immutable CAS artifact", "retry reconciliation with the same run")
	}
	if result.Usage.VideoTokens > creatorVideoTokenLimit {
		return true, controlplane.NewPolicyError(controlplane.CodeShotBudgetExceeded, "provider video token usage exceeded the confirmed per-shot cap", "do not commit this output")
	}
	if result.Cost.ActualMicros != nil || result.Cost.Verified || result.Cost.BillingMode != creatorBillingMode {
		return true, controlplane.NewPolicyError(controlplane.CodeCashChargeNotAllowed, "subscription evidence must keep cash amount null and unverified", "stop live calls and inspect Adapter billing semantics")
	}
	if result.Model != input.Route {
		return true, controlplane.NewConflictError(controlplane.CodePlanStale, "provider result model differs from the confirmed route")
	}
	type completionResult struct{ exists bool }
	_, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (completionResult, error) {
		var planID, providerJobID, reservationID, operationID uuid.UUID
		var reservedVideoTokens int64
		var planHash, requestHash, state, upstreamTaskID, requestID string
		var route providercontract.ModelSnapshot
		var confirmed creatorPreparedRequest
		var createdAt time.Time
		if err := tx.QueryRow(ctx, `
			SELECT r.plan_id,r.provider_job_id,r.reservation_id,r.operation_id,r.reserved_video_tokens,
			       p.plan_hash,r.request_hash,r.state,p.route_snapshot,
			       r.request_snapshot,COALESCE(r.upstream_task_id,''),
			       COALESCE(r.upstream_request_id,''),r.created_at
			FROM video_pipeline.creator_live_shot_runs r
			JOIN video_pipeline.creator_live_shot_plans p ON p.id=r.plan_id
			WHERE r.id=$1 FOR UPDATE OF r`, runID,
		).Scan(&planID, &providerJobID, &reservationID, &operationID, &reservedVideoTokens,
			&planHash, &requestHash, &state, &route, &confirmed, &upstreamTaskID,
			&requestID, &createdAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return completionResult{}, nil
			}
			return completionResult{exists: true}, fmt.Errorf("lock creator completion: %w", err)
		}
		if !reflect.DeepEqual(confirmed.Input, input) {
			return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider completion input differs from the confirmed immutable request")
		}
		if (upstreamTaskID != "" && upstreamTaskID != result.UpstreamTaskID) ||
			(requestID != "" && requestID != result.RequestID) {
			return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider completion differs from the durable upstream identity")
		}
		if upstreamTaskID == "" {
			upstreamTaskID = result.UpstreamTaskID
		}
		if requestID == "" {
			requestID = result.RequestID
		}
		if upstreamTaskID == "" {
			return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider completion has no durable upstream task identity")
		}
		artifact := controlplane.CreatorLiveShotArtifact{Digest: result.ArtifactDigest, MediaType: result.MediaType, SizeBytes: result.ArtifactSize, Width: result.Width, Height: result.Height, DurationMillis: result.DurationMillis, DownloadURL: controlplane.APIBase + "/creator/live-shot-runs/" + runID.String() + "/artifact"}
		var providerRegion *string
		if result.ProviderRegion != "" {
			providerRegion = &result.ProviderRegion
		}
		manifest := controlplane.CreatorLiveShotManifest{SchemaVersion: "creator-live-shot-manifest.v1", ManifestID: uuid.NewSHA1(runID, []byte("manifest")).String(), Evidence: "live_provider_call", RunID: runID.String(), PlanID: planID.String(), PlanHash: planHash, Provider: creatorRouteProjection(route), ProviderRegion: providerRegion, ProviderJobID: providerJobID.String(), UpstreamTaskID: upstreamTaskID, RequestID: requestID, InputHash: requestHash, OutputHash: result.ArtifactDigest, Media: artifact, Usage: creatorUsageProjection(result.Usage), Budget: controlplane.CreatorBudgetEvidence{BudgetApprovalID: input.BudgetApprovalID, ReservationID: reservationID.String(), ReservedTasks: 1, ReservedVideoTokens: reservedVideoTokens, SettledVideoTokens: nonzeroInt64(result.Usage.VideoTokens), Settlement: "CONSERVATIVE_SUBSCRIPTION_RESERVATION"}, CashCost: controlplane.CreatorCashCost{AmountMicros: nil, Verified: false, BillingMode: creatorBillingMode}, CreatedAt: createdAt}
		manifestHash, err := digestValue(manifest)
		if err != nil {
			return completionResult{exists: true}, err
		}
		if state == "SUCCEEDED" {
			var exact bool
			if err := tx.QueryRow(ctx, `
				SELECT progress=100 AND submit_count=1
				   AND upstream_task_id IS NOT DISTINCT FROM NULLIF($2,'')
				   AND upstream_request_id IS NOT DISTINCT FROM NULLIF($3,'')
				   AND output_hash=$4 AND output_media_type=$5 AND output_size_bytes=$6
				   AND output_width=$7 AND output_height=$8 AND output_duration_ms=$9
				   AND usage=$10 AND cash_cost=$11 AND manifest=$12 AND manifest_hash=$13
				   AND error_code IS NULL AND terminal_at IS NOT NULL
				FROM video_pipeline.creator_live_shot_runs WHERE id=$1`,
				runID, upstreamTaskID, requestID, result.ArtifactDigest, result.MediaType,
				result.ArtifactSize, result.Width, result.Height, result.DurationMillis,
				result.Usage, manifest.CashCost, manifest, manifestHash,
			).Scan(&exact); err != nil {
				return completionResult{exists: true}, fmt.Errorf("verify creator completion replay: %w", err)
			}
			if !exact {
				return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider completion replay differs from immutable terminal evidence")
			}
			if err := settleCreatorOperation(ctx, tx, operationID, "SUCCEEDED"); err != nil {
				return completionResult{exists: true}, err
			}
			return completionResult{exists: true}, nil
		}
		if state == "FAILED" || state == "CANCELLED" {
			return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRunTerminal, "provider completion cannot reverse creator terminal truth")
		}
		tag, err := tx.Exec(ctx, `
			UPDATE video_pipeline.creator_live_shot_runs
			SET state='SUCCEEDED',progress=100,submit_count=1,upstream_task_id=$2,
			    upstream_request_id=NULLIF($3,''),output_hash=$4,output_media_type=$5,
			    output_size_bytes=$6,output_width=$7,output_height=$8,output_duration_ms=$9,
			    usage=$10,cash_cost=$11,manifest=$12,manifest_hash=$13,error_code=NULL,
			    updated_at=now(),terminal_at=now()
			WHERE id=$1 AND state=$14`, runID, upstreamTaskID, requestID, result.ArtifactDigest,
			result.MediaType, result.ArtifactSize, result.Width, result.Height,
			result.DurationMillis, result.Usage, manifest.CashCost, manifest, manifestHash, state)
		if err != nil {
			return completionResult{exists: true}, fmt.Errorf("commit creator completion: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return completionResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "creator run changed before completion commit")
		}
		if err := settleCreatorOperation(ctx, tx, operationID, "SUCCEEDED"); err != nil {
			return completionResult{exists: true}, err
		}
		return completionResult{exists: true}, nil
	})
	return true, err
}

func (p *Postgres) recordCreatorQC(ctx context.Context, runID uuid.UUID, input orchestration.RunQCInput, result orchestration.QCResult) (bool, error) {
	var state, hash string
	err := p.pool.QueryRow(ctx, `SELECT state,COALESCE(output_hash,'') FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, runID).Scan(&state, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if state != "SUCCEEDED" || hash != input.Provider.ArtifactDigest || !result.Passed {
		return true, controlplane.NewConflictError(controlplane.CodeArtifactCommitFailed, "creator structural QC did not bind the committed successful artifact")
	}
	return true, nil
}

func (p *Postgres) creatorProviderJobPrepared(ctx context.Context, runID uuid.UUID) (bool, bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1)`, runID).Scan(&exists)
	return exists, exists, err
}

func (p *Postgres) finalizeCreatorRun(ctx context.Context, runID, operationID uuid.UUID, input orchestration.FinalizeShotRunInput) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1 AND operation_id=$2)`, runID, operationID).Scan(&exists); err != nil || !exists {
		return exists, err
	}
	if input.State == "SUCCEEDED" {
		var state string
		err := p.pool.QueryRow(ctx, `SELECT state FROM video_pipeline.creator_live_shot_runs WHERE id=$1`, runID).Scan(&state)
		if err != nil {
			return true, err
		}
		if state != "SUCCEEDED" {
			return true, controlplane.NewConflictError(controlplane.CodeManifestInvalid, "creator run cannot succeed before CAS and Manifest commit")
		}
		if _, err := p.pool.Exec(ctx, `UPDATE video_pipeline.operation_requests SET state='SUCCEEDED',updated_at=now() WHERE id=$1 AND aggregate_id=$2 AND state IN ('ACCEPTED','RUNNING')`, operationID, runID); err != nil {
			return true, err
		}
		return true, nil
	}
	type finalizationResult struct{}
	_, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (finalizationResult, error) {
		var state, errorCode string
		if err := tx.QueryRow(ctx, `
			SELECT state,COALESCE(error_code,'')
			FROM video_pipeline.creator_live_shot_runs
			WHERE id=$1 AND operation_id=$2 FOR UPDATE`, runID, operationID,
		).Scan(&state, &errorCode); err != nil {
			return finalizationResult{}, fmt.Errorf("lock creator failure finalization: %w", err)
		}
		failureCode := strings.TrimSpace(input.FailureCode)
		if state == "FAILED" {
			if errorCode != failureCode {
				return finalizationResult{}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "creator failure finalization replay differs from immutable terminal evidence")
			}
			if err := settleCreatorOperation(ctx, tx, operationID, "FAILED"); err != nil {
				return finalizationResult{}, err
			}
			return finalizationResult{}, nil
		}
		if state == "SUCCEEDED" || state == "CANCELLED" {
			return finalizationResult{}, controlplane.NewConflictError(controlplane.CodeRunTerminal, "creator failure finalization cannot reverse terminal truth")
		}
		tag, err := tx.Exec(ctx, `
			UPDATE video_pipeline.creator_live_shot_runs
			SET state='FAILED',error_code=NULLIF($2,''),updated_at=now(),terminal_at=now()
			WHERE id=$1 AND state=$3`, runID, failureCode, state)
		if err != nil {
			return finalizationResult{}, fmt.Errorf("commit creator failure finalization: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return finalizationResult{}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "creator run changed before failure finalization")
		}
		if err := settleCreatorOperation(ctx, tx, operationID, "FAILED"); err != nil {
			return finalizationResult{}, err
		}
		return finalizationResult{}, nil
	})
	return true, err
}

func settleCreatorOperation(ctx context.Context, tx pgx.Tx, operationID uuid.UUID, state string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE video_pipeline.operation_requests
		SET state=$2,updated_at=now()
		WHERE id=$1 AND state IN ('ACCEPTED','RUNNING','CANCEL_REQUESTED')`, operationID, state)
	if err != nil {
		return fmt.Errorf("settle creator operation: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var stored string
	if err := tx.QueryRow(ctx, `SELECT state FROM video_pipeline.operation_requests WHERE id=$1 FOR SHARE`, operationID).Scan(&stored); err != nil {
		return fmt.Errorf("verify creator operation replay: %w", err)
	}
	if stored != state {
		return controlplane.NewConflictError(controlplane.CodeRevisionConflict, "creator operation terminal state differs from the run")
	}
	return nil
}

func (p *Postgres) cancelCreatorProviderJob(ctx context.Context, runID uuid.UUID, result orchestration.CancelProviderResult) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM video_pipeline.creator_live_shot_runs WHERE id=$1)`, runID).Scan(&exists); err != nil || !exists {
		return exists, err
	}
	expectedCash, err := creatorCancellationCashCost(result.Cost)
	if err != nil {
		return true, err
	}
	type cancellationResult struct {
		exists      bool
		unconfirmed bool
	}
	txResult, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (cancellationResult, error) {
		var operationID uuid.UUID
		var state, upstreamTaskID, requestID, errorCode string
		var usage providercontract.Usage
		var cashCost controlplane.CreatorCashCost
		if err := tx.QueryRow(ctx, `
			SELECT operation_id,state,COALESCE(upstream_task_id,''),COALESCE(upstream_request_id,''),
			       COALESCE(error_code,''),usage,cash_cost
			FROM video_pipeline.creator_live_shot_runs WHERE id=$1 FOR UPDATE`, runID,
		).Scan(&operationID, &state, &upstreamTaskID, &requestID, &errorCode, &usage, &cashCost); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return cancellationResult{}, nil
			}
			return cancellationResult{exists: true}, fmt.Errorf("lock creator cancellation: %w", err)
		}
		if (upstreamTaskID != "" && result.UpstreamTaskID != "" && upstreamTaskID != result.UpstreamTaskID) ||
			(requestID != "" && result.RequestID != "" && requestID != result.RequestID) {
			return cancellationResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider cancellation differs from the durable upstream identity")
		}
		if result.NoRemoteTask && (upstreamTaskID != "" || requestID != "") {
			result = orchestration.CancelProviderResult{State: "UNKNOWN", ErrorCode: "CANCEL_NOT_CONFIRMED", UpstreamTaskID: upstreamTaskID, RequestID: requestID}
		} else if result.NoRemoteTask {
			result.State = "CANCELLED"
			result.ErrorCode = ""
		}
		if result.UpstreamTaskID == "" {
			result.UpstreamTaskID = upstreamTaskID
		}
		if result.RequestID == "" {
			result.RequestID = requestID
		}
		if state == "SUCCEEDED" {
			if err := settleCreatorOperation(ctx, tx, operationID, "SUCCEEDED"); err != nil {
				return cancellationResult{exists: true}, err
			}
			return cancellationResult{exists: true}, nil
		}
		if state == "CANCELLED" || state == "FAILED" {
			expectedState := result.State
			expectedCode := strings.TrimSpace(result.ErrorCode)
			if expectedState == "CANCELLED" {
				expectedCode = ""
			} else if expectedState == "FAILED" && expectedCode == "" {
				expectedCode = "PROVIDER_FAILED"
			}
			if state != expectedState || upstreamTaskID != result.UpstreamTaskID || requestID != result.RequestID ||
				errorCode != expectedCode || !reflect.DeepEqual(usage, result.Usage) ||
				!reflect.DeepEqual(cashCost, expectedCash) {
				return cancellationResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider cancellation replay differs from immutable terminal evidence")
			}
			if err := settleCreatorOperation(ctx, tx, operationID, state); err != nil {
				return cancellationResult{exists: true}, err
			}
			return cancellationResult{exists: true}, nil
		}
		terminalState := result.State
		terminalCode := strings.TrimSpace(result.ErrorCode)
		unconfirmed := false
		switch result.State {
		case "CANCELLED":
			terminalCode = ""
		case "FAILED":
			if terminalCode == "" {
				terminalCode = "PROVIDER_FAILED"
			}
		case "SUCCEEDED", "UNKNOWN":
			terminalState = "RECONCILING"
			terminalCode = "CANCEL_NOT_CONFIRMED"
			unconfirmed = true
		default:
			return cancellationResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "provider cancellation returned an invalid state")
		}
		terminalAt := "NULL"
		if terminalState == "CANCELLED" || terminalState == "FAILED" {
			terminalAt = "now()"
		}
		query := fmt.Sprintf(`
			UPDATE video_pipeline.creator_live_shot_runs
			SET state=$2,upstream_task_id=NULLIF($3,''),upstream_request_id=NULLIF($4,''),
			    usage=$5,cash_cost=$6,error_code=NULLIF($7,''),updated_at=now(),terminal_at=%s
			WHERE id=$1 AND state=$8`, terminalAt)
		tag, err := tx.Exec(ctx, query, runID, terminalState, result.UpstreamTaskID, result.RequestID,
			result.Usage, expectedCash, terminalCode, state)
		if err != nil {
			return cancellationResult{exists: true}, fmt.Errorf("commit creator cancellation: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return cancellationResult{exists: true}, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "creator run changed before cancellation commit")
		}
		operationState := "RUNNING"
		if terminalState == "CANCELLED" {
			operationState = "CANCELLED"
		} else if terminalState == "FAILED" {
			operationState = "FAILED"
		}
		if err := settleCreatorOperation(ctx, tx, operationID, operationState); err != nil {
			return cancellationResult{exists: true}, err
		}
		return cancellationResult{exists: true, unconfirmed: unconfirmed}, nil
	})
	if err != nil {
		return true, err
	}
	if txResult.unconfirmed {
		return true, errors.New("provider cancellation remains unconfirmed for the creator run")
	}
	return txResult.exists, nil
}

func creatorCancellationCashCost(cost providercontract.Cost) (controlplane.CreatorCashCost, error) {
	billingMode := strings.TrimSpace(cost.BillingMode)
	if billingMode == "" {
		billingMode = creatorBillingMode
	}
	if cost.ActualMicros != nil || cost.Verified || billingMode != creatorBillingMode {
		return controlplane.CreatorCashCost{}, controlplane.NewPolicyError(controlplane.CodeCashChargeNotAllowed, "subscription cancellation evidence must not contain a cash charge", "reconcile the Agent Plan task before settling")
	}
	var currency *string
	if value := strings.TrimSpace(cost.Currency); value != "" {
		currency = &value
	}
	return controlplane.CreatorCashCost{AmountMicros: nil, Currency: currency, Verified: false, BillingMode: billingMode}, nil
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
func int64Value(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
func intValue(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
