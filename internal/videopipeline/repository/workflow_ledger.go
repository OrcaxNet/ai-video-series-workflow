package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type immutableProviderRequest struct {
	Input    orchestration.ExecuteProviderJobInput `json:"input"`
	Prepared orchestration.PreparedProviderJob     `json:"prepared"`
}

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
		var effectiveHash, shotHash, freshness, aspectProfile, profileHash string
		var narrative []byte
		var durationMillis, fps, width, height int
		var storedProfileID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT context_revision_ids, asset_version_refs, effective_context_hash,
			       narrative, ssr.content_hash, ssr.generation_profile_id, freshness,
			       ssr.duration_ms, ssr.fps, ssr.width, ssr.height,
			       ssr.aspect_profile, gp.content_hash
			FROM video_pipeline.shot_spec_revisions ssr
			JOIN video_pipeline.generation_profiles gp
			  ON gp.id = ssr.generation_profile_id
			WHERE ssr.id = $1
			FOR SHARE OF ssr, gp`,
			shotID,
		).Scan(
			&contextIDs, &assetIDs, &effectiveHash, &narrative, &shotHash,
			&storedProfileID, &freshness, &durationMillis, &fps, &width, &height,
			&aspectProfile, &profileHash,
		); err != nil {
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
		contextRefs, contextHashes, err := loadPromptContextEvidence(ctx, tx, contextIDs)
		if err != nil {
			return orchestration.PromptSnapshotRef{}, err
		}
		assets, assetHashes, err := loadPromptAssetEvidence(ctx, tx, assetIDs)
		if err != nil {
			return orchestration.PromptSnapshotRef{}, err
		}
		output := providercontract.OutputSpec{
			Width: width, Height: height, Resolution: fmt.Sprintf("%dp", height),
			AspectRatio: aspectRatioFromProfile(aspectProfile), FPS: fps,
			DurationMillis: durationMillis, Format: "mp4",
		}
		inputHashes := map[string]string{
			"shot_spec":          shotHash,
			"generation_profile": profileHash,
		}
		maps.Copy(inputHashes, contextHashes)
		maps.Copy(inputHashes, assetHashes)

		normalized := map[string]any{
			"shotSpecRevisionId":  input.ShotSpecRevisionID,
			"shotContentHash":     shotHash,
			"contextRevisionIds":  uuidStrings(contextIDs),
			"assetVersionRefs":    uuidStrings(assetIDs),
			"inputRevisionHashes": inputHashes,
			"output":              output,
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

		promptHash, err := workflowPromptHash(
			input.ShotSpecRevisionID,
			input.GenerationProfileRef,
			effectiveHash,
			assetIDs,
			json.RawMessage(narrative),
			output,
			inputHashes,
		)
		if err != nil {
			return orchestration.PromptSnapshotRef{}, err
		}
		promptID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("prompt:"+promptHash))
		tag, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.prompt_snapshots
				(id, shot_spec_revision_id, schema_version, compiler_version, prompt_template_ref,
				 effective_context_snapshot_id, asset_version_refs, positive_prompt, negative_prompt,
				 model_payload, normalized_input_hash, content_hash, output_spec,
				 input_revision_hashes)
			VALUES ($1, $2, 'v1', 'control-plane-compiler-v1', 'video.prompt.v1',
			        $3, $4, $5, '', $6, $7, $7, $8, $9)
			ON CONFLICT (shot_spec_revision_id, content_hash) DO NOTHING`,
			promptID, shotID, effectiveID, assetIDs, string(narrative),
			map[string]any{"generationProfileRevisionId": input.GenerationProfileRef},
			promptHash, output, inputHashes,
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
		if err := persistPromptLineage(
			ctx, tx, promptID, shotID, profileID, shotHash, profileHash,
			contextIDs, assets,
		); err != nil {
			return orchestration.PromptSnapshotRef{}, err
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
		exact, err := loadExactPromptSnapshot(ctx, tx, promptID)
		if err != nil {
			return orchestration.PromptSnapshotRef{}, err
		}
		if exact.Context != contextRefs || !reflectPromptAssetsEqual(exact.Assets, assets) {
			return orchestration.PromptSnapshotRef{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"compiled prompt evidence differs from its immutable database record",
			)
		}
		return exact, nil
	})
}

// ResolvePromptSnapshot loads and revalidates the exact immutable execution
// record before a paid Provider submission. Callers may carry only ID+digest;
// executable Prompt, context, assets, output, and input hashes always come
// from PostgreSQL.
func (p *Postgres) ResolvePromptSnapshot(
	ctx context.Context,
	promptSnapshotID string,
) (orchestration.PromptSnapshotRef, error) {
	promptID, err := uuid.Parse(promptSnapshotID)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, errors.New("prompt snapshot ID must be a UUID")
	}
	return withSerializable(ctx, p.pool, func(tx pgx.Tx) (orchestration.PromptSnapshotRef, error) {
		return loadExactPromptSnapshot(ctx, tx, promptID)
	})
}

func loadExactPromptSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	promptID uuid.UUID,
) (orchestration.PromptSnapshotRef, error) {
	var (
		shotID, profileID         uuid.UUID
		contextIDs, assetIDs      []uuid.UUID
		schemaVersion             string
		compilerVersion           string
		effectiveHash             string
		positivePrompt            string
		negativePrompt            string
		normalizedHash            string
		contentHash               string
		output                    providercontract.OutputSpec
		storedInputRevisionHashes map[string]string
	)
	if err := tx.QueryRow(ctx, `
		SELECT ps.shot_spec_revision_id, ssr.generation_profile_id,
		       ps.schema_version, ps.compiler_version, ecs.content_hash,
		       ecs.context_revision_ids, ps.asset_version_refs,
		       ps.positive_prompt, ps.negative_prompt,
		       ps.normalized_input_hash, ps.content_hash,
		       ps.output_spec, ps.input_revision_hashes
		FROM video_pipeline.prompt_snapshots ps
		JOIN video_pipeline.shot_spec_revisions ssr
		  ON ssr.id = ps.shot_spec_revision_id
		JOIN video_pipeline.effective_context_snapshots ecs
		  ON ecs.id = ps.effective_context_snapshot_id
		WHERE ps.id = $1
		FOR SHARE OF ps, ssr, ecs`,
		promptID,
	).Scan(
		&shotID, &profileID, &schemaVersion, &compilerVersion, &effectiveHash,
		&contextIDs, &assetIDs, &positivePrompt, &negativePrompt,
		&normalizedHash, &contentHash, &output, &storedInputRevisionHashes,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return orchestration.PromptSnapshotRef{}, controlplane.NewNotFoundError(
				"prompt snapshot",
				promptID.String(),
			)
		}
		return orchestration.PromptSnapshotRef{}, fmt.Errorf("read immutable prompt snapshot: %w", err)
	}
	if schemaVersion != "v1" || compilerVersion != "control-plane-compiler-v1" {
		return orchestration.PromptSnapshotRef{}, controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"prompt snapshot compiler identity is not executable",
		)
	}
	contextRefs, contextHashes, err := loadPromptContextEvidence(ctx, tx, contextIDs)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, err
	}
	assets, assetHashes, err := loadPromptAssetEvidence(ctx, tx, assetIDs)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, err
	}
	var shotHash, profileHash string
	if err := tx.QueryRow(ctx, `
		SELECT ssr.content_hash, gp.content_hash
		FROM video_pipeline.shot_spec_revisions ssr
		JOIN video_pipeline.generation_profiles gp
		  ON gp.id = ssr.generation_profile_id
		WHERE ssr.id = $1 AND gp.id = $2
		FOR SHARE OF ssr, gp`,
		shotID, profileID,
	).Scan(&shotHash, &profileHash); err != nil {
		return orchestration.PromptSnapshotRef{}, fmt.Errorf("read immutable prompt producers: %w", err)
	}
	expectedInputRevisionHashes := map[string]string{
		"shot_spec":          shotHash,
		"generation_profile": profileHash,
	}
	maps.Copy(expectedInputRevisionHashes, contextHashes)
	maps.Copy(expectedInputRevisionHashes, assetHashes)
	if !maps.Equal(storedInputRevisionHashes, expectedInputRevisionHashes) {
		return orchestration.PromptSnapshotRef{}, controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"prompt snapshot input hashes differ from their immutable producers",
		)
	}
	expectedHash, err := workflowPromptHash(
		shotID.String(),
		profileID.String(),
		effectiveHash,
		assetIDs,
		json.RawMessage(positivePrompt),
		output,
		expectedInputRevisionHashes,
	)
	if err != nil {
		return orchestration.PromptSnapshotRef{}, err
	}
	expectedID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("prompt:"+expectedHash))
	if contentHash != expectedHash || normalizedHash != expectedHash || promptID != expectedID {
		return orchestration.PromptSnapshotRef{}, controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"prompt snapshot hash or ID differs from its immutable content",
		)
	}
	if positivePrompt == "" || output.Width <= 0 || output.Height <= 0 ||
		output.FPS <= 0 || output.DurationMillis <= 0 || output.Format == "" {
		return orchestration.PromptSnapshotRef{}, controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"prompt snapshot execution fields are incomplete",
		)
	}
	if err := verifyPromptLineage(
		ctx, tx, promptID, shotID, profileID, shotHash, profileHash,
		contextIDs, contextHashes, assetIDs, assets,
	); err != nil {
		return orchestration.PromptSnapshotRef{}, err
	}
	return orchestration.PromptSnapshotRef{
		ID: promptID.String(), Digest: contentHash,
		PositivePrompt: positivePrompt, NegativePrompt: negativePrompt,
		Context: contextRefs, Assets: assets, Output: output,
		InputRevisionHashes: storedInputRevisionHashes,
	}, nil
}

func persistPromptLineage(
	ctx context.Context,
	tx pgx.Tx,
	promptID, shotID, profileID uuid.UUID,
	shotHash, profileHash string,
	contextIDs []uuid.UUID,
	assets []providercontract.AssetRef,
) error {
	inputs := []struct {
		inputType      string
		revisionID     uuid.UUID
		hash           string
		dependencyRole string
	}{
		{
			inputType: "SHOT_SPEC", revisionID: shotID,
			hash: shotHash, dependencyRole: "primary-shot",
		},
		{
			inputType: "GENERATION_PROFILE", revisionID: profileID,
			hash: profileHash, dependencyRole: "generation-profile",
		},
	}
	for _, contextID := range contextIDs {
		var scope, hash string
		if err := tx.QueryRow(ctx, `
			SELECT scope_type, content_hash
			FROM video_pipeline.context_revisions
			WHERE id = $1
			FOR SHARE`,
			contextID,
		).Scan(&scope, &hash); err != nil {
			return fmt.Errorf("read Prompt context lineage: %w", err)
		}
		inputs = append(inputs, struct {
			inputType      string
			revisionID     uuid.UUID
			hash           string
			dependencyRole string
		}{
			inputType: "CONTEXT", revisionID: contextID,
			hash: hash, dependencyRole: "context:" + strings.ToLower(scope),
		})
	}
	for _, input := range inputs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.prompt_snapshot_inputs
				(prompt_snapshot_id, input_type, input_revision_id, input_hash, dependency_role)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`,
			promptID, input.inputType, input.revisionID, input.hash, input.dependencyRole,
		); err != nil {
			return fmt.Errorf("insert Prompt input lineage: %w", err)
		}
	}
	for index, asset := range assets {
		assetVersionID, err := uuid.Parse(asset.Revision)
		if err != nil {
			return fmt.Errorf("parse Prompt asset lineage revision: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.prompt_snapshot_assets
				(prompt_snapshot_id, alias, asset_version_id, asset_hash, provider_role)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`,
			promptID, fmt.Sprintf("asset-%03d", index+1),
			assetVersionID, asset.SHA256, string(asset.Role),
		); err != nil {
			return fmt.Errorf("insert Prompt asset lineage: %w", err)
		}
	}
	return nil
}

func verifyPromptLineage(
	ctx context.Context,
	tx pgx.Tx,
	promptID, shotID, profileID uuid.UUID,
	shotHash, profileHash string,
	contextIDs []uuid.UUID,
	contextHashes map[string]string,
	assetIDs []uuid.UUID,
	assets []providercontract.AssetRef,
) error {
	var exactInputs int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.prompt_snapshot_inputs
		WHERE prompt_snapshot_id = $1`,
		promptID,
	).Scan(&exactInputs); err != nil {
		return fmt.Errorf("count Prompt input lineage: %w", err)
	}
	if exactInputs != 2+len(contextIDs) {
		return controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"Prompt input lineage is incomplete or contains unexpected dependencies",
		)
	}
	expectedInputs := []struct {
		inputType string
		id        uuid.UUID
		hash      string
		role      string
	}{
		{"SHOT_SPEC", shotID, shotHash, "primary-shot"},
		{"GENERATION_PROFILE", profileID, profileHash, "generation-profile"},
	}
	contextByID := map[string]string{
		"series":  "",
		"episode": "",
		"scene":   "",
		"shot":    "",
	}
	for key, hash := range contextHashes {
		contextByID[strings.TrimPrefix(key, "context:")] = hash
	}
	for _, contextID := range contextIDs {
		var scope string
		if err := tx.QueryRow(ctx, `
			SELECT scope_type
			FROM video_pipeline.context_revisions
			WHERE id = $1
			FOR SHARE`,
			contextID,
		).Scan(&scope); err != nil {
			return fmt.Errorf("read Prompt context lineage scope: %w", err)
		}
		scope = strings.ToLower(scope)
		expectedInputs = append(expectedInputs, struct {
			inputType string
			id        uuid.UUID
			hash      string
			role      string
		}{
			"CONTEXT", contextID, contextByID[scope], "context:" + scope,
		})
	}
	for _, expected := range expectedInputs {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1
			  FROM video_pipeline.prompt_snapshot_inputs
			  WHERE prompt_snapshot_id = $1
			    AND input_type = $2
			    AND input_revision_id = $3
			    AND input_hash = $4
			    AND dependency_role = $5
			)`,
			promptID, expected.inputType, expected.id, expected.hash, expected.role,
		).Scan(&exists); err != nil {
			return fmt.Errorf("verify Prompt input lineage: %w", err)
		}
		if !exists {
			return controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"Prompt input lineage differs from its immutable producers",
			)
		}
	}
	var exactAssets int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.prompt_snapshot_assets
		WHERE prompt_snapshot_id = $1`,
		promptID,
	).Scan(&exactAssets); err != nil {
		return fmt.Errorf("count Prompt asset lineage: %w", err)
	}
	if exactAssets != len(assetIDs) || len(assetIDs) != len(assets) {
		return controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"Prompt asset lineage is incomplete or contains unexpected dependencies",
		)
	}
	for index, assetID := range assetIDs {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1
			  FROM video_pipeline.prompt_snapshot_assets
			  WHERE prompt_snapshot_id = $1
			    AND alias = $2
			    AND asset_version_id = $3
			    AND asset_hash = $4
			    AND provider_role = $5
			)`,
			promptID, fmt.Sprintf("asset-%03d", index+1), assetID,
			assets[index].SHA256, string(assets[index].Role),
		).Scan(&exists); err != nil {
			return fmt.Errorf("verify Prompt asset lineage: %w", err)
		}
		if !exists {
			return controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"Prompt asset lineage differs from its immutable producers",
			)
		}
	}
	return nil
}

func workflowPromptHash(
	shotSpecRevisionID string,
	generationProfileRevisionID string,
	effectiveContextHash string,
	assetVersionIDs []uuid.UUID,
	narrative json.RawMessage,
	output providercontract.OutputSpec,
	inputRevisionHashes map[string]string,
) (string, error) {
	return digestValue(map[string]any{
		"schemaVersion":             "v1",
		"compilerVersion":           "control-plane-compiler-v1",
		"shotSpecRevisionId":        shotSpecRevisionID,
		"generationProfileRevision": generationProfileRevisionID,
		"effectiveContextHash":      effectiveContextHash,
		"assetVersionRefs":          uuidStrings(assetVersionIDs),
		"narrative":                 narrative,
		"output":                    output,
		"inputRevisionHashes":       inputRevisionHashes,
	})
}

func generationRunSpecDigest(
	shotSpecRevisionID string,
	promptSnapshotID string,
	promptHash string,
	generationProfileID string,
	generationPlanID string,
	route providercontract.ModelSnapshot,
	creativeAttempt int,
) (string, error) {
	return digestValue(map[string]any{
		"shotSpecRevisionId": shotSpecRevisionID,
		"promptSnapshotId":   promptSnapshotID,
		"promptHash":         promptHash,
		"profileId":          generationProfileID,
		"generationPlanId":   generationPlanID,
		"route":              route,
		"creativeAttempt":    creativeAttempt,
	})
}

func providerRouteSnapshot(
	route controlplane.ModelRouteSnapshot,
) providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{
		CapabilityAlias: route.CapabilityAlias,
		Provider:        route.Provider,
		ModelID:         route.ModelID,
		EndpointID:      route.EndpointID,
		RouteVersion:    route.RouteVersion,
		CapabilityHash:  route.CapabilityHash,
		Verification:    "control_plane_capability_snapshot",
	}
}

func loadPromptContextEvidence(
	ctx context.Context,
	tx pgx.Tx,
	contextIDs []uuid.UUID,
) (providercontract.ContextRefs, map[string]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, scope_type, content_hash
		FROM video_pipeline.context_revisions
		WHERE id = ANY($1::uuid[])
		  AND status = 'APPROVED'
		  AND scope_type IN ('SERIES', 'EPISODE', 'SCENE', 'SHOT')
		FOR SHARE`,
		contextIDs,
	)
	if err != nil {
		return providercontract.ContextRefs{}, nil, fmt.Errorf("read prompt contexts: %w", err)
	}
	defer rows.Close()
	type contextEvidence struct {
		scope string
		hash  string
	}
	byID := make(map[uuid.UUID]contextEvidence, len(contextIDs))
	for rows.Next() {
		var id uuid.UUID
		var evidence contextEvidence
		if err := rows.Scan(&id, &evidence.scope, &evidence.hash); err != nil {
			return providercontract.ContextRefs{}, nil, fmt.Errorf("scan prompt context: %w", err)
		}
		byID[id] = evidence
	}
	if err := rows.Err(); err != nil {
		return providercontract.ContextRefs{}, nil, fmt.Errorf("iterate prompt contexts: %w", err)
	}
	var refs providercontract.ContextRefs
	hashes := make(map[string]string, len(contextIDs))
	for _, id := range contextIDs {
		evidence, ok := byID[id]
		if !ok {
			return providercontract.ContextRefs{}, nil, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"effective context contains an unavailable or unapproved revision",
				"resolve and approve the complete four-level context",
			)
		}
		hashes["context:"+strings.ToLower(evidence.scope)] = evidence.hash
		switch evidence.scope {
		case "SERIES":
			refs.SeriesSnapshotID = id.String()
		case "EPISODE":
			refs.EpisodeSnapshotID = id.String()
		case "SCENE":
			refs.SceneSnapshotID = id.String()
		case "SHOT":
			refs.ShotSnapshotID = id.String()
		}
	}
	if refs.SeriesSnapshotID == "" || refs.EpisodeSnapshotID == "" ||
		refs.SceneSnapshotID == "" || refs.ShotSnapshotID == "" ||
		len(byID) != 4 {
		return providercontract.ContextRefs{}, nil, controlplane.NewPolicyError(
			controlplane.CodeStaleDependency,
			"effective context must bind one approved SERIES, EPISODE, SCENE, and SHOT revision",
			"resolve and approve the complete four-level context",
		)
	}
	return refs, hashes, nil
}

func loadPromptAssetEvidence(
	ctx context.Context,
	tx pgx.Tx,
	assetVersionIDs []uuid.UUID,
) ([]providercontract.AssetRef, map[string]string, error) {
	if len(assetVersionIDs) == 0 {
		return nil, map[string]string{}, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT av.id, av.asset_id, av.content_hash, av.artifact_uri,
		       av.media_type, av.dimensions, ls.license_id, ls.license_hash
		FROM video_pipeline.asset_versions av
		JOIN video_pipeline.license_snapshots ls
		  ON ls.id = av.license_snapshot_id
		WHERE av.id = ANY($1::uuid[])
		  AND av.status = 'APPROVED'
		  AND ls.policy_status = 'ALLOWED'
		  AND (ls.expires_at IS NULL OR ls.expires_at > now())
		FOR SHARE OF av, ls`,
		assetVersionIDs,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("read prompt assets: %w", err)
	}
	defer rows.Close()
	type assetEvidence struct {
		ref  providercontract.AssetRef
		hash string
	}
	byID := make(map[uuid.UUID]assetEvidence, len(assetVersionIDs))
	for rows.Next() {
		var (
			versionID, assetID uuid.UUID
			contentHash, uri   string
			mediaType          string
			dimensions         []byte
			licenseID          string
			licenseHash        string
		)
		if err := rows.Scan(
			&versionID, &assetID, &contentHash, &uri, &mediaType, &dimensions,
			&licenseID, &licenseHash,
		); err != nil {
			return nil, nil, fmt.Errorf("scan prompt asset: %w", err)
		}
		modality, role, err := promptAssetType(mediaType)
		if err != nil {
			return nil, nil, err
		}
		var mediaSpec struct {
			Width          int   `json:"width"`
			Height         int   `json:"height"`
			DurationMillis int64 `json:"durationMillis"`
		}
		if len(dimensions) != 0 {
			if err := json.Unmarshal(dimensions, &mediaSpec); err != nil {
				return nil, nil, fmt.Errorf("decode prompt asset dimensions: %w", err)
			}
		}
		byID[versionID] = assetEvidence{
			hash: contentHash,
			ref: providercontract.AssetRef{
				ID: assetID.String(), Revision: versionID.String(), Kind: modality,
				Role: role, URI: uri, SHA256: contentHash,
				LicenseReference: licenseID + ":" + licenseHash,
				MediaType:        mediaType, Width: mediaSpec.Width, Height: mediaSpec.Height,
				DurationMillis: mediaSpec.DurationMillis,
			},
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate prompt assets: %w", err)
	}
	refs := make([]providercontract.AssetRef, 0, len(assetVersionIDs))
	hashes := make(map[string]string, len(assetVersionIDs))
	for _, id := range assetVersionIDs {
		evidence, ok := byID[id]
		if !ok {
			return nil, nil, controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"prompt asset is unavailable, unapproved, or no longer licensed",
				"refresh the approved asset and license snapshot",
			)
		}
		refs = append(refs, evidence.ref)
		hashes["asset:"+id.String()] = evidence.hash
	}
	return refs, hashes, nil
}

func loadShotAssetIDs(
	ctx context.Context,
	tx pgx.Tx,
	shotIDs []uuid.UUID,
) ([]uuid.UUID, error) {
	rows, err := tx.Query(ctx, `
		SELECT asset_version_refs
		FROM video_pipeline.shot_spec_revisions
		WHERE id = ANY($1::uuid[])
		ORDER BY id
		FOR SHARE`,
		shotIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("read frozen shot asset references: %w", err)
	}
	defer rows.Close()
	seen := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var refs []uuid.UUID
		if err := rows.Scan(&refs); err != nil {
			return nil, fmt.Errorf("scan frozen shot asset references: %w", err)
		}
		for _, id := range refs {
			seen[id] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate frozen shot asset references: %w", err)
	}
	if len(seen) == 0 {
		return nil, nil
	}
	assetIDs := make([]uuid.UUID, 0, len(seen))
	for id := range seen {
		assetIDs = append(assetIDs, id)
	}
	sort.Slice(assetIDs, func(left, right int) bool {
		return assetIDs[left].String() < assetIDs[right].String()
	})
	return assetIDs, nil
}

// requireFrozenPlanShots locks and revalidates the complete shot set before a
// paid submission. One valid current shot cannot mask drift elsewhere in the
// execution package.
func requireFrozenPlanShots(
	ctx context.Context,
	tx pgx.Tx,
	shotIDs []uuid.UUID,
	profileID uuid.UUID,
	seriesID uuid.UUID,
	episodeID uuid.UUID,
) error {
	rows, err := tx.Query(ctx, `
		SELECT ssr.id, ssr.generation_profile_id, ssr.freshness,
		       ssr.gate2_decision_id
		FROM video_pipeline.shot_spec_revisions ssr
		JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		WHERE ssr.id = ANY($1::uuid[])
		  AND ep.id = $2
		  AND ep.series_id = $3
		ORDER BY ssr.id
		FOR SHARE OF ssr`,
		shotIDs, episodeID, seriesID,
	)
	if err != nil {
		return fmt.Errorf("lock frozen generation-plan shots: %w", err)
	}
	defer rows.Close()
	type frozenShot struct {
		id, generationProfileID, gate2DecisionID uuid.UUID
		freshness                                string
	}
	shots := make([]frozenShot, 0, len(shotIDs))
	for rows.Next() {
		var shot frozenShot
		if err := rows.Scan(
			&shot.id, &shot.generationProfileID, &shot.freshness, &shot.gate2DecisionID,
		); err != nil {
			return fmt.Errorf("scan frozen generation-plan shot: %w", err)
		}
		shots = append(shots, shot)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate frozen generation-plan shots: %w", err)
	}
	if len(shots) != len(shotIDs) {
		return controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"generation plan no longer resolves its complete frozen shot set",
			"freeze a new plan for the exact approved shots",
		)
	}
	for _, shot := range shots {
		if shot.generationProfileID != profileID ||
			shot.freshness != "FRESH" && shot.freshness != "REVALIDATED" {
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"a frozen generation-plan shot has drifted before paid submission",
				"revalidate the complete shot set and freeze a new execution package",
			)
		}
		if err := requireApprovedDecision(
			ctx, tx, shot.gate2DecisionID.String(), "G2", seriesID, episodeID,
			"SHOT_SPEC_REVISION", shot.id,
		); err != nil {
			return err
		}
	}
	return nil
}

func promptAssetType(mediaType string) (providercontract.Modality, providercontract.AssetRole, error) {
	switch {
	case strings.HasPrefix(mediaType, "image/"):
		return providercontract.ModalityImage, providercontract.AssetRoleReferenceImage, nil
	case strings.HasPrefix(mediaType, "video/"):
		return providercontract.ModalityVideo, providercontract.AssetRoleReferenceVideo, nil
	case strings.HasPrefix(mediaType, "audio/"):
		return providercontract.ModalityAudio, providercontract.AssetRoleReferenceAudio, nil
	default:
		return "", "", controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"prompt asset media type is not supported by the video Provider contract",
			"replace the asset with an image, video, or audio revision",
		)
	}
}

func aspectRatioFromProfile(profile string) string {
	if separator := strings.IndexByte(profile, '_'); separator > 0 {
		return profile[:separator]
	}
	return profile
}

func reflectPromptAssetsEqual(left, right []providercontract.AssetRef) bool {
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
		if err := requireBudgetApproval(
			ctx, tx, input.BudgetApprovalID, seriesID, episodeID,
			input.GenerationPlanID, "VIDEO", plan.BudgetLimit,
		); err != nil {
			return orchestration.GenerationRunRef{}, err
		}
		runDigest, err := generationRunSpecDigest(
			input.ShotSpecRevisionID,
			input.PromptSnapshot.ID,
			promptHash,
			input.GenerationProfileRef,
			input.GenerationPlanID,
			input.Route,
			input.CreativeAttempt,
		)
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
					"generationPlanId":   input.GenerationPlanID,
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
) (orchestration.PreparedProviderJob, error) {
	runID, err := uuid.Parse(input.Run.RunID)
	if err != nil {
		return orchestration.PreparedProviderJob{}, errors.New("runId must be a UUID")
	}
	providerProfileID, err := uuid.Parse(input.ProviderProfileID)
	if err != nil {
		return orchestration.PreparedProviderJob{}, errors.New("providerProfileId must be a UUID")
	}
	prepared, err := withSerializable(ctx, p.pool, func(
		tx pgx.Tx,
	) (orchestration.PreparedProviderJob, error) {
		var attemptID, promptID, shotID, profileID, gate2DecisionID uuid.UUID
		var seriesID, episodeID uuid.UUID
		var runDigest, runState, persistedBudgetApprovalID, generationPlanID string
		var promptHash string
		var creativeAttempt int
		var persistedRouteJSON []byte
		if err := tx.QueryRow(ctx, `
			SELECT ga.id, gr.prompt_snapshot_id, gr.shot_spec_revision_id,
			       gr.generation_profile_id, gr.creative_attempt,
			       gr.run_spec_digest, gr.state, gr.budget_approval_id,
			       COALESCE(audit.payload->>'generationPlanId', ''),
			       ep.series_id, ep.id, ps.content_hash, ga.model_snapshot,
			       ssr.gate2_decision_id
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga
			  ON ga.generation_run_id = gr.id AND ga.sequence = 1
			JOIN video_pipeline.prompt_snapshots ps ON ps.id = gr.prompt_snapshot_id
			JOIN video_pipeline.shot_spec_revisions ssr
			  ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			LEFT JOIN LATERAL (
				SELECT payload
				FROM video_pipeline.audit_events
				WHERE aggregate_type = 'GENERATION_RUN'
				  AND aggregate_id = gr.id
				  AND action = 'generation_run.created'
				ORDER BY occurred_at
				LIMIT 1
			) audit ON true
			WHERE gr.id = $1
			FOR UPDATE OF gr, ga`,
			runID,
		).Scan(
			&attemptID, &promptID, &shotID, &profileID, &creativeAttempt,
			&runDigest, &runState, &persistedBudgetApprovalID,
			&generationPlanID, &seriesID, &episodeID, &promptHash,
			&persistedRouteJSON, &gate2DecisionID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf("lock provider run: %w", err)
		}
		if runDigest != input.Run.RunSpecDigest {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch digest differs from the persisted run",
			)
		}
		if runState == "CANCELLED" || runState == "FAILED" {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"a terminal generation run cannot create or replay a paid Provider job",
			)
		}
		if generationPlanID == "" {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"generation run has no immutable generation plan binding",
				"create a new run from the exact approved plan",
			)
		}
		if input.BudgetApprovalID != persistedBudgetApprovalID {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"provider dispatch budget approval differs from the persisted run",
				"use the exact approval frozen when the run was created",
			)
		}
		var persistedRoute providercontract.ModelSnapshot
		if err := json.Unmarshal(persistedRouteJSON, &persistedRoute); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"decode persisted Provider route: %w", err,
			)
		}
		if persistedRoute != input.Route {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch route differs from the route frozen for the run",
			)
		}
		exactPrompt, err := loadExactPromptSnapshot(ctx, tx, promptID)
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if input.Prompt.ID != promptID.String() ||
			input.Prompt.Digest != promptHash ||
			!reflect.DeepEqual(input.Prompt, exactPrompt) {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch prompt differs from the prompt frozen for the run",
			)
		}
		plan, err := readPlan(ctx, tx, generationPlanID)
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if plan.SeriesID != seriesID.String() || plan.Plan.State == "BLOCKED" ||
			plan.EpisodeRevisionID == "" ||
			!containsString(plan.ShotSpecRevisionIDs, shotID.String()) {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"provider run is outside the current immutable generation plan",
				"create a new run from the exact approved episode and shot set",
			)
		}
		episodeRevisionID, err := uuid.Parse(plan.EpisodeRevisionID)
		if err != nil {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"generation plan episode revision is invalid",
				"create a new plan for the exact approved episode revision",
			)
		}
		planShotIDs, err := parseUUIDs(plan.ShotSpecRevisionIDs)
		if err != nil {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeStaleDependency,
				"generation plan shot revisions are invalid",
				"create a new plan for the exact approved shot set",
			)
		}
		var episodeStatus string
		if err := tx.QueryRow(ctx, `
			SELECT status
			FROM video_pipeline.episode_revisions
			WHERE id = $1 AND episode_id = $2
			FOR SHARE`,
			episodeRevisionID, episodeID,
		).Scan(&episodeStatus); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf("read provider episode gate: %w", err)
		}
		if episodeStatus != "G2_APPROVED" && episodeStatus != "G3_LOCKED" {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"provider episode revision is no longer approved for generation",
				"approve the exact episode revision before paid submission",
			)
		}
		if err := requireFrozenPlanShots(
			ctx, tx, planShotIDs, profileID, seriesID, episodeID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if err := requireApprovedDecision(
			ctx, tx, gate2DecisionID.String(), "G2", seriesID, episodeID,
			"SHOT_SPEC_REVISION", shotID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		assetIDs, err := loadShotAssetIDs(ctx, tx, planShotIDs)
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if err := lockPostProductionRights(ctx, tx, assetIDs); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if err := requireAssetLicenses(
			ctx, tx, assetIDs, p.now().UTC(), plan.ExecutionPolicy,
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if err := requireContentSafetyDecision(
			ctx, tx, plan.ExecutionPolicy, seriesID, episodeRevisionID,
			planShotIDs, p.now().UTC(),
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if input.BudgetMaximumMicros != plan.BudgetLimit.AmountMicros ||
			input.BudgetCurrency != plan.BudgetLimit.Currency {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"provider dispatch budget differs from the immutable generation plan",
				"use the exact VIDEO amount and currency frozen by the plan",
			)
		}
		workflowRoute := controlplane.ModelRouteSnapshot{
			CapabilityAlias:   input.Route.CapabilityAlias,
			ProviderProfileID: input.ProviderProfileID,
			Provider:          input.Route.Provider, ModelID: input.Route.ModelID,
			EndpointID: input.Route.EndpointID, RouteVersion: input.Route.RouteVersion,
			CapabilityHash: input.Route.CapabilityHash,
		}
		if !sameRoute(plan.Plan.RouteSnapshot, workflowRoute) {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch route differs from the immutable generation plan",
			)
		}
		recomputedDigest, err := generationRunSpecDigest(
			shotID.String(),
			promptID.String(),
			promptHash,
			profileID.String(),
			generationPlanID,
			persistedRoute,
			creativeAttempt,
		)
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if recomputedDigest != runDigest {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"persisted generation run digest no longer matches its frozen inputs",
			)
		}
		if err := requireBudgetApproval(
			ctx, tx, persistedBudgetApprovalID, seriesID, episodeID,
			generationPlanID, "VIDEO", plan.BudgetLimit,
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		approvalID, err := uuid.Parse(persistedBudgetApprovalID)
		if err != nil {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"budget approval identifier is invalid",
				"approve the exact immutable generation plan budget",
			)
		}
		var approvedMicros int64
		var approvedCurrency string
		if err := tx.QueryRow(ctx, `
			SELECT budget_limit_micros, budget_currency
			FROM video_pipeline.review_tasks
			WHERE id = $1 AND state = 'APPROVED'
			FOR UPDATE`,
			approvalID,
		).Scan(&approvedMicros, &approvedCurrency); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"lock cumulative budget approval: %w", err,
			)
		}
		if approvedMicros != plan.BudgetLimit.AmountMicros ||
			approvedCurrency != plan.BudgetLimit.Currency {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"locked budget approval differs from the immutable generation plan",
				"approve the exact plan amount and currency",
			)
		}
		var capabilityID uuid.UUID
		var pricingVersion string
		var capabilityLimitsJSON []byte
		var capabilityProvider, capabilityModel, capabilityRouteVersion string
		if err := tx.QueryRow(ctx, `
			SELECT pcs.id, COALESCE(pcs.pricing_rule_version, ''),
			       pcs.limits, pp.provider, pcs.model_id, pcs.route_version
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
		).Scan(
			&capabilityID, &pricingVersion, &capabilityLimitsJSON,
			&capabilityProvider, &capabilityModel, &capabilityRouteVersion,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
					controlplane.CodeCapability,
					"the frozen provider capability is no longer dispatchable",
					"refresh the generation plan and provider route",
				)
			}
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"read frozen provider capability: %w", err,
			)
		}
		if capabilityProvider != input.Route.Provider ||
			capabilityModel != input.Route.ModelID ||
			capabilityRouteVersion != input.Route.RouteVersion {
			return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider dispatch model differs from the frozen capability snapshot",
			)
		}
		var capabilityLimits map[string]any
		if err := json.Unmarshal(capabilityLimitsJSON, &capabilityLimits); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"decode Provider capability pricing limits: %w", err,
			)
		}
		if err := requireExecutionPolicy(
			capabilityLimits, plan.ExecutionPolicy, plan.Plan.ProviderCallCount,
		); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		unitPrice, priced := numericLimit(capabilityLimits, "unitPriceMicros")
		if !priced || unitPrice <= 0 || pricingVersion == "" {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"the frozen Provider route has no executable unit-price snapshot",
				"refresh pricing and approve a fully priced generation plan",
			)
		}
		estimatedMicros := saturatingMicros(
			float64(exactPrompt.Output.DurationMillis)/1000,
			unitPrice,
		)
		if estimatedMicros <= 0 {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"the per-run Provider estimate is not positive",
				"refresh the Prompt output duration and pricing snapshot",
			)
		}
		reservationID := uuid.NewSHA1(runID, []byte("budget-reservation"))
		budget := providercontract.BudgetEnvelope{
			EstimatedCostMicros: estimatedMicros,
			MaxCostMicros:       estimatedMicros,
			MaxAttempts:         1,
		}
		bindReservation := func() (orchestration.PreparedProviderJob, error) {
			reservation, err := providercontract.BindBudgetReservation(
				providercontract.BudgetReservation{
					ReservationID: reservationID.String(),
					Currency:      approvedCurrency, AmountMicros: estimatedMicros,
					PricingVersion: pricingVersion,
					ConfirmedBy:    persistedBudgetApprovalID,
				},
				providercontract.BudgetBindingInput{
					RunID: input.Run.RunID, InputHash: runDigest,
					Model: persistedRoute, Budget: budget,
				},
			)
			if err != nil {
				return orchestration.PreparedProviderJob{}, fmt.Errorf(
					"bind durable Provider budget reservation: %w", err,
				)
			}
			return orchestration.PreparedProviderJob{
				Budget: budget, BudgetReservation: reservation,
				ProductTruth: orchestration.PreparedProductTruth{
					ShotSpecRevisionID: shotID.String(),
					Run: orchestration.GenerationRunRef{
						RunID: input.Run.RunID, RunSpecDigest: runDigest,
						Attempt: creativeAttempt,
					},
					PromptSnapshotID: promptID.String(), PromptSnapshotHash: promptHash,
					GenerationPlanID:    generationPlanID,
					BudgetApprovalID:    persistedBudgetApprovalID,
					BudgetMaximumMicros: approvedMicros, BudgetCurrency: approvedCurrency,
					ProviderProfileID: providerProfileID.String(), Route: persistedRoute,
				},
			}, nil
		}
		prepared, err := bindReservation()
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		jobID := uuid.NewSHA1(runID, []byte("provider-job"))
		jobKey := "provider-job-" + input.Run.RunID
		requestEnvelope := immutableProviderRequest{
			Input: input, Prepared: prepared,
		}
		requestHash, err := digestValue(requestEnvelope)
		if err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		requestSnapshot, err := json.Marshal(requestEnvelope)
		if err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"encode provider request snapshot: %w", err,
			)
		}
		verifyPreparedJob := func() error {
			var storedJobID, storedAttemptID, storedCapabilityID, storedReservationID uuid.UUID
			var storedRequestHash string
			var storedRequestSnapshot []byte
			if err := tx.QueryRow(ctx, `
				SELECT id, generation_attempt_id, capability_snapshot_id,
				       budget_reservation_id, request_hash, request_snapshot
				FROM video_pipeline.provider_jobs
				WHERE provider_profile_id = $1 AND idempotency_key = $2
				FOR SHARE`,
				providerProfileID, jobKey,
			).Scan(
				&storedJobID, &storedAttemptID, &storedCapabilityID,
				&storedReservationID, &storedRequestHash, &storedRequestSnapshot,
			); err != nil {
				return fmt.Errorf("read prepared immutable Provider job: %w", err)
			}
			var storedEnvelope immutableProviderRequest
			if err := json.Unmarshal(storedRequestSnapshot, &storedEnvelope); err != nil {
				return fmt.Errorf("decode prepared immutable Provider job: %w", err)
			}
			if storedJobID != jobID ||
				storedAttemptID != attemptID ||
				storedCapabilityID != capabilityID ||
				storedReservationID != reservationID ||
				storedRequestHash != requestHash ||
				!reflect.DeepEqual(storedEnvelope, requestEnvelope) {
				return controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"existing Provider job differs from the exact frozen Run request",
				)
			}
			return nil
		}
		var existingAmount int64
		var existingCurrency, existingPricing, existingStatus string
		existingErr := tx.QueryRow(ctx, `
			SELECT amount_micros, currency, pricing_rule_version, status
			FROM video_pipeline.budget_reservations
			WHERE id = $1
			FOR UPDATE`,
			reservationID,
		).Scan(
			&existingAmount, &existingCurrency, &existingPricing, &existingStatus,
		)
		if existingErr == nil {
			if existingAmount != estimatedMicros ||
				existingCurrency != approvedCurrency ||
				existingPricing != pricingVersion ||
				(existingStatus != "RESERVED" && existingStatus != "SETTLED") {
				return orchestration.PreparedProviderJob{}, controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"existing Provider reservation differs from the frozen run allocation",
				)
			}
			if err := verifyPreparedJob(); err != nil {
				return orchestration.PreparedProviderJob{}, err
			}
			return prepared, nil
		}
		if !errors.Is(existingErr, pgx.ErrNoRows) {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"read durable Provider reservation: %w", existingErr,
			)
		}
		var allocatedMicros int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(
			  CASE br.status
			    WHEN 'RESERVED' THEN br.amount_micros
			    WHEN 'SETTLED' THEN COALESCE(actual.amount_micros, br.amount_micros)
			    ELSE 0
			  END
			), 0)
			FROM video_pipeline.budget_reservations br
			JOIN video_pipeline.generation_runs gr ON gr.id = br.generation_run_id
			LEFT JOIN LATERAL (
			  SELECT cl.amount_micros
			  FROM video_pipeline.cost_ledger cl
			  WHERE cl.budget_reservation_id = br.id
			    AND cl.entry_type = 'ACTUAL'
			    AND cl.amount_micros >= 0
			    AND cl.verified = true
			    AND cl.currency = br.currency
			    AND cl.pricing_rule_version = br.pricing_rule_version
			  ORDER BY cl.created_at DESC
			  LIMIT 1
			) actual ON true
			WHERE gr.budget_approval_id = $1
			  AND br.status IN ('RESERVED', 'SETTLED')`,
			persistedBudgetApprovalID,
		).Scan(&allocatedMicros); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"read cumulative Provider budget allocation: %w", err,
			)
		}
		if estimatedMicros > approvedMicros ||
			allocatedMicros > approvedMicros-estimatedMicros {
			return orchestration.PreparedProviderJob{}, controlplane.NewPolicyError(
				controlplane.CodeBudgetExceeded,
				"the cumulative Provider reservation exceeds the approved generation plan",
				"release or settle earlier runs, reduce candidates, or approve a new plan",
			)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.budget_reservations
				(id, generation_run_id, amount_micros, currency, pricing_rule_version,
				 estimate_payload, status, confirmed_by, confirmed_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'RESERVED', $7, now())
			ON CONFLICT (id) DO NOTHING`,
			reservationID, runID, estimatedMicros, approvedCurrency,
			pricingVersion, map[string]any{
				"generationPlanId":  generationPlanID,
				"budgetApprovalId":  persistedBudgetApprovalID,
				"estimatedMicros":   estimatedMicros,
				"planMaximumMicros": approvedMicros,
				"promptSnapshotId":  promptID.String(),
				"runSpecDigest":     runDigest,
				"modelSnapshot":     persistedRoute,
			},
			input.BudgetApprovalID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf("reserve workflow budget: %w", err)
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
			return orchestration.PreparedProviderJob{}, fmt.Errorf("insert provider job: %w", err)
		}
		if err := verifyPreparedJob(); err != nil {
			return orchestration.PreparedProviderJob{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.cost_ledger
				(id, provider_job_id, budget_reservation_id, entry_type,
				 amount_micros, currency, pricing_rule_version, verified)
			VALUES ($1, $2, $3, 'RESERVATION', $4, $5, $6, true)
			ON CONFLICT (id) DO NOTHING`,
			uuid.NewSHA1(jobID, []byte("reservation-cost")),
			jobID, reservationID, estimatedMicros, approvedCurrency, pricingVersion,
		); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf(
				"insert Provider reservation ledger entry: %w", err,
			)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_runs
			SET state = 'QUEUED'
			WHERE id = $1 AND state IN ('VALIDATED', 'UNKNOWN', 'RECONCILING')`,
			runID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf("queue generation run: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_attempts
			SET state = 'QUEUED'
			WHERE id = $1 AND state IN ('VALIDATED', 'UNKNOWN', 'RECONCILING')`,
			attemptID,
		); err != nil {
			return orchestration.PreparedProviderJob{}, fmt.Errorf("queue generation attempt: %w", err)
		}
		return prepared, nil
	})
	return prepared, err
}

// CompleteProviderJob atomically commits provider provenance, CAS metadata,
// cost, and terminal run state.
// CompletePreparedProviderJob reloads the exact prompt-bearing dispatch from
// PostgreSQL so a separate completion process never needs prompt text or asset
// transport locations in its invocation or local ledger.
func (p *Postgres) CompletePreparedProviderJob(
	ctx context.Context,
	step orchestration.WorkflowStep,
	runIDRaw string,
	result orchestration.ProviderResult,
) error {
	runID, err := uuid.Parse(runIDRaw)
	if err != nil {
		return errors.New("runId must be a UUID")
	}
	var requestSnapshot []byte
	if err := p.pool.QueryRow(ctx, `
		SELECT pj.request_snapshot
		FROM video_pipeline.provider_jobs pj
		JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
		WHERE ga.generation_run_id = $1
		ORDER BY ga.sequence, pj.created_at
		LIMIT 1`,
		runID,
	).Scan(&requestSnapshot); err != nil {
		return fmt.Errorf("read prepared Provider completion input: %w", err)
	}
	var prepared immutableProviderRequest
	if err := json.Unmarshal(requestSnapshot, &prepared); err != nil {
		return fmt.Errorf("decode prepared Provider completion input: %w", err)
	}
	if prepared.Input.Run.RunID != runIDRaw || !prepared.Input.PersistProductTruth {
		return controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"prepared Provider completion input is not bound to the exact product-truth run",
		)
	}
	return p.CompleteProviderJob(ctx, step, prepared.Input, result)
}

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
	type completionOutcome struct {
		budgetExceeded bool
	}
	outcome, err := withSerializable(ctx, p.pool, func(
		tx pgx.Tx,
	) (completionOutcome, error) {
		var jobID, attemptID, reservationID uuid.UUID
		var runState, attemptState, jobState string
		var reservedMicros int64
		var reservedCurrency, reservedPricing, reservationStatus string
		var requestHash string
		var requestSnapshot []byte
		if err := tx.QueryRow(ctx, `
			SELECT pj.id, pj.generation_attempt_id, pj.budget_reservation_id,
			       gr.state, ga.state, pj.state,
			       br.amount_micros, br.currency, br.pricing_rule_version,
			       br.status, pj.request_hash, pj.request_snapshot
			FROM video_pipeline.provider_jobs pj
			JOIN video_pipeline.generation_attempts ga ON ga.id = pj.generation_attempt_id
			JOIN video_pipeline.generation_runs gr ON gr.id = ga.generation_run_id
			JOIN video_pipeline.budget_reservations br ON br.id = pj.budget_reservation_id
			WHERE gr.id = $1
			FOR UPDATE OF pj, ga, gr, br`,
			runID,
		).Scan(
			&jobID, &attemptID, &reservationID,
			&runState, &attemptState, &jobState,
			&reservedMicros, &reservedCurrency, &reservedPricing,
			&reservationStatus, &requestHash, &requestSnapshot,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("lock provider completion: %w", err)
		}
		var preparedSnapshot immutableProviderRequest
		if err := json.Unmarshal(requestSnapshot, &preparedSnapshot); err != nil {
			return completionOutcome{}, fmt.Errorf(
				"decode immutable Provider request snapshot: %w", err,
			)
		}
		recomputedRequestHash, err := digestValue(preparedSnapshot)
		if err != nil {
			return completionOutcome{}, err
		}
		if !reflect.DeepEqual(preparedSnapshot.Input, input) {
			return completionOutcome{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider completion input differs from the prepared immutable request",
			)
		}
		if recomputedRequestHash != requestHash ||
			preparedSnapshot.Prepared.Budget.EstimatedCostMicros != reservedMicros ||
			preparedSnapshot.Prepared.Budget.MaxCostMicros != reservedMicros ||
			preparedSnapshot.Prepared.BudgetReservation.ReservationID != reservationID.String() ||
			preparedSnapshot.Prepared.BudgetReservation.AmountMicros != reservedMicros ||
			preparedSnapshot.Prepared.BudgetReservation.Currency != reservedCurrency ||
			preparedSnapshot.Prepared.BudgetReservation.PricingVersion != reservedPricing ||
			preparedSnapshot.Prepared.BudgetReservation.ValidateFor(
				providercontract.BudgetBindingInput{
					RunID: input.Run.RunID, InputHash: input.Run.RunSpecDigest,
					Model: input.Route, Budget: preparedSnapshot.Prepared.Budget,
				},
			) != nil {
			return completionOutcome{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider completion reservation differs from the prepared immutable request",
			)
		}
		if result.Model != preparedSnapshot.Input.Route {
			return completionOutcome{}, controlplane.NewConflictError(
				controlplane.CodeRevisionConflict,
				"provider completion model differs from the prepared immutable route",
			)
		}
		if runState == "CANCELLED" || runState == "FAILED" ||
			jobState == "CANCELLED" || jobState == "FAILED" ||
			attemptState == "CANCELLED" || attemptState == "FAILED" {
			return completionOutcome{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"a late Provider success cannot reverse a terminal generation outcome",
			)
		}
		if reservationStatus != "RESERVED" && reservationStatus != "SETTLED" {
			return completionOutcome{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"provider completion has no active or settled budget reservation",
			)
		}
		hasActual := result.Cost.ActualMicros != nil
		var actualMicros int64
		if hasActual {
			actualMicros = *result.Cost.ActualMicros
		}
		actualTrustedForAllocation := hasActual &&
			result.Cost.Verified &&
			actualMicros >= 0 &&
			result.Cost.Currency == reservedCurrency &&
			result.Cost.PricingVersion == reservedPricing
		budgetExceeded := result.Cost.ActualMicros == nil ||
			!result.Cost.Verified ||
			result.Cost.EstimatedMicros < 0 ||
			actualMicros < 0 ||
			result.Cost.EstimatedMicros > reservedMicros ||
			actualMicros > reservedMicros ||
			result.Cost.Currency != reservedCurrency ||
			result.Cost.PricingVersion != reservedPricing
		ledgerID := uuid.NewSHA1(jobID, []byte("actual-cost"))
		if hasActual {
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, units, unit_name,
					 pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'ACTUAL', $4, $5, $6, $7, $8, $9)
				ON CONFLICT (id) DO NOTHING`,
				ledgerID, jobID, reservationID, actualMicros, result.Cost.Currency,
				result.Usage.InputUnits+result.Usage.OutputUnits, result.Usage.Unit,
				result.Cost.PricingVersion, actualTrustedForAllocation,
			); err != nil {
				return completionOutcome{}, fmt.Errorf("insert actual provider cost: %w", err)
			}
			var storedActual int64
			var storedCurrency, storedPricing string
			var storedVerified bool
			if err := tx.QueryRow(ctx, `
				SELECT amount_micros, currency, pricing_rule_version, verified
				FROM video_pipeline.cost_ledger
				WHERE id = $1
				FOR SHARE`,
				ledgerID,
			).Scan(
				&storedActual, &storedCurrency, &storedPricing, &storedVerified,
			); err != nil {
				return completionOutcome{}, fmt.Errorf(
					"read actual provider cost: %w", err,
				)
			}
			if storedActual != actualMicros ||
				storedCurrency != result.Cost.Currency ||
				storedPricing != result.Cost.PricingVersion ||
				storedVerified != actualTrustedForAllocation {
				return completionOutcome{}, controlplane.NewConflictError(
					controlplane.CodeRevisionConflict,
					"Provider completion cost differs from its immutable ledger entry",
				)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.budget_reservations
			SET status = 'SETTLED'
			WHERE id = $1 AND status = 'RESERVED'`,
			reservationID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("settle Provider budget reservation: %w", err)
		}
		if actualTrustedForAllocation && actualMicros < reservedMicros {
			releaseMicros := reservedMicros - actualMicros
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.cost_ledger
					(id, provider_job_id, budget_reservation_id, entry_type,
					 amount_micros, currency, pricing_rule_version, verified)
				VALUES ($1, $2, $3, 'RELEASE', $4, $5, $6, true)
				ON CONFLICT (id) DO NOTHING`,
				uuid.NewSHA1(jobID, []byte("unused-reservation-release")),
				jobID, reservationID, releaseMicros, reservedCurrency, reservedPricing,
			); err != nil {
				return completionOutcome{}, fmt.Errorf(
					"release unused Provider reservation: %w", err,
				)
			}
		}
		if budgetExceeded {
			errorSnapshot, encodeErr := json.Marshal(map[string]any{
				"code":                   "BUDGET_EXCEEDED",
				"reservedMicros":         reservedMicros,
				"reservedCurrency":       reservedCurrency,
				"reservedPricingVersion": reservedPricing,
				"providerCost":           result.Cost,
				"actualTrustedForBudget": actualTrustedForAllocation,
			})
			if encodeErr != nil {
				return completionOutcome{}, fmt.Errorf(
					"encode budget failure evidence: %w", encodeErr,
				)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.provider_jobs
				SET upstream_task_id = $2, upstream_request_id = $3,
				    state = 'FAILED', updated_at = now(), terminal_at = now(),
				    error_code = 'BUDGET_EXCEEDED', error_snapshot = $4
				WHERE id = $1
				  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
				jobID, result.UpstreamTaskID, result.RequestID, errorSnapshot,
			); err != nil {
				return completionOutcome{}, fmt.Errorf(
					"fail over-budget Provider job: %w", err,
				)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_attempts
				SET state = 'FAILED', failure_code = 'BUDGET_EXCEEDED',
				    heartbeat_at = now(), finished_at = now()
				WHERE id = $1
				  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
				attemptID,
			); err != nil {
				return completionOutcome{}, fmt.Errorf(
					"fail over-budget generation attempt: %w", err,
				)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE video_pipeline.generation_runs
				SET state = 'FAILED', failure_class = 'BUDGET',
				    failure_code = 'BUDGET_EXCEEDED', finished_at = now()
				WHERE id = $1
				  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
				runID,
			); err != nil {
				return completionOutcome{}, fmt.Errorf(
					"fail over-budget generation run: %w", err,
				)
			}
			if err := insertAuditAndOutbox(
				ctx, tx,
				uuid.NewSHA1(runID, []byte("provider-budget-failed-audit")),
				uuid.NewSHA1(runID, []byte("provider-budget-failed-outbox")),
				controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
				"provider_job.completed", "GENERATION_RUN", runID,
				nil, nil, "BUDGET_EXCEEDED", step.TraceID,
				map[string]any{
					"providerJobId":  jobID.String(),
					"reservedMicros": reservedMicros,
					"cost":           result.Cost,
					"state":          "FAILED",
				},
				p.now().UTC(),
			); err != nil {
				return completionOutcome{}, err
			}
			return completionOutcome{budgetExceeded: true}, nil
		}
		artifactID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("artifact:"+result.ArtifactDigest))
		mediaType := result.MediaType
		if mediaType == "" {
			mediaType = "video/mp4"
		}
		mediaSpec := map[string]any{
			"width": result.Width, "height": result.Height,
			"durationMillis": result.DurationMillis,
			"modelSnapshot":  result.Model, "usage": result.Usage, "cost": result.Cost,
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.artifacts
				(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE')
			ON CONFLICT (content_hash) DO NOTHING`,
			artifactID, result.ArtifactDigest, result.ArtifactURI, mediaType, result.ArtifactSize,
			mediaSpec,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("insert provider artifact: %w", err)
		}
		if err := tx.QueryRow(ctx,
			`SELECT id
			 FROM video_pipeline.artifacts
			 WHERE content_hash = $1
			   AND artifact_uri = $2
			   AND media_type = $3
			   AND size_bytes = $4
			   AND media_spec = $5
			   AND status = 'ACTIVE'
			 FOR SHARE`,
			result.ArtifactDigest, result.ArtifactURI, mediaType,
			result.ArtifactSize, mediaSpec,
		).Scan(&artifactID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return completionOutcome{}, controlplane.NewConflictError(
					controlplane.CodeConflict,
					"provider artifact hash is bound to incompatible CAS metadata",
				)
			}
			return completionOutcome{}, fmt.Errorf("resolve ACTIVE provider artifact: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.run_artifacts
				(generation_run_id, artifact_id, role)
			VALUES ($1, $2, 'OUTPUT')
			ON CONFLICT DO NOTHING`,
			runID, artifactID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("link provider artifact: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.provider_jobs
			SET upstream_task_id = $2, upstream_request_id = $3, state = 'SUCCEEDED',
			    progress = 100, updated_at = now(), terminal_at = now(), error_code = NULL,
			    error_snapshot = NULL
			WHERE id = $1
			  AND state NOT IN ('FAILED', 'CANCELLED')`,
			jobID, result.UpstreamTaskID, result.RequestID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("complete provider job: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_attempts
			SET state = 'SUCCEEDED', heartbeat_at = now(),
			    started_at = COALESCE(started_at, now()), finished_at = now()
			WHERE id = $1
			  AND state NOT IN ('FAILED', 'CANCELLED')`,
			attemptID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("complete generation attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.generation_runs
			SET state = CASE WHEN state = 'PAUSED' THEN 'PAUSED' ELSE 'SUCCEEDED' END,
			    failure_class = CASE WHEN state = 'PAUSED' THEN failure_class ELSE NULL END,
			    failure_code = CASE WHEN state = 'PAUSED' THEN failure_code ELSE NULL END,
			    started_at = COALESCE(started_at, now()), finished_at = now()
			WHERE id = $1
			  AND state NOT IN ('FAILED', 'CANCELLED')`,
			runID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("complete generation run: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE video_pipeline.shot_spec_revisions ssr
			SET lifecycle_state = 'QC_PENDING'
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1
			  AND gr.state = 'SUCCEEDED'
			  AND ssr.id = gr.shot_spec_revision_id`,
			runID,
		); err != nil {
			return completionOutcome{}, fmt.Errorf("advance shot to QC: %w", err)
		}
		if runState != "SUCCEEDED" {
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
				return completionOutcome{}, err
			}
		}
		return completionOutcome{}, nil
	})
	if err != nil {
		return err
	}
	if outcome.budgetExceeded {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"Provider cost exceeded or drifted from the immutable reservation",
			"create a new priced plan and budget approval before retrying",
		)
	}
	return nil
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
		var runState, runDigest string
		var artifactLinked bool
		if err := tx.QueryRow(ctx, `
			SELECT gr.state, gr.run_spec_digest,
			       EXISTS (
			         SELECT 1
			         FROM video_pipeline.run_artifacts ra
			         JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
			         WHERE ra.generation_run_id = gr.id
			           AND ra.role = 'OUTPUT'
			           AND a.content_hash = $2
			           AND a.status = 'ACTIVE'
			       )
			FROM video_pipeline.generation_runs gr
			WHERE gr.id = $1
			FOR UPDATE OF gr`,
			runID, input.Provider.ArtifactDigest,
		).Scan(&runState, &runDigest, &artifactLinked); err != nil {
			return struct{}{}, fmt.Errorf("lock QC generation run: %w", err)
		}
		if runState != "SUCCEEDED" ||
			runDigest != input.Run.RunSpecDigest ||
			!artifactLinked {
			return struct{}{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"QC evidence must bind the succeeded run and its exact ACTIVE Provider artifact",
			)
		}
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
			WHERE gr.id = $1
			  AND gr.state = 'SUCCEEDED'
			  AND ssr.id = gr.shot_spec_revision_id`,
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

// PrepareEpisodePostProduction resolves the exact successful shot outputs,
// four-level context snapshots, prompt snapshots, approved gates, dialogue,
// and optional licensed background audio into one immutable work request.
func (p *Postgres) PrepareEpisodePostProduction(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.FinalizeEpisodeInput,
) (postproduction.Request, error) {
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return postproduction.Request{}, errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return postproduction.Request{}, errors.New("post-production run IDs must be UUIDs")
	}
	var episodeHash string
	if err := p.pool.QueryRow(ctx, `
		SELECT content_hash
		FROM video_pipeline.episode_revisions
		WHERE id = $1 AND status = 'G2_APPROVED'`,
		episodeRevisionID,
	).Scan(&episodeHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return postproduction.Request{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production episode is not G2_APPROVED",
				"approve the exact episode revision at G2",
			)
		}
		return postproduction.Request{}, fmt.Errorf("read post-production episode: %w", err)
	}

	clips := make([]postproduction.Clip, 0, len(runIDs))
	cues := make([]postproduction.Cue, 0)
	sourceRevisionIDs := make([]string, 0, len(runIDs))
	voiceAssetIDs := make(map[uuid.UUID]struct{})
	var timelineOffset int64
	for index, runID := range runIDs {
		var (
			clip                           postproduction.Clip
			narrative                      []byte
			assetVersionRefs               []uuid.UUID
			artifactDigest                 string
			artifactURI                    string
			mediaType                      string
			licenseReference               string
			sizeBytes                      int64
			width, height, framesPerSecond int
		)
		if err := p.pool.QueryRow(ctx, `
			SELECT gr.id, ssr.id, ssr.content_hash, ssr.duration_ms, ssr.narrative,
			       ssr.asset_version_refs,
			       ps.id, ps.content_hash, ecs.id, ecs.content_hash,
			       a.content_hash, a.artifact_uri, a.media_type, a.size_bytes,
			       COALESCE((a.media_spec->>'width')::integer, ssr.width),
			       COALESCE((a.media_spec->>'height')::integer, ssr.height),
			       ssr.fps,
			       COALESCE((
			         SELECT string_agg(ls.license_id || ':' || ls.license_hash, ';' ORDER BY ls.id)
			         FROM unnest(ssr.asset_version_refs) requested(id)
			         JOIN video_pipeline.asset_versions av ON av.id = requested.id
			         JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
			         WHERE ls.policy_status = 'ALLOWED'
			           AND (ls.expires_at IS NULL OR ls.expires_at > now())
			           AND (av.consent_asset_id IS NULL OR EXISTS (
			             SELECT 1
			             FROM video_pipeline.consent_assets ca
			             WHERE ca.id = av.consent_asset_id
			               AND ca.status = 'ACTIVE'
			               AND (ca.expires_at IS NULL OR ca.expires_at > now())
			           ))
			       ), 'provider-output:no-input-assets')
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			JOIN video_pipeline.episode_revisions er ON er.id = $2 AND er.episode_id = ep.id
			JOIN video_pipeline.prompt_snapshots ps ON ps.id = gr.prompt_snapshot_id
			JOIN video_pipeline.effective_context_snapshots ecs ON ecs.id = ps.effective_context_snapshot_id
			JOIN video_pipeline.run_artifacts ra
			  ON ra.generation_run_id = gr.id AND ra.role = 'OUTPUT'
			JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
			WHERE gr.id = $1
			  AND gr.state = 'SUCCEEDED'
			  AND EXISTS (
			    SELECT 1 FROM video_pipeline.qc_reports qr
			    WHERE qr.generation_run_id = gr.id AND qr.state = 'PASSED'
			  )
			  AND NOT EXISTS (
			    SELECT 1
			    FROM unnest(ssr.asset_version_refs) requested(id)
			    LEFT JOIN video_pipeline.asset_versions av ON av.id = requested.id
			    LEFT JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
			    WHERE av.id IS NULL
			       OR av.status <> 'APPROVED'
			       OR ls.id IS NULL
			       OR ls.policy_status <> 'ALLOWED'
			       OR (ls.expires_at IS NOT NULL AND ls.expires_at <= now())
			       OR (av.consent_asset_id IS NOT NULL AND NOT EXISTS (
			         SELECT 1
			         FROM video_pipeline.consent_assets ca
			         WHERE ca.id = av.consent_asset_id
			           AND ca.status = 'ACTIVE'
			           AND (ca.expires_at IS NULL OR ca.expires_at > now())
			       ))
			  )
			  AND a.status = 'ACTIVE'
			  AND COALESCE(a.media_spec->>'kind', 'shot_video') = 'shot_video'
			ORDER BY a.created_at
			LIMIT 1`,
			runID, episodeRevisionID,
		).Scan(
			&clip.RunID, &clip.ShotSpecRevisionID, &clip.ShotSpecHash,
			&clip.DurationMillis, &narrative, &assetVersionRefs,
			&clip.PromptSnapshotID, &clip.PromptSnapshotHash,
			&clip.ContextSnapshotID, &clip.ContextSnapshotHash,
			&artifactDigest, &artifactURI, &mediaType, &sizeBytes,
			&width, &height, &framesPerSecond, &licenseReference,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return postproduction.Request{}, controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"post-production run is outside the episode or lacks successful QC output",
					"finish and approve every exact shot run before finalization",
				)
			}
			return postproduction.Request{}, fmt.Errorf("resolve post-production run %s: %w", runID, err)
		}
		if clip.RunID != input.RunIDs[index] {
			return postproduction.Request{}, errors.New("post-production run order changed during resolution")
		}
		clip.Artifact = postproduction.Artifact{
			Kind: "shot_video", Digest: artifactDigest, URI: artifactURI,
			MediaType: mediaType, SizeBytes: sizeBytes, DurationMillis: clip.DurationMillis,
			Width: width, Height: height, FPS: framesPerSecond,
		}
		clip.LicenseReference = licenseReference
		shotCues, err := dialogueCues(
			narrative, clip.ShotSpecRevisionID, timelineOffset, clip.DurationMillis,
		)
		if err != nil {
			return postproduction.Request{}, err
		}
		if err := collectVoiceAssetBindings(
			shotCues,
			assetVersionRefs,
			input.Config.Evidence == postproduction.EvidenceLive,
			voiceAssetIDs,
		); err != nil {
			return postproduction.Request{}, fmt.Errorf(
				"shot %s voice authorization: %w", clip.ShotSpecRevisionID, err,
			)
		}
		cues = append(cues, shotCues...)
		clips = append(clips, clip)
		sourceRevisionIDs = append(sourceRevisionIDs, clip.ShotSpecRevisionID)
		timelineOffset += clip.DurationMillis
	}
	if err := p.validateVoiceAssets(ctx, episodeRevisionID, voiceAssetIDs); err != nil {
		return postproduction.Request{}, err
	}
	cueHash, err := digestValue(map[string]any{
		"episodeRevisionId": input.EpisodeRevisionID,
		"sourceRevisions":   sourceRevisionIDs,
		"cues":              cues,
	})
	if err != nil {
		return postproduction.Request{}, err
	}
	subtitleID := uuid.NewSHA1(episodeRevisionID, []byte("subtitle:"+cueHash))
	subtitle, err := postproduction.NewSubtitleRevision(
		subtitleID.String(), "", 1, input.Config.SubtitleLanguage, sourceRevisionIDs, cues,
	)
	if err != nil {
		return postproduction.Request{}, err
	}
	gates, err := p.postProductionGateBindings(ctx, episodeRevisionID)
	if err != nil {
		return postproduction.Request{}, err
	}
	var background *postproduction.Artifact
	if input.Config.BackgroundAudioAssetVersionID != "" {
		resolved, err := p.resolveBackgroundAudio(
			ctx, episodeRevisionID, input.Config.BackgroundAudioAssetVersionID,
		)
		if err != nil {
			return postproduction.Request{}, err
		}
		background = &resolved
	}
	request := postproduction.Request{
		SchemaVersion:       postproduction.SchemaVersion,
		Evidence:            input.Config.Evidence,
		EpisodeRevisionID:   input.EpisodeRevisionID,
		EpisodeRevisionHash: episodeHash,
		RunIDs:              append([]string(nil), input.RunIDs...),
		Clips:               clips,
		Subtitle:            subtitle,
		BackgroundAudio:     background,
		Speech: postproduction.SpeechConfig{
			Route:               input.Config.SpeechRoute,
			ProviderProfileID:   input.Config.SpeechProviderProfileID,
			BudgetApprovalID:    input.Config.SpeechBudgetApprovalID,
			BudgetMaximumMicros: input.Config.SpeechBudgetMaximumMicros,
			BudgetCurrency:      input.Config.SpeechBudgetCurrency,
		},
		Output: postproduction.OutputPolicy{
			Width: 1280, Height: 720, FPS: 24, Format: "mp4",
			BurnSubtitles: input.Config.BurnSubtitles, AudioSampleRate: 48_000,
			AudioChannels: 2, EnforcePoCDuration: input.Config.EnforcePoCDuration,
		},
		Gates:   gates,
		TraceID: step.TraceID,
	}
	if err := request.Validate(); err != nil {
		return postproduction.Request{}, fmt.Errorf("prepared post-production request: %w", err)
	}
	return request, nil
}

// AuthorizeEpisodePostProduction is the paid-submit authorization boundary.
// It locks and rechecks every exact contributing asset, license, and consent
// under the immutable plan policy in one SERIALIZABLE transaction.
func (p *Postgres) AuthorizeEpisodePostProduction(
	ctx context.Context,
	_ orchestration.WorkflowStep,
	input orchestration.FinalizeEpisodeInput,
) error {
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return errors.New("post-production run IDs must be UUIDs")
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		if err := p.requireCurrentEpisodeAssetRights(
			ctx,
			tx,
			episodeRevisionID,
			runIDs,
			input.GenerationPlanID,
			input.Config.BackgroundAudioAssetVersionID,
		); err != nil {
			return struct{}{}, err
		}
		if err := p.requireCurrentPostProductionBudget(
			ctx, tx, episodeRevisionID, input.GenerationPlanID, input.Config,
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// CommitEpisodePostProduction records only CAS identities and provider-safe
// evidence. It rechecks the episode/run gate inside one SERIALIZABLE
// transaction and links final video, SRT, dialogue, Manifest, and Service BOM
// to every contributing run without changing their immutable revisions.
func (p *Postgres) CommitEpisodePostProduction(
	ctx context.Context,
	step orchestration.WorkflowStep,
	input orchestration.FinalizeEpisodeInput,
	result postproduction.Result,
) error {
	if err := result.Validate(); err != nil {
		return err
	}
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return errors.New("post-production run IDs must be UUIDs")
	}
	if result.EpisodeRevisionID != input.EpisodeRevisionID ||
		result.Evidence != input.Config.Evidence {
		return errors.New("post-production result does not match the frozen workflow input")
	}
	speechCost, err := summarizeSpeechCost(result.SpeechAttempts)
	if err != nil {
		return err
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		if err := p.requireCurrentEpisodeAssetRights(
			ctx,
			tx,
			episodeRevisionID,
			runIDs,
			input.GenerationPlanID,
			input.Config.BackgroundAudioAssetVersionID,
		); err != nil {
			return struct{}{}, err
		}
		if err := p.requireCurrentPostProductionBudget(
			ctx, tx, episodeRevisionID, input.GenerationPlanID, input.Config,
		); err != nil {
			return struct{}{}, err
		}
		var eligibleRuns int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
			JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
			JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
			JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
			JOIN video_pipeline.episode_revisions er ON er.id = $2 AND er.episode_id = ep.id
			WHERE gr.id = ANY($1::uuid[])
			  AND er.status = 'G2_APPROVED'
			  AND gr.state = 'SUCCEEDED'
			  AND EXISTS (
			    SELECT 1 FROM video_pipeline.qc_reports qr
			    WHERE qr.generation_run_id = gr.id AND qr.state = 'PASSED'
			  )`,
			runIDs, episodeRevisionID,
		).Scan(&eligibleRuns); err != nil {
			return struct{}{}, fmt.Errorf("validate post-production inputs: %w", err)
		}
		if eligibleRuns != len(runIDs) {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production inputs changed before commit",
				"restart from the current approved episode and successful runs",
			)
		}
		type artifactRole struct {
			artifact postproduction.Artifact
			role     string
		}
		outputs := []artifactRole{
			{artifact: result.FinalVideo, role: "OUTPUT"},
			{artifact: result.Dialogue, role: "AUDIO"},
			{artifact: result.Subtitle, role: "SUBTITLE"},
			{artifact: result.Manifest, role: "MANIFEST"},
			{artifact: result.ServiceBOM, role: "MANIFEST"},
		}
		for _, output := range outputs {
			artifactID := uuid.NewSHA1(
				uuid.NameSpaceOID, []byte("artifact:"+output.artifact.Digest),
			)
			mediaSpec := map[string]any{
				"kind": output.artifact.Kind, "durationMillis": output.artifact.DurationMillis,
				"width": output.artifact.Width, "height": output.artifact.Height,
				"fps": output.artifact.FPS, "evidence": result.Evidence,
				"postProductionManifestHash": result.ManifestHash,
				"speechCost":                 speechCost,
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO video_pipeline.artifacts
					(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
				VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE')
				ON CONFLICT (content_hash) DO NOTHING`,
				artifactID, output.artifact.Digest, output.artifact.URI,
				output.artifact.MediaType, output.artifact.SizeBytes, mediaSpec,
			); err != nil {
				return struct{}{}, fmt.Errorf("insert %s artifact: %w", output.artifact.Kind, err)
			}
			var storedURI, storedMediaType, storedKind string
			var storedSize int64
			if err := tx.QueryRow(ctx, `
				SELECT id, artifact_uri, media_type, size_bytes,
				       COALESCE(media_spec->>'kind', '')
				FROM video_pipeline.artifacts
				WHERE content_hash = $1
				  AND status = 'ACTIVE'
				FOR SHARE`,
				output.artifact.Digest,
			).Scan(
				&artifactID, &storedURI, &storedMediaType, &storedSize, &storedKind,
			); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return struct{}{}, controlplane.NewConflictError(
						controlplane.CodeConflict,
						fmt.Sprintf(
							"%s artifact hash is bound to a non-ACTIVE CAS record",
							output.artifact.Kind,
						),
					)
				}
				return struct{}{}, fmt.Errorf(
					"resolve ACTIVE %s artifact: %w", output.artifact.Kind, err,
				)
			}
			if storedURI != output.artifact.URI ||
				storedMediaType != output.artifact.MediaType ||
				storedSize != output.artifact.SizeBytes ||
				storedKind != output.artifact.Kind {
				return struct{}{}, fmt.Errorf(
					"artifact %s content hash is already bound to incompatible metadata",
					output.artifact.Kind,
				)
			}
			for _, runID := range runIDs {
				if _, err := tx.Exec(ctx, `
					INSERT INTO video_pipeline.run_artifacts
						(generation_run_id, artifact_id, role)
					VALUES ($1, $2, $3)
					ON CONFLICT DO NOTHING`,
					runID, artifactID, output.role,
				); err != nil {
					return struct{}{}, fmt.Errorf("link %s artifact: %w", output.artifact.Kind, err)
				}
			}
		}
		auditID := uuid.NewSHA1(
			episodeRevisionID, []byte("postproduction:"+result.ManifestHash),
		)
		if err := insertAuditAndOutbox(
			ctx, tx,
			auditID,
			uuid.NewSHA1(auditID, []byte("outbox")),
			controlplane.Actor{ActorID: "temporal-worker", Role: "OPERATOR"},
			"episode.postproduction.completed", "EPISODE_REVISION", episodeRevisionID,
			nil, nil, "", step.TraceID,
			map[string]any{
				"runIds": input.RunIDs, "evidence": result.Evidence,
				"finalVideoHash": result.FinalVideo.Digest,
				"subtitleHash":   result.Subtitle.Digest,
				"dialogueHash":   result.Dialogue.Digest,
				"manifestHash":   result.ManifestHash,
				"serviceBomHash": result.ServiceBOMHash,
				"speechCost":     speechCost,
			},
			p.now().UTC(),
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
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
	// Reject invalid current rights before any generation manifest bytes are
	// written to CAS. CommitEpisodeManifest repeats this inside its write
	// transaction to close the build/commit race.
	if _, err := withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		if err := p.requireCurrentEpisodeAssetRights(
			ctx,
			tx,
			episodeRevisionID,
			runIDs,
			input.GenerationPlanID,
			input.BackgroundAudioAssetVersionID,
		); err != nil {
			return struct{}{}, err
		}
		if _, err := requireActivePostProductionArtifacts(
			ctx, tx, runIDs, input.PostProductionManifestHash,
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}); err != nil {
		return nil, err
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
			    SELECT jsonb_agg(
			      to_jsonb(a) || jsonb_build_object(
			        'role', ra.role,
			        'media_spec', a.media_spec || jsonb_build_object(
			          'kind', COALESCE(NULLIF(a.media_spec->>'kind', ''), 'shot_video')
			        )
			      )
			      ORDER BY ra.role, a.id
			    )
			    FROM video_pipeline.run_artifacts ra
			    JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
			    WHERE ra.generation_run_id = gr.id
			      AND a.status = 'ACTIVE'
			      AND COALESCE(a.media_spec->>'kind', '') <> 'generation-manifest'
			      AND (
			        NOT (a.media_spec ? 'postProductionManifestHash')
			        OR (
			          $3 <> ''
			          AND a.media_spec->>'postProductionManifestHash' = $3
			        )
			      )
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
			runID, episodeRevisionID, input.PostProductionManifestHash,
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
	var postProductionCost speechCostEvidence
	if input.PostProductionManifestHash != "" {
		var rawCost []byte
		if err := p.pool.QueryRow(ctx, `
			SELECT COALESCE(a.media_spec->'speechCost', '{}'::jsonb)
			FROM video_pipeline.artifacts a
			WHERE a.content_hash = $1
			  AND a.media_spec->>'kind' = 'postproduction_manifest'
			  AND a.status = 'ACTIVE'`,
			input.PostProductionManifestHash,
		).Scan(&rawCost); err != nil {
			return nil, fmt.Errorf("assemble post-production cost summary: %w", err)
		}
		if err := json.Unmarshal(rawCost, &postProductionCost); err != nil {
			return nil, fmt.Errorf("decode post-production cost summary: %w", err)
		}
	}
	combinedMicros := totalMicros
	combinedCurrency := currency
	currencyMismatch := false
	if postProductionCost.ActualMicros != nil {
		switch {
		case combinedCurrency == "":
			combinedMicros = *postProductionCost.ActualMicros
			combinedCurrency = postProductionCost.Currency
		case combinedCurrency == postProductionCost.Currency:
			combinedMicros += *postProductionCost.ActualMicros
		default:
			currencyMismatch = true
		}
	}
	rows, err := p.pool.Query(ctx, `
		SELECT DISTINCT a.artifact_uri
		FROM video_pipeline.run_artifacts ra
		JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
		WHERE ra.generation_run_id = ANY($1::uuid[])
		  AND ra.role = 'OUTPUT'
		  AND a.status = 'ACTIVE'
		  AND (
		    ($2 = '' AND COALESCE(a.media_spec->>'kind', 'shot_video') = 'shot_video')
		    OR
		    ($2 <> ''
		     AND a.media_spec->>'kind' = 'final_video'
		     AND a.media_spec->>'postProductionManifestHash' = $2)
		  )
		ORDER BY a.artifact_uri`,
		runIDs, input.PostProductionManifestHash,
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
	if input.PostProductionManifestHash != "" && len(outputs) != 1 {
		return nil, controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"G3 requires exactly one ACTIVE final video for the current post-production manifest",
			"commit the exact current post-production revision before opening G3",
		)
	}
	inputs := []string{"episode-revision:" + input.EpisodeRevisionID}
	for _, runID := range input.RunIDs {
		inputs = append(inputs, "generation-run:"+runID)
	}
	payload := map[string]any{
		"schemaVersion":              "v1",
		"scopeType":                  "EPISODE",
		"episodeRevisionId":          input.EpisodeRevisionID,
		"workflowId":                 step.WorkflowID,
		"postProductionManifestHash": input.PostProductionManifestHash,
		"providerExecutions":         executions,
		"inputs":                     inputs,
		"outputs":                    outputs,
		"costSummary": map[string]any{
			"actualMicros":      combinedMicros,
			"currency":          combinedCurrency,
			"currencyMismatch":  currencyMismatch,
			"postProductionTTS": postProductionCost,
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
	var binding generationManifestCommitBinding
	if err := json.Unmarshal(payload, &binding); err != nil {
		return fmt.Errorf("decode manifest artifact bindings: %w", err)
	}
	if binding.PostProductionManifestHash != input.PostProductionManifestHash {
		return errors.New("manifest payload does not bind the requested post-production manifest")
	}
	episodeRevisionID, err := uuid.Parse(input.EpisodeRevisionID)
	if err != nil {
		return errors.New("episodeRevisionId must be a UUID")
	}
	runIDs, err := parseUUIDs(input.RunIDs)
	if err != nil {
		return errors.New("manifest run IDs must be UUIDs")
	}
	payloadArtifactReferences, err := artifactReferencesFromPayload(
		binding, runIDs,
	)
	if err != nil {
		return err
	}
	payloadPostProductionReferences, err := postProductionReferencesFromPayload(
		payloadArtifactReferences, binding.PostProductionManifestHash, runIDs,
	)
	if err != nil {
		return err
	}
	_, err = withSerializable(ctx, p.pool, func(tx pgx.Tx) (struct{}, error) {
		if err := p.requireCurrentEpisodeAssetRights(
			ctx,
			tx,
			episodeRevisionID,
			runIDs,
			input.GenerationPlanID,
			input.BackgroundAudioAssetVersionID,
		); err != nil {
			return struct{}{}, err
		}
		if err := requireActiveManifestArtifacts(
			ctx, tx, payloadArtifactReferences,
		); err != nil {
			return struct{}{}, err
		}
		currentPostProduction, err := requireActivePostProductionArtifacts(
			ctx, tx, runIDs, input.PostProductionManifestHash,
		)
		if err != nil {
			return struct{}{}, err
		}
		if input.PostProductionManifestHash != "" &&
			(len(binding.Outputs) != 1 ||
				binding.Outputs[0] != currentPostProduction.FinalVideoURI) {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest outputs no longer match the ACTIVE current final video",
				"rebuild the generation manifest from current post-production truth",
			)
		}
		if !samePostProductionArtifactReferences(
			payloadPostProductionReferences,
			currentPostProduction.References,
		) {
			return struct{}{}, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest post-production references no longer match ACTIVE product truth",
				"rebuild the generation manifest from the complete current post-production artifact set",
			)
		}
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
			`SELECT id
			 FROM video_pipeline.artifacts
			 WHERE content_hash = $1 AND status = 'ACTIVE'
			 FOR SHARE`,
			artifact.Digest,
		).Scan(&artifactID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return struct{}{}, controlplane.NewConflictError(
					controlplane.CodeConflict,
					"generation manifest hash is bound to a non-ACTIVE CAS record",
				)
			}
			return struct{}{}, fmt.Errorf("resolve ACTIVE manifest artifact: %w", err)
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

type generationManifestCommitBinding struct {
	PostProductionManifestHash string `json:"postProductionManifestHash"`
	ProviderExecutions         []struct {
		RunID     string `json:"runId"`
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
	Outputs []string `json:"outputs"`
}

type manifestArtifactReference struct {
	RunID                      uuid.UUID
	ArtifactID                 uuid.UUID
	ContentHash                string
	ArtifactURI                string
	Kind                       string
	Role                       string
	PostProductionManifestHash string
}

type manifestArtifactReferenceKey struct {
	RunID      uuid.UUID
	ArtifactID uuid.UUID
	Role       string
}

type postProductionArtifactReference struct {
	RunID       uuid.UUID
	ArtifactID  uuid.UUID
	ContentHash string
	ArtifactURI string
	Kind        string
	Role        string
}

type activePostProductionArtifacts struct {
	FinalVideoURI string
	References    []postProductionArtifactReference
}

func artifactReferencesFromPayload(
	binding generationManifestCommitBinding,
	runIDs []uuid.UUID,
) ([]manifestArtifactReference, error) {
	expectedRuns := make(map[uuid.UUID]struct{}, len(runIDs))
	for _, runID := range runIDs {
		expectedRuns[runID] = struct{}{}
	}
	seenRuns := make(map[uuid.UUID]struct{}, len(runIDs))
	seenReferences := make(map[manifestArtifactReferenceKey]struct{})
	references := make([]manifestArtifactReference, 0, len(runIDs)*6)
	for _, execution := range binding.ProviderExecutions {
		runID, err := uuid.Parse(execution.RunID)
		if err != nil {
			return nil, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest contains an invalid provider execution run",
				"rebuild the manifest from the exact persisted runs",
			)
		}
		if _, ok := expectedRuns[runID]; !ok {
			return nil, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest contains a provider execution outside the exact run set",
				"rebuild the manifest from the exact persisted runs",
			)
		}
		if _, duplicate := seenRuns[runID]; duplicate {
			return nil, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest contains duplicate provider executions",
				"rebuild one provider execution projection per exact run",
			)
		}
		seenRuns[runID] = struct{}{}
		for _, artifact := range execution.Artifacts {
			artifactID, err := uuid.Parse(artifact.ID)
			if err != nil {
				return nil, controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"generation manifest contains an invalid artifact identity",
					"rebuild the manifest from current persisted artifact identities",
				)
			}
			kind := artifact.MediaSpec.Kind
			if kind == "" {
				// Provider artifacts created before kind was explicitly frozen in
				// the manifest use the repository's canonical default.
				kind = "shot_video"
			}
			reference := manifestArtifactReference{
				RunID:                      runID,
				ArtifactID:                 artifactID,
				ContentHash:                artifact.ContentHash,
				ArtifactURI:                artifact.ArtifactURI,
				Kind:                       kind,
				Role:                       artifact.Role,
				PostProductionManifestHash: artifact.MediaSpec.PostProductionManifestHash,
			}
			if reference.ContentHash == "" ||
				reference.ArtifactURI == "" ||
				reference.Kind == "" ||
				reference.Role == "" {
				return nil, controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"generation manifest contains an incomplete artifact reference",
					"rebuild the manifest from complete persisted artifact identities",
				)
			}
			key := manifestArtifactReferenceKey{
				RunID: runID, ArtifactID: artifactID, Role: reference.Role,
			}
			if _, duplicate := seenReferences[key]; duplicate {
				return nil, controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"generation manifest contains a duplicate artifact reference",
					"rebuild the manifest from one frozen reference per persisted run link",
				)
			}
			seenReferences[key] = struct{}{}
			references = append(references, reference)
		}
	}
	if len(seenRuns) != len(expectedRuns) {
		return nil, controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"generation manifest omits provider executions for an exact run",
			"rebuild the manifest from every exact persisted run",
		)
	}
	return references, nil
}

func postProductionReferencesFromPayload(
	artifacts []manifestArtifactReference,
	manifestHash string,
	runIDs []uuid.UUID,
) ([]postProductionArtifactReference, error) {
	references := make([]postProductionArtifactReference, 0, len(runIDs)*5)
	for _, artifact := range artifacts {
		if artifact.PostProductionManifestHash == "" {
			continue
		}
		if manifestHash == "" ||
			artifact.PostProductionManifestHash != manifestHash {
			return nil, controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest contains an artifact from a different post-production revision",
				"rebuild the manifest from the exact current post-production manifest hash",
			)
		}
		references = append(references, postProductionArtifactReference{
			RunID:       artifact.RunID,
			ArtifactID:  artifact.ArtifactID,
			ContentHash: artifact.ContentHash,
			ArtifactURI: artifact.ArtifactURI,
			Kind:        artifact.Kind,
			Role:        artifact.Role,
		})
	}
	if manifestHash == "" {
		return nil, nil
	}
	if _, err := validatePostProductionArtifactReferences(
		references, runIDs, manifestHash,
	); err != nil {
		return nil, err
	}
	return references, nil
}

func requireActiveManifestArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	references []manifestArtifactReference,
) error {
	if len(references) == 0 {
		return nil
	}
	runIDs := make([]uuid.UUID, 0, len(references))
	artifactIDs := make([]uuid.UUID, 0, len(references))
	roles := make([]string, 0, len(references))
	for _, reference := range references {
		runIDs = append(runIDs, reference.RunID)
		artifactIDs = append(artifactIDs, reference.ArtifactID)
		roles = append(roles, reference.Role)
	}
	rows, err := tx.Query(ctx, `
		WITH requested AS (
		  SELECT *
		  FROM unnest($1::uuid[], $2::uuid[], $3::text[])
		    WITH ORDINALITY AS requested(run_id, artifact_id, role, ordinal)
		)
		SELECT requested.ordinal,
		       ra.generation_run_id,
		       a.id,
		       a.content_hash,
		       a.artifact_uri,
		       COALESCE(NULLIF(a.media_spec->>'kind', ''), 'shot_video'),
		       ra.role,
		       a.status
		FROM requested
		JOIN video_pipeline.run_artifacts ra
		  ON ra.generation_run_id = requested.run_id
		 AND ra.artifact_id = requested.artifact_id
		 AND ra.role = requested.role
		JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
		ORDER BY requested.ordinal
		FOR SHARE OF ra, a`,
		runIDs, artifactIDs, roles,
	)
	if err != nil {
		return fmt.Errorf("verify frozen manifest artifacts: %w", err)
	}
	verified := 0
	for rows.Next() {
		var (
			ordinal int64
			actual  manifestArtifactReference
			status  string
		)
		if err := rows.Scan(
			&ordinal,
			&actual.RunID,
			&actual.ArtifactID,
			&actual.ContentHash,
			&actual.ArtifactURI,
			&actual.Kind,
			&actual.Role,
			&status,
		); err != nil {
			rows.Close()
			return fmt.Errorf("scan frozen manifest artifact: %w", err)
		}
		index := int(ordinal - 1)
		if index < 0 || index >= len(references) {
			rows.Close()
			return fmt.Errorf("verify frozen manifest artifact: invalid reference ordinal %d", ordinal)
		}
		expected := references[index]
		if status != "ACTIVE" ||
			actual.RunID != expected.RunID ||
			actual.ArtifactID != expected.ArtifactID ||
			actual.ContentHash != expected.ContentHash ||
			actual.ArtifactURI != expected.ArtifactURI ||
			actual.Kind != expected.Kind ||
			actual.Role != expected.Role {
			rows.Close()
			return controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"generation manifest artifact no longer matches its ACTIVE persisted run link",
				"rebuild the manifest from the complete current artifact set",
			)
		}
		verified++
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return fmt.Errorf("iterate frozen manifest artifacts: %w", iterationErr)
	}
	if verified != len(references) {
		return controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"generation manifest artifact run link no longer exists",
			"rebuild the manifest from the complete current artifact set",
		)
	}
	return nil
}

func requireActivePostProductionArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	runIDs []uuid.UUID,
	manifestHash string,
) (activePostProductionArtifacts, error) {
	if manifestHash == "" {
		return activePostProductionArtifacts{}, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT ra.generation_run_id, a.id, a.content_hash, a.artifact_uri,
		       a.media_spec->>'kind', ra.role
		FROM video_pipeline.run_artifacts ra
		JOIN video_pipeline.artifacts a ON a.id = ra.artifact_id
		WHERE ra.generation_run_id = ANY($1::uuid[])
		  AND a.media_spec->>'postProductionManifestHash' = $2
		  AND a.status = 'ACTIVE'
		ORDER BY ra.generation_run_id, a.media_spec->>'kind', a.id
		FOR SHARE OF ra, a`,
		runIDs, manifestHash,
	)
	if err != nil {
		return activePostProductionArtifacts{}, fmt.Errorf(
			"verify ACTIVE current post-production artifacts: %w", err,
		)
	}
	references := make([]postProductionArtifactReference, 0, len(runIDs)*5)
	for rows.Next() {
		var reference postProductionArtifactReference
		if err := rows.Scan(
			&reference.RunID,
			&reference.ArtifactID,
			&reference.ContentHash,
			&reference.ArtifactURI,
			&reference.Kind,
			&reference.Role,
		); err != nil {
			rows.Close()
			return activePostProductionArtifacts{}, fmt.Errorf(
				"scan ACTIVE current post-production artifact: %w", err,
			)
		}
		references = append(references, reference)
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return activePostProductionArtifacts{}, fmt.Errorf(
			"iterate ACTIVE current post-production artifacts: %w", iterationErr,
		)
	}
	finalVideoURI, err := validatePostProductionArtifactReferences(
		references, runIDs, manifestHash,
	)
	if err != nil {
		return activePostProductionArtifacts{}, err
	}
	return activePostProductionArtifacts{
		FinalVideoURI: finalVideoURI,
		References:    references,
	}, nil
}

func validatePostProductionArtifactReferences(
	references []postProductionArtifactReference,
	runIDs []uuid.UUID,
	manifestHash string,
) (string, error) {
	requiredRoles := map[string]string{
		"final_video":             "OUTPUT",
		"subtitle_srt":            "SUBTITLE",
		"dialogue_audio":          "AUDIO",
		"postproduction_manifest": "MANIFEST",
		"service_bom":             "MANIFEST",
	}
	expectedRuns := make(map[uuid.UUID]struct{}, len(runIDs))
	for _, runID := range runIDs {
		expectedRuns[runID] = struct{}{}
	}
	artifactByKind := make(map[string]postProductionArtifactReference)
	runsByKind := make(map[string]map[uuid.UUID]struct{})
	seenRunKind := make(map[string]struct{}, len(references))
	for _, reference := range references {
		if _, ok := expectedRuns[reference.RunID]; !ok ||
			reference.Kind == "" ||
			reference.ContentHash == "" ||
			reference.ArtifactURI == "" ||
			reference.Role == "" {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production artifact reference is incomplete or outside the exact run set",
				"rebuild the manifest from complete current post-production product truth",
			)
		}
		if expectedRole, required := requiredRoles[reference.Kind]; required && reference.Role != expectedRole {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production artifact role differs from the required product contract",
				"rebuild the exact final video, subtitle, dialogue, manifest, and Service BOM links",
			)
		}
		if reference.Kind == "postproduction_manifest" &&
			reference.ContentHash != manifestHash {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production manifest artifact differs from the current manifest hash",
				"rebuild the manifest from the exact current post-production revision",
			)
		}
		runKindKey := reference.RunID.String() + "\x00" + reference.Kind
		if _, duplicate := seenRunKind[runKindKey]; duplicate {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production artifact kind is duplicated for a run",
				"commit one immutable artifact of each post-production kind",
			)
		}
		seenRunKind[runKindKey] = struct{}{}
		if frozen, exists := artifactByKind[reference.Kind]; exists {
			if frozen.ArtifactID != reference.ArtifactID ||
				frozen.ContentHash != reference.ContentHash ||
				frozen.ArtifactURI != reference.ArtifactURI ||
				frozen.Role != reference.Role {
				return "", controlplane.NewPolicyError(
					controlplane.CodeGateRequired,
					"post-production kind resolves to different artifacts across runs",
					"link one deterministic post-production artifact set to every exact run",
				)
			}
		} else {
			artifactByKind[reference.Kind] = reference
		}
		if runsByKind[reference.Kind] == nil {
			runsByKind[reference.Kind] = make(map[uuid.UUID]struct{}, len(runIDs))
		}
		runsByKind[reference.Kind][reference.RunID] = struct{}{}
	}
	for kind := range artifactByKind {
		if len(runsByKind[kind]) != len(expectedRuns) {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"post-production artifact is not linked to every exact run",
				"restore the complete current post-production run links before opening G3",
			)
		}
	}
	for kind := range requiredRoles {
		if _, ok := artifactByKind[kind]; !ok {
			return "", controlplane.NewPolicyError(
				controlplane.CodeGateRequired,
				"current post-production artifact set is incomplete",
				"commit final video, subtitle, dialogue, manifest, and Service BOM before opening G3",
			)
		}
	}
	finalVideo := artifactByKind["final_video"]
	if finalVideo.ArtifactURI == "" {
		return "", controlplane.NewPolicyError(
			controlplane.CodeGateRequired,
			"current post-production manifest does not resolve to one ACTIVE final video",
			"commit the exact current post-production revision before opening G3",
		)
	}
	return finalVideo.ArtifactURI, nil
}

func samePostProductionArtifactReferences(
	left []postProductionArtifactReference,
	right []postProductionArtifactReference,
) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[postProductionArtifactReference]int, len(left))
	for _, reference := range left {
		counts[reference]++
	}
	for _, reference := range right {
		if counts[reference] == 0 {
			return false
		}
		counts[reference]--
	}
	return true
}

func (p *Postgres) requireCurrentEpisodeAssetRights(
	ctx context.Context,
	tx pgx.Tx,
	episodeRevisionID uuid.UUID,
	runIDs []uuid.UUID,
	generationPlanID string,
	backgroundAudioAssetVersionID string,
) error {
	plan, err := readPlan(ctx, tx, generationPlanID)
	if err != nil {
		return err
	}
	if plan.EpisodeRevisionID != episodeRevisionID.String() {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"post-production rights policy is not bound to the exact episode revision",
			"restart from the immutable generation plan for this episode revision",
		)
	}

	rows, err := tx.Query(ctx, `
		SELECT gr.id, ssr.id, ssr.asset_version_refs, ep.series_id
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id = gr.shot_spec_revision_id
		JOIN video_pipeline.shots sh ON sh.id = ssr.shot_id
		JOIN video_pipeline.scenes sc ON sc.id = sh.scene_id
		JOIN video_pipeline.episodes ep ON ep.id = sc.episode_id
		JOIN video_pipeline.episode_revisions er
		  ON er.id = $2 AND er.episode_id = ep.id
		WHERE gr.id = ANY($1::uuid[])
		ORDER BY gr.id
		FOR SHARE OF gr, ssr, er`,
		runIDs, episodeRevisionID,
	)
	if err != nil {
		return fmt.Errorf("lock post-production asset lineage: %w", err)
	}
	defer rows.Close()

	assetSet := make(map[uuid.UUID]struct{})
	seenRuns := make(map[uuid.UUID]struct{}, len(runIDs))
	var seriesID uuid.UUID
	for rows.Next() {
		var runID, shotRevisionID, currentSeriesID uuid.UUID
		var assetIDs []uuid.UUID
		if err := rows.Scan(&runID, &shotRevisionID, &assetIDs, &currentSeriesID); err != nil {
			return fmt.Errorf("scan post-production asset lineage: %w", err)
		}
		if seriesID == uuid.Nil {
			seriesID = currentSeriesID
		} else if seriesID != currentSeriesID {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"post-production runs span more than one series",
				"use exact runs from one approved episode",
			)
		}
		if plan.SeriesID != currentSeriesID.String() ||
			!containsString(plan.ShotSpecRevisionIDs, shotRevisionID.String()) {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"post-production run lineage is outside the immutable rights plan",
				"restart with the exact approved plan and shot revisions",
			)
		}
		seenRuns[runID] = struct{}{}
		for _, assetID := range assetIDs {
			assetSet[assetID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate post-production asset lineage: %w", err)
	}
	if len(seenRuns) != len(runIDs) {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"one or more post-production runs have no current episode asset lineage",
			"restart with exact successful runs from the approved episode",
		)
	}

	var backgroundID uuid.UUID
	if backgroundAudioAssetVersionID != "" {
		backgroundID, err = uuid.Parse(backgroundAudioAssetVersionID)
		if err != nil {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"background audio asset version identifier is invalid",
				"select an immutable licensed MUSIC/AUDIO asset version",
			)
		}
		assetSet[backgroundID] = struct{}{}
	}
	if len(assetSet) == 0 {
		return nil
	}
	assetIDs := make([]uuid.UUID, 0, len(assetSet))
	for assetID := range assetSet {
		assetIDs = append(assetIDs, assetID)
	}
	sort.Slice(assetIDs, func(i, j int) bool {
		return strings.Compare(assetIDs[i].String(), assetIDs[j].String()) < 0
	})

	if err := lockPostProductionRights(ctx, tx, assetIDs); err != nil {
		return err
	}
	var seriesAssetCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM video_pipeline.asset_versions av
		JOIN video_pipeline.assets source_asset ON source_asset.id = av.asset_id
		WHERE av.id = ANY($1::uuid[])
		  AND source_asset.series_id = $2`,
		assetIDs, seriesID,
	).Scan(&seriesAssetCount); err != nil {
		return fmt.Errorf("validate post-production asset series: %w", err)
	}
	if seriesAssetCount != len(assetIDs) {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"one or more post-production assets are missing or outside the approved series",
			"replace the asset with an approved revision from this series",
		)
	}
	if backgroundID != uuid.Nil {
		var compatible bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1
			  FROM video_pipeline.asset_versions av
			  JOIN video_pipeline.assets source_asset ON source_asset.id = av.asset_id
			  WHERE av.id = $1
			    AND source_asset.series_id = $2
			    AND source_asset.asset_type IN ('MUSIC', 'AUDIO')
			)`,
			backgroundID, seriesID,
		).Scan(&compatible); err != nil {
			return fmt.Errorf("validate background audio type: %w", err)
		}
		if !compatible {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"background audio is not a compatible MUSIC/AUDIO asset",
				"select an approved licensed MUSIC/AUDIO revision from this series",
			)
		}
	}
	return requireAssetLicenses(ctx, tx, assetIDs, p.now().UTC(), plan.ExecutionPolicy)
}

func (p *Postgres) requireCurrentPostProductionBudget(
	ctx context.Context,
	tx pgx.Tx,
	episodeRevisionID uuid.UUID,
	generationPlanID string,
	config orchestration.PostProductionConfig,
) error {
	plan, err := readPlan(ctx, tx, generationPlanID)
	if err != nil {
		return err
	}
	required := controlplane.BudgetLimit{
		AmountMicros: config.SpeechBudgetMaximumMicros,
		Currency:     config.SpeechBudgetCurrency,
	}
	if plan.EpisodeRevisionID != episodeRevisionID.String() ||
		!sameBudgetLimit(plan.SpeechBudgetLimit, required) {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"post-production speech budget is outside the immutable generation plan",
			"create a current plan for the exact episode, TTS amount, and currency",
		)
	}
	var seriesID, episodeID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT ep.series_id, ep.id
		FROM video_pipeline.episode_revisions er
		JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
		WHERE er.id = $1
		FOR SHARE OF er, ep`,
		episodeRevisionID,
	).Scan(&seriesID, &episodeID); err != nil {
		return fmt.Errorf("resolve post-production budget scope: %w", err)
	}
	return requireBudgetApproval(
		ctx, tx, config.SpeechBudgetApprovalID, seriesID, episodeID,
		generationPlanID, "SPEECH", required,
	)
}

func lockPostProductionRights(
	ctx context.Context,
	tx pgx.Tx,
	assetIDs []uuid.UUID,
) error {
	lockQueries := []struct {
		name  string
		query string
	}{
		{
			name: "asset versions",
			query: `
				SELECT av.id
				FROM video_pipeline.asset_versions av
				WHERE av.id = ANY($1::uuid[])
				ORDER BY av.id
				FOR SHARE OF av`,
		},
		{
			name: "license snapshots",
			query: `
				SELECT ls.id
				FROM video_pipeline.asset_versions av
				JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
				WHERE av.id = ANY($1::uuid[])
				ORDER BY ls.id
				FOR SHARE OF ls`,
		},
		{
			name: "consent assets",
			query: `
				SELECT ca.id
				FROM video_pipeline.asset_versions av
				JOIN video_pipeline.consent_assets ca ON ca.id = av.consent_asset_id
				WHERE av.id = ANY($1::uuid[])
				ORDER BY ca.id
				FOR SHARE OF ca`,
		},
	}
	for _, lock := range lockQueries {
		rows, err := tx.Query(ctx, lock.query, assetIDs)
		if err != nil {
			return fmt.Errorf("lock post-production %s: %w", lock.name, err)
		}
		for rows.Next() {
			var ignored uuid.UUID
			if err := rows.Scan(&ignored); err != nil {
				rows.Close()
				return fmt.Errorf("scan locked post-production %s: %w", lock.name, err)
			}
		}
		iterationErr := rows.Err()
		rows.Close()
		if iterationErr != nil {
			return fmt.Errorf("iterate locked post-production %s: %w", lock.name, iterationErr)
		}
	}
	return nil
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
			if currentState != "SUCCEEDED" && currentState != "FAILED" &&
				currentState != "CANCELLED" {
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
					WHERE id = $1
					  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel generation run: %w", err)
				}
				if _, err := tx.Exec(ctx, `
					UPDATE video_pipeline.shot_spec_revisions ssr
					SET lifecycle_state = 'CANCELLED'
					FROM video_pipeline.generation_runs gr
					WHERE gr.id = $1
					  AND gr.state = 'CANCELLED'
					  AND ssr.id = gr.shot_spec_revision_id`,
					runID,
				); err != nil {
					return struct{}{}, fmt.Errorf("cancel shot revision: %w", err)
				}
				if err := releaseRunBudgetReservation(
					ctx, tx, runID, "provider-cancelled",
				); err != nil {
					return struct{}{}, err
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
			if currentState != "SUCCEEDED" && currentState != "FAILED" &&
				currentState != "CANCELLED" {
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
					WHERE id = $1
					  AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
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
		if (currentState == "CANCELLED" || currentState == "FAILED") &&
			input.State != currentState {
			return struct{}{}, controlplane.NewConflictError(
				controlplane.CodeConflict,
				"a workflow completion cannot reverse an existing terminal run state",
			)
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
				if err := releaseRunBudgetReservation(
					ctx, tx, runID, "workflow-failed-before-settlement",
				); err != nil {
					return struct{}{}, err
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

func releaseRunBudgetReservation(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	reason string,
) error {
	var reservationID, jobID uuid.UUID
	var amountMicros int64
	var currency, pricingVersion, status string
	err := tx.QueryRow(ctx, `
		SELECT br.id, pj.id, br.amount_micros, br.currency,
		       br.pricing_rule_version, br.status
		FROM video_pipeline.budget_reservations br
		JOIN video_pipeline.provider_jobs pj ON pj.budget_reservation_id = br.id
		WHERE br.generation_run_id = $1
		FOR UPDATE OF br, pj`,
		runID,
	).Scan(
		&reservationID, &jobID, &amountMicros, &currency, &pricingVersion, &status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock releasable Provider reservation: %w", err)
	}
	if status != "RESERVED" {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE video_pipeline.budget_reservations
		SET status = 'RELEASED'
		WHERE id = $1 AND status = 'RESERVED'`,
		reservationID,
	); err != nil {
		return fmt.Errorf("release Provider budget reservation: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.cost_ledger
			(id, provider_job_id, budget_reservation_id, entry_type,
			 amount_micros, currency, pricing_rule_version, verified)
		VALUES ($1, $2, $3, 'RELEASE', $4, $5, $6, true)
		ON CONFLICT (id) DO NOTHING`,
		uuid.NewSHA1(jobID, []byte("release:"+reason)),
		jobID, reservationID, amountMicros, currency, pricingVersion,
	); err != nil {
		return fmt.Errorf("record Provider budget release: %w", err)
	}
	return nil
}

func dialogueCues(
	narrative []byte,
	shotRevisionID string,
	timelineOffset int64,
	shotDuration int64,
) ([]postproduction.Cue, error) {
	type dialogueRow struct {
		ID          string `json:"id"`
		Speaker     string `json:"speaker"`
		Text        string `json:"text"`
		VoiceRef    string `json:"voiceRef"`
		StartMillis int64  `json:"startMillis"`
		EndMillis   int64  `json:"endMillis"`
		StartMS     int64  `json:"startMs"`
		EndMS       int64  `json:"endMs"`
	}
	var payload struct {
		Dialogue  []dialogueRow `json:"dialogue"`
		Dialogues []dialogueRow `json:"dialogues"`
		Lines     []dialogueRow `json:"lines"`
	}
	if err := json.Unmarshal(narrative, &payload); err != nil {
		return nil, fmt.Errorf("decode shot %s dialogue: %w", shotRevisionID, err)
	}
	rows := payload.Dialogue
	if len(rows) == 0 {
		rows = payload.Dialogues
	}
	if len(rows) == 0 {
		rows = payload.Lines
	}
	cues := make([]postproduction.Cue, 0, len(rows))
	for index, row := range rows {
		start, end := row.StartMillis, row.EndMillis
		if start == 0 && row.StartMS != 0 {
			start = row.StartMS
		}
		if end == 0 && row.EndMS != 0 {
			end = row.EndMS
		}
		if start < 0 || end <= start || end > shotDuration {
			return nil, fmt.Errorf(
				"shot %s dialogue %d has invalid local timing", shotRevisionID, index,
			)
		}
		id := strings.TrimSpace(row.ID)
		if id == "" {
			id = uuid.NewSHA1(
				uuid.NameSpaceOID,
				[]byte(fmt.Sprintf("dialogue:%s:%d", shotRevisionID, index)),
			).String()
		}
		cue := postproduction.Cue{
			ID: id, Speaker: row.Speaker, Text: row.Text, VoiceRef: row.VoiceRef,
			StartMillis: timelineOffset + start, EndMillis: timelineOffset + end,
		}
		if err := cue.Validate(); err != nil {
			return nil, fmt.Errorf("shot %s dialogue %d: %w", shotRevisionID, index, err)
		}
		cues = append(cues, cue)
	}
	return cues, nil
}

func collectVoiceAssetBindings(
	cues []postproduction.Cue,
	shotAssetVersionIDs []uuid.UUID,
	requireVoice bool,
	result map[uuid.UUID]struct{},
) error {
	allowed := make(map[uuid.UUID]struct{}, len(shotAssetVersionIDs))
	for _, assetVersionID := range shotAssetVersionIDs {
		allowed[assetVersionID] = struct{}{}
	}
	for _, cue := range cues {
		voiceRef := strings.TrimSpace(cue.VoiceRef)
		if voiceRef == "" {
			if requireVoice {
				return controlplane.NewPolicyError(
					controlplane.CodeConsentRequired,
					"live speech cue has no approved voice asset binding",
					"bind an approved VOICE/AUDIO asset version before live TTS",
				)
			}
			continue
		}
		voiceAssetID, err := uuid.Parse(voiceRef)
		if err != nil {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"voiceRef is not an immutable asset version identifier",
				"use the approved VOICE/AUDIO asset version UUID",
			)
		}
		if _, ok := allowed[voiceAssetID]; !ok {
			return controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"voiceRef is not included in the exact shot asset bindings",
				"add the approved voice asset revision and reapprove the shot",
			)
		}
		result[voiceAssetID] = struct{}{}
	}
	return nil
}

func (p *Postgres) validateVoiceAssets(
	ctx context.Context,
	episodeRevisionID uuid.UUID,
	requested map[uuid.UUID]struct{},
) error {
	if len(requested) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(requested))
	for id := range requested {
		ids = append(ids, id)
	}
	var invalidLicense, invalidConsent int
	if err := p.pool.QueryRow(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE
		    av.id IS NULL
		    OR source_asset.id IS NULL
		    OR source_asset.series_id <> ep.series_id
		    OR source_asset.asset_type NOT IN ('VOICE', 'AUDIO')
		    OR av.status <> 'APPROVED'
		    OR ls.id IS NULL
		    OR ls.policy_status <> 'ALLOWED'
		    OR (ls.expires_at IS NOT NULL AND ls.expires_at <= now())
		  ),
		  COUNT(*) FILTER (WHERE
		    av.consent_asset_id IS NOT NULL
		    AND (
		      ca.id IS NULL
		      OR ca.status <> 'ACTIVE'
		      OR (ca.expires_at IS NOT NULL AND ca.expires_at <= now())
		    )
		  )
		FROM video_pipeline.episode_revisions er
		JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
		CROSS JOIN unnest($1::uuid[]) requested(id)
		LEFT JOIN video_pipeline.asset_versions av ON av.id = requested.id
		LEFT JOIN video_pipeline.assets source_asset ON source_asset.id = av.asset_id
		LEFT JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
		LEFT JOIN video_pipeline.consent_assets ca ON ca.id = av.consent_asset_id
		WHERE er.id = $2`,
		ids, episodeRevisionID,
	).Scan(&invalidLicense, &invalidConsent); err != nil {
		return fmt.Errorf("validate voice assets: %w", err)
	}
	if invalidLicense > 0 {
		return controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"one or more voice assets are missing, unapproved, unlicensed, or outside the series",
			"select an active approved VOICE/AUDIO asset revision for every live cue",
		)
	}
	if invalidConsent > 0 {
		return controlplane.NewPolicyError(
			controlplane.CodeConsentRequired,
			"one or more bound voice consents are revoked or expired",
			"renew or replace the voice consent before live TTS",
		)
	}
	return nil
}

type speechCostEvidence struct {
	AttemptCount    int    `json:"attemptCount"`
	EstimatedMicros int64  `json:"estimatedMicros"`
	ActualMicros    *int64 `json:"actualMicros"`
	Currency        string `json:"currency"`
	Verified        bool   `json:"verified"`
}

func summarizeSpeechCost(
	attempts []postproduction.ProviderAttempt,
) (speechCostEvidence, error) {
	summary := speechCostEvidence{AttemptCount: len(attempts), Verified: len(attempts) > 0}
	var actual int64
	actualKnown := len(attempts) > 0
	for index, attempt := range attempts {
		if index == 0 {
			summary.Currency = attempt.Cost.Currency
		} else if summary.Currency != attempt.Cost.Currency {
			return speechCostEvidence{}, errors.New("speech attempt currencies cannot be combined")
		}
		if attempt.Cost.EstimatedMicros > math.MaxInt64-summary.EstimatedMicros {
			return speechCostEvidence{}, errors.New("speech estimate total overflows int64")
		}
		summary.EstimatedMicros += attempt.Cost.EstimatedMicros
		if attempt.Cost.ActualMicros == nil {
			actualKnown = false
		} else {
			if *attempt.Cost.ActualMicros > math.MaxInt64-actual {
				return speechCostEvidence{}, errors.New("speech actual total overflows int64")
			}
			actual += *attempt.Cost.ActualMicros
		}
		summary.Verified = summary.Verified && attempt.Cost.Verified
	}
	if actualKnown {
		summary.ActualMicros = &actual
	} else {
		summary.Verified = false
	}
	return summary, nil
}

func (p *Postgres) postProductionGateBindings(
	ctx context.Context,
	episodeRevisionID uuid.UUID,
) ([]postproduction.GateBinding, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT DISTINCT ON (ad.gate) ad.id, ad.gate, to_jsonb(ad)
		FROM video_pipeline.approval_decisions ad
		JOIN video_pipeline.episode_revisions er ON er.episode_id = ad.episode_id
		WHERE er.id = $1
		  AND ad.gate IN ('G1', 'G2')
		  AND ad.decision = 'APPROVED'
		ORDER BY ad.gate, ad.decided_at DESC`,
		episodeRevisionID,
	)
	if err != nil {
		return nil, fmt.Errorf("read post-production gate bindings: %w", err)
	}
	defer rows.Close()
	bindings := make([]postproduction.GateBinding, 0, 2)
	for rows.Next() {
		var decisionID uuid.UUID
		var gate string
		var payload []byte
		if err := rows.Scan(&decisionID, &gate, &payload); err != nil {
			return nil, fmt.Errorf("scan post-production gate binding: %w", err)
		}
		hash, err := digestValue(json.RawMessage(payload))
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, postproduction.GateBinding{
			Gate: gate, DecisionID: decisionID.String(), Decision: "APPROVED", ContentHash: hash,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate post-production gate bindings: %w", err)
	}
	return bindings, nil
}

func (p *Postgres) resolveBackgroundAudio(
	ctx context.Context,
	episodeRevisionID uuid.UUID,
	assetVersionIDRaw string,
) (postproduction.Artifact, error) {
	assetVersionID, err := uuid.Parse(assetVersionIDRaw)
	if err != nil {
		return postproduction.Artifact{}, errors.New("backgroundAudioAssetVersionId must be a UUID")
	}
	var artifact postproduction.Artifact
	artifact.Kind = "background_audio"
	if err := p.pool.QueryRow(ctx, `
		SELECT av.content_hash, av.artifact_uri, av.media_type,
		       COALESCE(a.size_bytes, 0),
		       COALESCE((a.media_spec->>'durationMillis')::bigint, 0)
		FROM video_pipeline.asset_versions av
		JOIN video_pipeline.assets source_asset ON source_asset.id = av.asset_id
		JOIN video_pipeline.episode_revisions er ON er.id = $2
		JOIN video_pipeline.episodes ep ON ep.id = er.episode_id
		JOIN video_pipeline.license_snapshots ls ON ls.id = av.license_snapshot_id
		LEFT JOIN video_pipeline.artifacts a ON a.content_hash = av.content_hash
		WHERE av.id = $1
		  AND source_asset.series_id = ep.series_id
		  AND source_asset.asset_type IN ('MUSIC', 'AUDIO')
		  AND av.status = 'APPROVED'
		  AND ls.policy_status = 'ALLOWED'
		  AND ls.commercial_use
		  AND (ls.expires_at IS NULL OR ls.expires_at > now())
		  AND (av.consent_asset_id IS NULL OR EXISTS (
		    SELECT 1
		    FROM video_pipeline.consent_assets ca
		    WHERE ca.id = av.consent_asset_id
		      AND ca.status = 'ACTIVE'
		      AND (ca.expires_at IS NULL OR ca.expires_at > now())
		  ))`,
		assetVersionID, episodeRevisionID,
	).Scan(
		&artifact.Digest, &artifact.URI, &artifact.MediaType,
		&artifact.SizeBytes, &artifact.DurationMillis,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return postproduction.Artifact{}, controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"background audio is not an approved compatible asset",
				"select an active licensed audio revision for this series",
			)
		}
		return postproduction.Artifact{}, fmt.Errorf("resolve background audio: %w", err)
	}
	if err := artifact.Validate(); err != nil {
		return postproduction.Artifact{}, err
	}
	return artifact, nil
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
