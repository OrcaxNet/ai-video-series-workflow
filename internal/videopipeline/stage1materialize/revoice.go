package stage1materialize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const SpeechVoiceRevisionSchemaVersion = "flo104.speech-voice-revision.v1"

// SpeechVoiceRevisionInput is a secret-free, no-Provider input. It changes only
// the speech configuration and VOICE product truth; all ten successful video
// runs in ParentExecutionPackage remain immutable.
type SpeechVoiceRevisionInput struct {
	SchemaVersion                    string   `json:"schemaVersion"`
	BatchID                          string   `json:"batchId"`
	ParentExecutionPackageHash       string   `json:"parentExecutionPackageHash"`
	ParentVoiceAssetVersionID        string   `json:"parentVoiceAssetVersionId"`
	Provider                         string   `json:"provider"`
	ProviderID                       string   `json:"providerId"`
	Region                           string   `json:"region"`
	Endpoint                         string   `json:"endpoint"`
	ModelID                          string   `json:"modelId"`
	RouteVersion                     string   `json:"routeVersion"`
	ResourceID                       string   `json:"resourceId"`
	Speaker                          string   `json:"speaker"`
	PlanName                         string   `json:"planName"`
	PricingVersion                   string   `json:"pricingVersion"`
	AuthorizedCueID                  string   `json:"authorizedCueId"`
	MaximumAFPMilli                  int64    `json:"maximumAfpMilli"`
	MaximumNonSubscriptionCashMicros int64    `json:"maximumNonSubscriptionCashMicros"`
	MaxAttempts                      int      `json:"maxAttempts"`
	LicenseID                        string   `json:"licenseId"`
	LicenseSourceURI                 string   `json:"licenseSourceUri"`
	Territories                      []string `json:"territories"`
	InternalMVPOnly                  bool     `json:"internalMvpOnly"`
}

func (input SpeechVoiceRevisionInput) Validate(parent stage1.ExecutionPackage) error {
	switch {
	case input.SchemaVersion != SpeechVoiceRevisionSchemaVersion:
		return errors.New("speech voice revision schemaVersion is invalid")
	case input.BatchID != parent.BatchID:
		return errors.New("speech voice revision is bound to another batch")
	case input.ParentExecutionPackageHash != parent.ContentHash:
		return errors.New("speech voice revision parent package hash drifted")
	case input.Provider != "volcengine_ark":
		return errors.New("speech voice revision provider must be volcengine_ark")
	case strings.TrimSpace(input.ProviderID) == "" || input.Region != "cn-beijing":
		return errors.New("speech voice revision provider identity or region is invalid")
	case input.Endpoint != runtimeconfig.AgentPlanTTSEndpoint:
		return errors.New("speech voice revision must use the exact Agent Plan endpoint")
	case input.ModelID != volcengineprovider.AgentPlanTTSModelID:
		return errors.New("speech voice revision model is invalid")
	case input.RouteVersion != volcengineprovider.AgentPlanTTSRouteVersion:
		return errors.New("speech voice revision routeVersion is invalid")
	case input.ResourceID != volcengineprovider.AgentPlanTTSResourceID:
		return errors.New("speech voice revision resourceId is invalid")
	case strings.TrimSpace(input.Speaker) == "" || strings.TrimSpace(input.AuthorizedCueID) == "":
		return errors.New("speech voice revision speaker and authorized cue are required")
	case strings.TrimSpace(input.PlanName) == "" || strings.TrimSpace(input.PricingVersion) == "":
		return errors.New("speech voice revision plan and pricing versions are required")
	case input.MaximumAFPMilli <= 0 || input.MaximumNonSubscriptionCashMicros != 0:
		return errors.New("speech voice revision requires a positive AFP and zero-cash ceiling")
	case input.MaxAttempts != 1:
		return errors.New("speech voice revision canary requires MaxAttempts=1")
	case strings.TrimSpace(input.LicenseID) == "" || strings.TrimSpace(input.LicenseSourceURI) == "":
		return errors.New("speech voice revision license identity is required")
	case len(input.Territories) == 0 || !input.InternalMVPOnly:
		return errors.New("speech voice revision must be limited to an explicit internal MVP territory")
	}
	if _, err := uuid.Parse(input.ParentVoiceAssetVersionID); err != nil {
		return errors.New("parent voice asset version must be a UUID")
	}
	return nil
}

type SpeechVoiceRevisionReport struct {
	SchemaVersion                    string                            `json:"schemaVersion"`
	BatchID                          string                            `json:"batchId"`
	RevisionInputHash                string                            `json:"revisionInputHash"`
	ParentExecutionPackageHash       string                            `json:"parentExecutionPackageHash"`
	ExecutionPackageHash             string                            `json:"executionPackageHash"`
	ProviderProfileID                string                            `json:"providerProfileId"`
	CapabilitySnapshotID             string                            `json:"capabilitySnapshotId"`
	Route                            providercontract.ModelSnapshot    `json:"route"`
	Voice                            postproduction.SpeechVoiceBinding `json:"voice"`
	VoiceDescriptorURI               string                            `json:"voiceDescriptorUri"`
	VoiceDescriptorBytes             int64                             `json:"voiceDescriptorBytes"`
	AuthorizedCueID                  string                            `json:"authorizedCueId"`
	CueUnicodeCharacters             int                               `json:"cueUnicodeCharacters"`
	EstimatedAFPMilli                int64                             `json:"estimatedAfpMilli"`
	MaximumAFPMilli                  int64                             `json:"maximumAfpMilli"`
	MaximumNonSubscriptionCashMicros int64                             `json:"maximumNonSubscriptionCashMicros"`
	MaxAttempts                      int                               `json:"maxAttempts"`
	Job                              postproduction.SpeechJobIdentity  `json:"job"`
	ProviderJobsBefore               int64                             `json:"providerJobsBefore"`
	ProviderJobsAfter                int64                             `json:"providerJobsAfter"`
	ProviderJobDelta                 int64                             `json:"providerJobDelta"`
	ProviderCalls                    int64                             `json:"providerCalls"`
	ApprovalCommentID                string                            `json:"approvalCommentId"`
}

// RevoiceStage1 materializes a child VOICE/license/capability revision and a
// new frozen execution package. It has no Provider client or Adapter URL.
func RevoiceStage1(
	ctx context.Context,
	pool *pgxpool.Pool,
	cas *artifactstore.Store,
	plan stage1.Plan,
	parent stage1.ExecutionPackage,
	input SpeechVoiceRevisionInput,
	approval Approval,
) (stage1.ExecutionPackage, SpeechVoiceRevisionReport, error) {
	if pool == nil || cas == nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("PostgreSQL pool and CAS are required")
	}
	if err := plan.Validate(); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	if err := parent.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("validate parent execution package: %w", err)
	}
	if err := input.Validate(parent); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	if strings.TrimSpace(approval.CommentID) == "" || strings.TrimSpace(approval.ActorID) == "" ||
		!approval.ValidUntil.After(time.Now().UTC()) {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("a current explicit approval is required")
	}

	revisionInputHash, err := digest(input)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	parentVersionID := uuid.MustParse(input.ParentVoiceAssetVersionID)
	voiceVersionID := uuid.NewSHA1(parentVersionID, []byte("speech-v2:voice:"+revisionInputHash))
	licenseSnapshotID := uuid.NewSHA1(parentVersionID, []byte("speech-v2:license:"+revisionInputHash))
	providerProfileID := uuid.NewSHA1(parentVersionID, []byte("speech-v2:provider:"+revisionInputHash))
	capabilitySnapshotID := uuid.NewSHA1(providerProfileID, []byte("speech-v2:capability:"+revisionInputHash))

	descriptorPayload := map[string]any{
		"schemaVersion": "v2", "provider": input.Provider,
		"modelId": input.ModelID, "resourceId": input.ResourceID,
		"speaker": input.Speaker, "routeVersion": input.RouteVersion,
		"voiceClone": false, "internalMvpOnly": true,
		"parentAssetVersionId": input.ParentVoiceAssetVersionID,
		"assetVersionId":       voiceVersionID.String(),
		"licenseSnapshotId":    licenseSnapshotID.String(),
		"revisionInputHash":    revisionInputHash,
	}
	descriptorBytes, err := controlplane.CanonicalJSON(descriptorPayload)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("encode VOICE descriptor: %w", err)
	}
	descriptor, err := cas.Put(ctx, bytes.NewReader(descriptorBytes))
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("commit VOICE descriptor to CAS: %w", err)
	}
	licenseHash, err := digest(map[string]any{
		"subjectRef": input.Provider + ":" + input.ResourceID + ":" + input.Speaker,
		"licenseId":  input.LicenseID, "sourceUri": input.LicenseSourceURI,
		"territories": input.Territories, "commercialUse": true,
		"internalMvpOnly": true, "validUntil": approval.ValidUntil.UTC(),
		"authorizationCommentId": approval.CommentID,
		"voiceDescriptorHash":    descriptor.Digest,
		"revisionInputHash":      revisionInputHash,
	})
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	adapterConfig := runtimeconfig.VolcengineProvider{
		ProviderID: input.ProviderID, Region: input.Region,
		SpeechEndpoint: input.Endpoint, SpeechModel: input.ModelID,
		SpeechSpeaker: input.Speaker, PlanName: input.PlanName,
		PricingVersion: input.PricingVersion,
	}
	capabilityHash := volcengineprovider.AgentPlanTTSCapabilityHash(adapterConfig)
	route := providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilitySpeech),
		Provider:        "VOLCENGINE", ModelID: input.ModelID,
		RouteVersion: input.RouteVersion, CapabilityHash: capabilityHash,
		Verification: providercontract.PendingKey,
	}

	var providerJobsBefore int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_jobs`).Scan(&providerJobsBefore); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("count Provider jobs before revoice: %w", err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("begin VOICE revision: %w", err)
	}
	defer tx.Rollback(ctx)
	var (
		voiceAssetID   uuid.UUID
		parentRevision int
		parentHash     string
		seriesID       uuid.UUID
	)
	if err := tx.QueryRow(ctx, `
		SELECT av.asset_id, av.revision, av.content_hash, source_asset.series_id
		FROM video_pipeline.asset_versions av
		JOIN video_pipeline.assets source_asset ON source_asset.id = av.asset_id
		JOIN video_pipeline.episodes ep ON ep.series_id = source_asset.series_id
		JOIN video_pipeline.episode_revisions er ON er.episode_id = ep.id
		WHERE av.id = $1
		  AND er.id = $2
		  AND source_asset.asset_type = 'VOICE'
		  AND av.status = 'APPROVED'
		FOR UPDATE OF av, source_asset`,
		parentVersionID, uuid.MustParse(parent.PostProduction.EpisodeRevisionID),
	).Scan(&voiceAssetID, &parentRevision, &parentHash, &seriesID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("approved parent VOICE revision is not in the frozen episode series")
		}
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("lock parent VOICE revision: %w", err)
	}
	_ = seriesID
	if err := ensureActiveInputArtifact(
		ctx, tx, descriptor, "audio/x-voice-profile+json",
		map[string]any{
			"kind": "voice_profile", "provider": input.Provider,
			"modelId": input.ModelID, "resourceId": input.ResourceID,
			"speaker": input.Speaker, "routeVersion": input.RouteVersion,
			"revisionInputHash": revisionInputHash,
		},
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.license_snapshots
			(id, subject_type, subject_ref, license_id, license_hash,
			 policy_status, territories, commercial_use, expires_at,
			 source_uri, reviewed_by, reviewed_at)
		VALUES ($1, 'VOICE', $2, $3, $4, 'ALLOWED', $5, true, $6, $7, $8, $9)
		ON CONFLICT DO NOTHING`,
		licenseSnapshotID, input.Provider+":"+input.ResourceID+":"+input.Speaker,
		input.LicenseID, licenseHash, input.Territories, approval.ValidUntil.UTC(),
		input.LicenseSourceURI, approval.ActorID, time.Now().UTC(),
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("insert VOICE license snapshot: %w", err)
	}
	var storedLicenseHash string
	if err := tx.QueryRow(ctx, `
		SELECT license_hash
		FROM video_pipeline.license_snapshots
		WHERE id = $1 AND subject_type = 'VOICE' AND subject_ref = $2
		  AND license_id = $3 AND license_hash = $4 AND policy_status = 'ALLOWED'
		  AND territories = $5 AND commercial_use AND expires_at = $6
		FOR SHARE`,
		licenseSnapshotID, input.Provider+":"+input.ResourceID+":"+input.Speaker,
		input.LicenseID, licenseHash, input.Territories, approval.ValidUntil.UTC(),
	).Scan(&storedLicenseHash); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("verify VOICE license snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.asset_versions
			(id, asset_id, revision, parent_revision_id, status, content_hash,
			 artifact_uri, media_type, dimensions, source_ref,
			 license_snapshot_id, created_by)
		VALUES ($1, $2, $3, $4, 'APPROVED', $5, $6,
		        'audio/x-voice-profile+json', $7, $8, $9, $10)
		ON CONFLICT DO NOTHING`,
		voiceVersionID, voiceAssetID, parentRevision+1, parentVersionID,
		descriptor.Digest, descriptor.URI,
		map[string]any{
			"provider": input.Provider, "modelId": input.ModelID,
			"resourceId": input.ResourceID, "speaker": input.Speaker,
			"routeVersion": input.RouteVersion,
		},
		"flo104-speech-v2:"+revisionInputHash, licenseSnapshotID, approval.ActorID,
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("insert VOICE asset revision: %w", err)
	}
	var storedVoiceHash string
	if err := tx.QueryRow(ctx, `
		SELECT content_hash
		FROM video_pipeline.asset_versions
		WHERE id = $1 AND asset_id = $2 AND revision = $3
		  AND parent_revision_id = $4 AND status = 'APPROVED'
		  AND content_hash = $5 AND artifact_uri = $6
		  AND license_snapshot_id = $7
		FOR SHARE`,
		voiceVersionID, voiceAssetID, parentRevision+1, parentVersionID,
		descriptor.Digest, descriptor.URI, licenseSnapshotID,
	).Scan(&storedVoiceHash); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("verify VOICE asset revision: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.provider_profiles
			(id, provider, display_name, base_url_ref, credential_ref,
			 enabled, mode, health, config_hash)
		VALUES ($1, 'VOLCENGINE', 'FLO-104 Agent Plan speech v2', $2,
		        'env:ARK_API_KEY', true, 'LIVE', 'READY', $3)
		ON CONFLICT DO NOTHING`,
		providerProfileID, input.Endpoint, capabilityHash,
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("insert speech-v2 provider profile: %w", err)
	}
	var verifiedProfile int
	if err := tx.QueryRow(ctx, `
		SELECT 1
		FROM video_pipeline.provider_profiles
		WHERE id = $1 AND provider = 'VOLCENGINE'
		  AND display_name = 'FLO-104 Agent Plan speech v2'
		  AND base_url_ref = $2 AND credential_ref = 'env:ARK_API_KEY'
		  AND enabled AND mode = 'LIVE' AND health = 'READY'
		  AND config_hash = $3
		FOR SHARE`,
		providerProfileID, input.Endpoint, capabilityHash,
	).Scan(&verifiedProfile); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("verify speech-v2 provider profile: %w", err)
	}
	limits := map[string]any{
		"resourceId": input.ResourceID, "defaultSpeaker": input.Speaker,
		"maximumCharacters": volcengineprovider.AgentPlanTTSMaxChars,
		"billingMode":       "subscription", "afpMilliPerCharacter": int64(135),
		"authorizedCueId":                  input.AuthorizedCueID,
		"maximumAfpMilli":                  input.MaximumAFPMilli,
		"maximumNonSubscriptionCashMicros": input.MaximumNonSubscriptionCashMicros,
		"maxAttempts":                      input.MaxAttempts,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.provider_capability_snapshots
			(id, provider_profile_id, capability_alias, model_id, route_version,
			 supported_inputs, limits, pricing_rule_version, capability_hash,
			 status, effective_at, expires_at)
		VALUES ($1, $2, 'speech.primary', $3, $4,
		        ARRAY['text','voice_profile'], $5, 'agent-plan-subscription-v2',
		        $6, 'ACTIVE', $7, $8)
		ON CONFLICT DO NOTHING`,
		capabilitySnapshotID, providerProfileID, input.ModelID,
		input.RouteVersion, limits, capabilityHash, time.Now().UTC(),
		approval.ValidUntil.UTC(),
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("insert speech-v2 capability: %w", err)
	}
	var verifiedCapability int
	if err := tx.QueryRow(ctx, `
		SELECT 1
		FROM video_pipeline.provider_capability_snapshots
		WHERE id = $1 AND provider_profile_id = $2
		  AND capability_alias = 'speech.primary'
		  AND model_id = $3 AND route_version = $4
		  AND supported_inputs = ARRAY['text','voice_profile']
		  AND limits->>'resourceId' = $5
		  AND limits->>'defaultSpeaker' = $6
		  AND limits->>'authorizedCueId' = $7
		  AND (limits->>'maximumAfpMilli')::bigint = $8
		  AND (limits->>'maximumNonSubscriptionCashMicros')::bigint = $9
		  AND (limits->>'maxAttempts')::integer = $10
		  AND pricing_rule_version = 'agent-plan-subscription-v2'
		  AND capability_hash = $11 AND status = 'ACTIVE'
		  AND expires_at = $12
		FOR SHARE`,
		capabilitySnapshotID, providerProfileID, input.ModelID, input.RouteVersion,
		input.ResourceID, input.Speaker, input.AuthorizedCueID,
		input.MaximumAFPMilli, input.MaximumNonSubscriptionCashMicros,
		input.MaxAttempts, capabilityHash, approval.ValidUntil.UTC(),
	).Scan(&verifiedCapability); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("verify speech-v2 capability: %w", err)
	}
	voiceBinding := postproduction.SpeechVoiceBinding{
		AssetID: voiceAssetID.String(), ParentAssetVersionID: parentVersionID.String(),
		AssetVersionID: voiceVersionID.String(), AssetVersionHash: descriptor.Digest,
		LicenseSnapshotID: licenseSnapshotID.String(), LicenseSnapshotHash: licenseHash,
		Provider: input.Provider, ModelID: input.ModelID,
		ResourceID: input.ResourceID, Speaker: input.Speaker,
	}
	revised := parent
	revised.PostProduction.Config.SpeechRoute = route
	revised.PostProduction.Config.SpeechProviderProfileID = providerProfileID.String()
	revised.PostProduction.Config.SpeechIdentityVersion = postproduction.SpeechIdentityV2
	revised.PostProduction.Config.SpeechVoice = &voiceBinding
	revised.PostProduction.Config.SpeechAuthorizedCueID = input.AuthorizedCueID
	revised.PostProduction.Config.SpeechMaximumAFPMilli = input.MaximumAFPMilli
	revised.PostProduction.Config.SpeechMaximumCashMicros = input.MaximumNonSubscriptionCashMicros
	revised.PostProduction.Config.SpeechMaxAttempts = input.MaxAttempts
	revised.PostProduction.TraceID = parent.PostProduction.TraceID + "-speech-v2"
	revised, err = stage1.SealExecutionPackage(revised)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	if err := revised.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("validate revised execution package: %w", err)
	}
	auditPayload := map[string]any{
		"revisionInputHash":                revisionInputHash,
		"parentExecutionPackageHash":       parent.ContentHash,
		"executionPackageHash":             revised.ContentHash,
		"parentVoiceAssetVersionId":        parentVersionID,
		"parentVoiceAssetHash":             parentHash,
		"voiceAssetVersionId":              voiceVersionID,
		"voiceAssetHash":                   descriptor.Digest,
		"licenseSnapshotId":                licenseSnapshotID,
		"licenseSnapshotHash":              licenseHash,
		"providerProfileId":                providerProfileID,
		"capabilitySnapshotId":             capabilitySnapshotID,
		"authorizedCueId":                  input.AuthorizedCueID,
		"maximumAfpMilli":                  input.MaximumAFPMilli,
		"maximumNonSubscriptionCashMicros": input.MaximumNonSubscriptionCashMicros,
		"maxAttempts":                      input.MaxAttempts,
		"providerCalls":                    0,
		"authorizationCommentId":           approval.CommentID,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.audit_events
			(id, occurred_at, actor_id, actor_role, action, aggregate_type,
			 aggregate_id, reason_code, trace_id, payload)
		VALUES ($1, $2, $3, 'ADMIN', 'speech.voice_revision.materialized',
		        'ASSET_VERSION', $4, 'FLO104_SPEECH_V2_CANARY', $5, $6)
		ON CONFLICT DO NOTHING`,
		uuid.NewSHA1(voiceVersionID, []byte("materialization-audit")), time.Now().UTC(),
		approval.ActorID, voiceVersionID, revised.PostProduction.TraceID, auditPayload,
	); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("insert speech-v2 audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("commit speech-v2 revision: %w", err)
	}

	ledger := repository.NewForPool(pool)
	prepared, err := ledger.PrepareEpisodePostProduction(ctx, orchestration.WorkflowStep{
		WorkflowID: "stage1-revoice-" + revised.ContentHash,
		ActivityID: "prepare-speech-v2", ActivityType: "offline-no-provider",
		TraceID: revised.PostProduction.TraceID,
	}, revised.PostProduction)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("verify revised post-production request: %w", err)
	}
	var authorizedCue *postproduction.Cue
	for index := range prepared.Subtitle.Cues {
		if prepared.Subtitle.Cues[index].ID == input.AuthorizedCueID {
			authorizedCue = &prepared.Subtitle.Cues[index]
			break
		}
	}
	if authorizedCue == nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("authorized cue is absent from the frozen subtitle revision")
	}
	identity, err := postproduction.DeriveSpeechJobIdentity(postproduction.SpeechRequest{
		EpisodeRevisionID: prepared.EpisodeRevisionID,
		SubtitleRevision:  prepared.Subtitle, Cue: *authorizedCue,
		Config: prepared.Speech, Evidence: prepared.Evidence,
		TraceID: prepared.TraceID, BudgetMicros: prepared.Speech.BudgetMaximumMicros,
	})
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, err
	}
	characters := utf8.RuneCountInString(strings.TrimSpace(authorizedCue.Text))
	estimatedAFPMilli := int64(characters) * 135
	if estimatedAFPMilli <= 0 || estimatedAFPMilli > input.MaximumAFPMilli {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("authorized cue AFP estimate exceeds the frozen canary")
	}
	var providerJobsAfter int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_jobs`).Scan(&providerJobsAfter); err != nil {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, fmt.Errorf("count Provider jobs after revoice: %w", err)
	}
	if providerJobsAfter != providerJobsBefore {
		return stage1.ExecutionPackage{}, SpeechVoiceRevisionReport{}, errors.New("revoice unexpectedly changed Provider job truth")
	}
	report := SpeechVoiceRevisionReport{
		SchemaVersion: SpeechVoiceRevisionSchemaVersion,
		BatchID:       input.BatchID, RevisionInputHash: revisionInputHash,
		ParentExecutionPackageHash: parent.ContentHash,
		ExecutionPackageHash:       revised.ContentHash,
		ProviderProfileID:          providerProfileID.String(),
		CapabilitySnapshotID:       capabilitySnapshotID.String(), Route: route,
		Voice: voiceBinding, VoiceDescriptorURI: descriptor.URI,
		VoiceDescriptorBytes: descriptor.Size,
		AuthorizedCueID:      input.AuthorizedCueID,
		CueUnicodeCharacters: characters, EstimatedAFPMilli: estimatedAFPMilli,
		MaximumAFPMilli:                  input.MaximumAFPMilli,
		MaximumNonSubscriptionCashMicros: input.MaximumNonSubscriptionCashMicros,
		MaxAttempts:                      input.MaxAttempts, Job: identity,
		ProviderJobsBefore: providerJobsBefore, ProviderJobsAfter: providerJobsAfter,
		ProviderJobDelta: 0, ProviderCalls: 0,
		ApprovalCommentID: approval.CommentID,
	}
	return revised, report, nil
}

func decodeSpeechVoiceRevision(data []byte) (SpeechVoiceRevisionInput, error) {
	var input SpeechVoiceRevisionInput
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return SpeechVoiceRevisionInput{}, err
	}
	return input, nil
}
