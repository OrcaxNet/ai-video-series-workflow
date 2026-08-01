// Package stage1materialize imports an administrator-approved Stage 1 product
// bundle into PostgreSQL and CAS without opening a Provider execution path.
package stage1materialize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	productSchema = "flo104.sample1.product-input.v1"
	batchID       = "flo104-sample-1"
	actorRole     = "ADMIN"
)

type Files struct {
	Product string
	Source  string
	Safety  string
	Visual  string
}

type Approval struct {
	CommentID  string
	ActorID    string
	ValidUntil time.Time
}

type Report struct {
	SchemaVersion        string            `json:"schemaVersion"`
	BatchID              string            `json:"batchId"`
	InputPackageHash     string            `json:"inputPackageHash"`
	ExecutionPackageHash string            `json:"executionPackageHash"`
	Counts               map[string]int64  `json:"counts"`
	CAS                  map[string]string `json:"cas"`
	ProviderCalls        int64             `json:"providerCalls"`
	ProviderJobs         int64             `json:"providerJobs"`
	BudgetReservations   int64             `json:"budgetReservations"`
	CostLedgerEntries    int64             `json:"costLedgerEntries"`
	ApprovalCommentID    string            `json:"approvalCommentId"`
	ApprovalValidUntil   time.Time         `json:"approvalValidUntil"`
}

type productInput struct {
	SchemaVersion         string              `json:"schemaVersion"`
	IssueID               string              `json:"issueId"`
	BatchID               string              `json:"batchId"`
	CreatedBy             json.RawMessage     `json:"createdBy"`
	Reserved              reservedIDs         `json:"reservedIds"`
	Source                sourceInput         `json:"source"`
	CreativeBrief         json.RawMessage     `json:"creativeBrief"`
	DialogueSummary       dialogueSummary     `json:"dialogueSummary"`
	GenerationProfile     generationProfile   `json:"generationProfile"`
	ReusableAssets        []reusableAsset     `json:"reusableAssets"`
	LicenseSnapshots      []licenseInput      `json:"licenseSnapshots"`
	SharedPrompt          sharedPrompt        `json:"sharedPrompt"`
	Shots                 []shotInput         `json:"shots"`
	ContentSafetyEvidence safetyInput         `json:"contentSafetyEvidence"`
	GenerationPlan        generationPlanInput `json:"generationPlan"`
	Approvals             []approvalInput     `json:"approvalsToMaterialize"`
	PostProduction        postInput           `json:"postProduction"`
	AuthorizationBoundary json.RawMessage     `json:"authorizationBoundary"`
	RecordState           string              `json:"recordState"`
	RequiredNextChecks    []string            `json:"requiredNextChecks"`
	MaterializationRule   string              `json:"materializationRule"`
}

type reservedIDs struct {
	SeriesID, SourceRevisionID, EpisodeID, EpisodeRevisionID                  string
	SceneID, SceneRevisionID, ScriptRevisionID, StoryboardRevisionID          string
	GenerationProfileID, GenerationProfileRevisionID, GenerationPlanID        string
	VisualAssetID, VisualAssetVersionID, VisualLicenseSnapshotID              string
	VoiceAssetID, VoiceAssetVersionID, VoiceLicenseSnapshotID                 string
	SafetyEvidenceArtifactID, G1DecisionID, G2DecisionID, SafetyDecisionID    string
	VideoBudgetApprovalID, SpeechBudgetApprovalID                             string
	VideoProviderProfileID, SpeechProviderProfileID                           string
	SeriesContextRevisionID, EpisodeContextRevisionID, SceneContextRevisionID string
}

func (r *reservedIDs) UnmarshalJSON(data []byte) error {
	type wire struct {
		SeriesID                    string `json:"seriesId"`
		SourceRevisionID            string `json:"sourceRevisionId"`
		EpisodeID                   string `json:"episodeId"`
		EpisodeRevisionID           string `json:"episodeRevisionId"`
		SceneID                     string `json:"sceneId"`
		SceneRevisionID             string `json:"sceneRevisionId"`
		ScriptRevisionID            string `json:"scriptRevisionId"`
		StoryboardRevisionID        string `json:"storyboardRevisionId"`
		GenerationProfileID         string `json:"generationProfileId"`
		GenerationProfileRevisionID string `json:"generationProfileRevisionId"`
		GenerationPlanID            string `json:"generationPlanId"`
		VisualAssetID               string `json:"visualAssetId"`
		VisualAssetVersionID        string `json:"visualAssetVersionId"`
		VisualLicenseSnapshotID     string `json:"visualLicenseSnapshotId"`
		VoiceAssetID                string `json:"voiceAssetId"`
		VoiceAssetVersionID         string `json:"voiceAssetVersionId"`
		VoiceLicenseSnapshotID      string `json:"voiceLicenseSnapshotId"`
		SafetyEvidenceArtifactID    string `json:"safetyEvidenceArtifactId"`
		G1DecisionID                string `json:"g1DecisionId"`
		G2DecisionID                string `json:"g2DecisionId"`
		SafetyDecisionID            string `json:"safetyDecisionId"`
		VideoBudgetApprovalID       string `json:"videoBudgetApprovalId"`
		SpeechBudgetApprovalID      string `json:"speechBudgetApprovalId"`
		VideoProviderProfileID      string `json:"videoProviderProfileId"`
		SpeechProviderProfileID     string `json:"speechProviderProfileId"`
		SeriesContextRevisionID     string `json:"seriesContextRevisionId"`
		EpisodeContextRevisionID    string `json:"episodeContextRevisionId"`
		SceneContextRevisionID      string `json:"sceneContextRevisionId"`
	}
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*r = reservedIDs(value)
	return nil
}

type sourceInput struct {
	Title       string          `json:"title"`
	Language    string          `json:"language"`
	Filename    string          `json:"filename"`
	SHA256      string          `json:"sha256"`
	Bytes       int64           `json:"bytes"`
	ArtifactURI string          `json:"artifactUriAfterIngest"`
	Rights      json.RawMessage `json:"rights"`
}

type dialogueSummary struct {
	Text                  string `json:"text"`
	UnicodeCharacterCount int    `json:"unicodeCharacterCount"`
	MaximumAllowed        int    `json:"maximumAllowed"`
	TTSAFPMilliEstimate   int64  `json:"ttsAfpMilliEstimate"`
}

type routeInput struct {
	CapabilityAlias string `json:"capabilityAlias"`
	Provider        string `json:"provider"`
	ModelID         string `json:"modelId"`
	RouteVersion    string `json:"routeVersion"`
	Verification    string `json:"verification"`
}
type generationProfile struct {
	ProfileID       string          `json:"profileId"`
	RevisionID      string          `json:"revisionId"`
	Stage           string          `json:"stage"`
	AspectProfile   string          `json:"aspectProfile"`
	EpisodeTargetMS int             `json:"episodeTargetMs"`
	ShotMinMS       int             `json:"shotMinMs"`
	ShotMaxMS       int             `json:"shotMaxMs"`
	VideoRoute      routeInput      `json:"videoRoute"`
	SpeechRoute     routeInput      `json:"speechRoute"`
	RetryPolicy     json.RawMessage `json:"retryPolicy"`
}
type reusableAsset struct {
	AssetID           string          `json:"assetId"`
	AssetVersionID    string          `json:"assetVersionId"`
	AssetType         string          `json:"assetType"`
	ScopeType         string          `json:"scopeType"`
	Filename          string          `json:"filename"`
	SHA256            string          `json:"sha256"`
	Bytes             int64           `json:"bytes"`
	MediaType         string          `json:"mediaType"`
	Width             int             `json:"width"`
	Height            int             `json:"height"`
	ArtifactURI       string          `json:"artifactUriAfterIngest"`
	LicenseSnapshotID string          `json:"licenseSnapshotId"`
	Provider          string          `json:"provider"`
	Model             string          `json:"model"`
	ResourceID        string          `json:"resourceId"`
	Speaker           string          `json:"speaker"`
	VoiceClone        bool            `json:"voiceClone"`
	Source            json.RawMessage `json:"source"`
	Roles             []string        `json:"roles"`
	LicenseState      string          `json:"licenseState"`
}
type licenseInput struct {
	ID                string     `json:"id"`
	SubjectType       string     `json:"subjectType"`
	SubjectRef        string     `json:"subjectRef"`
	LicenseID         string     `json:"licenseId"`
	PolicyStatus      string     `json:"policyStatus"`
	Territories       []string   `json:"territories"`
	CommercialUse     bool       `json:"commercialUse"`
	ExpiresAt         *time.Time `json:"expiresAt"`
	SourceURI         string     `json:"sourceUri"`
	ReviewInstruction string     `json:"reviewInstruction"`
}
type sharedPrompt struct {
	NegativePrompt   string      `json:"negativePrompt"`
	AssetVersionRefs []string    `json:"assetVersionRefs"`
	Output           outputInput `json:"output"`
}

type outputInput struct {
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	AspectRatio    string `json:"aspectRatio"`
	FPS            int    `json:"fps"`
	DurationMillis int    `json:"durationMillis"`
	Format         string `json:"format"`
	GenerateAudio  bool   `json:"generateAudio"`
}

func (o outputInput) providerSpec() providercontract.OutputSpec {
	return providercontract.OutputSpec{
		Width: o.Width, Height: o.Height, Resolution: fmt.Sprintf("%dp", o.Height),
		AspectRatio: o.AspectRatio, FPS: o.FPS, DurationMillis: o.DurationMillis,
		Format: o.Format, GenerateAudio: o.GenerateAudio,
	}
}

type shotInput struct {
	ShotID                     string          `json:"shotId"`
	DBShotID                   string          `json:"dbShotId"`
	Ordinal                    int             `json:"ordinal"`
	ShotSpecRevisionID         string          `json:"shotSpecRevisionId"`
	ShotSpecContentHash        string          `json:"shotSpecContentHash"`
	PromptSnapshotID           string          `json:"promptSnapshotId"`
	PromptSnapshotContentHash  string          `json:"promptSnapshotContentHash"`
	EffectiveContextSnapshotID string          `json:"effectiveContextSnapshotId"`
	RunID                      string          `json:"runId"`
	RunSpecDigest              string          `json:"runSpecDigest"`
	AttemptID                  string          `json:"attemptId"`
	IdempotencyKey             string          `json:"idempotencyKey"`
	Timeline                   json.RawMessage `json:"timeline"`
	Narrative                  json.RawMessage `json:"narrative"`
	Cinematography             json.RawMessage `json:"cinematography"`
	Continuity                 json.RawMessage `json:"continuity"`
	PositivePrompt             string          `json:"positivePrompt"`
	EstimatedVideoTokens       int64           `json:"estimatedVideoTokens"`
	PredictedAFPMilli          int64           `json:"predictedAfpMilli"`
	EstimatedCashMicros        int64           `json:"estimatedNonSubscriptionCashMicros"`
}
type safetyInput struct {
	ArtifactID    string    `json:"artifactId"`
	Filename      string    `json:"filename"`
	SHA256        string    `json:"sha256"`
	Bytes         int64     `json:"bytes"`
	ArtifactURI   string    `json:"artifactUriAfterIngest"`
	PolicyVersion string    `json:"policyVersion"`
	ValidUntil    time.Time `json:"validUntil"`
}
type generationPlanInput struct {
	ID                     string   `json:"id"`
	EpisodeRevisionID      string   `json:"episodeRevisionId"`
	ShotSpecRevisionIDs    []string `json:"shotSpecRevisionIds"`
	VideoBudgetApprovalID  string   `json:"videoBudgetApprovalId"`
	SpeechBudgetApprovalID string   `json:"speechBudgetApprovalId"`
	Gate2DecisionID        string   `json:"gate2DecisionId"`
	SafetyDecisionID       string   `json:"contentSafetyDecisionId"`
	SafetyPolicyVersion    string   `json:"contentSafetyPolicyVersion"`
	Territories            []string `json:"territories"`
	ProductForms           []string `json:"productForms"`
	MaximumCalls           int      `json:"maximumCalls"`
	MaximumVideoTokens     int64    `json:"maximumVideoTokens"`
	MaximumAFPMilli        int64    `json:"maximumAfpMilli"`
	MaximumCashMicros      int64    `json:"maximumCashMicros"`
	Currency               string   `json:"currency"`
}
type approvalInput struct {
	DecisionID             string     `json:"decisionId"`
	Gate                   string     `json:"gate"`
	RequestedDecision      string     `json:"requestedDecision"`
	ActorRole              string     `json:"actorRole"`
	BindingScope           []string   `json:"bindingScope"`
	PolicyVersion          string     `json:"policyVersion"`
	ValidUntil             *time.Time `json:"validUntil"`
	ApprovalID             string     `json:"approvalId"`
	ReviewType             string     `json:"reviewType"`
	BudgetScope            string     `json:"budgetScope"`
	GenerationPlanID       string     `json:"generationPlanId"`
	LimitMicros            int64      `json:"limitMicros"`
	Currency               string     `json:"currency"`
	AuthorizationCommentID string     `json:"authorizationCommentId"`
	ActorAuthorityBasis    string     `json:"actorAuthorityBasis"`
}
type postInput struct {
	EpisodeRevisionID         string          `json:"episodeRevisionId"`
	GenerationPlanID          string          `json:"generationPlanId"`
	RunIDs                    []string        `json:"runIds"`
	Evidence                  string          `json:"evidence"`
	SpeechProviderProfileID   string          `json:"speechProviderProfileId"`
	SpeechBudgetApprovalID    string          `json:"speechBudgetApprovalId"`
	SpeechBudgetMaximumMicros int64           `json:"speechBudgetMaximumMicros"`
	SpeechBudgetCurrency      string          `json:"speechBudgetCurrency"`
	Speaker                   string          `json:"speaker"`
	VoiceAssetVersionID       string          `json:"voiceAssetVersionId"`
	SubtitleLanguage          string          `json:"subtitleLanguage"`
	SubtitleEncoding          string          `json:"subtitleEncoding"`
	BurnSubtitles             bool            `json:"burnSubtitles"`
	IndependentDialogueTrack  bool            `json:"independentDialogueTrack"`
	BackgroundMusic           bool            `json:"backgroundMusic"`
	Output                    json.RawMessage `json:"output"`
	QualityThresholds         json.RawMessage `json:"qualityThresholds"`
	PersistProductTruth       bool            `json:"persistProductTruth"`
	TraceID                   string          `json:"traceId"`
}

type prepared struct {
	product                                                         productInput
	productBytes, sourceBytes, safetyBytes, visualBytes, voiceBytes []byte
	productHash, voiceHash, profileHash                             string
	visualAsset, voiceAsset                                         reusableAsset
	visualLicense, voiceLicense                                     licenseInput
	videoRoute, speechRoute                                         providercontract.ModelSnapshot
	videoCapabilityID, speechCapabilityID                           uuid.UUID
}

func Materialize(ctx context.Context, pool *pgxpool.Pool, cas *artifactstore.Store, plan stage1.Plan, files Files, approval Approval) (stage1.ExecutionPackage, Report, error) {
	if pool == nil || cas == nil {
		return stage1.ExecutionPackage{}, Report{}, errors.New("PostgreSQL pool and CAS are required")
	}
	if err := plan.Validate(); err != nil {
		return stage1.ExecutionPackage{}, Report{}, err
	}
	p, err := prepare(files, plan, approval)
	if err != nil {
		return stage1.ExecutionPackage{}, Report{}, err
	}
	casObjects := map[string]artifactstore.Artifact{}
	for name, data := range map[string][]byte{"product_input": p.productBytes, "source": p.sourceBytes, "safety": p.safetyBytes, "visual": p.visualBytes, "voice_descriptor": p.voiceBytes} {
		object, putErr := cas.Put(ctx, bytes.NewReader(data))
		if putErr != nil {
			return stage1.ExecutionPackage{}, Report{}, fmt.Errorf("ingest %s: %w", name, putErr)
		}
		casObjects[name] = object
	}
	package_, err := materializeDB(ctx, pool, plan, p, approval, casObjects)
	if err != nil {
		return stage1.ExecutionPackage{}, Report{}, err
	}
	if err := package_.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, Report{}, fmt.Errorf("validate materialized package: %w", err)
	}
	report, err := verify(ctx, pool, package_, p, approval, casObjects)
	return package_, report, err
}

func prepare(files Files, plan stage1.Plan, approval Approval) (prepared, error) {
	if strings.TrimSpace(approval.CommentID) == "" || strings.TrimSpace(approval.ActorID) == "" || !approval.ValidUntil.After(time.Now().UTC()) {
		return prepared{}, errors.New("a current explicit ADMIN approval is required")
	}
	read := func(path string, maximum int64) ([]byte, error) {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if info.Size() > maximum {
			return nil, errors.New("input file exceeds the materializer limit")
		}
		return os.ReadFile(path)
	}
	productBytes, err := read(files.Product, 4<<20)
	if err != nil {
		return prepared{}, fmt.Errorf("read product input: %w", err)
	}
	var product productInput
	if err := json.Unmarshal(productBytes, &product); err != nil {
		return prepared{}, fmt.Errorf("decode product input: %w", err)
	}
	sourceBytes, err := read(files.Source, 2<<20)
	if err != nil {
		return prepared{}, fmt.Errorf("read source: %w", err)
	}
	safetyBytes, err := read(files.Safety, 2<<20)
	if err != nil {
		return prepared{}, fmt.Errorf("read safety evidence: %w", err)
	}
	visualBytes, err := read(files.Visual, 16<<20)
	if err != nil {
		return prepared{}, fmt.Errorf("read visual evidence: %w", err)
	}
	productHash := sum(productBytes)
	if product.SchemaVersion != productSchema || product.BatchID != batchID || product.BatchID != plan.BatchID || len(product.Shots) != stage1.RequiredPrimaryJobs {
		return prepared{}, errors.New("product input is not the approved FLO-104 sample 1 schema")
	}
	if sum(sourceBytes) != product.Source.SHA256 || int64(len(sourceBytes)) != product.Source.Bytes || sum(safetyBytes) != product.ContentSafetyEvidence.SHA256 || int64(len(safetyBytes)) != product.ContentSafetyEvidence.Bytes {
		return prepared{}, errors.New("source or safety evidence differs from the fixed product input")
	}
	var visual reusableAsset
	var voice reusableAsset
	for _, asset := range product.ReusableAssets {
		if asset.AssetType == "IMAGE" {
			visual = asset
		}
		if asset.AssetType == "VOICE" {
			voice = asset
		}
	}
	if sum(visualBytes) != visual.SHA256 || int64(len(visualBytes)) != visual.Bytes {
		return prepared{}, errors.New("visual evidence differs from the fixed product input")
	}
	var visualLicense, voiceLicense licenseInput
	for _, license := range product.LicenseSnapshots {
		switch license.ID {
		case product.Reserved.VisualLicenseSnapshotID:
			visualLicense = license
		case product.Reserved.VoiceLicenseSnapshotID:
			voiceLicense = license
		}
	}
	if visual.AssetID == "" || voice.AssetID == "" || visualLicense.ID == "" || voiceLicense.ID == "" {
		return prepared{}, errors.New("fixed visual/voice assets and licenses are required")
	}
	if product.GenerationProfile.VideoRoute.ModelID != stage1.FormalVideoModel || product.GenerationProfile.VideoRoute.Verification != providercontract.PendingKey || product.GenerationProfile.SpeechRoute.ModelID != "doubao-seed-tts-2.0" || product.GenerationProfile.SpeechRoute.Verification != providercontract.PendingKey {
		return prepared{}, errors.New("product input routes are outside the approved pending_key boundary")
	}
	if product.GenerationPlan.MaximumCalls != stage1.MaximumNewProviderJobs || product.GenerationPlan.MaximumVideoTokens != plan.MaximumVideoTokens || product.GenerationPlan.MaximumAFPMilli != plan.MonthlyMaximumAFPMilli || product.GenerationPlan.MaximumCashMicros != plan.MaximumCashMicros || int64(product.DialogueSummary.UnicodeCharacterCount) > plan.MaximumDialogueCharacters {
		return prepared{}, errors.New("product input exceeds an approved Stage 1 limit")
	}
	if !product.ContentSafetyEvidence.ValidUntil.Equal(approval.ValidUntil) {
		return prepared{}, errors.New("ADMIN approval validity does not match the frozen safety package")
	}
	if err := validateIDs(product); err != nil {
		return prepared{}, err
	}
	voiceDescriptor := map[string]any{"schemaVersion": "v1", "provider": voice.Provider, "model": voice.Model, "resourceId": voice.ResourceID, "speaker": voice.Speaker, "voiceClone": voice.VoiceClone, "inputPackageHash": productHash}
	voiceBytes, err := controlplane.CanonicalJSON(voiceDescriptor)
	if err != nil {
		return prepared{}, err
	}
	profileHash, _ := digest(product.GenerationProfile)
	videoHash, _ := digest(map[string]any{"route": product.GenerationProfile.VideoRoute, "providerProfileId": product.Reserved.VideoProviderProfileID, "inputPackageHash": productHash})
	speechHash, _ := digest(map[string]any{"route": product.GenerationProfile.SpeechRoute, "providerProfileId": product.Reserved.SpeechProviderProfileID, "inputPackageHash": productHash})
	videoRoute := modelRoute(product.GenerationProfile.VideoRoute, videoHash)
	speechRoute := modelRoute(product.GenerationProfile.SpeechRoute, speechHash)
	return prepared{product: product, productBytes: productBytes, sourceBytes: sourceBytes, safetyBytes: safetyBytes, visualBytes: visualBytes, voiceBytes: voiceBytes, productHash: productHash, voiceHash: sum(voiceBytes), profileHash: profileHash, visualAsset: visual, voiceAsset: voice, visualLicense: visualLicense, voiceLicense: voiceLicense, videoRoute: videoRoute, speechRoute: speechRoute, videoCapabilityID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("capability:"+videoHash)), speechCapabilityID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("capability:"+speechHash))}, nil
}

func modelRoute(input routeInput, hash string) providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{CapabilityAlias: input.CapabilityAlias, Provider: "VOLCENGINE", ModelID: input.ModelID, RouteVersion: input.RouteVersion, CapabilityHash: hash, Verification: providercontract.PendingKey}
}

func validateIDs(product productInput) error {
	values := []string{product.Reserved.SeriesID, product.Reserved.SourceRevisionID, product.Reserved.EpisodeID, product.Reserved.EpisodeRevisionID, product.Reserved.SceneID, product.Reserved.SceneRevisionID, product.Reserved.ScriptRevisionID, product.Reserved.StoryboardRevisionID, product.Reserved.GenerationProfileID, product.Reserved.GenerationProfileRevisionID, product.Reserved.GenerationPlanID, product.Reserved.VisualAssetID, product.Reserved.VisualAssetVersionID, product.Reserved.VisualLicenseSnapshotID, product.Reserved.VoiceAssetID, product.Reserved.VoiceAssetVersionID, product.Reserved.VoiceLicenseSnapshotID, product.Reserved.SafetyEvidenceArtifactID, product.Reserved.G1DecisionID, product.Reserved.G2DecisionID, product.Reserved.SafetyDecisionID, product.Reserved.VideoBudgetApprovalID, product.Reserved.SpeechBudgetApprovalID, product.Reserved.VideoProviderProfileID, product.Reserved.SpeechProviderProfileID, product.Reserved.SeriesContextRevisionID, product.Reserved.EpisodeContextRevisionID, product.Reserved.SceneContextRevisionID}
	seen := map[string]struct{}{}
	for _, shot := range product.Shots {
		values = append(values, shot.DBShotID, shot.ShotSpecRevisionID, shot.PromptSnapshotID, shot.EffectiveContextSnapshotID, shot.RunID)
	}
	for _, value := range values {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("reserved identifier %q is not a UUID", value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("duplicate reserved identifier %s", value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func materializeDB(ctx context.Context, pool *pgxpool.Pool, plan stage1.Plan, p prepared, approval Approval, objects map[string]artifactstore.Artifact) (stage1.ExecutionPackage, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	defer tx.Rollback(ctx)
	var existing []byte
	var existingInputHash, existingApprovalComment string
	err = tx.QueryRow(ctx, `SELECT payload->'executionPackage', payload->>'inputPackageHash', payload->>'approvalCommentId' FROM video_pipeline.audit_events WHERE action='stage1.execution_package.materialized' AND aggregate_id=$1 ORDER BY occurred_at DESC LIMIT 1 FOR SHARE`, mustUUID(p.product.Reserved.GenerationPlanID)).Scan(&existing, &existingInputHash, &existingApprovalComment)
	if err == nil {
		if existingInputHash != p.productHash || existingApprovalComment != approval.CommentID {
			return stage1.ExecutionPackage{}, errors.New("generation plan is already bound to another imported package or approval")
		}
		var package_ stage1.ExecutionPackage
		if json.Unmarshal(existing, &package_) != nil || package_.Validate(plan) != nil {
			return stage1.ExecutionPackage{}, errors.New("existing materialization audit is invalid")
		}
		return package_, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return stage1.ExecutionPackage{}, err
	}
	now := time.Now().UTC()
	ids := p.product.Reserved
	providerOutput := p.product.SharedPrompt.Output.providerSpec()
	exec := func(label, query string, args ...any) error {
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return nil
	}

	// Provider snapshots are product truth only. The command never constructs an Adapter client.
	if err := exec("video provider profile", `INSERT INTO video_pipeline.provider_profiles (id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash) VALUES ($1,'VOLCENGINE','FLO-104 Agent Plan video','internal://volcengine-provider','env:ARK_API_KEY',true,'LIVE','READY',$2)`, mustUUID(ids.VideoProviderProfileID), p.videoRoute.CapabilityHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("speech provider profile", `INSERT INTO video_pipeline.provider_profiles (id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash) VALUES ($1,'VOLCENGINE','FLO-104 Agent Plan speech','internal://volcengine-tts-provider','env:ARK_API_KEY',true,'LIVE','READY',$2)`, mustUUID(ids.SpeechProviderProfileID), p.speechRoute.CapabilityHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	limits := map[string]any{"allowedTerritories": p.product.GenerationPlan.Territories, "productForms": p.product.GenerationPlan.ProductForms, "contentSafetyPolicyVersions": []string{p.product.ContentSafetyEvidence.PolicyVersion}, "remainingCalls": p.product.GenerationPlan.MaximumCalls, "unitPriceMicros": 1, "accountingNote": "Agent Plan subscription; one-micro-per-second reservation floor, actual cost comes from Provider usage evidence"}
	for _, cap := range []struct {
		id                                   uuid.UUID
		profile, alias, model, version, hash string
		inputs                               []string
	}{{p.videoCapabilityID, ids.VideoProviderProfileID, "video.primary", p.videoRoute.ModelID, p.videoRoute.RouteVersion, p.videoRoute.CapabilityHash, []string{"prompt", "reference_image", "reference_audio"}}, {p.speechCapabilityID, ids.SpeechProviderProfileID, "speech.primary", p.speechRoute.ModelID, p.speechRoute.RouteVersion, p.speechRoute.CapabilityHash, []string{"text", "voice_profile"}}} {
		if err := exec("provider capability", `INSERT INTO video_pipeline.provider_capability_snapshots (id,provider_profile_id,capability_alias,model_id,route_version,supported_inputs,limits,pricing_rule_version,capability_hash,status,effective_at,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,'agent-plan-subscription-v1',$8,'ACTIVE',$9,$10)`, cap.id, mustUUID(cap.profile), cap.alias, cap.model, cap.version, cap.inputs, limits, cap.hash, now, approval.ValidUntil); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	if err := exec("generation profile", `INSERT INTO video_pipeline.generation_profiles (id,profile_id,revision,schema_version,status,stage,aspect_profile,episode_target_ms,shot_min_ms,shot_max_ms,capability_routes,media_processing,render_defaults,qc_thresholds,retry_policy,budget_policy,license_policy,content_hash,created_by) VALUES ($1,$2,1,'v1','ACTIVE',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, mustUUID(ids.GenerationProfileRevisionID), mustUUID(ids.GenerationProfileID), p.product.GenerationProfile.Stage, p.product.GenerationProfile.AspectProfile, p.product.GenerationProfile.EpisodeTargetMS, p.product.GenerationProfile.ShotMinMS, p.product.GenerationProfile.ShotMaxMS, map[string]any{"video": p.videoRoute, "speech": p.speechRoute}, map[string]any{"postProduction": p.product.PostProduction}, providerOutput, p.product.PostProduction.QualityThresholds, p.product.GenerationProfile.RetryPolicy, p.product.AuthorizationBoundary, map[string]any{"territories": p.product.GenerationPlan.Territories, "productForms": p.product.GenerationPlan.ProductForms}, p.profileHash, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("series", `INSERT INTO video_pipeline.series (id,title,status,default_profile_id,rights_declaration,created_by) VALUES ($1,$2,'ACTIVE',$3,$4,$5)`, mustUUID(ids.SeriesID), p.product.Source.Title, mustUUID(ids.GenerationProfileRevisionID), p.product.Source.Rights, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("source", `INSERT INTO video_pipeline.source_revisions (id,series_id,revision,status,content_hash,artifact_uri,language,rights_snapshot,created_by) VALUES ($1,$2,1,'APPROVED',$3,$4,$5,$6,$7)`, mustUUID(ids.SourceRevisionID), mustUUID(ids.SeriesID), objects["source"].Digest, objects["source"].URI, p.product.Source.Language, p.product.Source.Rights, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("episode", `INSERT INTO video_pipeline.episodes (id,series_id,ordinal,title) VALUES ($1,$2,1,$3)`, mustUUID(ids.EpisodeID), mustUUID(ids.SeriesID), p.product.Source.Title); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	episodeHash, _ := digest(map[string]any{"sourceHash": objects["source"].Digest, "creativeBrief": p.product.CreativeBrief, "dialogue": p.product.DialogueSummary, "inputPackageHash": p.productHash})
	if err := exec("episode revision", `INSERT INTO video_pipeline.episode_revisions (id,episode_id,revision,status,target_duration_ms,content_hash,created_by) VALUES ($1,$2,1,'G2_APPROVED',$3,$4,$5)`, mustUUID(ids.EpisodeRevisionID), mustUUID(ids.EpisodeID), p.product.GenerationProfile.EpisodeTargetMS, episodeHash, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("scene", `INSERT INTO video_pipeline.scenes (id,episode_id,ordinal) VALUES ($1,$2,1)`, mustUUID(ids.SceneID), mustUUID(ids.EpisodeID)); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	sceneHash, _ := digest(map[string]any{"creativeBrief": p.product.CreativeBrief, "shots": p.product.Shots, "inputPackageHash": p.productHash})
	if err := exec("scene revision", `INSERT INTO video_pipeline.scene_revisions (id,scene_id,episode_revision_id,revision,status,content_hash,payload,created_by) VALUES ($1,$2,$3,1,'APPROVED',$4,$5,$6)`, mustUUID(ids.SceneRevisionID), mustUUID(ids.SceneID), mustUUID(ids.EpisodeRevisionID), sceneHash, map[string]any{"creativeBrief": p.product.CreativeBrief}, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	scriptHash, _ := digest(map[string]any{"dialogue": p.product.DialogueSummary, "shots": p.product.Shots, "inputPackageHash": p.productHash})
	storyboardHash, _ := digest(map[string]any{"scriptHash": scriptHash, "shots": p.product.Shots, "inputPackageHash": p.productHash})
	if err := exec("script", `INSERT INTO video_pipeline.episode_script_revisions (id,episode_id,revision,status,schema_version,payload,content_hash,created_by) VALUES ($1,$2,1,'APPROVED','v1',$3,$4,$5)`, mustUUID(ids.ScriptRevisionID), mustUUID(ids.EpisodeID), map[string]any{"dialogue": p.product.DialogueSummary, "shots": p.product.Shots}, scriptHash, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("storyboard", `INSERT INTO video_pipeline.storyboard_revisions (id,episode_id,script_revision_id,revision,status,content_hash,created_by) VALUES ($1,$2,$3,1,'APPROVED',$4,$5)`, mustUUID(ids.StoryboardRevisionID), mustUUID(ids.EpisodeID), mustUUID(ids.ScriptRevisionID), storyboardHash, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}

	visualLicense := p.visualLicense
	voiceLicense := p.voiceLicense
	visualLicenseHash, _ := digest(map[string]any{"license": visualLicense, "inputPackageHash": p.productHash})
	voiceLicenseHash, _ := digest(map[string]any{"license": voiceLicense, "effectivePolicyStatus": "ALLOWED", "adminApproval": approval.CommentID, "validUntil": approval.ValidUntil, "inputPackageHash": p.productHash})
	if err := exec("visual license", `INSERT INTO video_pipeline.license_snapshots (id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,expires_at,source_uri,reviewed_by,reviewed_at) VALUES ($1,$2,$3,$4,$5,'ALLOWED',$6,$7,$8,$9,$10,$11)`, mustUUID(ids.VisualLicenseSnapshotID), visualLicense.SubjectType, visualLicense.SubjectRef, visualLicense.LicenseID, visualLicenseHash, visualLicense.Territories, visualLicense.CommercialUse, visualLicense.ExpiresAt, visualLicense.SourceURI, approval.ActorID, now); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("voice license", `INSERT INTO video_pipeline.license_snapshots (id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,expires_at,source_uri,reviewed_by,reviewed_at) VALUES ($1,'VOICE',$2,$3,$4,'ALLOWED',$5,true,$6,$7,$8,$9)`, mustUUID(ids.VoiceLicenseSnapshotID), voiceLicense.SubjectRef, voiceLicense.LicenseID, voiceLicenseHash, voiceLicense.Territories, approval.ValidUntil, voiceLicense.SourceURI, approval.ActorID, now); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("visual asset", `INSERT INTO video_pipeline.assets (id,series_id,asset_type,scope_type,scope_id) VALUES ($1,$2,'IMAGE','SERIES',$2)`, mustUUID(ids.VisualAssetID), mustUUID(ids.SeriesID)); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("visual version", `INSERT INTO video_pipeline.asset_versions (id,asset_id,revision,status,content_hash,artifact_uri,media_type,dimensions,source_ref,license_snapshot_id,created_by) VALUES ($1,$2,1,'APPROVED',$3,$4,'image/png',$5,$6,$7,$8)`, mustUUID(ids.VisualAssetVersionID), mustUUID(ids.VisualAssetID), objects["visual"].Digest, objects["visual"].URI, map[string]any{"width": p.visualAsset.Width, "height": p.visualAsset.Height}, "flo104-product-input:"+p.productHash, mustUUID(ids.VisualLicenseSnapshotID), p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("voice asset", `INSERT INTO video_pipeline.assets (id,series_id,asset_type,scope_type,scope_id) VALUES ($1,$2,'VOICE','SERIES',$2)`, mustUUID(ids.VoiceAssetID), mustUUID(ids.SeriesID)); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("voice version", `INSERT INTO video_pipeline.asset_versions (id,asset_id,revision,status,content_hash,artifact_uri,media_type,dimensions,source_ref,license_snapshot_id,created_by) VALUES ($1,$2,1,'APPROVED',$3,$4,'audio/x-voice-profile+json',$5,$6,$7,$8)`, mustUUID(ids.VoiceAssetVersionID), mustUUID(ids.VoiceAssetID), objects["voice_descriptor"].Digest, objects["voice_descriptor"].URI, map[string]any{"speaker": p.product.PostProduction.Speaker}, "flo104-product-input:"+p.productHash, mustUUID(ids.VoiceLicenseSnapshotID), p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("safety artifact", `INSERT INTO video_pipeline.artifacts (id,content_hash,artifact_uri,media_type,size_bytes,media_spec,status) VALUES ($1,$2,$3,'application/json',$4,$5,'ACTIVE')`, mustUUID(ids.SafetyEvidenceArtifactID), objects["safety"].Digest, objects["safety"].URI, objects["safety"].Size, map[string]any{"kind": "content_safety_evidence", "policyVersion": p.product.ContentSafetyEvidence.PolicyVersion, "inputPackageHash": p.productHash}); err != nil {
		return stage1.ExecutionPackage{}, err
	}

	// Four-level context is required by the paid boundary. The product reserves
	// the reusable levels; deterministic shot-level revisions are derived here.
	seriesContextHash, _ := digest(map[string]any{"scope": "SERIES", "creativeBrief": p.product.CreativeBrief, "inputPackageHash": p.productHash})
	episodeContextHash, _ := digest(map[string]any{"scope": "EPISODE", "dialogue": p.product.DialogueSummary, "inputPackageHash": p.productHash})
	sceneContextHash, _ := digest(map[string]any{"scope": "SCENE", "sceneRevisionHash": sceneHash, "inputPackageHash": p.productHash})
	for _, c := range []struct {
		id, scope, scopeID, hash string
		payload                  any
	}{{ids.SeriesContextRevisionID, "SERIES", ids.SeriesID, seriesContextHash, map[string]any{"creativeBrief": p.product.CreativeBrief}}, {ids.EpisodeContextRevisionID, "EPISODE", ids.EpisodeID, episodeContextHash, map[string]any{"dialogue": p.product.DialogueSummary}}, {ids.SceneContextRevisionID, "SCENE", ids.SceneID, sceneContextHash, map[string]any{"sceneRevisionHash": sceneHash}}} {
		if err := exec("context", `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by) VALUES ($1,$2,$3,$4,1,'APPROVED','v1','stage1-product-input-v1',$5,$6,$7)`, mustUUID(c.id), mustUUID(ids.SeriesID), c.scope, mustUUID(c.scopeID), c.payload, c.hash, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	// Exact gate decisions and budget reviews are inserted before the dependent
	// shot/run rows. Every binding uses the database-derived immutable hash.
	for _, d := range []struct{ id, gate, role, explanation string }{{ids.G1DecisionID, "G1", "PRODUCER", "Approved fixed FLO-104 sample 1 script"}, {ids.G2DecisionID, "G2", "PRODUCER", "Approved fixed FLO-104 sample 1 storyboard and shots"}, {ids.SafetyDecisionID, "SAFETY", actorRole, string(mustJSON(map[string]any{"policyVersion": p.product.ContentSafetyEvidence.PolicyVersion, "evidenceHash": objects["safety"].Digest, "validUntil": approval.ValidUntil, "explanation": "ADMIN-approved offline safety evidence; " + approval.CommentID}))}} {
		if err := exec("approval decision", `INSERT INTO video_pipeline.approval_decisions (id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id) VALUES ($1,$2,$3,$4,'APPROVED','FLO104_FIXED_SAMPLE1',$5,$6,$7,$8,$9)`, mustUUID(d.id), mustUUID(ids.SeriesID), mustUUID(ids.EpisodeID), d.gate, d.explanation, approval.ActorID, d.role, now, p.product.PostProduction.TraceID); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	bind := func(decision, typ, id, hash string) error {
		return exec("approval binding", `INSERT INTO video_pipeline.approval_bindings (decision_id,object_type,revision_id,content_hash) VALUES ($1,$2,$3,$4)`, mustUUID(decision), typ, mustUUID(id), hash)
	}
	if err := bind(ids.G1DecisionID, "EPISODE_REVISION", ids.EpisodeRevisionID, episodeHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := bind(ids.G1DecisionID, "SCRIPT_REVISION", ids.ScriptRevisionID, scriptHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := bind(ids.G2DecisionID, "EPISODE_REVISION", ids.EpisodeRevisionID, episodeHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := bind(ids.G2DecisionID, "STORYBOARD_REVISION", ids.StoryboardRevisionID, storyboardHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := bind(ids.SafetyDecisionID, "EPISODE_REVISION", ids.EpisodeRevisionID, episodeHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := bind(ids.SafetyDecisionID, "ARTIFACT", ids.SafetyEvidenceArtifactID, objects["safety"].Digest); err != nil {
		return stage1.ExecutionPackage{}, err
	}

	assetIDs := []uuid.UUID{mustUUID(ids.VisualAssetVersionID), mustUUID(ids.VoiceAssetVersionID)}
	contextBase := []uuid.UUID{mustUUID(ids.SeriesContextRevisionID), mustUUID(ids.EpisodeContextRevisionID), mustUUID(ids.SceneContextRevisionID)}
	jobs := make([]stage1.FrozenJob, 0, len(p.product.Shots))
	runIDs := make([]string, 0, len(p.product.Shots))
	shotHashes := map[string]string{}
	for _, shot := range p.product.Shots {
		shotHash, _ := digest(map[string]any{"shot": shot, "profileHash": p.profileHash, "inputPackageHash": p.productHash})
		shotHashes[shot.ShotSpecRevisionID] = shotHash
		shotContextID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("flo104-shot-context:"+shot.ShotSpecRevisionID+":"+p.productHash))
		shotContextHash, _ := digest(map[string]any{"scope": "SHOT", "shot": shot, "inputPackageHash": p.productHash})
		if err := exec("shot", `INSERT INTO video_pipeline.shots (id,scene_id,ordinal) VALUES ($1,$2,$3)`, mustUUID(shot.DBShotID), mustUUID(ids.SceneID), shot.Ordinal); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("shot context", `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by) VALUES ($1,$2,'SHOT',$3,1,'APPROVED','v1','stage1-product-input-v1',$4,$5,$6)`, shotContextID, mustUUID(ids.SeriesID), mustUUID(shot.DBShotID), map[string]any{"timeline": shot.Timeline, "narrative": shot.Narrative, "cinematography": shot.Cinematography, "continuity": shot.Continuity}, shotContextHash, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		contexts := append(append([]uuid.UUID{}, contextBase...), shotContextID)
		contextHashes := map[string]string{"context:series": seriesContextHash, "context:episode": episodeContextHash, "context:scene": sceneContextHash, "context:shot": shotContextHash}
		effectiveHash, _ := digest(map[string]any{"contextRevisionIds": contexts, "contextHashes": contextHashes, "inputPackageHash": p.productHash})
		if err := exec("shot spec", `INSERT INTO video_pipeline.shot_spec_revisions (id,shot_id,storyboard_revision_id,revision,lifecycle_state,freshness,duration_ms,aspect_profile,fps,width,height,cast_count,primary_action_count,narrative,asset_version_refs,context_revision_ids,effective_context_hash,continuity,cinematography,generation_profile_id,gate2_decision_id,content_hash,created_by) VALUES ($1,$2,$3,1,'READY','FRESH',5000,$4,24,1280,720,1,1,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, mustUUID(shot.ShotSpecRevisionID), mustUUID(shot.DBShotID), mustUUID(ids.StoryboardRevisionID), p.product.GenerationProfile.AspectProfile, shot.Narrative, assetIDs, contexts, effectiveHash, shot.Continuity, shot.Cinematography, mustUUID(ids.GenerationProfileRevisionID), mustUUID(ids.G2DecisionID), shotHash, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := bind(ids.G2DecisionID, "SHOT_SPEC_REVISION", shot.ShotSpecRevisionID, shotHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := bind(ids.SafetyDecisionID, "SHOT_SPEC_REVISION", shot.ShotSpecRevisionID, shotHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("effective context", `INSERT INTO video_pipeline.effective_context_snapshots (id,shot_spec_revision_id,schema_version,resolver_version,context_revision_ids,normalized_payload,content_hash) VALUES ($1,$2,'v1','stage1-product-input-v1',$3,$4,$5)`, mustUUID(shot.EffectiveContextSnapshotID), mustUUID(shot.ShotSpecRevisionID), contexts, map[string]any{"contextHashes": contextHashes, "inputPackageHash": p.productHash}, effectiveHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		inputHashes := map[string]string{"shot_spec": shotHash, "generation_profile": p.profileHash, "context:series": seriesContextHash, "context:episode": episodeContextHash, "context:scene": sceneContextHash, "context:shot": shotContextHash, "asset:" + ids.VisualAssetVersionID: objects["visual"].Digest, "asset:" + ids.VoiceAssetVersionID: objects["voice_descriptor"].Digest}
		promptHash, err := repository.ImportedPromptHash(shot.ShotSpecRevisionID, ids.GenerationProfileRevisionID, effectiveHash, assetIDs, shot.PositivePrompt, p.product.SharedPrompt.NegativePrompt, providerOutput, inputHashes, p.productHash)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("prompt snapshot", `INSERT INTO video_pipeline.prompt_snapshots (id,shot_spec_revision_id,schema_version,compiler_version,prompt_template_ref,effective_context_snapshot_id,asset_version_refs,positive_prompt,negative_prompt,model_payload,normalized_input_hash,content_hash,output_spec,input_revision_hashes) VALUES ($1,$2,'v1','stage1-product-input-v1','flo104.sample1.product-input.v1',$3,$4,$5,$6,$7,$8,$8,$9,$10)`, mustUUID(shot.PromptSnapshotID), mustUUID(shot.ShotSpecRevisionID), mustUUID(shot.EffectiveContextSnapshotID), assetIDs, shot.PositivePrompt, p.product.SharedPrompt.NegativePrompt, map[string]any{"generationProfileRevisionId": ids.GenerationProfileRevisionID, "inputPackageHash": p.productHash}, promptHash, providerOutput, inputHashes); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		for _, in := range []struct {
			typ        string
			id         uuid.UUID
			hash, role string
		}{{"SHOT_SPEC", mustUUID(shot.ShotSpecRevisionID), shotHash, "primary-shot"}, {"GENERATION_PROFILE", mustUUID(ids.GenerationProfileRevisionID), p.profileHash, "generation-profile"}, {"CONTEXT", contexts[0], seriesContextHash, "context:series"}, {"CONTEXT", contexts[1], episodeContextHash, "context:episode"}, {"CONTEXT", contexts[2], sceneContextHash, "context:scene"}, {"CONTEXT", contexts[3], shotContextHash, "context:shot"}} {
			if err := exec("prompt input", `INSERT INTO video_pipeline.prompt_snapshot_inputs (prompt_snapshot_id,input_type,input_revision_id,input_hash,dependency_role) VALUES ($1,$2,$3,$4,$5)`, mustUUID(shot.PromptSnapshotID), in.typ, in.id, in.hash, in.role); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		for index, a := range []struct{ id, hash, role string }{{ids.VisualAssetVersionID, objects["visual"].Digest, "reference_image"}, {ids.VoiceAssetVersionID, objects["voice_descriptor"].Digest, "reference_audio"}} {
			if err := exec("prompt asset", `INSERT INTO video_pipeline.prompt_snapshot_assets (prompt_snapshot_id,alias,asset_version_id,asset_hash,provider_role) VALUES ($1,$2,$3,$4,$5)`, mustUUID(shot.PromptSnapshotID), fmt.Sprintf("asset-%03d", index+1), mustUUID(a.id), a.hash, a.role); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		if err := exec("prompt import audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','prompt_snapshot.imported','PROMPT_SNAPSHOT',$4,'FLO104_FIXED_SAMPLE1',$5,$6)`, uuid.NewSHA1(mustUUID(shot.PromptSnapshotID), []byte("import-audit")), now, approval.ActorID, mustUUID(shot.PromptSnapshotID), p.product.PostProduction.TraceID, map[string]any{"inputPackageHash": p.productHash, "originalPromptHash": shot.PromptSnapshotContentHash, "derivedPromptHash": promptHash, "approvalCommentId": approval.CommentID}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		runDigest, err := repository.GenerationRunSpecDigest(shot.ShotSpecRevisionID, shot.PromptSnapshotID, promptHash, ids.GenerationProfileRevisionID, ids.GenerationPlanID, p.videoRoute, 1)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("generation run", `INSERT INTO video_pipeline.generation_runs (id,shot_spec_revision_id,prompt_snapshot_id,generation_profile_id,temporal_workflow_id,run_spec_digest,creative_attempt,state,dry_run,budget_approval_id,trace_id,created_by) VALUES ($1,$2,$3,$4,$5,$6,1,'VALIDATED',false,$7,$8,$9)`, mustUUID(shot.RunID), mustUUID(shot.ShotSpecRevisionID), mustUUID(shot.PromptSnapshotID), mustUUID(ids.GenerationProfileRevisionID), "flo104-stage1-"+shot.RunID, runDigest, ids.VideoBudgetApprovalID, p.product.PostProduction.TraceID, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		attemptUUID := uuid.NewSHA1(mustUUID(shot.RunID), []byte("attempt:1"))
		if err := exec("generation attempt", `INSERT INTO video_pipeline.generation_attempts (id,generation_run_id,sequence,attempt_kind,state,input_hash,model_snapshot,parameter_diff) VALUES ($1,$2,1,'PROVIDER_REQUEST','VALIDATED',$3,$4,$5)`, attemptUUID, mustUUID(shot.RunID), runDigest, p.videoRoute, map[string]any{"originalRunSpecDigest": shot.RunSpecDigest, "inputPackageHash": p.productHash}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		jobs = append(jobs, stage1.FrozenJob{ShotID: shot.ShotID, ShotSpecRevisionID: shot.ShotSpecRevisionID, AttemptID: shot.AttemptID, IdempotencyKey: shot.IdempotencyKey, Run: orchestration.GenerationRunRef{RunID: shot.RunID, RunSpecDigest: runDigest, Attempt: 1}, PromptSnapshotID: shot.PromptSnapshotID, PromptSnapshotHash: promptHash, GenerationPlanID: ids.GenerationPlanID, BudgetApprovalID: ids.VideoBudgetApprovalID, BudgetMaximumMicros: p.product.GenerationPlan.MaximumCashMicros, BudgetCurrency: p.product.GenerationPlan.Currency, ProviderProfileID: ids.VideoProviderProfileID, Route: p.videoRoute, EstimatedVideoTokens: shot.EstimatedVideoTokens, PredictedAFPMilli: shot.PredictedAFPMilli, EstimatedNonSubscriptionCashMicros: shot.EstimatedCashMicros, WorkflowID: "flo104-stage1-" + shot.RunID, ActivityID: "submit-" + shot.ShotID, TraceID: p.product.PostProduction.TraceID})
		runIDs = append(runIDs, shot.RunID)
	}

	execPolicy := controlplane.ExecutionPolicy{TargetTerritory: p.product.GenerationPlan.Territories[0], ProductForm: p.product.GenerationPlan.ProductForms[0], ContentSafetyPolicyVersion: p.product.ContentSafetyEvidence.PolicyVersion, ContentSafetyDecisionID: ids.SafetyDecisionID}
	planHash, _ := digest(map[string]any{"inputPackageHash": p.productHash, "shotSpecRevisionIds": p.product.GenerationPlan.ShotSpecRevisionIDs, "route": p.videoRoute, "budget": p.product.GenerationPlan.MaximumCashMicros, "executionPolicy": execPolicy})
	planRecord := controlplane.GenerationPlan{GenerationPlanID: ids.GenerationPlanID, State: "READY", DryRun: false, ShotCount: len(p.product.Shots), ProviderCallCount: len(p.product.Shots), RouteSnapshot: controlplane.ModelRouteSnapshot{CapabilityAlias: p.videoRoute.CapabilityAlias, ProviderProfileID: ids.VideoProviderProfileID, Provider: p.videoRoute.Provider, ModelID: p.videoRoute.ModelID, RouteVersion: p.videoRoute.RouteVersion, CapabilityHash: p.videoRoute.CapabilityHash}, ExecutionPolicy: execPolicy, Estimate: controlplane.CostEstimate{UnitsMinimum: 50, UnitsMaximum: 50, Unit: "video_seconds", AmountMinimum: nil, AmountMaximum: nil, Currency: "CNY", PricingRuleVersion: "agent-plan-subscription-v1", ValidUntil: approval.ValidUntil}, SpeechBudgetLimit: &controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}, BudgetDecision: "APPROVED", PlanHash: planHash}
	if err := exec("plan operation", `INSERT INTO video_pipeline.operation_requests (id,operation_type,aggregate_type,aggregate_id,state,trace_id,requested_by) VALUES ($1,'CREATE_GENERATION_PLAN','SERIES',$2,'SUCCEEDED',$3,$4)`, mustUUID(ids.GenerationPlanID), mustUUID(ids.SeriesID), p.product.PostProduction.TraceID, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("plan idempotency", `INSERT INTO video_pipeline.idempotency_records (scope,idempotency_key,request_hash,operation_id,response_status,response_body,expires_at) VALUES ('stage1-materialize',$1,$2,$3,201,$4,$5)`, p.productHash, planHash, mustUUID(ids.GenerationPlanID), planRecord, approval.ValidUntil); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("plan audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','generation_plan.created','GENERATION_PLAN',$4,'FLO104_FIXED_SAMPLE1',$5,$6)`, uuid.NewSHA1(mustUUID(ids.GenerationPlanID), []byte("plan-audit")), now, approval.ActorID, mustUUID(ids.GenerationPlanID), p.product.PostProduction.TraceID, map[string]any{"seriesId": ids.SeriesID, "episodeRevisionId": ids.EpisodeRevisionID, "shotSpecRevisionIds": p.product.GenerationPlan.ShotSpecRevisionIDs, "candidatesPerShot": 1, "pricingRuleVersion": "agent-plan-subscription-v1", "planHash": planHash, "state": "READY", "budgetDecision": "APPROVED", "budgetLimit": controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}, "speechBudgetLimit": controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}, "executionPolicy": execPolicy, "inputPackageHash": p.productHash, "approvalCommentId": approval.CommentID}); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	for _, review := range []struct{ id, scope string }{{ids.VideoBudgetApprovalID, "VIDEO"}, {ids.SpeechBudgetApprovalID, "SPEECH"}} {
		if err := exec("budget review", `INSERT INTO video_pipeline.review_tasks (id,series_id,episode_id,review_type,state,reason_codes,assigned_role,decided_at,generation_plan_id,budget_scope,budget_limit_micros,budget_currency) VALUES ($1,$2,$3,'BUDGET','APPROVED',ARRAY['FLO104_FIXED_SAMPLE1'],'ADMIN',$4,$5,$6,$7,$8)`, mustUUID(review.id), mustUUID(ids.SeriesID), mustUUID(ids.EpisodeID), now, mustUUID(ids.GenerationPlanID), review.scope, p.product.GenerationPlan.MaximumCashMicros, p.product.GenerationPlan.Currency); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	post := orchestration.FinalizeEpisodeInput{EpisodeRevisionID: ids.EpisodeRevisionID, RunIDs: runIDs, GenerationPlanID: ids.GenerationPlanID, TraceID: p.product.PostProduction.TraceID, PersistProductTruth: true, Config: orchestration.PostProductionConfig{Enabled: true, Evidence: postproduction.EvidenceLive, SpeechRoute: p.speechRoute, SpeechProviderProfileID: ids.SpeechProviderProfileID, SpeechBudgetApprovalID: ids.SpeechBudgetApprovalID, SpeechBudgetMaximumMicros: p.product.PostProduction.SpeechBudgetMaximumMicros, SpeechBudgetCurrency: p.product.PostProduction.SpeechBudgetCurrency, SubtitleLanguage: p.product.PostProduction.SubtitleLanguage, BurnSubtitles: p.product.PostProduction.BurnSubtitles, EnforcePoCDuration: true}}
	package_, err := stage1.SealExecutionPackage(stage1.ExecutionPackage{SchemaVersion: stage1.ExecutionPackageSchemaVersion, BatchID: batchID, PrimaryJobs: jobs, PostProduction: post})
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := package_.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("materialization audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','stage1.execution_package.materialized','GENERATION_PLAN',$4,'FLO104_FIXED_SAMPLE1',$5,$6)`, uuid.NewSHA1(mustUUID(ids.GenerationPlanID), []byte("stage1-materialization")), now, approval.ActorID, mustUUID(ids.GenerationPlanID), p.product.PostProduction.TraceID, map[string]any{"inputPackageHash": p.productHash, "sourceHash": objects["source"].Digest, "safetyHash": objects["safety"].Digest, "visualHash": objects["visual"].Digest, "voiceDescriptorHash": objects["voice_descriptor"].Digest, "executionPackageHash": package_.ContentHash, "executionPackage": package_, "approvalCommentId": approval.CommentID, "approvalValidUntil": approval.ValidUntil, "originalShotHashes": originalHashes(p.product.Shots), "derivedShotHashes": shotHashes, "providerCalls": 0}); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	return package_, nil
}

func verify(ctx context.Context, pool *pgxpool.Pool, package_ stage1.ExecutionPackage, p prepared, approval Approval, objects map[string]artifactstore.Artifact) (Report, error) {
	counts := map[string]int64{}
	for name, query := range map[string]string{"shots": "SELECT count(*) FROM video_pipeline.shot_spec_revisions WHERE storyboard_revision_id='10400000-0000-4000-8000-000000000008'", "prompts": "SELECT count(*) FROM video_pipeline.prompt_snapshots WHERE compiler_version='stage1-product-input-v1'", "runs": "SELECT count(*) FROM video_pipeline.generation_runs WHERE trace_id='flo104-sample1-formal-v1'", "contexts": "SELECT count(*) FROM video_pipeline.context_revisions WHERE resolver_version='stage1-product-input-v1'"} {
		var count int64
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			return Report{}, err
		}
		counts[name] = count
	}
	productTruth := repository.NewForPool(pool)
	for _, job := range package_.PrimaryJobs {
		prompt, err := productTruth.ResolvePromptSnapshot(ctx, job.PromptSnapshotID)
		if err != nil {
			return Report{}, fmt.Errorf("resolve imported prompt %s: %w", job.PromptSnapshotID, err)
		}
		if prompt.Digest != job.PromptSnapshotHash || prompt.ID != job.PromptSnapshotID {
			return Report{}, errors.New("execution package prompt differs from PostgreSQL product truth")
		}
	}
	var providerJobs, reservations, cost int64
	if err := pool.QueryRow(ctx, `
		WITH package_runs AS (
			SELECT id FROM video_pipeline.generation_runs WHERE trace_id = $1
		), package_attempts AS (
			SELECT ga.id FROM video_pipeline.generation_attempts ga
			JOIN package_runs pr ON pr.id = ga.generation_run_id
		), package_jobs AS (
			SELECT pj.id, pj.budget_reservation_id FROM video_pipeline.provider_jobs pj
			JOIN package_attempts pa ON pa.id = pj.generation_attempt_id
		)
		SELECT
			(SELECT count(*) FROM package_jobs),
			(SELECT count(*) FROM video_pipeline.budget_reservations br
			 JOIN package_runs pr ON pr.id = br.generation_run_id),
			(SELECT count(*) FROM video_pipeline.cost_ledger cl
			 JOIN package_jobs pj ON pj.id = cl.provider_job_id)`,
		p.product.PostProduction.TraceID,
	).Scan(&providerJobs, &reservations, &cost); err != nil {
		return Report{}, err
	}
	if providerJobs != 0 || reservations != 0 || cost != 0 {
		return Report{}, errors.New("offline materialization created a paid-boundary record")
	}
	casMap := map[string]string{}
	for name, object := range objects {
		casMap[name] = object.Digest
	}
	return Report{SchemaVersion: "flo104.sample1.materialization-report.v1", BatchID: batchID, InputPackageHash: p.productHash, ExecutionPackageHash: package_.ContentHash, Counts: counts, CAS: casMap, ProviderCalls: 0, ProviderJobs: providerJobs, BudgetReservations: reservations, CostLedgerEntries: cost, ApprovalCommentID: approval.CommentID, ApprovalValidUntil: approval.ValidUntil}, nil
}

func digest(value any) (string, error) {
	payload, err := controlplane.CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return sum(payload), nil
}
func sum(data []byte) string          { value := sha256.Sum256(data); return hex.EncodeToString(value[:]) }
func mustUUID(value string) uuid.UUID { return uuid.MustParse(value) }
func mustJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
func originalHashes(shots []shotInput) map[string]map[string]string {
	result := map[string]map[string]string{}
	for _, shot := range shots {
		result[shot.ShotID] = map[string]string{"shotSpec": shot.ShotSpecContentHash, "prompt": shot.PromptSnapshotContentHash, "run": shot.RunSpecDigest}
	}
	return result
}

// CanonicalProductHash is exported for unit tests and offline verification.
func CanonicalProductHash(data []byte) string { return sum(data) }

// SortedJobIDs returns stable prompt-free evidence for reports.
func SortedJobIDs(package_ stage1.ExecutionPackage) []string {
	ids := make([]string, 0, len(package_.PrimaryJobs))
	for _, job := range package_.PrimaryJobs {
		ids = append(ids, job.IdempotencyKey)
	}
	sort.Strings(ids)
	return ids
}
