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
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/analyzerseal"
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
	productSchema       = "flo104.sample1.product-input.v1"
	batchID             = "flo104-sample-1"
	nativeProductSchema = stage1.NativeProductSchemaVersion
	actorRole           = "ADMIN"
)

type Files struct {
	Product      string
	Source       string
	Safety       string
	Visual       string
	AnalyzerRoot string
	AnalyzerSeal string
	CodeCommit   string
	BuildSHA256  string
}

type materializationPolicy struct {
	NativeOnly        bool
	BatchID           string
	ProductSchema     string
	ResolverVersion   string
	PromptTemplateRef string
	WorkflowPrefix    string
	ReasonCode        string
	ReportSchema      string
	DisplayName       string
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
	VideoBudgetState     string            `json:"videoBudgetState,omitempty"`
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
	Width          int                                  `json:"width"`
	Height         int                                  `json:"height"`
	AspectRatio    string                               `json:"aspectRatio"`
	FPS            int                                  `json:"fps"`
	DurationMillis int                                  `json:"durationMillis"`
	Format         string                               `json:"format"`
	GenerateAudio  bool                                 `json:"generateAudio"`
	AudioStrategy  providercontract.AudioStrategy       `json:"audioStrategy,omitempty"`
	AudioDelivery  providercontract.NativeAudioDelivery `json:"audioDelivery,omitempty"`
}

func (o outputInput) providerSpec() providercontract.OutputSpec {
	return providercontract.OutputSpec{
		Width: o.Width, Height: o.Height, Resolution: fmt.Sprintf("%dp", o.Height),
		AspectRatio: o.AspectRatio, FPS: o.FPS, DurationMillis: o.DurationMillis,
		Format: o.Format, GenerateAudio: o.GenerateAudio,
		AudioStrategy: o.AudioStrategy, AudioDelivery: o.AudioDelivery,
	}
}

type ambienceInput struct {
	Identity           string `json:"identity"`
	Version            string `json:"version"`
	ContinuityIntoNext bool   `json:"continuityIntoNext"`
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
	Ambience                   ambienceInput   `json:"ambience"`
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
	product                                                                        productInput
	productBytes, sourceBytes, safetyBytes, visualBytes, voiceBytes, analyzerBytes []byte
	productHash, voiceHash, profileHash                                            string
	visualAsset, voiceAsset                                                        reusableAsset
	visualLicense, voiceLicense                                                    licenseInput
	videoRoute, speechRoute                                                        providercontract.ModelSnapshot
	videoCapabilityID, speechCapabilityID                                          uuid.UUID
	policy                                                                         materializationPolicy
	analyzerEvidence                                                               analyzerseal.Evidence
	codeCommit, buildSHA256                                                        string
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
	inputs := map[string][]byte{
		"product_input": p.productBytes, "source": p.sourceBytes,
		"safety": p.safetyBytes, "visual": p.visualBytes,
	}
	if p.policy.NativeOnly {
		inputs["analyzer_seal"] = p.analyzerBytes
	} else {
		inputs["voice_descriptor"] = p.voiceBytes
	}
	casObjects := map[string]artifactstore.Artifact{}
	for name, data := range inputs {
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
	policy, err := policyFor(product, plan)
	if err != nil {
		return prepared{}, err
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
	if len(product.Shots) != stage1.RequiredPrimaryJobs {
		return prepared{}, errors.New("product input must freeze exactly ten primary shots")
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
	if visual.AssetID == "" || visualLicense.ID == "" {
		return prepared{}, errors.New("fixed visual asset and license are required")
	}
	if product.GenerationProfile.VideoRoute.ModelID != stage1.FormalVideoModel || product.GenerationProfile.VideoRoute.Verification != providercontract.PendingKey {
		return prepared{}, errors.New("product input routes are outside the approved pending_key boundary")
	}
	if policy.NativeOnly {
		if voice.AssetID != "" || voiceLicense.ID != "" || product.GenerationProfile.SpeechRoute != (routeInput{}) ||
			product.DialogueSummary.TTSAFPMilliEstimate != 0 || !nativePostEmpty(product.PostProduction) {
			return prepared{}, errors.New("FLO-154 native product must omit VOICE, Speech route/budget, and TTS estimates")
		}
		output := product.SharedPrompt.Output.providerSpec()
		if output.ResolvedAudioStrategy() != providercontract.AudioStrategyNativePreferred ||
			!output.GenerateAudio || output.AudioDelivery != providercontract.NativeAudioMix {
			return prepared{}, errors.New("FLO-154 native product must freeze native_preferred generateAudio=true native_mix")
		}
		if err := validateNativeShots(product.Shots); err != nil {
			return prepared{}, err
		}
	} else {
		if voice.AssetID == "" || voiceLicense.ID == "" ||
			product.GenerationProfile.SpeechRoute.ModelID != "doubao-seed-tts-2.0" ||
			product.GenerationProfile.SpeechRoute.Verification != providercontract.PendingKey {
			return prepared{}, errors.New("fixed FLO-104 voice asset, license, and Speech route are required")
		}
	}
	if product.GenerationPlan.MaximumCalls != stage1.MaximumNewProviderJobs || product.GenerationPlan.MaximumVideoTokens != plan.MaximumVideoTokens || product.GenerationPlan.MaximumAFPMilli != plan.MonthlyMaximumAFPMilli || product.GenerationPlan.MaximumCashMicros != plan.MaximumCashMicros || int64(product.DialogueSummary.UnicodeCharacterCount) > plan.MaximumDialogueCharacters {
		return prepared{}, errors.New("product input exceeds an approved Stage 1 limit")
	}
	if !product.ContentSafetyEvidence.ValidUntil.Equal(approval.ValidUntil) {
		return prepared{}, errors.New("ADMIN approval validity does not match the frozen safety package")
	}
	if err := validateIDs(product, policy.NativeOnly); err != nil {
		return prepared{}, err
	}
	var voiceBytes []byte
	var analyzerBytes []byte
	var analyzerEvidence analyzerseal.Evidence
	if policy.NativeOnly {
		if len(files.CodeCommit) != 40 || !validLowerHex(files.CodeCommit) || !validLowerDigest(files.BuildSHA256) {
			return prepared{}, errors.New("FLO-154 native materialization requires fixed code commit and build SHA-256")
		}
		_, analyzerEvidence, err = analyzerseal.Verify(files.AnalyzerRoot, files.AnalyzerSeal)
		if err != nil {
			return prepared{}, fmt.Errorf("verify FLO-154 analyzer seal: %w", err)
		}
		if analyzerEvidence.SealSHA256 != plan.NativeAudio.AnalyzerSealSHA256 {
			return prepared{}, errors.New("FLO-154 analyzer seal differs from the approved plan")
		}
		analyzerBytes, err = read(files.AnalyzerSeal, 1<<20)
		if err != nil {
			return prepared{}, fmt.Errorf("read FLO-154 analyzer seal: %w", err)
		}
	} else {
		voiceDescriptor := map[string]any{"schemaVersion": "v1", "provider": voice.Provider, "model": voice.Model, "resourceId": voice.ResourceID, "speaker": voice.Speaker, "voiceClone": voice.VoiceClone, "inputPackageHash": productHash}
		voiceBytes, err = controlplane.CanonicalJSON(voiceDescriptor)
		if err != nil {
			return prepared{}, err
		}
	}
	profileHash, _ := digest(product.GenerationProfile)
	videoHash, _ := digest(map[string]any{"route": product.GenerationProfile.VideoRoute, "providerProfileId": product.Reserved.VideoProviderProfileID, "inputPackageHash": productHash})
	videoRoute := modelRoute(product.GenerationProfile.VideoRoute, videoHash)
	result := prepared{
		product: product, productBytes: productBytes, sourceBytes: sourceBytes,
		safetyBytes: safetyBytes, visualBytes: visualBytes, voiceBytes: voiceBytes,
		analyzerBytes: analyzerBytes, productHash: productHash, voiceHash: sum(voiceBytes),
		profileHash: profileHash, visualAsset: visual, voiceAsset: voice,
		visualLicense: visualLicense, voiceLicense: voiceLicense, videoRoute: videoRoute,
		videoCapabilityID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("capability:"+videoHash)),
		policy:            policy, analyzerEvidence: analyzerEvidence,
		codeCommit: files.CodeCommit, buildSHA256: files.BuildSHA256,
	}
	if !policy.NativeOnly {
		speechHash, _ := digest(map[string]any{"route": product.GenerationProfile.SpeechRoute, "providerProfileId": product.Reserved.SpeechProviderProfileID, "inputPackageHash": productHash})
		result.speechRoute = modelRoute(product.GenerationProfile.SpeechRoute, speechHash)
		result.speechCapabilityID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("capability:"+speechHash))
	}
	return result, nil
}

func policyFor(product productInput, plan stage1.Plan) (materializationPolicy, error) {
	switch {
	case product.SchemaVersion == productSchema && product.BatchID == batchID &&
		product.BatchID == plan.BatchID && !plan.IsNativeOnly():
		return materializationPolicy{
			BatchID: batchID, ProductSchema: productSchema,
			ResolverVersion: "stage1-product-input-v1", PromptTemplateRef: productSchema,
			WorkflowPrefix: "flo104-stage1-", ReasonCode: "FLO104_FIXED_SAMPLE1",
			ReportSchema: "flo104.sample1.materialization-report.v1",
			DisplayName:  "FLO-104 Agent Plan video",
		}, nil
	case product.SchemaVersion == nativeProductSchema && product.BatchID == plan.BatchID &&
		plan.IsNativeOnly():
		return materializationPolicy{
			NativeOnly: true, BatchID: product.BatchID, ProductSchema: nativeProductSchema,
			ResolverVersion: "flo154-native-materializer-v1", PromptTemplateRef: nativeProductSchema,
			WorkflowPrefix: "flo154-native-", ReasonCode: "FLO154_NATIVE_SAMPLE1",
			ReportSchema: "flo154.native-materialization-report.v1",
			DisplayName:  "FLO-154 Agent Plan native video",
		}, nil
	default:
		return materializationPolicy{}, errors.New("product input schema/batch differs from the approved readiness plan")
	}
}

func nativePostEmpty(post postInput) bool {
	return post.SpeechProviderProfileID == "" && post.SpeechBudgetApprovalID == "" &&
		post.SpeechBudgetMaximumMicros == 0 && post.SpeechBudgetCurrency == "" &&
		post.Speaker == "" && post.VoiceAssetVersionID == ""
}

func validateNativeShots(shots []shotInput) error {
	identities := map[string]struct{}{}
	continuities := 0
	lipSyncDialogue := 0
	for _, shot := range shots {
		if strings.TrimSpace(shot.Ambience.Identity) == "" || strings.TrimSpace(shot.Ambience.Version) == "" {
			return errors.New("every FLO-154 shot must freeze ambience identity and version")
		}
		identities[shot.Ambience.Identity+"\x00"+shot.Ambience.Version] = struct{}{}
		if shot.Ambience.ContinuityIntoNext {
			continuities++
		}
		var cinematography map[string]any
		var narrative struct {
			Dialogue []json.RawMessage `json:"dialogue"`
		}
		if json.Unmarshal(shot.Cinematography, &cinematography) != nil ||
			json.Unmarshal(shot.Narrative, &narrative) != nil {
			return errors.New("FLO-154 shot cinematography or narrative is invalid")
		}
		if required, _ := nestedBool(cinematography, "lipSyncRequired", "requiresLipSync"); required && len(narrative.Dialogue) > 0 {
			lipSyncDialogue++
		}
	}
	if len(identities) < 2 || continuities < 2 || lipSyncDialogue < 4 {
		return errors.New("FLO-154 sample requires two ambience groups, two continuities, and four lip-sync dialogue shots")
	}
	return nil
}

func modelRoute(input routeInput, hash string) providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{CapabilityAlias: input.CapabilityAlias, Provider: "VOLCENGINE", ModelID: input.ModelID, RouteVersion: input.RouteVersion, CapabilityHash: hash, Verification: providercontract.PendingKey}
}

func validateIDs(product productInput, nativeOnly bool) error {
	values := []string{product.Reserved.SeriesID, product.Reserved.SourceRevisionID, product.Reserved.EpisodeID, product.Reserved.EpisodeRevisionID, product.Reserved.SceneID, product.Reserved.SceneRevisionID, product.Reserved.ScriptRevisionID, product.Reserved.StoryboardRevisionID, product.Reserved.GenerationProfileID, product.Reserved.GenerationProfileRevisionID, product.Reserved.GenerationPlanID, product.Reserved.VisualAssetID, product.Reserved.VisualAssetVersionID, product.Reserved.VisualLicenseSnapshotID, product.Reserved.SafetyEvidenceArtifactID, product.Reserved.G1DecisionID, product.Reserved.G2DecisionID, product.Reserved.SafetyDecisionID, product.Reserved.VideoBudgetApprovalID, product.Reserved.VideoProviderProfileID, product.Reserved.SeriesContextRevisionID, product.Reserved.EpisodeContextRevisionID, product.Reserved.SceneContextRevisionID}
	if !nativeOnly {
		values = append(values,
			product.Reserved.VoiceAssetID, product.Reserved.VoiceAssetVersionID,
			product.Reserved.VoiceLicenseSnapshotID, product.Reserved.SpeechBudgetApprovalID,
			product.Reserved.SpeechProviderProfileID,
		)
	}
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
	if err := ensureActiveInputArtifact(
		ctx, tx, objects["visual"], "image/png",
		map[string]any{
			"kind": "reference_image", "width": p.visualAsset.Width,
			"height": p.visualAsset.Height, "inputPackageHash": p.productHash,
		},
	); err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf("persist visual artifact evidence: %w", err)
	}
	if p.policy.NativeOnly {
		if err := ensureActiveInputArtifact(
			ctx, tx, objects["analyzer_seal"], "application/vnd.video-series.analyzer-seal+json",
			map[string]any{
				"kind": "analyzer_seal", "inputPackageHash": p.productHash,
				"codeCommitSha": p.codeCommit, "buildSha256": p.buildSHA256,
			},
		); err != nil {
			return stage1.ExecutionPackage{}, fmt.Errorf("persist analyzer seal evidence: %w", err)
		}
	} else if err := ensureActiveInputArtifact(
		ctx, tx, objects["voice_descriptor"], "audio/x-voice-profile+json",
		map[string]any{
			"kind": "voice_profile", "speaker": p.product.PostProduction.Speaker,
			"inputPackageHash": p.productHash,
		},
	); err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf("persist voice artifact evidence: %w", err)
	}
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
		if err := ensureGenerationRunPlanBindings(
			ctx, tx, package_, p.product.Reserved.GenerationProfileRevisionID,
			approval, p.productHash, time.Now().UTC(),
		); err != nil {
			return stage1.ExecutionPackage{}, fmt.Errorf(
				"repair immutable generation plan bindings: %w", err,
			)
		}
		// A replay is also the controlled repair path for packages materialized
		// before input artifact size metadata and imported-run plan audit bindings
		// became mandatory. Commit only identity-checked evidence derived from the
		// existing package; the package identity remains bound to its original
		// materialization audit event.
		if err := tx.Commit(ctx); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		return package_, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return stage1.ExecutionPackage{}, err
	}
	now := time.Now().UTC()
	ids := p.product.Reserved
	// UUID aliases are valid transport spellings but may not become distinct
	// TEXT budget buckets in persisted product truth.
	ids.VideoBudgetApprovalID = mustUUID(ids.VideoBudgetApprovalID).String()
	if !p.policy.NativeOnly {
		ids.SpeechBudgetApprovalID = mustUUID(ids.SpeechBudgetApprovalID).String()
	}
	providerOutput := p.product.SharedPrompt.Output.providerSpec()
	exec := func(label, query string, args ...any) error {
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return nil
	}

	// Provider snapshots are product truth only. The command never constructs an Adapter client.
	if err := exec("video provider profile", `INSERT INTO video_pipeline.provider_profiles (id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash) VALUES ($1,'VOLCENGINE',$2,'internal://volcengine-provider','env:ARK_API_KEY',true,'LIVE','READY',$3)`, mustUUID(ids.VideoProviderProfileID), p.policy.DisplayName, p.videoRoute.CapabilityHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if !p.policy.NativeOnly {
		if err := exec("speech provider profile", `INSERT INTO video_pipeline.provider_profiles (id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash) VALUES ($1,'VOLCENGINE','FLO-104 Agent Plan speech','internal://volcengine-tts-provider','env:ARK_API_KEY',true,'LIVE','READY',$2)`, mustUUID(ids.SpeechProviderProfileID), p.speechRoute.CapabilityHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	limits := map[string]any{"allowedTerritories": p.product.GenerationPlan.Territories, "productForms": p.product.GenerationPlan.ProductForms, "contentSafetyPolicyVersions": []string{p.product.ContentSafetyEvidence.PolicyVersion}, "remainingCalls": p.product.GenerationPlan.MaximumCalls, "unitPriceMicros": 1, "accountingNote": "Agent Plan subscription; one-micro-per-second reservation floor, actual cost comes from Provider usage evidence"}
	if p.policy.NativeOnly {
		limits["supportsNativeAudio"] = true
		limits["nativeAudioDelivery"] = string(providercontract.NativeAudioMix)
		limits["maximumSpeechSubmits"] = 0
	}
	type capability struct {
		id                                   uuid.UUID
		profile, alias, model, version, hash string
		inputs                               []string
	}
	capabilities := []capability{{p.videoCapabilityID, ids.VideoProviderProfileID, "video.primary", p.videoRoute.ModelID, p.videoRoute.RouteVersion, p.videoRoute.CapabilityHash, []string{"prompt", "reference_image"}}}
	if !p.policy.NativeOnly {
		capabilities[0].inputs = append(capabilities[0].inputs, "reference_audio")
		capabilities = append(capabilities, capability{p.speechCapabilityID, ids.SpeechProviderProfileID, "speech.primary", p.speechRoute.ModelID, p.speechRoute.RouteVersion, p.speechRoute.CapabilityHash, []string{"text", "voice_profile"}})
	}
	for _, cap := range capabilities {
		if err := exec("provider capability", `INSERT INTO video_pipeline.provider_capability_snapshots (id,provider_profile_id,capability_alias,model_id,route_version,supported_inputs,limits,pricing_rule_version,capability_hash,status,effective_at,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,'agent-plan-subscription-v1',$8,'ACTIVE',$9,$10)`, cap.id, mustUUID(cap.profile), cap.alias, cap.model, cap.version, cap.inputs, limits, cap.hash, now, approval.ValidUntil); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	routes := map[string]any{"video": p.videoRoute}
	if !p.policy.NativeOnly {
		routes["speech"] = p.speechRoute
	}
	if err := exec("generation profile", `INSERT INTO video_pipeline.generation_profiles (id,profile_id,revision,schema_version,status,stage,aspect_profile,episode_target_ms,shot_min_ms,shot_max_ms,capability_routes,media_processing,render_defaults,qc_thresholds,retry_policy,budget_policy,license_policy,content_hash,created_by) VALUES ($1,$2,1,'v1','ACTIVE',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, mustUUID(ids.GenerationProfileRevisionID), mustUUID(ids.GenerationProfileID), p.product.GenerationProfile.Stage, p.product.GenerationProfile.AspectProfile, p.product.GenerationProfile.EpisodeTargetMS, p.product.GenerationProfile.ShotMinMS, p.product.GenerationProfile.ShotMaxMS, routes, map[string]any{"postProduction": p.product.PostProduction}, providerOutput, p.product.PostProduction.QualityThresholds, p.product.GenerationProfile.RetryPolicy, p.product.AuthorizationBoundary, map[string]any{"territories": p.product.GenerationPlan.Territories, "productForms": p.product.GenerationPlan.ProductForms}, p.profileHash, p.product.CreatedBy); err != nil {
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
	if err := exec("visual license", `INSERT INTO video_pipeline.license_snapshots (id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,expires_at,source_uri,reviewed_by,reviewed_at) VALUES ($1,$2,$3,$4,$5,'ALLOWED',$6,$7,$8,$9,$10,$11)`, mustUUID(ids.VisualLicenseSnapshotID), visualLicense.SubjectType, visualLicense.SubjectRef, visualLicense.LicenseID, visualLicenseHash, visualLicense.Territories, visualLicense.CommercialUse, visualLicense.ExpiresAt, visualLicense.SourceURI, approval.ActorID, now); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if !p.policy.NativeOnly {
		voiceLicenseHash, _ := digest(map[string]any{"license": voiceLicense, "effectivePolicyStatus": "ALLOWED", "adminApproval": approval.CommentID, "validUntil": approval.ValidUntil, "inputPackageHash": p.productHash})
		if err := exec("voice license", `INSERT INTO video_pipeline.license_snapshots (id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,expires_at,source_uri,reviewed_by,reviewed_at) VALUES ($1,'VOICE',$2,$3,$4,'ALLOWED',$5,true,$6,$7,$8,$9)`, mustUUID(ids.VoiceLicenseSnapshotID), voiceLicense.SubjectRef, voiceLicense.LicenseID, voiceLicenseHash, voiceLicense.Territories, approval.ValidUntil, voiceLicense.SourceURI, approval.ActorID, now); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	if err := exec("visual asset", `INSERT INTO video_pipeline.assets (id,series_id,asset_type,scope_type,scope_id) VALUES ($1,$2,'IMAGE','SERIES',$2)`, mustUUID(ids.VisualAssetID), mustUUID(ids.SeriesID)); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("visual version", `INSERT INTO video_pipeline.asset_versions (id,asset_id,revision,status,content_hash,artifact_uri,media_type,dimensions,source_ref,license_snapshot_id,created_by) VALUES ($1,$2,1,'APPROVED',$3,$4,'image/png',$5,$6,$7,$8)`, mustUUID(ids.VisualAssetVersionID), mustUUID(ids.VisualAssetID), objects["visual"].Digest, objects["visual"].URI, map[string]any{"width": p.visualAsset.Width, "height": p.visualAsset.Height}, p.policy.ProductSchema+":"+p.productHash, mustUUID(ids.VisualLicenseSnapshotID), p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if !p.policy.NativeOnly {
		if err := exec("voice asset", `INSERT INTO video_pipeline.assets (id,series_id,asset_type,scope_type,scope_id) VALUES ($1,$2,'VOICE','SERIES',$2)`, mustUUID(ids.VoiceAssetID), mustUUID(ids.SeriesID)); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("voice version", `INSERT INTO video_pipeline.asset_versions (id,asset_id,revision,status,content_hash,artifact_uri,media_type,dimensions,source_ref,license_snapshot_id,created_by) VALUES ($1,$2,1,'APPROVED',$3,$4,'audio/x-voice-profile+json',$5,$6,$7,$8)`, mustUUID(ids.VoiceAssetVersionID), mustUUID(ids.VoiceAssetID), objects["voice_descriptor"].Digest, objects["voice_descriptor"].URI, map[string]any{"speaker": p.product.PostProduction.Speaker}, p.policy.ProductSchema+":"+p.productHash, mustUUID(ids.VoiceLicenseSnapshotID), p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	if err := exec("safety artifact", `INSERT INTO video_pipeline.artifacts (id,content_hash,artifact_uri,media_type,size_bytes,media_spec,status) VALUES ($1,$2,$3,'application/json',$4,$5,'ACTIVE')`, mustUUID(ids.SafetyEvidenceArtifactID), objects["safety"].Digest, objects["safety"].URI, objects["safety"].Size, map[string]any{"kind": "content_safety_evidence", "policyVersion": p.product.ContentSafetyEvidence.PolicyVersion, "inputPackageHash": p.productHash}); err != nil {
		return stage1.ExecutionPackage{}, err
	}

	// Four-level context is required by the paid boundary. The product reserves
	// the reusable levels; deterministic shot-level revisions are derived here.
	seriesContextHash, _ := digest(map[string]any{"scope": "SERIES", "creativeBrief": p.product.CreativeBrief, "inputPackageHash": p.productHash})
	episodeContextHash, _ := digest(map[string]any{"scope": "EPISODE", "dialogue": p.product.DialogueSummary, "inputPackageHash": p.productHash})
	sceneContextHash, _ := digest(map[string]any{"scope": "SCENE", "sceneRevisionHash": sceneHash, "inputPackageHash": p.productHash})
	baseContexts := []struct {
		id, scope, scopeID, hash string
		payload                  any
	}{{ids.SeriesContextRevisionID, "SERIES", ids.SeriesID, seriesContextHash, map[string]any{"creativeBrief": p.product.CreativeBrief}}, {ids.EpisodeContextRevisionID, "EPISODE", ids.EpisodeID, episodeContextHash, map[string]any{"dialogue": p.product.DialogueSummary}}}
	if !p.policy.NativeOnly {
		baseContexts = append(baseContexts, struct {
			id, scope, scopeID, hash string
			payload                  any
		}{ids.SceneContextRevisionID, "SCENE", ids.SceneID, sceneContextHash, map[string]any{"sceneRevisionHash": sceneHash}})
	}
	for _, c := range baseContexts {
		if err := exec("context", `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by) VALUES ($1,$2,$3,$4,1,'APPROVED','v1',$5,$6,$7,$8)`, mustUUID(c.id), mustUUID(ids.SeriesID), c.scope, mustUUID(c.scopeID), p.policy.ResolverVersion, c.payload, c.hash, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	// Exact gate decisions and budget reviews are inserted before the dependent
	// shot/run rows. Every binding uses the database-derived immutable hash.
	approvalSubject := "fixed FLO-104 sample 1"
	if p.policy.NativeOnly {
		approvalSubject = "fixed FLO-154 native sample 1"
	}
	for _, d := range []struct{ id, gate, role, explanation string }{{ids.G1DecisionID, "G1", "PRODUCER", "Approved " + approvalSubject + " script"}, {ids.G2DecisionID, "G2", "PRODUCER", "Approved " + approvalSubject + " storyboard and shots"}, {ids.SafetyDecisionID, "SAFETY", actorRole, string(mustJSON(map[string]any{"policyVersion": p.product.ContentSafetyEvidence.PolicyVersion, "evidenceHash": objects["safety"].Digest, "validUntil": approval.ValidUntil, "explanation": "ADMIN-approved offline safety evidence; " + approval.CommentID}))}} {
		if err := exec("approval decision", `INSERT INTO video_pipeline.approval_decisions (id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id) VALUES ($1,$2,$3,$4,'APPROVED',$5,$6,$7,$8,$9,$10)`, mustUUID(d.id), mustUUID(ids.SeriesID), mustUUID(ids.EpisodeID), d.gate, p.policy.ReasonCode, d.explanation, approval.ActorID, d.role, now, p.product.PostProduction.TraceID); err != nil {
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

	assetIDs := []uuid.UUID{mustUUID(ids.VisualAssetVersionID)}
	if !p.policy.NativeOnly {
		assetIDs = append(assetIDs, mustUUID(ids.VoiceAssetVersionID))
	}
	contextBase := []uuid.UUID{mustUUID(ids.SeriesContextRevisionID), mustUUID(ids.EpisodeContextRevisionID)}
	if !p.policy.NativeOnly {
		contextBase = append(contextBase, mustUUID(ids.SceneContextRevisionID))
	}
	jobs := make([]stage1.FrozenJob, 0, len(p.product.Shots))
	runIDs := make([]string, 0, len(p.product.Shots))
	shotHashes := map[string]string{}
	for _, shot := range p.product.Shots {
		shotHash, _ := digest(map[string]any{"shot": shot, "profileHash": p.profileHash, "inputPackageHash": p.productHash})
		shotHashes[shot.ShotSpecRevisionID] = shotHash
		shotContextID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(p.policy.WorkflowPrefix+"shot-context:"+shot.ShotSpecRevisionID+":"+p.productHash))
		shotContextHash, _ := digest(map[string]any{"scope": "SHOT", "shot": shot, "inputPackageHash": p.productHash})
		currentSceneContextID := mustUUID(ids.SceneContextRevisionID)
		currentSceneContextHash := sceneContextHash
		if p.policy.NativeOnly {
			if shot.Ordinal > 1 {
				currentSceneContextID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(p.policy.WorkflowPrefix+"scene-context:"+shot.ShotSpecRevisionID+":"+p.productHash))
			}
			scenePayload := map[string]any{"sceneRevisionHash": sceneHash, "audio": map[string]any{"ambience": map[string]any{"identity": shot.Ambience.Identity, "version": shot.Ambience.Version}}, "audio.ambience.continuity": shot.Ambience.ContinuityIntoNext}
			currentSceneContextHash, _ = digest(map[string]any{"scope": "SCENE", "payload": scenePayload, "inputPackageHash": p.productHash})
			if err := exec("native scene context", `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by) VALUES ($1,$2,'SCENE',$3,$4,'APPROVED','v1',$5,$6,$7,$8)`, currentSceneContextID, mustUUID(ids.SeriesID), mustUUID(ids.SceneID), shot.Ordinal, p.policy.ResolverVersion, scenePayload, currentSceneContextHash, p.product.CreatedBy); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		if err := exec("shot", `INSERT INTO video_pipeline.shots (id,scene_id,ordinal) VALUES ($1,$2,$3)`, mustUUID(shot.DBShotID), mustUUID(ids.SceneID), shot.Ordinal); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("shot context", `INSERT INTO video_pipeline.context_revisions (id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by) VALUES ($1,$2,'SHOT',$3,1,'APPROVED','v1',$4,$5,$6,$7)`, shotContextID, mustUUID(ids.SeriesID), mustUUID(shot.DBShotID), p.policy.ResolverVersion, map[string]any{"timeline": shot.Timeline, "narrative": shot.Narrative, "cinematography": shot.Cinematography, "continuity": shot.Continuity}, shotContextHash, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		contexts := append([]uuid.UUID{}, contextBase...)
		if p.policy.NativeOnly {
			contexts = append(contexts, currentSceneContextID)
		}
		contexts = append(contexts, shotContextID)
		contextHashes := map[string]string{"context:series": seriesContextHash, "context:episode": episodeContextHash, "context:scene": currentSceneContextHash, "context:shot": shotContextHash}
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
		if err := exec("effective context", `INSERT INTO video_pipeline.effective_context_snapshots (id,shot_spec_revision_id,schema_version,resolver_version,context_revision_ids,normalized_payload,content_hash) VALUES ($1,$2,'v1',$3,$4,$5,$6)`, mustUUID(shot.EffectiveContextSnapshotID), mustUUID(shot.ShotSpecRevisionID), p.policy.ResolverVersion, contexts, map[string]any{"contextHashes": contextHashes, "inputPackageHash": p.productHash}, effectiveHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		inputHashes := map[string]string{"shot_spec": shotHash, "generation_profile": p.profileHash, "context:series": seriesContextHash, "context:episode": episodeContextHash, "context:scene": currentSceneContextHash, "context:shot": shotContextHash, "asset:" + ids.VisualAssetVersionID: objects["visual"].Digest}
		if !p.policy.NativeOnly {
			inputHashes["asset:"+ids.VoiceAssetVersionID] = objects["voice_descriptor"].Digest
		}
		promptHash, err := repository.ImportedPromptHashForCompiler(p.policy.ResolverVersion, shot.ShotSpecRevisionID, ids.GenerationProfileRevisionID, effectiveHash, assetIDs, shot.PositivePrompt, p.product.SharedPrompt.NegativePrompt, providerOutput, inputHashes, p.productHash)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("prompt snapshot", `INSERT INTO video_pipeline.prompt_snapshots (id,shot_spec_revision_id,schema_version,compiler_version,prompt_template_ref,effective_context_snapshot_id,asset_version_refs,positive_prompt,negative_prompt,model_payload,normalized_input_hash,content_hash,output_spec,input_revision_hashes) VALUES ($1,$2,'v1',$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,$12)`, mustUUID(shot.PromptSnapshotID), mustUUID(shot.ShotSpecRevisionID), p.policy.ResolverVersion, p.policy.PromptTemplateRef, mustUUID(shot.EffectiveContextSnapshotID), assetIDs, shot.PositivePrompt, p.product.SharedPrompt.NegativePrompt, map[string]any{"generationProfileRevisionId": ids.GenerationProfileRevisionID, "inputPackageHash": p.productHash}, promptHash, providerOutput, inputHashes); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		for _, in := range []struct {
			typ        string
			id         uuid.UUID
			hash, role string
		}{{"SHOT_SPEC", mustUUID(shot.ShotSpecRevisionID), shotHash, "primary-shot"}, {"GENERATION_PROFILE", mustUUID(ids.GenerationProfileRevisionID), p.profileHash, "generation-profile"}, {"CONTEXT", contexts[0], seriesContextHash, "context:series"}, {"CONTEXT", contexts[1], episodeContextHash, "context:episode"}, {"CONTEXT", contexts[2], currentSceneContextHash, "context:scene"}, {"CONTEXT", contexts[3], shotContextHash, "context:shot"}} {
			if err := exec("prompt input", `INSERT INTO video_pipeline.prompt_snapshot_inputs (prompt_snapshot_id,input_type,input_revision_id,input_hash,dependency_role) VALUES ($1,$2,$3,$4,$5)`, mustUUID(shot.PromptSnapshotID), in.typ, in.id, in.hash, in.role); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		promptAssets := []struct{ id, hash, role string }{{ids.VisualAssetVersionID, objects["visual"].Digest, "reference_image"}}
		if !p.policy.NativeOnly {
			promptAssets = append(promptAssets, struct{ id, hash, role string }{ids.VoiceAssetVersionID, objects["voice_descriptor"].Digest, "reference_audio"})
		}
		for index, a := range promptAssets {
			if err := exec("prompt asset", `INSERT INTO video_pipeline.prompt_snapshot_assets (prompt_snapshot_id,alias,asset_version_id,asset_hash,provider_role) VALUES ($1,$2,$3,$4,$5)`, mustUUID(shot.PromptSnapshotID), fmt.Sprintf("asset-%03d", index+1), mustUUID(a.id), a.hash, a.role); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		if err := exec("prompt import audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','prompt_snapshot.imported','PROMPT_SNAPSHOT',$4,$5,$6,$7)`, uuid.NewSHA1(mustUUID(shot.PromptSnapshotID), []byte("import-audit")), now, approval.ActorID, mustUUID(shot.PromptSnapshotID), p.policy.ReasonCode, p.product.PostProduction.TraceID, map[string]any{"inputPackageHash": p.productHash, "originalPromptHash": shot.PromptSnapshotContentHash, "derivedPromptHash": promptHash, "approvalCommentId": approval.CommentID}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		runDigest, err := repository.GenerationRunSpecDigest(shot.ShotSpecRevisionID, shot.PromptSnapshotID, promptHash, ids.GenerationProfileRevisionID, ids.GenerationPlanID, p.videoRoute, 1)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("generation run", `INSERT INTO video_pipeline.generation_runs (id,shot_spec_revision_id,prompt_snapshot_id,generation_profile_id,temporal_workflow_id,run_spec_digest,creative_attempt,state,dry_run,budget_approval_id,trace_id,created_by) VALUES ($1,$2,$3,$4,$5,$6,1,'VALIDATED',false,$7,$8,$9)`, mustUUID(shot.RunID), mustUUID(shot.ShotSpecRevisionID), mustUUID(shot.PromptSnapshotID), mustUUID(ids.GenerationProfileRevisionID), p.policy.WorkflowPrefix+shot.RunID, runDigest, ids.VideoBudgetApprovalID, p.product.PostProduction.TraceID, p.product.CreatedBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		attemptUUID := uuid.NewSHA1(mustUUID(shot.RunID), []byte("attempt:1"))
		if err := exec("generation attempt", `INSERT INTO video_pipeline.generation_attempts (id,generation_run_id,sequence,attempt_kind,state,input_hash,model_snapshot,parameter_diff) VALUES ($1,$2,1,'PROVIDER_REQUEST','VALIDATED',$3,$4,$5)`, attemptUUID, mustUUID(shot.RunID), runDigest, p.videoRoute, map[string]any{"originalRunSpecDigest": shot.RunSpecDigest, "inputPackageHash": p.productHash}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		jobs = append(jobs, stage1.FrozenJob{ShotID: shot.ShotID, ShotSpecRevisionID: shot.ShotSpecRevisionID, AttemptID: shot.AttemptID, IdempotencyKey: shot.IdempotencyKey, Run: orchestration.GenerationRunRef{RunID: shot.RunID, RunSpecDigest: runDigest, Attempt: 1}, PromptSnapshotID: shot.PromptSnapshotID, PromptSnapshotHash: promptHash, GenerationPlanID: ids.GenerationPlanID, BudgetApprovalID: ids.VideoBudgetApprovalID, BudgetMaximumMicros: p.product.GenerationPlan.MaximumCashMicros, BudgetCurrency: p.product.GenerationPlan.Currency, ProviderProfileID: ids.VideoProviderProfileID, Route: p.videoRoute, EstimatedVideoTokens: shot.EstimatedVideoTokens, PredictedAFPMilli: shot.PredictedAFPMilli, EstimatedNonSubscriptionCashMicros: shot.EstimatedCashMicros, WorkflowID: p.policy.WorkflowPrefix + shot.RunID, ActivityID: "submit-" + shot.ShotID, TraceID: p.product.PostProduction.TraceID})
		runIDs = append(runIDs, shot.RunID)
	}

	execPolicy := controlplane.ExecutionPolicy{TargetTerritory: p.product.GenerationPlan.Territories[0], ProductForm: p.product.GenerationPlan.ProductForms[0], ContentSafetyPolicyVersion: p.product.ContentSafetyEvidence.PolicyVersion, ContentSafetyDecisionID: ids.SafetyDecisionID}
	planHash, _ := digest(map[string]any{"inputPackageHash": p.productHash, "shotSpecRevisionIds": p.product.GenerationPlan.ShotSpecRevisionIDs, "route": p.videoRoute, "budget": p.product.GenerationPlan.MaximumCashMicros, "executionPolicy": execPolicy})
	budgetDecision := "APPROVED"
	if p.policy.NativeOnly {
		// The current Deep Research decision authorizes no-cost materialization
		// only. Keep the exact VIDEO review open so PrepareProviderJob fails
		// closed until QA fixes the hashes and a later authorization approves it.
		budgetDecision = "PENDING"
	}
	planRecord := controlplane.GenerationPlan{GenerationPlanID: ids.GenerationPlanID, State: "READY", DryRun: false, ShotCount: len(p.product.Shots), ProviderCallCount: len(p.product.Shots), RouteSnapshot: controlplane.ModelRouteSnapshot{CapabilityAlias: p.videoRoute.CapabilityAlias, ProviderProfileID: ids.VideoProviderProfileID, Provider: p.videoRoute.Provider, ModelID: p.videoRoute.ModelID, RouteVersion: p.videoRoute.RouteVersion, CapabilityHash: p.videoRoute.CapabilityHash}, ExecutionPolicy: execPolicy, Estimate: controlplane.CostEstimate{UnitsMinimum: 50, UnitsMaximum: 50, Unit: "video_seconds", AmountMinimum: nil, AmountMaximum: nil, Currency: "CNY", PricingRuleVersion: "agent-plan-subscription-v1", ValidUntil: approval.ValidUntil}, BudgetDecision: budgetDecision, PlanHash: planHash}
	if !p.policy.NativeOnly {
		planRecord.SpeechBudgetLimit = &controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}
	}
	if err := exec("plan operation", `INSERT INTO video_pipeline.operation_requests (id,operation_type,aggregate_type,aggregate_id,state,trace_id,requested_by) VALUES ($1,'CREATE_GENERATION_PLAN','SERIES',$2,'SUCCEEDED',$3,$4)`, mustUUID(ids.GenerationPlanID), mustUUID(ids.SeriesID), p.product.PostProduction.TraceID, p.product.CreatedBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("plan idempotency", `INSERT INTO video_pipeline.idempotency_records (scope,idempotency_key,request_hash,operation_id,response_status,response_body,expires_at) VALUES ('stage1-materialize',$1,$2,$3,201,$4,$5)`, p.productHash, planHash, mustUUID(ids.GenerationPlanID), planRecord, approval.ValidUntil); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	planAudit := map[string]any{"seriesId": ids.SeriesID, "episodeRevisionId": ids.EpisodeRevisionID, "shotSpecRevisionIds": p.product.GenerationPlan.ShotSpecRevisionIDs, "candidatesPerShot": 1, "pricingRuleVersion": "agent-plan-subscription-v1", "planHash": planHash, "state": "READY", "budgetDecision": budgetDecision, "budgetLimit": controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}, "executionPolicy": execPolicy, "inputPackageHash": p.productHash, "approvalCommentId": approval.CommentID}
	if !p.policy.NativeOnly {
		planAudit["speechBudgetLimit"] = controlplane.BudgetLimit{AmountMicros: p.product.GenerationPlan.MaximumCashMicros, Currency: p.product.GenerationPlan.Currency}
	} else {
		planAudit["maximumSpeechSubmits"] = 0
		planAudit["analyzerSealSha256"] = p.analyzerEvidence.SealSHA256
	}
	if err := exec("plan audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','generation_plan.created','GENERATION_PLAN',$4,$5,$6,$7)`, uuid.NewSHA1(mustUUID(ids.GenerationPlanID), []byte("plan-audit")), now, approval.ActorID, mustUUID(ids.GenerationPlanID), p.policy.ReasonCode, p.product.PostProduction.TraceID, planAudit); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	reviews := []struct{ id, scope string }{{ids.VideoBudgetApprovalID, "VIDEO"}}
	if !p.policy.NativeOnly {
		reviews = append(reviews, struct{ id, scope string }{ids.SpeechBudgetApprovalID, "SPEECH"})
	}
	for _, review := range reviews {
		reviewState := "APPROVED"
		var decidedAt any = now
		if p.policy.NativeOnly && review.scope == "VIDEO" {
			reviewState = "OPEN"
			decidedAt = nil
		}
		if err := exec("budget review", `INSERT INTO video_pipeline.review_tasks (id,series_id,episode_id,review_type,state,reason_codes,assigned_role,decided_at,generation_plan_id,budget_scope,budget_limit_micros,budget_currency) VALUES ($1,$2,$3,'BUDGET',$4,ARRAY[$5],'ADMIN',$6,$7,$8,$9,$10)`, mustUUID(review.id), mustUUID(ids.SeriesID), mustUUID(ids.EpisodeID), reviewState, p.policy.ReasonCode, decidedAt, mustUUID(ids.GenerationPlanID), review.scope, p.product.GenerationPlan.MaximumCashMicros, p.product.GenerationPlan.Currency); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}

	postConfig := orchestration.PostProductionConfig{Enabled: true, Evidence: postproduction.EvidenceLive, SubtitleLanguage: p.product.PostProduction.SubtitleLanguage, BurnSubtitles: p.product.PostProduction.BurnSubtitles, EnforcePoCDuration: true}
	if p.policy.NativeOnly {
		postConfig.AudioStrategy = providercontract.AudioStrategyNativePreferred
		postConfig.AnalyzerSealSHA256 = p.analyzerEvidence.SealSHA256
	} else {
		postConfig.SpeechRoute = p.speechRoute
		postConfig.SpeechProviderProfileID = ids.SpeechProviderProfileID
		postConfig.SpeechBudgetApprovalID = ids.SpeechBudgetApprovalID
		postConfig.SpeechBudgetMaximumMicros = p.product.PostProduction.SpeechBudgetMaximumMicros
		postConfig.SpeechBudgetCurrency = p.product.PostProduction.SpeechBudgetCurrency
	}
	post := orchestration.FinalizeEpisodeInput{EpisodeRevisionID: ids.EpisodeRevisionID, RunIDs: runIDs, GenerationPlanID: ids.GenerationPlanID, TraceID: p.product.PostProduction.TraceID, PersistProductTruth: true, Config: postConfig}
	packageInput := stage1.ExecutionPackage{SchemaVersion: stage1.ExecutionPackageSchemaVersion, BatchID: p.policy.BatchID, PrimaryJobs: jobs, PostProduction: post}
	if p.policy.NativeOnly {
		packageInput.NativeEvidence = &stage1.NativeExecutionEvidence{
			CodeCommitSHA: p.codeCommit, BuildSHA256: p.buildSHA256,
			ProductInputSHA256:       p.productHash,
			AnalyzerSealSHA256:       p.analyzerEvidence.SealSHA256,
			AnalyzerExecutableSHA256: p.analyzerEvidence.ExecutableSHA256,
			AnalyzerConfigSHA256:     p.analyzerEvidence.ConfigSHA256,
			AnalyzerComponentSHA256:  p.analyzerEvidence.Components,
			AssetSHA256:              map[string]string{"source": objects["source"].Digest, "safety": objects["safety"].Digest, "visual": objects["visual"].Digest, "analyzer_seal": objects["analyzer_seal"].Digest},
		}
	}
	package_, err := stage1.SealExecutionPackage(packageInput)
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := package_.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := ensureGenerationRunPlanBindings(
		ctx, tx, package_, ids.GenerationProfileRevisionID,
		approval, p.productHash, now,
	); err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf(
			"persist immutable generation plan bindings: %w", err,
		)
	}
	materializationAudit := map[string]any{"inputPackageHash": p.productHash, "sourceHash": objects["source"].Digest, "safetyHash": objects["safety"].Digest, "visualHash": objects["visual"].Digest, "executionPackageHash": package_.ContentHash, "executionPackage": package_, "approvalCommentId": approval.CommentID, "approvalValidUntil": approval.ValidUntil, "originalShotHashes": originalHashes(p.product.Shots), "derivedShotHashes": shotHashes, "providerCalls": 0}
	if p.policy.NativeOnly {
		materializationAudit["ttsCalls"] = 0
		materializationAudit["analyzerSealHash"] = objects["analyzer_seal"].Digest
		materializationAudit["codeCommitSha"] = p.codeCommit
		materializationAudit["buildSha256"] = p.buildSHA256
	} else {
		materializationAudit["voiceDescriptorHash"] = objects["voice_descriptor"].Digest
	}
	if err := exec("materialization audit", `INSERT INTO video_pipeline.audit_events (id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload) VALUES ($1,$2,$3,'ADMIN','stage1.execution_package.materialized','GENERATION_PLAN',$4,$5,$6,$7)`, uuid.NewSHA1(mustUUID(ids.GenerationPlanID), []byte("stage1-materialization")), now, approval.ActorID, mustUUID(ids.GenerationPlanID), p.policy.ReasonCode, p.product.PostProduction.TraceID, materializationAudit); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	return package_, nil
}

// ensureGenerationRunPlanBindings gives imported Runs the same immutable plan
// provenance that PrepareProviderJob requires from ordinary workflow-created
// Runs. A replay may repair a package created by an older materializer, but only
// after every persisted Run and first attempt exactly matches the already-sealed
// execution package.
func ensureGenerationRunPlanBindings(
	ctx context.Context,
	tx pgx.Tx,
	package_ stage1.ExecutionPackage,
	generationProfileID string,
	approval Approval,
	inputPackageHash string,
	occurredAt time.Time,
) error {
	reasonCode := "FLO104_FIXED_SAMPLE1"
	if package_.NativeEvidence != nil {
		reasonCode = "FLO154_NATIVE_SAMPLE1"
	}
	for _, job := range package_.PrimaryJobs {
		var shotID, promptID, profileID uuid.UUID
		var runDigest, state, budgetApprovalID string
		var attemptKind, attemptState, attemptInputHash string
		var creativeAttempt, attemptSequence int
		var modelSnapshotJSON []byte
		if err := tx.QueryRow(ctx, `
			SELECT gr.shot_spec_revision_id, gr.prompt_snapshot_id,
			       gr.generation_profile_id, gr.run_spec_digest, gr.state,
			       gr.creative_attempt, gr.budget_approval_id,
			       ga.sequence, ga.attempt_kind, ga.state, ga.input_hash,
			       ga.model_snapshot
			FROM video_pipeline.generation_runs gr
			JOIN video_pipeline.generation_attempts ga
			  ON ga.generation_run_id = gr.id AND ga.sequence = 1
			WHERE gr.id = $1
			FOR SHARE OF gr, ga`,
			mustUUID(job.Run.RunID),
		).Scan(
			&shotID, &promptID, &profileID, &runDigest, &state,
			&creativeAttempt, &budgetApprovalID, &attemptSequence,
			&attemptKind, &attemptState, &attemptInputHash, &modelSnapshotJSON,
		); err != nil {
			return fmt.Errorf("resolve imported generation run %s: %w", job.Run.RunID, err)
		}
		var modelSnapshot providercontract.ModelSnapshot
		if err := json.Unmarshal(modelSnapshotJSON, &modelSnapshot); err != nil {
			return fmt.Errorf("decode imported generation run route %s: %w", job.Run.RunID, err)
		}
		expectedAttemptKind := "PROVIDER_REQUEST"
		if creativeAttempt > 1 {
			expectedAttemptKind = "CREATIVE_REVISION"
		}
		if shotID.String() != job.ShotSpecRevisionID ||
			promptID.String() != job.PromptSnapshotID ||
			profileID.String() != generationProfileID ||
			runDigest != job.Run.RunSpecDigest || state != "VALIDATED" ||
			creativeAttempt != job.Run.Attempt ||
			budgetApprovalID != job.BudgetApprovalID ||
			attemptSequence != 1 || attemptKind != expectedAttemptKind ||
			attemptState != "VALIDATED" || attemptInputHash != runDigest ||
			modelSnapshot != job.Route {
			return fmt.Errorf("imported generation run %s differs from the sealed execution package", job.Run.RunID)
		}
		var paidBoundaryRecords int64
		if err := tx.QueryRow(ctx, `
			SELECT
			  (SELECT count(*) FROM video_pipeline.budget_reservations
			   WHERE generation_run_id = $1) +
			  (SELECT count(*) FROM video_pipeline.provider_jobs pj
			   JOIN video_pipeline.generation_attempts ga
			     ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $1) +
			  (SELECT count(*) FROM video_pipeline.cost_ledger cl
			   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
			   JOIN video_pipeline.generation_attempts ga
			     ON ga.id = pj.generation_attempt_id
			   WHERE ga.generation_run_id = $1)`,
			mustUUID(job.Run.RunID),
		).Scan(&paidBoundaryRecords); err != nil {
			return fmt.Errorf("inspect imported generation run paid boundary %s: %w", job.Run.RunID, err)
		}
		if paidBoundaryRecords != 0 {
			return fmt.Errorf("imported generation run %s already crossed the paid boundary", job.Run.RunID)
		}

		var auditCount int64
		var generationPlanID string
		if err := tx.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(payload->>'generationPlanId'), '')
			FROM video_pipeline.audit_events
			WHERE aggregate_type = 'GENERATION_RUN'
			  AND aggregate_id = $1
			  AND action = 'generation_run.created'`,
			mustUUID(job.Run.RunID),
		).Scan(&auditCount, &generationPlanID); err != nil {
			return fmt.Errorf("read imported generation plan binding %s: %w", job.Run.RunID, err)
		}
		if auditCount == 1 {
			if generationPlanID != job.GenerationPlanID {
				return fmt.Errorf("imported generation run %s is bound to another generation plan", job.Run.RunID)
			}
			continue
		}
		if auditCount != 0 {
			return fmt.Errorf("imported generation run %s has duplicate generation plan bindings", job.Run.RunID)
		}
		payload := map[string]any{
			"workflowId":         job.WorkflowID,
			"shotSpecRevisionId": job.ShotSpecRevisionID,
			"promptSnapshotId":   job.PromptSnapshotID,
			"runSpecDigest":      job.Run.RunSpecDigest,
			"creativeAttempt":    job.Run.Attempt,
			"generationPlanId":   job.GenerationPlanID,
			"inputPackageHash":   inputPackageHash,
			"approvalCommentId":  approval.CommentID,
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_pipeline.audit_events
				(id, occurred_at, actor_id, actor_role, action,
				 aggregate_type, aggregate_id, reason_code, trace_id, payload)
			VALUES ($1, $2, $3, 'ADMIN', 'generation_run.created',
			        'GENERATION_RUN', $4, $5, $6, $7)`,
			uuid.NewSHA1(mustUUID(job.Run.RunID), []byte("audit")), occurredAt,
			approval.ActorID, mustUUID(job.Run.RunID), reasonCode, job.TraceID, payload,
		); err != nil {
			return fmt.Errorf("persist imported generation plan binding %s: %w", job.Run.RunID, err)
		}
	}
	return nil
}

func ensureActiveInputArtifact(
	ctx context.Context,
	tx pgx.Tx,
	object artifactstore.Artifact,
	mediaType string,
	mediaSpec map[string]any,
) error {
	if object.Size <= 0 || strings.TrimSpace(object.Digest) == "" ||
		strings.TrimSpace(object.URI) == "" || strings.TrimSpace(mediaType) == "" {
		return errors.New("complete positive-size CAS artifact evidence is required")
	}
	artifactID := uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte("stage1-input-artifact:"+object.Digest),
	)
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.artifacts
			(id, content_hash, artifact_uri, media_type, size_bytes, media_spec, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE')
		ON CONFLICT (content_hash) DO NOTHING`,
		artifactID, object.Digest, object.URI, mediaType, object.Size, mediaSpec,
	); err != nil {
		return fmt.Errorf("insert input artifact: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM video_pipeline.artifacts
		WHERE content_hash = $1
		  AND artifact_uri = $2
		  AND media_type = $3
		  AND size_bytes = $4
		  AND media_spec = $5
		  AND status = 'ACTIVE'
		FOR SHARE`,
		object.Digest, object.URI, mediaType, object.Size, mediaSpec,
	).Scan(&artifactID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("artifact hash is bound to incompatible or inactive CAS metadata")
		}
		return fmt.Errorf("resolve ACTIVE input artifact: %w", err)
	}
	return nil
}

func verify(ctx context.Context, pool *pgxpool.Pool, package_ stage1.ExecutionPackage, p prepared, approval Approval, objects map[string]artifactstore.Artifact) (Report, error) {
	counts := map[string]int64{}
	queries := map[string]struct {
		query string
		args  []any
	}{
		"shots":    {"SELECT count(*) FROM video_pipeline.shot_spec_revisions WHERE storyboard_revision_id=$1", []any{mustUUID(p.product.Reserved.StoryboardRevisionID)}},
		"prompts":  {`SELECT count(*) FROM video_pipeline.prompt_snapshots ps JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ps.shot_spec_revision_id WHERE ssr.storyboard_revision_id=$1 AND ps.compiler_version=$2`, []any{mustUUID(p.product.Reserved.StoryboardRevisionID), p.policy.ResolverVersion}},
		"runs":     {"SELECT count(*) FROM video_pipeline.generation_runs WHERE trace_id=$1", []any{p.product.PostProduction.TraceID}},
		"contexts": {"SELECT count(*) FROM video_pipeline.context_revisions WHERE series_id=$1 AND resolver_version=$2", []any{mustUUID(p.product.Reserved.SeriesID), p.policy.ResolverVersion}},
	}
	for name, statement := range queries {
		var count int64
		if err := pool.QueryRow(ctx, statement.query, statement.args...).Scan(&count); err != nil {
			return Report{}, err
		}
		counts[name] = count
	}
	productTruth := repository.NewForPool(pool)
	expectedAssets := map[string]struct {
		assetID, digest, uri, mediaType string
		size                            int64
	}{
		p.product.Reserved.VisualAssetVersionID: {
			assetID:   p.product.Reserved.VisualAssetID,
			digest:    objects["visual"].Digest,
			uri:       objects["visual"].URI,
			mediaType: "image/png", size: objects["visual"].Size,
		},
	}
	if !p.policy.NativeOnly {
		expectedAssets[p.product.Reserved.VoiceAssetVersionID] = struct {
			assetID, digest, uri, mediaType string
			size                            int64
		}{
			assetID:   p.product.Reserved.VoiceAssetID,
			digest:    objects["voice_descriptor"].Digest,
			uri:       objects["voice_descriptor"].URI,
			mediaType: "audio/x-voice-profile+json", size: objects["voice_descriptor"].Size,
		}
	}
	for _, job := range package_.PrimaryJobs {
		var generationPlanID string
		if err := pool.QueryRow(ctx, `
			SELECT payload->>'generationPlanId'
			FROM video_pipeline.audit_events
			WHERE aggregate_type = 'GENERATION_RUN'
			  AND aggregate_id = $1
			  AND action = 'generation_run.created'
			ORDER BY occurred_at
			LIMIT 1`,
			mustUUID(job.Run.RunID),
		).Scan(&generationPlanID); err != nil {
			return Report{}, fmt.Errorf("resolve imported Run plan binding %s: %w", job.Run.RunID, err)
		}
		if generationPlanID != job.GenerationPlanID {
			return Report{}, errors.New("execution package Run differs from its immutable generation plan binding")
		}
		prompt, err := productTruth.ResolvePromptSnapshot(ctx, job.PromptSnapshotID)
		if err != nil {
			return Report{}, fmt.Errorf("resolve imported prompt %s: %w", job.PromptSnapshotID, err)
		}
		if prompt.Digest != job.PromptSnapshotHash || prompt.ID != job.PromptSnapshotID {
			return Report{}, errors.New("execution package prompt differs from PostgreSQL product truth")
		}
		seenAssets := make(map[string]struct{}, len(prompt.Assets))
		for _, asset := range prompt.Assets {
			expected, ok := expectedAssets[asset.Revision]
			if !ok || asset.ID != expected.assetID || asset.SHA256 != expected.digest ||
				asset.URI != expected.uri || asset.MediaType != expected.mediaType ||
				asset.SizeBytes != expected.size || asset.SizeBytes <= 0 {
				return Report{}, errors.New("execution package asset differs from ACTIVE PostgreSQL CAS evidence")
			}
			seenAssets[asset.Revision] = struct{}{}
		}
		if len(seenAssets) != len(expectedAssets) {
			return Report{}, errors.New("execution package is missing fixed prompt asset evidence")
		}
		budget := providercontract.BudgetEnvelope{
			EstimatedCostMicros: 1, MaxCostMicros: 1, MaxAttempts: 1,
		}
		reservation, err := providercontract.BindBudgetReservation(
			providercontract.BudgetReservation{
				ReservationID:  "offline-envelope-" + job.Run.RunID,
				Currency:       job.BudgetCurrency,
				AmountMicros:   1,
				PricingVersion: "offline-envelope-verification-v1",
				ConfirmedBy:    job.BudgetApprovalID,
			},
			providercontract.BudgetBindingInput{
				RunID: job.Run.RunID, InputHash: job.Run.RunSpecDigest,
				Model: job.Route, Budget: budget,
			},
		)
		if err != nil {
			return Report{}, fmt.Errorf("bind offline Provider envelope: %w", err)
		}
		request, err := orchestration.BuildProviderJobRequest(
			orchestration.ExecuteProviderJobInput{
				Run: job.Run, Prompt: prompt, Route: job.Route,
				BudgetApprovalID:    job.BudgetApprovalID,
				BudgetMaximumMicros: job.BudgetMaximumMicros,
				BudgetCurrency:      job.BudgetCurrency, ProviderProfileID: job.ProviderProfileID,
				TraceID: job.TraceID, PersistProductTruth: true,
			},
			orchestration.PreparedProviderJob{
				Budget: budget, BudgetReservation: reservation,
			},
		)
		if err != nil {
			return Report{}, fmt.Errorf("build offline Provider envelope: %w", err)
		}
		if len(request.Request.Assets) != len(expectedAssets) {
			return Report{}, errors.New("offline Provider envelope omitted fixed asset evidence")
		}
		if p.policy.NativeOnly {
			output := request.Request.Output
			if output.ResolvedAudioStrategy() != providercontract.AudioStrategyNativePreferred ||
				!output.GenerateAudio || output.AudioDelivery != providercontract.NativeAudioMix {
				return Report{}, errors.New("offline Provider envelope changed the frozen native audio request")
			}
		}
	}
	if p.policy.NativeOnly {
		var nativeCapabilities, speechProfiles, speechReviews, openVideoReviews, approvedVideoReviews int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_capability_snapshots WHERE provider_profile_id=$1 AND limits->>'supportsNativeAudio'='true' AND limits->>'nativeAudioDelivery'='native_mix' AND limits->>'maximumSpeechSubmits'='0'`, mustUUID(p.product.Reserved.VideoProviderProfileID)).Scan(&nativeCapabilities); err != nil {
			return Report{}, err
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_profiles WHERE base_url_ref='internal://volcengine-tts-provider' AND display_name LIKE 'FLO-154%'`).Scan(&speechProfiles); err != nil {
			return Report{}, err
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.review_tasks WHERE generation_plan_id=$1 AND budget_scope='SPEECH'`, mustUUID(p.product.Reserved.GenerationPlanID)).Scan(&speechReviews); err != nil {
			return Report{}, err
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='OPEN'), count(*) FILTER (WHERE state='APPROVED') FROM video_pipeline.review_tasks WHERE generation_plan_id=$1 AND budget_scope='VIDEO'`, mustUUID(p.product.Reserved.GenerationPlanID)).Scan(&openVideoReviews, &approvedVideoReviews); err != nil {
			return Report{}, err
		}
		if nativeCapabilities != 1 || speechProfiles != 0 || speechReviews != 0 ||
			openVideoReviews != 1 || approvedVideoReviews != 0 {
			return Report{}, errors.New("FLO-154 native materialization capability or zero-Speech boundary drifted")
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
	report := Report{SchemaVersion: p.policy.ReportSchema, BatchID: p.policy.BatchID, InputPackageHash: p.productHash, ExecutionPackageHash: package_.ContentHash, Counts: counts, CAS: casMap, ProviderCalls: 0, ProviderJobs: providerJobs, BudgetReservations: reservations, CostLedgerEntries: cost, ApprovalCommentID: approval.CommentID, ApprovalValidUntil: approval.ValidUntil}
	if p.policy.NativeOnly {
		report.VideoBudgetState = "OPEN"
	}
	return report, nil
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

func validLowerDigest(value string) bool { return len(value) == 64 && validLowerHex(value) }

func validLowerHex(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func nestedBool(value any, keys ...string) (bool, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range keys {
			if raw, ok := typed[key]; ok {
				if result, valid := raw.(bool); valid {
					return result, true
				}
			}
		}
		for _, child := range typed {
			if result, ok := nestedBool(child, keys...); ok {
				return result, true
			}
		}
	case []any:
		for _, child := range typed {
			if result, ok := nestedBool(child, keys...); ok {
				return result, true
			}
		}
	}
	return false, false
}

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
