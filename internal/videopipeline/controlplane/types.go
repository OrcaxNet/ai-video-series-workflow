package controlplane

import (
	"context"
	"encoding/json"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

// Actor is the authenticated product identity copied into immutable audit
// records. The API layer still verifies that the actor is allowed to perform
// the requested action before a command reaches PostgreSQL.
type Actor struct {
	ActorID string `json:"actorId"`
	Role    string `json:"role"`
}

type RightsDeclaration struct {
	Basis                string `json:"basis"`
	EvidenceArtifactHash string `json:"evidenceArtifactHash"`
}

type CreateSeriesCommand struct {
	SchemaVersion               string            `json:"schemaVersion"`
	Title                       string            `json:"title"`
	GenerationProfileRevisionID string            `json:"generationProfileRevisionId"`
	RightsDeclaration           RightsDeclaration `json:"rightsDeclaration"`
	Actor                       Actor             `json:"actor"`
}

type CreateSourceRevisionCommand struct {
	SchemaVersion    string `json:"schemaVersion"`
	ArtifactHash     string `json:"artifactHash"`
	ArtifactURI      string `json:"artifactUri"`
	Language         string `json:"language"`
	RightsSnapshotID string `json:"rightsSnapshotId"`
	ParentRevisionID string `json:"parentRevisionId,omitempty"`
	Actor            Actor  `json:"actor"`
}

type StartContentCompilationCommand struct {
	SchemaVersion     string             `json:"schemaVersion"`
	SourceHash        string             `json:"sourceHash"`
	Stages            []string           `json:"stages"`
	TextRouteSnapshot ModelRouteSnapshot `json:"textRouteSnapshot"`
	Actor             Actor              `json:"actor"`
}

type ModelRouteSnapshot struct {
	CapabilityAlias   string `json:"capabilityAlias"`
	ProviderProfileID string `json:"providerProfileId"`
	Provider          string `json:"provider"`
	ModelID           string `json:"modelId"`
	EndpointID        string `json:"endpointId,omitempty"`
	RouteVersion      string `json:"routeVersion"`
	CapabilityHash    string `json:"capabilityHash"`
}

type BudgetLimit struct {
	AmountMicros int64  `json:"amountMicros"`
	Currency     string `json:"currency"`
}

// ExecutionPolicy is the fail-closed dispatch envelope. It is frozen into the
// generation plan so a caller cannot change territory, product form, safety
// approval, or requested quota between planning and provider submission.
type ExecutionPolicy struct {
	TargetTerritory            string `json:"targetTerritory"`
	ProductForm                string `json:"productForm"`
	ContentSafetyPolicyVersion string `json:"contentSafetyPolicyVersion"`
	ContentSafetyDecisionID    string `json:"contentSafetyDecisionId"`
}

type CostEstimate struct {
	UnitsMinimum       float64   `json:"unitsMinimum"`
	UnitsMaximum       float64   `json:"unitsMaximum"`
	Unit               string    `json:"unit"`
	AmountMinimum      *int64    `json:"amountMinimum,omitempty"`
	AmountMaximum      *int64    `json:"amountMaximum,omitempty"`
	Currency           string    `json:"currency,omitempty"`
	PricingRuleVersion string    `json:"pricingRuleVersion"`
	ValidUntil         time.Time `json:"validUntil"`
}

type CreateGenerationPlanCommand struct {
	SchemaVersion       string             `json:"schemaVersion"`
	SeriesID            string             `json:"seriesId"`
	EpisodeRevisionID   string             `json:"episodeRevisionId,omitempty"`
	ShotSpecRevisionIDs []string           `json:"shotSpecRevisionIds"`
	CandidatesPerShot   int                `json:"candidatesPerShot"`
	RouteSnapshot       ModelRouteSnapshot `json:"routeSnapshot"`
	BudgetLimit         BudgetLimit        `json:"budgetLimit"`
	SpeechBudgetLimit   *BudgetLimit       `json:"speechBudgetLimit,omitempty"`
	ExecutionPolicy     ExecutionPolicy    `json:"executionPolicy"`
	Actor               Actor              `json:"actor"`
}

type GenerationPlan struct {
	GenerationPlanID  string             `json:"generationPlanId"`
	State             string             `json:"state"`
	DryRun            bool               `json:"dryRun"`
	ShotCount         int                `json:"shotCount"`
	ProviderCallCount int                `json:"providerCallCount"`
	RouteSnapshot     ModelRouteSnapshot `json:"routeSnapshot"`
	ExecutionPolicy   ExecutionPolicy    `json:"executionPolicy"`
	Estimate          CostEstimate       `json:"estimate"`
	SpeechBudgetLimit *BudgetLimit       `json:"speechBudgetLimit,omitempty"`
	BudgetDecision    string             `json:"budgetDecision"`
	PlanHash          string             `json:"planHash"`
}

type GenerationPlanRecord struct {
	Plan                GenerationPlan
	SeriesID            string
	EpisodeRevisionID   string
	ShotSpecRevisionIDs []string
	CandidatesPerShot   int
	PricingRuleVersion  string
	BudgetLimit         BudgetLimit
	SpeechBudgetLimit   *BudgetLimit
	ExecutionPolicy     ExecutionPolicy
}

type StartProductionCommand struct {
	SchemaVersion               string                 `json:"schemaVersion"`
	EpisodeRevisionID           string                 `json:"episodeRevisionId"`
	ShotSpecRevisionIDs         []string               `json:"shotSpecRevisionIds"`
	GenerationProfileRevisionID string                 `json:"generationProfileRevisionId"`
	Gate2DecisionID             string                 `json:"gate2DecisionId"`
	GenerationPlanID            string                 `json:"generationPlanId"`
	RouteSnapshot               ModelRouteSnapshot     `json:"routeSnapshot"`
	BudgetApprovalID            string                 `json:"budgetApprovalId"`
	ExecutionPolicy             ExecutionPolicy        `json:"executionPolicy"`
	PostProduction              *PostProductionCommand `json:"postProduction,omitempty"`
	Actor                       Actor                  `json:"actor"`
}

type PostProductionCommand struct {
	Enabled                       bool               `json:"enabled"`
	Evidence                      string             `json:"evidence"`
	SpeechRouteSnapshot           ModelRouteSnapshot `json:"speechRouteSnapshot"`
	SpeechBudgetApprovalID        string             `json:"speechBudgetApprovalId"`
	SpeechBudgetLimit             BudgetLimit        `json:"speechBudgetLimit"`
	SubtitleLanguage              string             `json:"subtitleLanguage"`
	BurnSubtitles                 bool               `json:"burnSubtitles"`
	BackgroundAudioAssetVersionID string             `json:"backgroundAudioAssetVersionId,omitempty"`
	EnforcePoCDuration            bool               `json:"enforcePoCDuration"`
}

type CreateGenerationRunCommand struct {
	SchemaVersion               string             `json:"schemaVersion"`
	ShotSpecRevisionID          string             `json:"shotSpecRevisionId"`
	PromptSnapshotID            string             `json:"promptSnapshotId"`
	GenerationProfileRevisionID string             `json:"generationProfileRevisionId"`
	GenerationPlanID            string             `json:"generationPlanId"`
	RouteSnapshot               ModelRouteSnapshot `json:"routeSnapshot"`
	BudgetApprovalID            string             `json:"budgetApprovalId"`
	ExecutionPolicy             ExecutionPolicy    `json:"executionPolicy"`
	CreativeAttempt             int                `json:"creativeAttempt"`
	FallbackReasonCode          string             `json:"fallbackReasonCode,omitempty"`
	Actor                       Actor              `json:"actor"`
}

type ApprovalBinding struct {
	ObjectType  string `json:"objectType"`
	RevisionID  string `json:"revisionId"`
	ContentHash string `json:"contentHash"`
}

type CreateApprovalDecisionCommand struct {
	SchemaVersion string            `json:"schemaVersion"`
	SeriesID      string            `json:"seriesId"`
	EpisodeID     string            `json:"episodeId,omitempty"`
	Gate          string            `json:"gate"`
	Decision      string            `json:"decision"`
	ReasonCode    string            `json:"reasonCode"`
	Explanation   string            `json:"explanation,omitempty"`
	PolicyVersion string            `json:"policyVersion,omitempty"`
	EvidenceHash  string            `json:"evidenceHash,omitempty"`
	ValidUntil    *time.Time        `json:"validUntil,omitempty"`
	Bindings      []ApprovalBinding `json:"bindings"`
	Actor         Actor             `json:"actor"`
}

type ApprovalDecision struct {
	CreateApprovalDecisionCommand
	DecisionID string    `json:"decisionId"`
	DecidedAt  time.Time `json:"decidedAt"`
	TraceID    string    `json:"traceId"`
}

type LockPublicationCommand struct {
	SchemaVersion   string `json:"schemaVersion"`
	ManifestID      string `json:"manifestId"`
	ManifestHash    string `json:"manifestHash"`
	QCReportID      string `json:"qcReportId"`
	QCReportHash    string `json:"qcReportHash"`
	Gate3DecisionID string `json:"gate3DecisionId"`
	Actor           Actor  `json:"actor"`
}

type PublicationLock struct {
	PublicationLockID string    `json:"publicationLockId"`
	RunID             string    `json:"runId"`
	ManifestID        string    `json:"manifestId"`
	ManifestHash      string    `json:"manifestHash"`
	QCReportID        string    `json:"qcReportId"`
	QCReportHash      string    `json:"qcReportHash"`
	Gate3DecisionID   string    `json:"gate3DecisionId"`
	LockedAt          time.Time `json:"lockedAt"`
}

type Operation struct {
	OperationID        string    `json:"operationId"`
	OperationType      string    `json:"operationType"`
	AggregateType      string    `json:"aggregateType"`
	AggregateID        string    `json:"aggregateId"`
	State              string    `json:"state"`
	TemporalWorkflowID string    `json:"temporalWorkflowId,omitempty"`
	TemporalRunID      string    `json:"temporalRunId,omitempty"`
	TraceID            string    `json:"traceId"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt,omitempty"`
}

type GenerationRun struct {
	RunID              string    `json:"runId"`
	ShotSpecRevisionID string    `json:"shotSpecRevisionId"`
	RunSpecDigest      string    `json:"runSpecDigest"`
	CreativeAttempt    int       `json:"creativeAttempt"`
	State              string    `json:"state"`
	FailureClass       string    `json:"failureClass,omitempty"`
	FailureCode        string    `json:"failureCode,omitempty"`
	TemporalWorkflowID string    `json:"temporalWorkflowId,omitempty"`
	TraceID            string    `json:"traceId"`
	CreatedAt          time.Time `json:"createdAt"`
}

// ShotWorkflowRecord is the immutable dispatch projection loaded after the API
// transaction commits and before Temporal starts the stable shot workflow.
type ShotWorkflowRecord struct {
	Run              GenerationRun
	PromptSnapshotID string
	PromptHash       string
	RouteSnapshot    ModelRouteSnapshot
	BudgetApprovalID string
	BudgetLimit      BudgetLimit
}

type FreshnessImpact struct {
	AffectedType       string `json:"affectedType"`
	AffectedRevisionID string `json:"affectedRevisionId"`
	State              string `json:"state"`
	ReasonCode         string `json:"reasonCode"`
}

type GenerationManifest struct {
	ManifestID         string           `json:"manifestId"`
	SchemaVersion      string           `json:"schemaVersion"`
	ScopeType          string           `json:"scopeType"`
	ScopeRevisionID    string           `json:"scopeRevisionId"`
	ManifestHash       string           `json:"manifestHash"`
	ArtifactURI        string           `json:"artifactUri"`
	ProviderExecutions []map[string]any `json:"providerExecutions"`
	Inputs             []string         `json:"inputs"`
	Outputs            []string         `json:"outputs"`
	CostSummary        map[string]any   `json:"costSummary"`
	Payload            map[string]any   `json:"payload"`
	LockedAt           *time.Time       `json:"lockedAt,omitempty"`
}

type Idempotency struct {
	Scope       string
	Key         string
	RequestHash string
}

type Stored[T any] struct {
	Value    T
	Replayed bool
}

// CreatorLiveShot is the narrow Studio facade over the durable shot workflow.
// Every identifier and hash is server-derived; browsers never provide product
// truth identifiers or provider routing data.
type CreatorLiveShotPlanCommand struct {
	Title              string                              `json:"title"`
	SceneText          string                              `json:"sceneText"`
	AspectRatio        string                              `json:"aspectRatio"`
	RightsAccepted     bool                                `json:"rightsAccepted"`
	SourceArtifactHash string                              `json:"sourceArtifactHash"`
	SourceArtifactURI  string                              `json:"sourceArtifactUri"`
	Route              providercontract.CapabilitySnapshot `json:"route"`
	Actor              Actor                               `json:"actor"`
}

type CreatorLiveShotPlan struct {
	SchemaVersion               string                      `json:"schemaVersion"`
	PlanID                      string                      `json:"planId"`
	SeriesID                    string                      `json:"seriesId"`
	SourceRevisionID            string                      `json:"sourceRevisionId"`
	EpisodeRevisionID           string                      `json:"episodeRevisionId"`
	SceneRevisionID             string                      `json:"sceneRevisionId"`
	ShotSpecRevisionID          string                      `json:"shotSpecRevisionId"`
	PromptSnapshotID            string                      `json:"promptSnapshotId"`
	GenerationProfileRevisionID string                      `json:"generationProfileRevisionId"`
	BudgetApprovalID            string                      `json:"budgetApprovalId"`
	SafetyDecisionID            string                      `json:"safetyDecisionId"`
	State                       string                      `json:"state"`
	Confirmable                 bool                        `json:"confirmable"`
	Blockers                    []string                    `json:"blockers"`
	Title                       string                      `json:"title"`
	SceneTextHash               string                      `json:"sceneTextHash"`
	AspectRatio                 string                      `json:"aspectRatio"`
	Output                      providercontract.OutputSpec `json:"output"`
	Route                       CreatorRoute                `json:"route"`
	ProviderProfileID           string                      `json:"providerProfileId"`
	BillingMode                 string                      `json:"billingMode"`
	TaskLimit                   int                         `json:"taskLimit"`
	VideoTokenLimit             int64                       `json:"videoTokenLimit"`
	ProjectTaskLimit            int                         `json:"projectTaskLimit"`
	ProjectVideoTokenLimit      int64                       `json:"projectVideoTokenLimit"`
	ProjectTasksUsed            int                         `json:"projectTasksUsed"`
	ProjectVideoTokensUsed      int64                       `json:"projectVideoTokensUsed"`
	ProjectActiveRuns           int                         `json:"projectActiveRuns"`
	ProviderCallCount           int                         `json:"providerCallCount"`
	ProviderSubmitCount         int                         `json:"providerSubmitCount"`
	PlanHash                    string                      `json:"planHash"`
	Spec                        CreatorLiveShotSpec         `json:"spec"`
	Budget                      CreatorLiveShotBudget       `json:"budget"`
	Bindings                    CreatorLiveShotBindings     `json:"bindings"`
	ExecutionPolicy             map[string]any              `json:"executionPolicy"`
	TraceID                     string                      `json:"traceId"`
	ExpiresAt                   time.Time                   `json:"expiresAt"`
	CreatedAt                   time.Time                   `json:"createdAt"`
}

type CreatorLiveShotSpec struct {
	Candidates      int    `json:"candidates"`
	DurationSeconds int    `json:"durationSeconds"`
	Resolution      string `json:"resolution"`
	Audio           bool   `json:"audio"`
	AspectRatio     string `json:"aspectRatio"`
}

type CreatorLiveShotBudget struct {
	MaxTasksThisConfirmation       int     `json:"maxTasksThisConfirmation"`
	MaxVideoTokensThisConfirmation int64   `json:"maxVideoTokensThisConfirmation"`
	ProjectTaskLimit               int     `json:"projectTaskLimit"`
	ProjectTokenLimit              int64   `json:"projectTokenLimit"`
	ProjectTasksUsed               int     `json:"projectTasksUsed"`
	ProjectTokensUsed              int64   `json:"projectTokensUsed"`
	CashAmountMaximum              *int64  `json:"cashAmountMaximum"`
	Currency                       *string `json:"currency"`
	Verified                       bool    `json:"verified"`
}

type CreatorLiveShotBindings struct {
	SourceRevisionID            string `json:"sourceRevisionId"`
	EpisodeRevisionID           string `json:"episodeRevisionId"`
	SceneRevisionID             string `json:"sceneRevisionId"`
	ShotSpecRevisionID          string `json:"shotSpecRevisionId"`
	PromptSnapshotID            string `json:"promptSnapshotId"`
	GenerationProfileRevisionID string `json:"generationProfileRevisionId"`
	GenerationPlanID            string `json:"generationPlanId"`
	BudgetApprovalID            string `json:"budgetApprovalId"`
	SafetyDecisionID            string `json:"safetyDecisionId"`
}

type CreatorRoute struct {
	CapabilityAlias string `json:"capabilityAlias"`
	Provider        string `json:"provider"`
	ModelID         string `json:"modelId"`
	EndpointID      string `json:"endpointId,omitempty"`
	RouteVersion    string `json:"routeVersion"`
	CapabilityHash  string `json:"capabilityHash"`
	Verification    string `json:"verification"`
	BillingMode     string `json:"billingMode"`
}

type ConfirmCreatorLiveShotCommand struct {
	Confirmed        bool                                `json:"confirmed"`
	PlanHash         string                              `json:"planHash"`
	LiveCallsEnabled bool                                `json:"-"`
	Route            providercontract.CapabilitySnapshot `json:"-"`
	Actor            Actor                               `json:"-"`
}

type CreatorLiveShotRun struct {
	SchemaVersion     string                   `json:"schemaVersion"`
	RunID             string                   `json:"runId"`
	PlanID            string                   `json:"planId"`
	SeriesID          string                   `json:"seriesId"`
	OperationID       string                   `json:"operationId"`
	ProviderJobID     string                   `json:"providerJobId"`
	State             string                   `json:"state"`
	Progress          *int                     `json:"progress"`
	PlanHash          string                   `json:"planHash"`
	Route             CreatorRoute             `json:"route"`
	UpstreamTaskID    *string                  `json:"providerTaskId"`
	ProviderRequestID *string                  `json:"providerRequestId"`
	SubmitCount       int                      `json:"submitCount"`
	Replayed          bool                     `json:"replayed"`
	ErrorCode         string                   `json:"errorCode,omitempty"`
	Failure           *CreatorFailure          `json:"failure"`
	Artifact          *CreatorLiveShotArtifact `json:"artifact"`
	Usage             CreatorUsage             `json:"usage"`
	CashCost          CreatorCashCost          `json:"cost"`
	ManifestHash      string                   `json:"manifestHash,omitempty"`
	Manifest          *CreatorManifestSummary  `json:"manifest"`
	TraceID           string                   `json:"traceId"`
	CreatedAt         time.Time                `json:"createdAt"`
	UpdatedAt         time.Time                `json:"updatedAt"`
}

type CreatorFailure struct {
	ErrorCode       string `json:"errorCode"`
	Retryable       bool   `json:"retryable"`
	SuggestedAction string `json:"suggestedAction"`
}

type CreatorManifestSummary struct {
	ID       string `json:"id"`
	Hash     string `json:"hash"`
	URL      string `json:"url"`
	Evidence string `json:"evidence"`
}

type CreatorCashCost struct {
	AmountMicros *int64  `json:"amountMicros"`
	Currency     *string `json:"currency"`
	Verified     bool    `json:"verified"`
	BillingMode  string  `json:"billingMode"`
}

type CreatorUsage struct {
	PromptVideoTokens     *int64 `json:"promptVideoTokens"`
	CompletionVideoTokens *int64 `json:"completionVideoTokens"`
	TotalVideoTokens      *int64 `json:"totalVideoTokens"`
	GeneratedDurationMS   *int64 `json:"generatedDurationMs"`
}

type CreatorLiveShotArtifact struct {
	Digest         string `json:"sha256"`
	MediaType      string `json:"mediaType"`
	SizeBytes      int64  `json:"sizeBytes"`
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	DurationMillis int64  `json:"durationMs,omitempty"`
	DownloadURL    string `json:"downloadUrl"`
}

type CreatorLiveShotManifest struct {
	SchemaVersion  string                  `json:"schemaVersion"`
	ManifestID     string                  `json:"manifestId"`
	Evidence       string                  `json:"evidence"`
	RunID          string                  `json:"runId"`
	PlanID         string                  `json:"planId"`
	PlanHash       string                  `json:"planHash"`
	Provider       CreatorRoute            `json:"provider"`
	ProviderRegion *string                 `json:"providerRegion"`
	ProviderJobID  string                  `json:"providerJobId"`
	UpstreamTaskID string                  `json:"upstreamTaskId"`
	RequestID      string                  `json:"requestId"`
	InputHash      string                  `json:"inputHash"`
	OutputHash     string                  `json:"outputHash"`
	Media          CreatorLiveShotArtifact `json:"media"`
	Usage          CreatorUsage            `json:"usage"`
	Budget         CreatorBudgetEvidence   `json:"budget"`
	CashCost       CreatorCashCost         `json:"cost"`
	CreatedAt      time.Time               `json:"createdAt"`
}

type CreatorBudgetEvidence struct {
	BudgetApprovalID    string `json:"budgetApprovalId"`
	ReservationID       string `json:"reservationId"`
	ReservedTasks       int    `json:"reservedTasks"`
	ReservedVideoTokens int64  `json:"reservedVideoTokens"`
	SettledVideoTokens  *int64 `json:"settledVideoTokens"`
	Settlement          string `json:"settlement"`
}

type CreatorArtifactRecord struct {
	Digest    string
	MediaType string
	SizeBytes int64
}

type CreatorLiveShotProject struct {
	SchemaVersion string               `json:"schemaVersion"`
	SeriesID      string               `json:"seriesId"`
	Plan          CreatorLiveShotPlan  `json:"plan"`
	Runs          []CreatorLiveShotRun `json:"runs"`
}

// CreatorStore is separate from Store to avoid widening every existing test
// fixture while keeping the production repository implementation explicit.
type CreatorStore interface {
	CreateCreatorLiveShotPlan(context.Context, CreatorLiveShotPlanCommand, Idempotency, string) (Stored[CreatorLiveShotPlan], error)
	ConfirmCreatorLiveShotPlan(context.Context, string, ConfirmCreatorLiveShotCommand, Idempotency, string) (Stored[CreatorLiveShotRun], error)
	ListCreatorLiveShots(context.Context, string, Actor) (CreatorLiveShotProject, error)
	GetCreatorLiveShotRun(context.Context, string, Actor) (CreatorLiveShotRun, error)
	GetCreatorLiveShotArtifact(context.Context, string, Actor) (CreatorArtifactRecord, error)
	GetCreatorLiveShotManifest(context.Context, string, Actor) (CreatorLiveShotManifest, error)
}

// Store is the PostgreSQL product-truth boundary. Each mutation guarantees
// aggregate state, immutable revisions, idempotency, audit, and outbox writes
// are committed atomically before callers invoke Temporal or a Provider.
type Store interface {
	Ping(context.Context) error
	CreateSeries(context.Context, CreateSeriesCommand, Idempotency, string) (Stored[Operation], error)
	CreateSourceRevision(context.Context, string, int, CreateSourceRevisionCommand, Idempotency, string) (Stored[Operation], error)
	StartContentCompilation(context.Context, string, StartContentCompilationCommand, Idempotency, string) (Stored[Operation], error)
	CreateGenerationPlan(context.Context, CreateGenerationPlanCommand, Idempotency, string) (Stored[GenerationPlan], error)
	GetGenerationPlan(context.Context, string) (GenerationPlanRecord, error)
	PrepareProduction(context.Context, string, int, StartProductionCommand, Idempotency, string) (Stored[Operation], error)
	CreateGenerationRun(context.Context, string, int, CreateGenerationRunCommand, Idempotency, string) (Stored[Operation], error)
	GetGenerationRun(context.Context, string) (GenerationRun, error)
	GetShotWorkflowRecord(context.Context, string) (ShotWorkflowRecord, error)
	RequestRunPause(context.Context, string, int, Actor, string, Idempotency, string) (Stored[Operation], error)
	RequestRunCancellation(context.Context, string, int, Actor, string, Idempotency, string) (Stored[Operation], error)
	RequestRunResume(context.Context, string, int, Actor, string, Idempotency, string) (Stored[Operation], error)
	CreateApprovalDecision(context.Context, CreateApprovalDecisionCommand, Idempotency, string) (Stored[ApprovalDecision], error)
	LockPublication(context.Context, string, LockPublicationCommand, Idempotency, string) (Stored[PublicationLock], error)
	ListRevisionImpacts(context.Context, string, string) ([]FreshnessImpact, error)
	GetManifest(context.Context, string, string) (GenerationManifest, error)
	GetOperation(context.Context, string) (Operation, error)
	FindActiveEpisodeWorkflow(context.Context, string) (string, error)
	MarkOperationStarted(context.Context, string, string, string) error
	MarkOperationSucceeded(context.Context, string) error
	MarkOperationFailed(context.Context, string, string) error
}

type WorkflowStart struct {
	WorkflowID string
	RunID      string
}

// WorkflowController is deliberately smaller than the Temporal SDK. Tests use
// a deterministic fake and production uses the adapter in temporal.go.
type WorkflowController interface {
	StartEpisode(context.Context, Operation, StartProductionCommand) (WorkflowStart, error)
	StartShot(context.Context, Operation) (WorkflowStart, error)
	Pause(context.Context, string, string, string) error
	Cancel(context.Context, string, string) error
	Resume(context.Context, string, string, string) (WorkflowStart, error)
	RecordApproval(context.Context, ApprovalDecision) error
}

// CanonicalJSON is exposed to the repository and tests so every idempotency and
// content digest uses the same deterministic encoder.
func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}
