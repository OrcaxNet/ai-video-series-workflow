package controlplane

import (
	"context"
	"encoding/json"
	"time"
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
	ExecutionPolicy     ExecutionPolicy
}

type StartProductionCommand struct {
	SchemaVersion               string             `json:"schemaVersion"`
	EpisodeRevisionID           string             `json:"episodeRevisionId"`
	ShotSpecRevisionIDs         []string           `json:"shotSpecRevisionIds"`
	GenerationProfileRevisionID string             `json:"generationProfileRevisionId"`
	Gate2DecisionID             string             `json:"gate2DecisionId"`
	GenerationPlanID            string             `json:"generationPlanId"`
	RouteSnapshot               ModelRouteSnapshot `json:"routeSnapshot"`
	BudgetApprovalID            string             `json:"budgetApprovalId"`
	ExecutionPolicy             ExecutionPolicy    `json:"executionPolicy"`
	Actor                       Actor              `json:"actor"`
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

// Store is the PostgreSQL product-truth boundary. Each mutation guarantees
// aggregate state, immutable revisions, idempotency, audit, and outbox writes
// are committed atomically before callers invoke Temporal or a Provider.
type Store interface {
	Ping(context.Context) error
	CreateSeries(context.Context, CreateSeriesCommand, Idempotency, string) (Stored[Operation], error)
	CreateSourceRevision(context.Context, string, int, CreateSourceRevisionCommand, Idempotency, string) (Stored[Operation], error)
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
