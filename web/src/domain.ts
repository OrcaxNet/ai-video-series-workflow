export type ViewId =
  | "live-shot"
  | "overview"
  | "assets"
  | "storyboard"
  | "jobs"
  | "delivery"
  | "lineage";

export type GateId = "G1" | "G2" | "G3";
export type GateState = "PENDING" | "APPROVED" | "RETURNED" | "BLOCKED";
export type Decision = "APPROVED" | "RETURNED";

export interface RevisionBinding {
  objectType: string;
  revisionId: string;
  contentHash: string;
  label: string;
}

export interface RevisionRecord {
  revision: number;
  revisionId: string;
  state: "CURRENT" | "APPROVED" | "SUPERSEDED" | "RETURNED";
  author: string;
  createdAt: string;
  note: string;
}

export interface Gate {
  id: GateId;
  eyebrow: string;
  title: string;
  description: string;
  state: GateState;
  etag: number;
  revision: number;
  revisionId: string;
  bindings: RevisionBinding[];
  history: RevisionRecord[];
  decidedBy?: string;
  decidedAt?: string;
  explanation?: string;
}

export type JobState =
  | "QUEUED"
  | "RUNNING"
  | "RETRYING"
  | "CANCEL_REQUESTED"
  | "CANCELLED"
  | "FAILED"
  | "SUCCEEDED"
  | "UNKNOWN"
  | "REQUIRES_ACTION";

export type JobErrorCode =
  | "unauthenticated"
  | "forbidden"
  | "rate_limited"
  | "quota_exceeded"
  | "budget_exceeded"
  | "invalid_request"
  | "content_blocked"
  | "provider_unavailable"
  | "timeout";

export type JobsViewState = "READY" | "LOADING" | "EMPTY" | "UNRECOVERABLE";

export interface JobFailure {
  errorCode: JobErrorCode;
  retryable: boolean;
  requiresAction: boolean;
  suggestedAction: string;
  retryAfterSeconds?: number;
  httpStatus?: number;
}

export interface ProviderJob {
  id: string;
  shot: string;
  shotId: string;
  capability: "text.primary" | "image.primary" | "video.primary" | "speech.primary";
  provider: string;
  model: string;
  evidence: "mock_only" | "pending_key" | "live_provider_call";
  state: JobState;
  progress: number;
  retryCount: number;
  attempt: number;
  isCurrentAttempt: boolean;
  supersedesJobId?: string;
  supersededByJobId?: string;
  traceId: string;
  taskId?: string;
  costMicros?: number;
  costVerified: boolean;
  updatedAt: string;
  failure?: JobFailure;
}

export interface CreateJobAttemptInput {
  sourceJob: ProviderJob;
  nextAttempt: number;
  generationAttemptId: string;
  idempotencyKey: string;
}

export interface CreateJobAttemptResult {
  providerJobId: string;
  generationAttemptId: string;
  state: "QUEUED";
  traceId: string;
  createdAt: string;
  taskId?: string;
  g3Revision: {
    revision: number;
    revisionId: string;
    etag: number;
    state: "BLOCKED";
    bindings: RevisionBinding[];
  };
}

export interface AssetRevision {
  version: number;
  revisionId: string;
  state: "APPROVED" | "DRAFT" | "STALE";
  createdAt: string;
  note: string;
}

export interface Asset {
  id: string;
  kind: "CHARACTER" | "LOCATION" | "PROP";
  name: string;
  subtitle: string;
  palette: string[];
  version: number;
  revisionId: string;
  state: "APPROVED" | "DRAFT" | "STALE";
  usages: number;
  rights: "cleared" | "review";
  revisions: AssetRevision[];
}

export interface Shot {
  id: string;
  code: string;
  title: string;
  duration: number;
  camera: string;
  action: string;
  dialogue?: string;
  assetIds: string[];
  promptRevision: number;
  contextHash: string;
  state: "READY" | "STALE" | "GENERATING" | "SUCCEEDED";
}

export interface ContextLayer {
  scope: "SERIES" | "EPISODE" | "SCENE" | "SHOT";
  label: string;
  revision: number;
  revisionId: string;
  inherited: boolean;
  entries: Array<{ key: string; value: string; override?: boolean }>;
}

export interface PromptRevision {
  version: number;
  revisionId: string;
  state: "LOCKED" | "CURRENT" | "SUPERSEDED";
  createdAt: string;
  summary: string;
  tokens: number;
  content: string;
  changes: Array<{ type: "add" | "remove"; text: string }>;
}

export interface Activity {
  id: string;
  at: string;
  actor: string;
  action: string;
  detail: string;
}

export interface ProviderCapability {
  alias: string;
  liveConfigured: boolean;
  liveCallsEnabled: boolean;
  dryRunAvailable: boolean;
  mockAvailable: boolean;
  defaultProvider: string;
  liveEvidence:
    | "not_configured"
    | "pending_key_validation"
    | "not_enabled_for_creator_v1"
    | "authenticated_adapter_snapshot"
    | "live_provider_call";
  mockEvidence: "mock_only";
}

export interface ToastMessage {
  id: number;
  tone: "success" | "warning" | "error" | "info";
  title: string;
  description: string;
}

export interface StudioState {
  view: ViewId;
  inspectorOpen: boolean;
  busy: boolean;
  project: {
    id: string;
    title: string;
    episode: string;
    episodeTitle: string;
    aspectRatio: string;
    targetDuration: string;
    targetAudience: string;
    visualStyle: string;
    mode: "mock";
  };
  capabilities: ProviderCapability[];
  gates: Record<GateId, Gate>;
  jobs: ProviderJob[];
  jobsViewState: JobsViewState;
  assets: Asset[];
  shots: Shot[];
  contextLayers: ContextLayer[];
  promptRevisions: PromptRevision[];
  selectedPromptVersion: number;
  selectedAssetId: string;
  activity: Activity[];
  toasts: ToastMessage[];
  lastProblem?: ApiProblem;
  liveShot: LiveShotState;
}

export type LiveShotPhase =
  | "DRAFT"
  | "PLANNING"
  | "AWAITING_CONFIRMATION"
  | "CONFIRMING"
  | "RECOVERING"
  | "TRACKING"
  | "TERMINAL";

export interface CreateLiveShotPlanInput {
  title: string;
  sceneText: string;
  aspectRatio: "16:9" | "9:16";
  rightsAccepted: boolean;
}

export interface CreatorRoute {
  capabilityAlias: "video.primary";
  provider: string;
  modelId: string;
  endpointId?: string;
  routeVersion: string;
  capabilityHash: string;
  verification: string;
  billingMode: "subscription";
}

export interface CreatorLiveShotSpec {
  candidates: 1;
  durationSeconds: 5;
  resolution: "720p";
  audio: false;
  aspectRatio: "16:9" | "9:16";
}

export interface CreatorLiveShotBudget {
  maxTasksThisConfirmation: number;
  maxVideoTokensThisConfirmation: number;
  projectTaskLimit: number;
  projectTokenLimit: number;
  projectTasksUsed: number;
  projectTokensUsed: number;
  cashAmountMaximum: number | null;
  currency: string | null;
  verified: boolean;
}

export interface CreatorLiveShotPlan {
  schemaVersion: "v1";
  planId: string;
  seriesId: string;
  state: "AWAITING_CONFIRMATION";
  confirmable: boolean;
  blockers: string[];
  title: string;
  sceneTextHash: string;
  aspectRatio: "16:9" | "9:16";
  providerCallCount: number;
  providerSubmitCount: number;
  planHash: string;
  spec: CreatorLiveShotSpec;
  route: CreatorRoute;
  budget: CreatorLiveShotBudget;
  bindings: Record<string, string>;
  executionPolicy: Record<string, unknown>;
  traceId: string;
  expiresAt: string;
  createdAt: string;
}

export type CreatorLiveShotRunState =
  | "QUEUED"
  | "RUNNING"
  | "UNKNOWN"
  | "RECONCILING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED"
  | "REQUIRES_ACTION";

export interface CreatorUsage {
  promptVideoTokens: number | null;
  completionVideoTokens: number | null;
  totalVideoTokens: number | null;
  generatedDurationMs: number | null;
}

export interface CreatorCashCost {
  amountMicros: number | null;
  currency: string | null;
  verified: false;
  billingMode: "subscription";
}

export interface CreatorLiveShotArtifact {
  sha256: string;
  mediaType: "video/mp4" | string;
  sizeBytes: number;
  width?: number;
  height?: number;
  durationMs?: number;
  downloadUrl: string;
}

export interface CreatorManifestSummary {
  id: string;
  hash: string;
  url: string;
  evidence: "live_provider_call";
}

export interface CreatorFailure {
  errorCode: string;
  retryable: boolean;
  suggestedAction: string;
}

export interface CreatorLiveShotRun {
  schemaVersion: "v1";
  runId: string;
  planId: string;
  seriesId: string;
  operationId: string;
  providerJobId: string;
  state: CreatorLiveShotRunState;
  progress: number | null;
  planHash: string;
  route: CreatorRoute;
  providerTaskId: string | null;
  providerRequestId: string | null;
  submitCount: number;
  replayed: boolean;
  failure: CreatorFailure | null;
  artifact: CreatorLiveShotArtifact | null;
  usage: CreatorUsage;
  cost: CreatorCashCost;
  manifestHash?: string;
  manifest: CreatorManifestSummary | null;
  traceId: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreatorLiveShotProject {
  schemaVersion: "v1";
  seriesId: string;
  plan: CreatorLiveShotPlan;
  runs: CreatorLiveShotRun[];
}

export interface CreatorLiveShotManifest {
  schemaVersion: "creator-live-shot-manifest.v1";
  manifestId: string;
  evidence: "live_provider_call";
  runId: string;
  planId: string;
  planHash: string;
  provider: CreatorRoute;
  providerRegion: string | null;
  providerJobId: string;
  upstreamTaskId: string;
  requestId: string;
  inputHash: string;
  outputHash: string;
  media: CreatorLiveShotArtifact;
  usage: CreatorUsage;
  budget: {
    budgetApprovalId: string;
    reservationId: string;
    reservedTasks: number;
    reservedVideoTokens: number;
    settledVideoTokens: number | null;
    settlement: string;
  };
  cost: CreatorCashCost;
  createdAt: string;
}

export interface LiveShotState {
  phase: LiveShotPhase;
  plan?: CreatorLiveShotPlan;
  run?: CreatorLiveShotRun;
  manifest?: CreatorLiveShotManifest;
  etag?: string;
  problem?: ApiProblem;
}

export interface CreateProjectInput {
  title: string;
  sourceText: string;
  aspectRatio: string;
  targetDuration: string;
  targetAudience: string;
  visualStyle: string;
}

export interface CreateProjectResult {
  operationId: string;
  seriesId: string;
  state: "ACCEPTED";
  traceId: string;
}

export interface ApprovalInput {
  gateId: GateId;
  decision: Decision;
  explanation: string;
  expectedRevision: number;
  idempotencyKey: string;
  bindings: RevisionBinding[];
}

export interface ApprovalResult {
  decisionId: string;
  gate: GateId;
  decision: Decision;
  decidedAt: string;
  traceId: string;
  expectedRevision: number;
}

export interface RegenerationResult {
  gate: GateId;
  revision: number;
  revisionId: string;
  etag: number;
  createdAt: string;
}

export interface ApiProblemShape {
  status: number;
  errorCode: string;
  title: string;
  detail: string;
  retryable: boolean;
  traceId: string;
  suggestedAction: string;
  affectedObjects?: Array<{
    objectType: string;
    objectId: string;
    currentRevision?: number;
  }>;
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly errorCode: string;
  readonly retryable: boolean;
  readonly traceId: string;
  readonly suggestedAction: string;
  readonly affectedObjects: NonNullable<ApiProblemShape["affectedObjects"]>;

  constructor(problem: ApiProblemShape) {
    super(problem.detail);
    this.name = "ApiProblem";
    this.status = problem.status;
    this.errorCode = problem.errorCode;
    this.retryable = problem.retryable;
    this.traceId = problem.traceId;
    this.suggestedAction = problem.suggestedAction;
    this.affectedObjects = problem.affectedObjects ?? [];
  }
}
