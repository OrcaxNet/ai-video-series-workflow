export type ViewId =
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
  traceId: string;
  taskId?: string;
  costMicros?: number;
  costVerified: boolean;
  updatedAt: string;
  failure?: JobFailure;
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
  liveEvidence: "not_configured" | "pending_key_validation" | "live_provider_call";
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
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly errorCode: string;
  readonly retryable: boolean;
  readonly traceId: string;
  readonly suggestedAction: string;

  constructor(problem: ApiProblemShape) {
    super(problem.detail);
    this.name = "ApiProblem";
    this.status = problem.status;
    this.errorCode = problem.errorCode;
    this.retryable = problem.retryable;
    this.traceId = problem.traceId;
    this.suggestedAction = problem.suggestedAction;
  }
}
