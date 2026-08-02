package stage1materialize

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
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
	formalProductSchema     = "flo100.product-input.v1"
	formalPlanSchema        = "flo100.generation-plan.v1"
	formalIntentSchema      = "flo100.execution-package-intent.v1"
	formalManifestHash      = "68f0a07e2ea2cd2740da07daca3bb2ce2d1a7572ed9a8756cd73101db7fbd835"
	formalFileManifestHash  = "73e075352c57451c622dcadb33934c3bf2740cbaef86cb6e025dd24084e05d5d"
	formalValidationHash    = "8a9c81ab83b3e1fe863c23730df362d86e83119fd2e0db377034ca1a341be8a9"
	formalCompatibilityHash = "bb48a4d81a383757867903fa9fd957df4aee8d3cc4aa9ed999a47d2c5247e375"
	formalIssueID           = "ed9f9834-3acf-4ccb-9016-706895e3d83a"
	formalSourceCommit      = "a033d316feb3ecf61ae5d4a508e7827ba4dfde02"
	formalPricingHash       = "9f45b53212dbb34d7ec003cf32b59532e530ce797b02ce0364ec2c77041934fd"
	formalVideoHash         = "0d1c97d70c7b332940279be334c127fa068069f83d58840fa57b4d3b10166eca"
	formalSpeechHash        = "be16957c81d71abcd026679e97b4eec7d1003cc3f4f66203b29662a95b6a9de5"
	formalRendererHash      = "5a01bdfd8241d5d00f28cb9296670ab224c5f30ff3bfcea8c1cd99ad28c1c60b"
	formalSafetyHash        = "ed8a4852873f8552477a2af1ac5b83900158405286a26fa5f04160de13c3d48f"
	formalLicenseHash       = "e458060085c9585366c4c52abe3f7fc4e52110f60bc73b61765bc3edc226c133"
)

var formalBatchIDs = []string{
	"flo100-gold-a-v1", "flo100-gold-b-v1", "flo100-gold-c-v1",
}

// FormalOptions pins the independently authorized package identity. The hash
// is required even though the directory also carries checksums: otherwise an
// attacker could rewrite both an input and its adjacent checksum file.
type FormalOptions struct {
	Root                string
	ExpectedPackageHash string
	Approval            Approval
}

type FormalReport struct {
	SchemaVersion             string              `json:"schemaVersion"`
	IssueID                   string              `json:"issueId"`
	SourceCodeCommit          string              `json:"sourceCodeCommit"`
	OfflinePackageHash        string              `json:"offlinePackageHash"`
	ExecutionPackages         []FormalBatchReport `json:"executionPackages"`
	Counts                    map[string]int64    `json:"counts"`
	CAS                       map[string]string   `json:"cas"`
	IntentIdempotencyKeys     []string            `json:"intentIdempotencyKeys"`
	ProviderCalls             int64               `json:"providerCalls"`
	ProviderJobs              int64               `json:"providerJobs"`
	BudgetReservations        int64               `json:"budgetReservations"`
	CostLedgerEntries         int64               `json:"costLedgerEntries"`
	NonSubscriptionCashMicros int64               `json:"nonSubscriptionCashMicros"`
	G1Status                  string              `json:"g1Status"`
	G2Status                  string              `json:"g2Status"`
	CurrentQuotaSnapshot      string              `json:"currentQuotaSnapshot"`
	LiveExecutionAuthorized   bool                `json:"liveExecutionAuthorized"`
	SerialBatchOrder          []string            `json:"serialBatchOrder"`
	ApprovalCommentID         string              `json:"approvalCommentId"`
	ApprovalValidUntil        time.Time           `json:"approvalValidUntil"`
}

type FormalBatchReport struct {
	BatchID               string   `json:"batchId"`
	ProductInputHash      string   `json:"productInputHash"`
	GenerationPlanHash    string   `json:"generationPlanHash"`
	ExecutionIntentHash   string   `json:"executionIntentHash"`
	ExecutionPackageHash  string   `json:"executionPackageHash"`
	ShotCount             int      `json:"shotCount"`
	AssetVersionIDs       []string `json:"assetVersionIds"`
	IntentIdempotencyKeys []string `json:"intentIdempotencyKeys"`
	LiveProviderSubmit    string   `json:"liveProviderSubmit"`
}

type formalManifest struct {
	SchemaVersion               string                `json:"schemaVersion"`
	IssueID                     string                `json:"issueId"`
	SourceCodeCommit            string                `json:"sourceCodeCommit"`
	ScopeAuthorizationCommentID string                `json:"scopeAuthorizationCommentId"`
	ScopeAuthorizationActorID   string                `json:"scopeAuthorizationActorId"`
	ValidUntil                  time.Time             `json:"validUntil"`
	FormalScope                 formalScope           `json:"formalScope"`
	Batches                     []formalManifestBatch `json:"batches"`
	FileManifestSHA256          string                `json:"fileManifestSha256"`
	ValidationReportHash        string                `json:"validationReportHash"`
	CompatibilityReportHash     string                `json:"compatibilityReportHash"`
	LiveExecutionAuthorized     bool                  `json:"liveExecutionAuthorized"`
	ProviderCalls               int                   `json:"providerCalls"`
	ContentHash                 string                `json:"contentHash"`
}

type formalScope struct {
	Samples     int `json:"samples"`
	GoldenShots int `json:"goldenShots"`
	Assets      int `json:"assets"`
}

type formalFileManifest struct {
	SchemaVersion string                   `json:"schemaVersion"`
	Files         []formalFileManifestItem `json:"files"`
}

type formalFileManifestItem struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type formalManifestBatch struct {
	BatchID             string  `json:"batchId"`
	ShotCount           int     `json:"shotCount"`
	TargetDuration      float64 `json:"targetDurationSeconds"`
	DialogueCharacters  int64   `json:"dialogueCharacters"`
	ProductInputHash    string  `json:"productInputHash"`
	GenerationPlanHash  string  `json:"generationPlanHash"`
	ExecutionIntentHash string  `json:"executionIntentHash"`
	LiveProviderSubmit  string  `json:"liveProviderSubmit"`
}

type formalProduct struct {
	SchemaVersion    string                `json:"schemaVersion"`
	IssueID          string                `json:"issueId"`
	BatchID          string                `json:"batchId"`
	State            string                `json:"state"`
	SourceCodeCommit string                `json:"sourceCodeCommit"`
	FrozenSources    formalFrozenSources   `json:"frozenSources"`
	Contexts         formalContexts        `json:"contexts"`
	AssetVersions    []formalAssetVersion  `json:"assetVersions"`
	Shots            []formalShot          `json:"shots"`
	Dialogue         formalDialogue        `json:"dialogue"`
	Gates            formalGates           `json:"gates"`
	Materialization  formalMaterialization `json:"materialization"`
	ContentHash      string                `json:"contentHash"`
}

type formalFrozenSources struct {
	ScopeAuthorizationCommentID string    `json:"scopeAuthorizationCommentId"`
	ScopeAuthorizationActorID   string    `json:"scopeAuthorizationActorId"`
	ValidUntil                  time.Time `json:"validUntil"`
}

type formalContexts struct {
	Series  json.RawMessage `json:"series"`
	Episode json.RawMessage `json:"episode"`
	Scene   json.RawMessage `json:"scene"`
}

type formalContextIdentity struct {
	ContentHash    string   `json:"contentHash"`
	TargetDuration float64  `json:"targetDurationSeconds"`
	FPS            int      `json:"fps"`
	IdentityRules  []string `json:"identityRules"`
	NegativeRules  []string `json:"negativeRules"`
}

type formalAssetSet struct {
	SchemaVersion string               `json:"schemaVersion"`
	Count         int                  `json:"count"`
	Versions      []formalAssetVersion `json:"versions"`
	ContentHash   string               `json:"contentHash"`
}

type formalAssetVersion struct {
	SchemaVersion      string          `json:"schemaVersion"`
	TextSpecID         string          `json:"textSpecId"`
	LogicalID          string          `json:"logicalId"`
	AssetType          string          `json:"assetType"`
	AssetID            string          `json:"assetId"`
	AssetVersionID     string          `json:"assetVersionId"`
	Revision           int             `json:"revision"`
	Status             string          `json:"status"`
	Description        string          `json:"description"`
	Artifact           formalArtifact  `json:"artifact"`
	Renderer           formalRenderer  `json:"renderer"`
	License            formalLicense   `json:"license"`
	SafetyEvidenceHash string          `json:"safetyEvidenceHash"`
	G1                 formalAssetG1   `json:"g1"`
	Restrictions       []string        `json:"restrictions"`
	Source             json.RawMessage `json:"source"`
	ContentHash        string          `json:"contentHash"`
}

type formalArtifact struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	MediaType string `json:"mediaType"`
	Bytes     int64  `json:"bytes"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	CASURI    string `json:"casUri"`
}

type formalRenderer struct {
	Provider               string `json:"provider"`
	ModelID                string `json:"modelId"`
	ModelVersion           string `json:"modelVersion"`
	CapabilitySnapshotHash string `json:"capabilitySnapshotHash"`
	ProviderPostCount      int    `json:"providerPostCount"`
}

type formalLicense struct {
	SnapshotID   string   `json:"snapshotId"`
	SnapshotHash string   `json:"snapshotHash"`
	Territories  []string `json:"territories"`
	ProductForms []string `json:"productForms"`
}

type formalAssetG1 struct {
	Decision                    string `json:"decision"`
	ApprovalRequiredBeforeVideo bool   `json:"approvalRequiredBeforeVideoSubmit"`
}

type formalShot struct {
	SchemaVersion         string            `json:"schemaVersion"`
	ShotID                string            `json:"shotId"`
	Ordinal               int               `json:"ordinal"`
	PreviousShotID        *string           `json:"previousShotId"`
	SourceSpanID          string            `json:"sourceSpanId"`
	DurationSeconds       float64           `json:"durationSeconds"`
	Characters            []string          `json:"characters"`
	SceneID               string            `json:"sceneId"`
	Props                 []string          `json:"props"`
	PrimaryAction         string            `json:"primaryAction"`
	Camera                string            `json:"camera"`
	ContinuityAnchor      string            `json:"continuityAnchor"`
	ExpectedVisibleFacts  string            `json:"expectedVisibleFacts"`
	ForbiddenFailures     string            `json:"forbiddenFailures"`
	SubtitleText          string            `json:"subtitleText"`
	VoiceMode             string            `json:"voiceMode"`
	AssetVersionIDs       []string          `json:"assetVersionIds"`
	AssetVersionHashes    []string          `json:"assetVersionHashes"`
	ContextHashes         map[string]string `json:"contextHashes"`
	DeterministicPrompt   string            `json:"deterministicPrompt"`
	PromptHash            string            `json:"promptHash"`
	PreviousPromptHash    *string           `json:"previousPromptHash"`
	InputHash             string            `json:"inputHash"`
	SourceGoldContentHash string            `json:"sourceGoldContentHash"`
	EvidenceTier          string            `json:"evidenceTier"`
	LiveEvidenceStatus    string            `json:"liveEvidenceStatus"`
	ContentHash           string            `json:"contentHash"`
	ShotSpecRevisionID    string            `json:"shotSpecRevisionId"`
	PromptSnapshotID      string            `json:"promptSnapshotId"`
}

type formalDialogue struct {
	UnicodeCharacters int64  `json:"unicodeCharacters"`
	ExactTextOnly     bool   `json:"exactTextOnly"`
	ReferenceText     string `json:"referenceText"`
}

type formalGates struct {
	G1                 string `json:"g1"`
	G2                 string `json:"g2"`
	Safety             string `json:"safety"`
	LiveProviderSubmit string `json:"liveProviderSubmit"`
}

type formalMaterialization struct {
	RequiresGeneralizedMaterializer bool `json:"requiresGeneralizedMaterializer"`
	ProviderCallsExpected           int  `json:"providerCallsExpectedDuringMaterialization"`
}

type formalPlan struct {
	SchemaVersion          string             `json:"schemaVersion"`
	BatchID                string             `json:"batchId"`
	GenerationPlanID       string             `json:"generationPlanId"`
	State                  string             `json:"state"`
	SourceCodeCommit       string             `json:"sourceCodeCommit"`
	DryRun                 bool               `json:"dryRun"`
	ProviderPostAuthorized bool               `json:"providerPostAuthorized"`
	Budget                 formalBudget       `json:"budget"`
	CandidatesPerShot      int                `json:"candidatesPerShot"`
	ShotSpecRevisionIDs    []string           `json:"shotSpecRevisionIds"`
	Idempotency            formalIdempotency  `json:"idempotency"`
	Route                  formalRoute        `json:"routeSnapshot"`
	SpeechRoute            *formalSpeechRoute `json:"speechRouteSnapshot"`
	ContentHash            string             `json:"contentHash"`
}

type formalBudget struct {
	VideoPrimaryJobs             int    `json:"videoPrimaryJobs"`
	VideoControlledRetries       int    `json:"videoControlledRetriesMaximum"`
	VideoMaximumJobs             int    `json:"videoMaximumJobs"`
	VideoMaximumTokens           int64  `json:"videoMaximumTokens"`
	VideoExpectedPrimaryAFPMilli int64  `json:"videoExpectedPrimaryAfpMilli"`
	VideoMaximumAFPMilli         int64  `json:"videoMaximumAfpMilliIncludingRetryAndDrift"`
	SpeechCharactersMaximum      int64  `json:"speechCharactersMaximum"`
	SpeechExpectedAFPMilli       int64  `json:"speechExpectedAfpMilli"`
	SpeechMaximumAFPMilli        int64  `json:"speechMaximumAfpMilli"`
	MaximumCashMicros            int64  `json:"maximumNonSubscriptionCashMicros"`
	MonthlyAccountCapAFPMilli    int64  `json:"monthlyAccountCapAfpMilli"`
	CurrentUsageAFPMilli         *int64 `json:"currentUsageAfpMilli"`
	PricingSnapshotHash          string `json:"pricingSnapshotHash"`
}

type formalIdempotency struct {
	Namespace string                 `json:"namespace"`
	Keys      []formalIdempotencyKey `json:"keys"`
}

type formalIdempotencyKey struct {
	ShotID string `json:"shotId"`
	Key    string `json:"key"`
}

type formalRoute struct {
	Provider               string `json:"provider"`
	ModelID                string `json:"modelId"`
	Region                 string `json:"region"`
	BillingMode            string `json:"billingMode"`
	RouteVersion           string `json:"routeVersion"`
	CapabilitySnapshotHash string `json:"capabilitySnapshotHash"`
	Verification           string `json:"verification"`
}

type formalSpeechRoute struct {
	formalRoute
	ResourceID string `json:"resourceId"`
	Speaker    string `json:"speaker"`
}

type formalIntent struct {
	SchemaVersion          string                      `json:"schemaVersion"`
	BatchID                string                      `json:"batchId"`
	State                  string                      `json:"state"`
	ProviderPostAuthorized bool                        `json:"providerPostAuthorized"`
	SourceCodeCommit       string                      `json:"sourceCodeCommit"`
	ProductInputHash       string                      `json:"productInputHash"`
	GenerationPlanHash     string                      `json:"generationPlanHash"`
	GenerationPlanID       string                      `json:"generationPlanId"`
	PrimaryJobs            []formalIntentJob           `json:"primaryJobs"`
	PostProduction         formalPostIntent            `json:"postProduction"`
	Materialization        formalIntentMaterialization `json:"materializationContract"`
	ContentHash            string                      `json:"contentHash"`
}

type formalIntentJob struct {
	ShotID                             string `json:"shotId"`
	ShotSpecRevisionID                 string `json:"shotSpecRevisionId"`
	PromptSnapshotID                   string `json:"promptSnapshotId"`
	PromptSnapshotHash                 string `json:"promptSnapshotHash"`
	RunID                              string `json:"runId"`
	AttemptID                          string `json:"attemptId"`
	IdempotencyKey                     string `json:"idempotencyKey"`
	EstimatedVideoTokens               int64  `json:"estimatedVideoTokens"`
	PredictedAFPMilli                  int64  `json:"predictedAfpMilli"`
	EstimatedNonSubscriptionCashMicros int64  `json:"estimatedNonSubscriptionCashMicros"`
	WorkflowID                         string `json:"workflowId"`
	ActivityID                         string `json:"activityId"`
	TraceID                            string `json:"traceId"`
	RouteHash                          string `json:"routeHash"`
}

type formalPostIntent struct {
	TargetDurationSeconds    float64 `json:"targetDurationSeconds"`
	SubtitleLanguage         string  `json:"subtitleLanguage"`
	SubtitleEncoding         string  `json:"subtitleEncoding"`
	BurnSubtitles            bool    `json:"burnSubtitles"`
	IndependentDialogueTrack bool    `json:"independentDialogueTrack"`
	BackgroundMusic          bool    `json:"backgroundMusic"`
	DialogueCharacters       int64   `json:"dialogueCharacters"`
	CERPolicy                string  `json:"cerPolicy"`
	SpeechRouteHash          *string `json:"speechRouteHash"`
}

type formalIntentMaterialization struct {
	MustPersistPostgreSQL bool `json:"mustPersistPostgreSQLProductTruth"`
	MustPersistCAS        bool `json:"mustPersistCASArtifacts"`
	MustSeal              bool `json:"mustSealWithRepositoryValidator"`
	ProviderCalls         int  `json:"providerCallsDuringMaterialization"`
}

type preparedFormal struct {
	root          string
	manifest      formalManifest
	manifestBytes []byte
	assets        formalAssetSet
	assetBytes    map[string][]byte
	safetyBytes   []byte
	licenseBytes  []byte
	batches       []preparedFormalBatch
}

type preparedFormalBatch struct {
	manifest        formalManifestBatch
	product         formalProduct
	plan            formalPlan
	intent          formalIntent
	productBytes    []byte
	planBytes       []byte
	intentBytes     []byte
	sourceBytes     []byte
	readiness       stage1.Plan
	assetVersionIDs []string
}

// MaterializeFLO100 validates the complete fixed A/B/C directory before any
// CAS or database write, then commits all three batches in one serializable
// transaction. It constructs no Provider client and stores no credential.
func MaterializeFLO100(
	ctx context.Context,
	pool *pgxpool.Pool,
	cas *artifactstore.Store,
	options FormalOptions,
) ([]stage1.ExecutionPackage, FormalReport, error) {
	if pool == nil || cas == nil {
		return nil, FormalReport{}, errors.New("PostgreSQL pool and CAS are required")
	}
	prepared, err := prepareFormal(options)
	if err != nil {
		return nil, FormalReport{}, err
	}
	objects, err := ingestFormalCAS(ctx, cas, prepared)
	if err != nil {
		return nil, FormalReport{}, err
	}
	packages, err := materializeFormalDB(ctx, pool, prepared, options.Approval, objects)
	if err != nil {
		return nil, FormalReport{}, err
	}
	report, err := verifyFormal(ctx, pool, prepared, options.Approval, packages, objects)
	if err != nil {
		return nil, FormalReport{}, err
	}
	return packages, report, nil
}

func prepareFormal(options FormalOptions) (preparedFormal, error) {
	root, err := filepath.Abs(strings.TrimSpace(options.Root))
	if err != nil || strings.TrimSpace(options.Root) == "" {
		return preparedFormal{}, errors.New("formal offline package root is required")
	}
	if options.ExpectedPackageHash != formalManifestHash {
		return preparedFormal{}, errors.New("formal offline package hash is not the independently pinned FLO-100 package")
	}
	if strings.TrimSpace(options.Approval.CommentID) == "" ||
		strings.TrimSpace(options.Approval.ActorID) == "" ||
		!options.Approval.ValidUntil.After(time.Now().UTC()) {
		return preparedFormal{}, errors.New("a current explicit offline-materialization approval is required")
	}
	checksums, err := verifyFormalChecksums(root)
	if err != nil {
		return preparedFormal{}, err
	}
	readJSON := func(relative string, destination any) ([]byte, error) {
		data, readErr := readFormalFile(root, relative, 8<<20)
		if readErr != nil {
			return nil, readErr
		}
		if _, present := checksums[relative]; !present {
			return nil, fmt.Errorf("%s is absent from the frozen checksum manifest", relative)
		}
		if unmarshalErr := json.Unmarshal(data, destination); unmarshalErr != nil {
			return nil, fmt.Errorf("decode %s: %w", relative, unmarshalErr)
		}
		return data, nil
	}
	var manifest formalManifest
	manifestBytes, err := readJSON("package-manifest.json", &manifest)
	if err != nil {
		return preparedFormal{}, err
	}
	if manifest.SchemaVersion != "flo100.offline-package-manifest.v1" ||
		manifest.IssueID != formalIssueID || manifest.SourceCodeCommit != formalSourceCommit ||
		manifest.ContentHash != options.ExpectedPackageHash || manifest.LiveExecutionAuthorized ||
		manifest.ProviderCalls != 0 || manifest.FormalScope != (formalScope{Samples: 3, GoldenShots: 30, Assets: 8}) ||
		len(manifest.Batches) != len(formalBatchIDs) {
		return preparedFormal{}, errors.New("formal package manifest is outside the approved FLO-100 offline boundary")
	}
	if manifest.ScopeAuthorizationCommentID != options.Approval.CommentID ||
		manifest.ScopeAuthorizationActorID != options.Approval.ActorID ||
		!manifest.ValidUntil.Equal(options.Approval.ValidUntil) {
		return preparedFormal{}, errors.New("formal package does not match the exact approval identity and validity")
	}
	if manifest.FileManifestSHA256 != formalFileManifestHash ||
		manifest.ValidationReportHash != formalValidationHash ||
		manifest.CompatibilityReportHash != formalCompatibilityHash ||
		checksums["validation/file-manifest.json"] != formalFileManifestHash {
		return preparedFormal{}, errors.New("formal file manifest hash drifted")
	}
	var fileManifest formalFileManifest
	fileManifestBytes, err := readFormalFile(root, "validation/file-manifest.json", 2<<20)
	if err != nil || json.Unmarshal(fileManifestBytes, &fileManifest) != nil ||
		fileManifest.SchemaVersion != "flo100.file-manifest.v1" || len(fileManifest.Files) != 48 {
		return preparedFormal{}, errors.New("formal file manifest is invalid")
	}
	seenManifestFiles := make(map[string]struct{}, len(fileManifest.Files))
	for _, file := range fileManifest.Files {
		if file.Path == "" || file.Bytes <= 0 || checksums[file.Path] != file.SHA256 {
			return preparedFormal{}, fmt.Errorf("formal file manifest entry %s drifted", file.Path)
		}
		if _, duplicate := seenManifestFiles[file.Path]; duplicate {
			return preparedFormal{}, fmt.Errorf("duplicate formal file manifest entry %s", file.Path)
		}
		info, statErr := os.Stat(filepath.Join(root, filepath.Clean(file.Path)))
		if statErr != nil || info.Size() != file.Bytes {
			return preparedFormal{}, fmt.Errorf("formal file manifest length drifted for %s", file.Path)
		}
		seenManifestFiles[file.Path] = struct{}{}
	}
	var validation struct {
		Status               string `json:"status"`
		ProviderCalls        int    `json:"providerCalls"`
		ProviderJobs         int    `json:"providerJobs"`
		NonSubscriptionCash  int    `json:"nonSubscriptionCashCny"`
		CurrentQuotaSnapshot string `json:"currentQuotaSnapshot"`
		ContentHash          string `json:"contentHash"`
	}
	if _, err := readJSON("validation/offline-validation-report.json", &validation); err != nil {
		return preparedFormal{}, err
	}
	if validation.ContentHash != formalValidationHash || validation.ProviderCalls != 0 ||
		validation.ProviderJobs != 0 || validation.NonSubscriptionCash != 0 ||
		validation.CurrentQuotaSnapshot != "MISSING_EXPECTED_BLOCKER" {
		return preparedFormal{}, errors.New("formal validation report no longer proves the offline cash-locked boundary")
	}
	var compatibility struct {
		ContentHash string `json:"contentHash"`
	}
	if _, err := readJSON("validation/merge-compatibility-report.json", &compatibility); err != nil {
		return preparedFormal{}, err
	}
	if compatibility.ContentHash != formalCompatibilityHash {
		return preparedFormal{}, errors.New("formal compatibility report hash drifted")
	}
	var assets formalAssetSet
	if _, err := readJSON("assets/asset-versions.json", &assets); err != nil {
		return preparedFormal{}, err
	}
	if assets.SchemaVersion != "flo100.visual-asset-set.v1" || assets.Count != 8 || len(assets.Versions) != 8 {
		return preparedFormal{}, errors.New("formal package must freeze exactly eight visual AssetVersions")
	}
	safetyBytes, err := readFormalFile(root, "snapshots/safety.json", 2<<20)
	if err != nil || checksums["snapshots/safety.json"] == "" {
		return preparedFormal{}, fmt.Errorf("read frozen safety snapshot: %w", err)
	}
	licenseBytes, err := readFormalFile(root, "snapshots/asset-license.json", 2<<20)
	if err != nil || checksums["snapshots/asset-license.json"] == "" {
		return preparedFormal{}, fmt.Errorf("read frozen license snapshot: %w", err)
	}
	var safety, license struct {
		ContentHash string `json:"contentHash"`
	}
	if json.Unmarshal(safetyBytes, &safety) != nil || safety.ContentHash != formalSafetyHash ||
		json.Unmarshal(licenseBytes, &license) != nil || license.ContentHash != formalLicenseHash {
		return preparedFormal{}, errors.New("formal safety or license snapshot drifted")
	}
	assetBytes := make(map[string][]byte, len(assets.Versions))
	assetsByID := make(map[string]formalAssetVersion, len(assets.Versions))
	for _, asset := range assets.Versions {
		if err := validateFormalAsset(root, asset, checksums); err != nil {
			return preparedFormal{}, err
		}
		if _, duplicate := assetsByID[asset.AssetVersionID]; duplicate {
			return preparedFormal{}, fmt.Errorf("duplicate formal AssetVersion %s", asset.AssetVersionID)
		}
		data, err := readFormalFile(root, asset.Artifact.Path, 16<<20)
		if err != nil {
			return preparedFormal{}, err
		}
		assetBytes[asset.AssetVersionID] = data
		assetsByID[asset.AssetVersionID] = asset
	}
	prepared := preparedFormal{
		root: root, manifest: manifest, manifestBytes: manifestBytes, assets: assets,
		assetBytes: assetBytes, safetyBytes: safetyBytes, licenseBytes: licenseBytes,
	}
	allShots := make(map[string]struct{}, 30)
	allIntentKeys := make(map[string]struct{}, 30)
	allReferencedAssets := make(map[string]struct{}, 8)
	for index, batchID := range formalBatchIDs {
		manifestBatch := manifest.Batches[index]
		if manifestBatch.BatchID != batchID || manifestBatch.ShotCount != 10 ||
			manifestBatch.LiveProviderSubmit != "DENIED" {
			return preparedFormal{}, errors.New("formal manifest batch order or live lock drifted")
		}
		base := filepath.ToSlash(filepath.Join("batches", batchID))
		var product formalProduct
		productBytes, err := readJSON(base+"/product-input.json", &product)
		if err != nil {
			return preparedFormal{}, err
		}
		var plan formalPlan
		planBytes, err := readJSON(base+"/generation-plan.json", &plan)
		if err != nil {
			return preparedFormal{}, err
		}
		var intent formalIntent
		intentBytes, err := readJSON(base+"/execution-package-intent.json", &intent)
		if err != nil {
			return preparedFormal{}, err
		}
		sourceBytes, err := readFormalFile(root, base+"/source.txt", 2<<20)
		if err != nil || checksums[base+"/source.txt"] == "" {
			return preparedFormal{}, fmt.Errorf("read %s source: %w", batchID, err)
		}
		batch := preparedFormalBatch{
			manifest: manifestBatch, product: product, plan: plan, intent: intent,
			productBytes: productBytes, planBytes: planBytes, intentBytes: intentBytes,
			sourceBytes: sourceBytes,
		}
		if err := validateFormalBatch(batch, index, manifest, assetsByID, allShots, allIntentKeys, allReferencedAssets); err != nil {
			return preparedFormal{}, err
		}
		batch.readiness = formalReadinessPlan(batch)
		if err := batch.readiness.Validate(); err != nil {
			return preparedFormal{}, fmt.Errorf("validate %s readiness envelope: %w", batchID, err)
		}
		assetSet := make(map[string]struct{})
		for _, shot := range product.Shots {
			for _, assetID := range shot.AssetVersionIDs {
				assetSet[assetID] = struct{}{}
			}
		}
		for assetID := range assetSet {
			batch.assetVersionIDs = append(batch.assetVersionIDs, assetID)
		}
		sort.Strings(batch.assetVersionIDs)
		prepared.batches = append(prepared.batches, batch)
	}
	if len(allShots) != 30 || len(allIntentKeys) != 30 || len(allReferencedAssets) != 8 {
		return preparedFormal{}, errors.New("formal package must contain 30 unique shots, 30 unique intent keys, and reference all eight assets")
	}
	return prepared, nil
}

func verifyFormalChecksums(root string) (map[string]string, error) {
	data, err := readFormalFile(root, "SHA256SUMS.txt", 2<<20)
	if err != nil {
		return nil, fmt.Errorf("read formal checksum manifest: %w", err)
	}
	checksums := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		separator := strings.Index(line, "  ")
		if separator != 64 || !validFormalDigest(line[:separator]) {
			return nil, errors.New("formal checksum manifest contains an invalid row")
		}
		relative := filepath.ToSlash(strings.TrimSpace(line[separator+2:]))
		if relative == "" || filepath.IsAbs(relative) || filepath.ToSlash(filepath.Clean(relative)) != relative || strings.HasPrefix(relative, "../") {
			return nil, errors.New("formal checksum manifest contains an unsafe path")
		}
		if _, duplicate := checksums[relative]; duplicate {
			return nil, fmt.Errorf("duplicate formal checksum path %s", relative)
		}
		payload, err := readFormalFile(root, relative, 32<<20)
		if err != nil {
			return nil, err
		}
		got := sum(payload)
		if got != line[:64] {
			return nil, fmt.Errorf("formal file %s differs from SHA256SUMS", relative)
		}
		checksums[relative] = got
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(checksums) != 50 {
		return nil, fmt.Errorf("formal checksum manifest contains %d entries, want 50", len(checksums))
	}
	return checksums, nil
}

func readFormalFile(root, relative string, maximum int64) ([]byte, error) {
	clean := filepath.Clean(relative)
	if filepath.IsAbs(relative) || clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, errors.New("formal package path escapes its root")
	}
	path := filepath.Join(root, clean)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, fmt.Errorf("formal package file %s is not a bounded regular file", relative)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != info.Size() {
		return nil, fmt.Errorf("formal package file %s changed while reading", relative)
	}
	return data, nil
}

func validateFormalAsset(root string, asset formalAssetVersion, checksums map[string]string) error {
	if asset.SchemaVersion != "flo100.visual-asset-version.v1" || asset.Revision != 1 ||
		asset.Status != "FROZEN_OFFLINE_AWAITING_G1_QA" || asset.Artifact.MediaType != "image/png" ||
		asset.Artifact.Width != 1200 || asset.Artifact.Height != 720 || asset.Artifact.Bytes <= 0 ||
		asset.Artifact.CASURI != "cas://sha256/"+asset.Artifact.SHA256 ||
		asset.Renderer.Provider != "local_product_authored" || asset.Renderer.ProviderPostCount != 0 ||
		asset.Renderer.ModelID != "flo100-deterministic-geometry-board" ||
		asset.Renderer.ModelVersion != "1.0.0" || asset.Renderer.CapabilitySnapshotHash != formalRendererHash ||
		asset.License.SnapshotHash != formalLicenseHash || !reflect.DeepEqual(asset.License.Territories, []string{"CN"}) ||
		!reflect.DeepEqual(asset.License.ProductForms, []string{"INTERNAL_POC_ACCEPTANCE"}) ||
		asset.SafetyEvidenceHash != formalSafetyHash || asset.G1.Decision != "PENDING_INDEPENDENT_QA" ||
		!asset.G1.ApprovalRequiredBeforeVideo || !validFormalDigest(asset.ContentHash) {
		return fmt.Errorf("formal AssetVersion %s is outside the approved pending-G1 boundary", asset.AssetVersionID)
	}
	if _, err := uuid.Parse(asset.AssetID); err != nil {
		return fmt.Errorf("formal assetId %s is invalid", asset.AssetID)
	}
	if _, err := uuid.Parse(asset.AssetVersionID); err != nil {
		return fmt.Errorf("formal assetVersionId %s is invalid", asset.AssetVersionID)
	}
	if _, err := uuid.Parse(asset.License.SnapshotID); err != nil {
		return fmt.Errorf("formal license snapshot %s is invalid", asset.License.SnapshotID)
	}
	if checksums[asset.Artifact.Path] != asset.Artifact.SHA256 {
		return fmt.Errorf("formal AssetVersion %s bytes differ from its exact PNG hash", asset.AssetVersionID)
	}
	info, err := os.Stat(filepath.Join(root, filepath.Clean(asset.Artifact.Path)))
	if err != nil || info.Size() != asset.Artifact.Bytes {
		return fmt.Errorf("formal AssetVersion %s byte length drifted", asset.AssetVersionID)
	}
	return nil
}

func validateFormalBatch(
	batch preparedFormalBatch,
	index int,
	manifest formalManifest,
	assets map[string]formalAssetVersion,
	allShots, allIntentKeys, allReferencedAssets map[string]struct{},
) error {
	p, plan, intent, expected := batch.product, batch.plan, batch.intent, batch.manifest
	if p.SchemaVersion != formalProductSchema || plan.SchemaVersion != formalPlanSchema ||
		intent.SchemaVersion != formalIntentSchema || p.BatchID != expected.BatchID ||
		plan.BatchID != expected.BatchID || intent.BatchID != expected.BatchID ||
		p.IssueID != formalIssueID || p.SourceCodeCommit != formalSourceCommit ||
		plan.SourceCodeCommit != formalSourceCommit || intent.SourceCodeCommit != formalSourceCommit ||
		p.ContentHash != expected.ProductInputHash || plan.ContentHash != expected.GenerationPlanHash ||
		intent.ContentHash != expected.ExecutionIntentHash || intent.ProductInputHash != p.ContentHash ||
		intent.GenerationPlanHash != plan.ContentHash || intent.GenerationPlanID != plan.GenerationPlanID {
		return fmt.Errorf("%s product, plan, intent, or pinned hash bindings drifted", expected.BatchID)
	}
	if p.State != "FROZEN_OFFLINE_AWAITING_G1_G2_QA" ||
		plan.State != "OFFLINE_FROZEN_LIVE_LOCKED" || !plan.DryRun || plan.ProviderPostAuthorized ||
		intent.State != "FROZEN_OFFLINE_NOT_EXECUTABLE" || intent.ProviderPostAuthorized ||
		p.Gates.G1 != "PENDING_INDEPENDENT_QA" || p.Gates.G2 != "PENDING_INDEPENDENT_QA" ||
		p.Gates.LiveProviderSubmit != "DENIED" || p.Materialization.ProviderCallsExpected != 0 ||
		!p.Materialization.RequiresGeneralizedMaterializer || !intent.Materialization.MustPersistPostgreSQL ||
		!intent.Materialization.MustPersistCAS || !intent.Materialization.MustSeal ||
		intent.Materialization.ProviderCalls != 0 {
		return fmt.Errorf("%s no longer proves the live-disabled materialization boundary", expected.BatchID)
	}
	if p.FrozenSources.ScopeAuthorizationCommentID != manifest.ScopeAuthorizationCommentID ||
		p.FrozenSources.ScopeAuthorizationActorID != manifest.ScopeAuthorizationActorID ||
		!p.FrozenSources.ValidUntil.Equal(manifest.ValidUntil) {
		return fmt.Errorf("%s authorization binding drifted", expected.BatchID)
	}
	if plan.Budget.VideoPrimaryJobs != 10 || plan.Budget.VideoControlledRetries != 1 ||
		plan.Budget.VideoMaximumJobs != 11 || plan.Budget.VideoMaximumTokens != 1_200_000 ||
		plan.Budget.VideoExpectedPrimaryAFPMilli != 25_047_000 ||
		plan.Budget.VideoMaximumAFPMilli != 30_306_870 || plan.Budget.MaximumCashMicros != 0 ||
		plan.Budget.MonthlyAccountCapAFPMilli != 135_000_000 || plan.Budget.CurrentUsageAFPMilli != nil ||
		plan.Budget.PricingSnapshotHash != formalPricingHash ||
		plan.Budget.SpeechCharactersMaximum != expected.DialogueCharacters ||
		p.Dialogue.UnicodeCharacters != expected.DialogueCharacters ||
		intent.PostProduction.DialogueCharacters != expected.DialogueCharacters {
		return fmt.Errorf("%s budget, quota blocker, or dialogue cap drifted", expected.BatchID)
	}
	if plan.Route.Provider != "volcengine_ark" || plan.Route.ModelID != stage1.FormalVideoModel ||
		plan.Route.Region != "cn-beijing" || plan.Route.BillingMode != "subscription_included_only" ||
		plan.Route.CapabilitySnapshotHash != formalVideoHash ||
		plan.Route.Verification != "PENDING_CURRENT_PLAN_QUOTA_SNAPSHOT" {
		return fmt.Errorf("%s video route drifted", expected.BatchID)
	}
	if expected.DialogueCharacters == 0 {
		if plan.SpeechRoute != nil || intent.PostProduction.SpeechRouteHash != nil || p.Dialogue.ReferenceText != "" {
			return fmt.Errorf("%s zero-dialogue batch unexpectedly enables speech", expected.BatchID)
		}
	} else if plan.SpeechRoute == nil || plan.SpeechRoute.CapabilitySnapshotHash != formalSpeechHash ||
		intent.PostProduction.SpeechRouteHash == nil || *intent.PostProduction.SpeechRouteHash != formalSpeechHash ||
		!p.Dialogue.ExactTextOnly || int64(len([]rune(p.Dialogue.ReferenceText))) != expected.DialogueCharacters {
		return fmt.Errorf("%s exact speech route or dialogue binding drifted", expected.BatchID)
	}
	if len(p.Shots) != 10 || len(plan.ShotSpecRevisionIDs) != 10 || len(plan.Idempotency.Keys) != 10 ||
		len(intent.PrimaryJobs) != 10 || len(p.AssetVersions) < 2 || len(p.AssetVersions) > 6 {
		return fmt.Errorf("%s must freeze 10 shots and its bounded visual set", expected.BatchID)
	}
	seriesContext, episodeContext, sceneContext, err := decodeFormalContexts(p.Contexts)
	if err != nil {
		return fmt.Errorf("%s contexts: %w", expected.BatchID, err)
	}
	if episodeContext.TargetDuration != expected.TargetDuration || seriesContext.FPS != 24 ||
		len(seriesContext.IdentityRules) == 0 || len(seriesContext.NegativeRules) == 0 {
		return fmt.Errorf("%s context duration or identity rules drifted", expected.BatchID)
	}
	productAssets := make(map[string]formalAssetVersion, len(p.AssetVersions))
	for _, asset := range p.AssetVersions {
		global, ok := assets[asset.AssetVersionID]
		if !ok || !reflect.DeepEqual(asset, global) {
			return fmt.Errorf("%s embeds a missing or drifted AssetVersion %s", expected.BatchID, asset.AssetVersionID)
		}
		productAssets[asset.AssetVersionID] = asset
	}
	var duration float64
	for shotIndex, shot := range p.Shots {
		intentJob := intent.PrimaryJobs[shotIndex]
		key := plan.Idempotency.Keys[shotIndex]
		wantPrefix := "GOLD-" + strings.ToUpper(string(rune('A'+index)))
		if shot.Ordinal != shotIndex+1 || !strings.HasPrefix(shot.ShotID, wantPrefix) ||
			plan.ShotSpecRevisionIDs[shotIndex] != shot.ShotSpecRevisionID || key.ShotID != shot.ShotID ||
			intentJob.ShotID != shot.ShotID || intentJob.ShotSpecRevisionID != shot.ShotSpecRevisionID ||
			intentJob.PromptSnapshotID != shot.PromptSnapshotID || intentJob.PromptSnapshotHash != shot.PromptHash ||
			intentJob.IdempotencyKey != key.Key || intentJob.RouteHash != formalVideoHash ||
			intentJob.EstimatedVideoTokens != 120_000 || intentJob.PredictedAFPMilli != 2_504_700 ||
			intentJob.EstimatedNonSubscriptionCashMicros != 0 || len(shot.AssetVersionIDs) == 0 ||
			len(shot.AssetVersionIDs) != len(shot.AssetVersionHashes) || shot.EvidenceTier != "LIVE_QUALITY" ||
			shot.LiveEvidenceStatus != "NOT_STARTED_PROVIDER_POST_DISABLED" ||
			!validFormalDigest(shot.ContentHash) || !validFormalDigest(shot.InputHash) ||
			key.Key != "flo100:"+expected.BatchID+":"+shot.ShotID+":attempt-1:"+shot.InputHash {
			return fmt.Errorf("%s shot %d immutable identity drifted", expected.BatchID, shotIndex+1)
		}
		for assetIndex, assetID := range shot.AssetVersionIDs {
			asset, ok := productAssets[assetID]
			if !ok || shot.AssetVersionHashes[assetIndex] != asset.Artifact.SHA256 {
				return fmt.Errorf("%s shot %s has missing or drifted visual evidence", expected.BatchID, shot.ShotID)
			}
			allReferencedAssets[assetID] = struct{}{}
		}
		if shot.ContextHashes["series"] != seriesContext.ContentHash ||
			shot.ContextHashes["episode"] != episodeContext.ContentHash ||
			shot.ContextHashes["scene"] != sceneContext.ContentHash {
			return fmt.Errorf("%s shot %s context hashes drifted", expected.BatchID, shot.ShotID)
		}
		if _, duplicate := allShots[shot.ShotID]; duplicate {
			return fmt.Errorf("duplicate formal shot %s", shot.ShotID)
		}
		if _, duplicate := allIntentKeys[key.Key]; duplicate {
			return fmt.Errorf("duplicate formal intent idempotency key %s", key.Key)
		}
		for _, value := range []string{shot.ShotSpecRevisionID, shot.PromptSnapshotID, intentJob.RunID} {
			if _, err := uuid.Parse(value); err != nil {
				return fmt.Errorf("%s shot %s contains invalid UUID %s", expected.BatchID, shot.ShotID, value)
			}
		}
		allShots[shot.ShotID] = struct{}{}
		allIntentKeys[key.Key] = struct{}{}
		duration += shot.DurationSeconds
	}
	if duration != expected.TargetDuration || intent.PostProduction.TargetDurationSeconds != expected.TargetDuration {
		return fmt.Errorf("%s target duration drifted", expected.BatchID)
	}
	return nil
}

func decodeFormalContexts(contexts formalContexts) (formalContextIdentity, formalContextIdentity, formalContextIdentity, error) {
	var series, episode, scene formalContextIdentity
	if err := json.Unmarshal(contexts.Series, &series); err != nil {
		return series, episode, scene, err
	}
	if err := json.Unmarshal(contexts.Episode, &episode); err != nil {
		return series, episode, scene, err
	}
	if err := json.Unmarshal(contexts.Scene, &scene); err != nil {
		return series, episode, scene, err
	}
	if !validFormalDigest(series.ContentHash) || !validFormalDigest(episode.ContentHash) || !validFormalDigest(scene.ContentHash) {
		return series, episode, scene, errors.New("context content hashes are invalid")
	}
	return series, episode, scene, nil
}

func formalReadinessPlan(batch preparedFormalBatch) stage1.Plan {
	shotIDs := make([]string, 0, len(batch.product.Shots))
	for _, shot := range batch.product.Shots {
		shotIDs = append(shotIDs, shot.ShotID)
	}
	return stage1.Plan{
		SchemaVersion: stage1.SchemaVersion, BatchID: batch.product.BatchID,
		VideoModel: stage1.FormalVideoModel, PrimaryShotIDs: shotIDs,
		MaximumNewJobs: 11, MaximumControlledRetries: 1, MaximumVideoTokens: 1_200_000,
		MonthlyBaselineAFPMilli: 0, MonthlyMaximumAFPMilli: batch.plan.Budget.MonthlyAccountCapAFPMilli,
		ReferenceJobAFPMilli: 2_504_700, MaximumAFPDriftBPS: 1_000,
		MaximumCashMicros: 0, MaximumDialogueCharacters: batch.plan.Budget.SpeechCharactersMaximum,
		MaximumTTSAFPMilli: batch.plan.Budget.SpeechCharactersMaximum * 135,
		RequiredEvidence:   []string{"artifact_hashes", "generation_manifest", "license_consent_gate", "provider_ids", "qc", "redaction_scan", "service_bom", "usage_cost"},
		TTSPreflight: stage1.TTSPreflight{
			CompletedNoCost: true, Provider: "volcengine_ark", Model: "doubao-seed-tts-2.0",
			Region: "cn-beijing", ResourceID: "seed-tts-2.0", CredentialReference: "ARK_API_KEY",
			CredentialAvailable: false, Pricing: "1350_afp_per_10000_chars",
			UsageAttribution: "provider_usage_tokens_per_request",
		},
		OfflineOnly: true,
	}
}

func ingestFormalCAS(ctx context.Context, cas *artifactstore.Store, prepared preparedFormal) (map[string]artifactstore.Artifact, error) {
	objects := make(map[string]artifactstore.Artifact)
	put := func(name string, data []byte) error {
		object, err := cas.Put(ctx, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("ingest formal %s: %w", name, err)
		}
		objects[name] = object
		return nil
	}
	if err := put("package_manifest", prepared.manifestBytes); err != nil {
		return nil, err
	}
	if err := put("safety", prepared.safetyBytes); err != nil {
		return nil, err
	}
	if err := put("license", prepared.licenseBytes); err != nil {
		return nil, err
	}
	for _, asset := range prepared.assets.Versions {
		if err := put("asset:"+asset.AssetVersionID, prepared.assetBytes[asset.AssetVersionID]); err != nil {
			return nil, err
		}
	}
	for _, batch := range prepared.batches {
		for name, data := range map[string][]byte{
			"product:" + batch.product.BatchID: batch.productBytes,
			"plan:" + batch.product.BatchID:    batch.planBytes,
			"intent:" + batch.product.BatchID:  batch.intentBytes,
			"source:" + batch.product.BatchID:  batch.sourceBytes,
		} {
			if err := put(name, data); err != nil {
				return nil, err
			}
		}
	}
	return objects, nil
}

func materializeFormalDB(
	ctx context.Context,
	pool *pgxpool.Pool,
	prepared preparedFormal,
	approval Approval,
	objects map[string]artifactstore.Artifact,
) ([]stage1.ExecutionPackage, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var existingCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM video_pipeline.audit_events
		WHERE action='flo100.execution_package.materialized'
		  AND reason_code='FLO100_FORMAL_OFFLINE_V1'`).Scan(&existingCount); err != nil {
		return nil, err
	}
	if existingCount != 0 && existingCount != 3 {
		return nil, errors.New("partial FLO-100 formal materialization exists; refusing an ambiguous replay")
	}
	if existingCount == 3 {
		if err := verifyFormalProjectionSeal(ctx, tx, prepared, objects); err != nil {
			return nil, err
		}
		packages, err := loadFormalReplay(ctx, tx, prepared)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return packages, nil
	}
	now := time.Now().UTC()
	exec := func(label, query string, args ...any) error {
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return nil
	}
	for name, object := range objects {
		mediaType := "application/json"
		if strings.HasPrefix(name, "source:") {
			mediaType = "text/plain; charset=utf-8"
		}
		if strings.HasPrefix(name, "asset:") {
			mediaType = "image/png"
		}
		if err := ensureActiveInputArtifact(ctx, tx, object, mediaType, map[string]any{
			"kind": "flo100_formal_input", "name": name,
			"offlinePackageHash": prepared.manifest.ContentHash,
		}); err != nil {
			return nil, err
		}
	}
	seriesID := formalUUID("series")
	videoProfileID := formalUUID("provider-profile:video")
	speechProfileID := formalUUID("provider-profile:speech")
	rendererProfileID := formalUUID("provider-profile:offline-renderer")
	videoCapabilityID := formalUUID("capability:video:" + formalVideoHash)
	speechCapabilityID := formalUUID("capability:speech:" + formalSpeechHash)
	rendererCapabilityID := formalUUID("capability:image:" + formalRendererHash)
	videoRoute := formalVideoRoute()
	speechRoute := formalSpeechModelRoute()
	if err := exec("formal video provider profile", `INSERT INTO video_pipeline.provider_profiles
		(id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash)
		VALUES ($1,'VOLCENGINE','FLO-100 frozen Agent Plan video','internal://disabled-flo100-video',NULL,false,'DRY_RUN','NOT_CONFIGURED',$2)`,
		videoProfileID, formalVideoHash); err != nil {
		return nil, err
	}
	if err := exec("formal speech provider profile", `INSERT INTO video_pipeline.provider_profiles
		(id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash)
		VALUES ($1,'VOLCENGINE','FLO-100 frozen Agent Plan speech','internal://disabled-flo100-speech',NULL,false,'DRY_RUN','NOT_CONFIGURED',$2)`,
		speechProfileID, formalSpeechHash); err != nil {
		return nil, err
	}
	if err := exec("formal offline renderer profile", `INSERT INTO video_pipeline.provider_profiles
		(id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash)
		VALUES ($1,'GENERIC','FLO-100 deterministic geometry renderer','internal://disabled-flo100-renderer',NULL,false,'DRY_RUN','NOT_CONFIGURED',$2)`,
		rendererProfileID, formalRendererHash); err != nil {
		return nil, err
	}
	for _, capability := range []struct {
		id, profile                 uuid.UUID
		alias, model, version, hash string
		inputs                      []string
	}{
		{videoCapabilityID, videoProfileID, "video.primary", stage1.FormalVideoModel, "agent-plan-large-v1", formalVideoHash, []string{"prompt", "reference_image"}},
		{speechCapabilityID, speechProfileID, "speech.primary", "doubao-seed-tts-2.0", "agent-plan-large-tts-v2", formalSpeechHash, []string{"text"}},
		{rendererCapabilityID, rendererProfileID, "image.primary", "flo100-deterministic-geometry-board", "1.0.0", formalRendererHash, []string{"prompt"}},
	} {
		if err := exec("formal pending capability", `INSERT INTO video_pipeline.provider_capability_snapshots
			(id,provider_profile_id,capability_alias,model_id,route_version,supported_inputs,limits,pricing_rule_version,capability_hash,status,effective_at,expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'agent-plan-subscription-v1',$8,'DRAFT',$9,$10)`,
			capability.id, capability.profile, capability.alias, capability.model, capability.version,
			capability.inputs, map[string]any{
				"billingMode": "subscription_included_only", "currentQuotaSnapshot": nil,
				"providerPostAuthorized": false, "monthlyAccountCapAfpMilli": int64(135_000_000),
			}, capability.hash, now, approval.ValidUntil); err != nil {
			return nil, err
		}
	}
	profileIDs := make(map[string]uuid.UUID, len(prepared.batches))
	profileHashes := make(map[string]string, len(prepared.batches))
	for _, batch := range prepared.batches {
		profileID := formalUUID("generation-profile:" + batch.product.BatchID)
		profileRef := formalUUID("generation-profile-ref:" + batch.product.BatchID)
		profileHash, err := digest(map[string]any{
			"batchId": batch.product.BatchID, "sourceCodeCommit": formalSourceCommit,
			"route": videoRoute, "speechRoute": speechRoute,
			"budget": batch.plan.Budget, "offlinePackageHash": prepared.manifest.ContentHash,
		})
		if err != nil {
			return nil, err
		}
		profileIDs[batch.product.BatchID] = profileID
		profileHashes[batch.product.BatchID] = profileHash
		if err := exec("formal generation profile", `INSERT INTO video_pipeline.generation_profiles
			(id,profile_id,revision,schema_version,status,stage,aspect_profile,episode_target_ms,shot_min_ms,shot_max_ms,
			 capability_routes,media_processing,render_defaults,qc_thresholds,retry_policy,budget_policy,license_policy,content_hash,created_by)
			VALUES ($1,$2,1,'flo100.formal-profile.v1','VALIDATED','M1','16:9_720P24',$3,4000,6000,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			profileID, profileRef, int(batch.manifest.TargetDuration*1000),
			map[string]any{"video": videoRoute, "speech": speechRoute},
			map[string]any{"postProduction": batch.intent.PostProduction},
			map[string]any{"width": 1280, "height": 720, "fps": 24, "format": "mp4"},
			map[string]any{"independentQARequired": true},
			map[string]any{"automaticRetry": false, "controlledRetriesMaximum": 1},
			map[string]any{"maximumNonSubscriptionCashMicros": 0, "monthlyAccountCapAfpMilli": 135_000_000},
			map[string]any{"territories": []string{"CN"}, "productForms": []string{"INTERNAL_POC_ACCEPTANCE"}, "g1Required": true},
			profileHash, approval.ActorID); err != nil {
			return nil, err
		}
	}
	if err := exec("formal series", `INSERT INTO video_pipeline.series
		(id,title,status,default_profile_id,rights_declaration,created_by)
		VALUES ($1,'FLO-100 formal GOLD A/B/C','DRAFT',$2,$3,$4)`,
		seriesID, profileIDs[formalBatchIDs[0]], map[string]any{
			"source": "FLO-109 original fictional fixtures", "thirdPartyMedia": false,
			"offlinePackageHash": prepared.manifest.ContentHash,
		}, approval.ActorID); err != nil {
		return nil, err
	}
	g1ID := formalUUID("gate:g1:all-assets")
	if err := exec("formal pending G1", `INSERT INTO video_pipeline.approval_decisions
		(id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id)
		VALUES ($1,$2,NULL,'G1','RETURNED','PENDING_INDEPENDENT_QA',$3,$4,'QA_PENDING',$5,'flo100-formal-g1')`,
		g1ID, seriesID, "Eight exact visual AssetVersions await independent G1 QA; live submission denied.", approval.ActorID, now); err != nil {
		return nil, err
	}
	for _, asset := range prepared.assets.Versions {
		object := objects["asset:"+asset.AssetVersionID]
		if err := exec("formal asset license", `INSERT INTO video_pipeline.license_snapshots
			(id,subject_type,subject_ref,license_id,license_hash,policy_status,territories,commercial_use,expires_at,source_uri,reviewed_by,reviewed_at)
			VALUES ($1,'ASSET',$2,'Internal PoC Test Fixture License v1.0',$3,'REQUIRES_REVIEW',ARRAY['CN'],false,$4,$5,$6,$7)`,
			mustUUID(asset.License.SnapshotID), asset.AssetVersionID, asset.License.SnapshotHash,
			approval.ValidUntil, objects["license"].URI, approval.ActorID, now); err != nil {
			return nil, err
		}
		if err := exec("formal asset", `INSERT INTO video_pipeline.assets
			(id,series_id,asset_type,scope_type,scope_id) VALUES ($1,$2,$3,'SERIES',$2)`,
			mustUUID(asset.AssetID), seriesID, asset.AssetType); err != nil {
			return nil, err
		}
		if err := exec("formal asset version", `INSERT INTO video_pipeline.asset_versions
			(id,asset_id,revision,status,content_hash,artifact_uri,media_type,dimensions,source_ref,execution_refs,license_snapshot_id,approval_decision_id,created_by)
			VALUES ($1,$2,1,'DRAFT',$3,$4,'image/png',$5,$6,$7,$8,$9,$10)`,
			mustUUID(asset.AssetVersionID), mustUUID(asset.AssetID), object.Digest, object.URI,
			map[string]any{"width": asset.Artifact.Width, "height": asset.Artifact.Height},
			"flo100:"+asset.TextSpecID,
			map[string]any{"metadataContentHash": asset.ContentHash, "renderer": asset.Renderer,
				"rendererCapabilitySnapshotId": rendererCapabilityID,
				"safetyEvidenceHash":           asset.SafetyEvidenceHash, "restrictions": asset.Restrictions,
				"offlinePackageHash": prepared.manifest.ContentHash},
			mustUUID(asset.License.SnapshotID), g1ID, approval.ActorID); err != nil {
			return nil, err
		}
		if err := exec("formal G1 asset binding", `INSERT INTO video_pipeline.approval_bindings
			(decision_id,object_type,revision_id,content_hash) VALUES ($1,'ASSET_VERSION',$2,$3)`,
			g1ID, mustUUID(asset.AssetVersionID), object.Digest); err != nil {
			return nil, err
		}
	}
	seriesContextID := formalUUID("context:series")
	var firstSeriesContext formalContextIdentity
	if err := json.Unmarshal(prepared.batches[0].product.Contexts.Series, &firstSeriesContext); err != nil {
		return nil, err
	}
	if err := exec("formal series context", `INSERT INTO video_pipeline.context_revisions
		(id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by)
		VALUES ($1,$2,'SERIES',$2,1,'APPROVED','flo100.context.v1','flo100-offline-v1',$3,$4,$5)`,
		seriesContextID, seriesID, prepared.batches[0].product.Contexts.Series, firstSeriesContext.ContentHash, approval.ActorID); err != nil {
		return nil, err
	}
	packages := make([]stage1.ExecutionPackage, 0, 3)
	for index, batch := range prepared.batches {
		package_, err := materializeFormalBatch(ctx, tx, exec, prepared, batch, index, approval, objects,
			seriesID, seriesContextID, profileIDs[batch.product.BatchID], profileHashes[batch.product.BatchID],
			videoProfileID, speechProfileID, videoRoute, speechRoute, now)
		if err != nil {
			return nil, err
		}
		packages = append(packages, package_)
	}
	if err := sealFormalProjection(ctx, tx, prepared, approval, objects, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return packages, nil
}

func materializeFormalBatch(
	ctx context.Context,
	tx pgx.Tx,
	exec func(string, string, ...any) error,
	prepared preparedFormal,
	batch preparedFormalBatch,
	batchIndex int,
	approval Approval,
	objects map[string]artifactstore.Artifact,
	seriesID, seriesContextID, profileID uuid.UUID,
	profileHash string,
	videoProfileID, speechProfileID uuid.UUID,
	videoRoute, speechRoute providercontract.ModelSnapshot,
	now time.Time,
) (stage1.ExecutionPackage, error) {
	batchID := batch.product.BatchID
	episodeID := formalUUID("episode:" + batchID)
	episodeRevisionID := formalUUID("episode-revision:" + batchID)
	sceneID := formalUUID("scene:" + batchID)
	sceneRevisionID := formalUUID("scene-revision:" + batchID)
	scriptID := formalUUID("script:" + batchID)
	storyboardID := formalUUID("storyboard:" + batchID)
	sourceID := formalUUID("source:" + batchID)
	episodeContextID := formalUUID("context:episode:" + batchID)
	sceneContextID := formalUUID("context:scene:" + batchID)
	g2ID := formalUUID("gate:g2:" + batchID)
	safetyID := formalUUID("gate:safety:" + batchID)
	safetyArtifactID := formalUUID("safety-artifact:" + batchID)
	videoBudgetID := formalUUID("budget:video:" + batchID)
	speechBudgetID := formalUUID("budget:speech:" + batchID)
	traceID := "flo100-formal-" + batchID
	createdBy := approval.ActorID
	sourceObject := objects["source:"+batchID]
	if err := exec("formal source", `INSERT INTO video_pipeline.source_revisions
		(id,series_id,revision,status,content_hash,artifact_uri,language,rights_snapshot,created_by)
		VALUES ($1,$2,$3,'APPROVED',$4,$5,'zh-CN',$6,$7)`, sourceID, seriesID, batchIndex+1,
		sourceObject.Digest, sourceObject.URI, map[string]any{"originalFiction": true, "offlinePackageHash": prepared.manifest.ContentHash}, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal episode", `INSERT INTO video_pipeline.episodes
		(id,series_id,ordinal,title) VALUES ($1,$2,$3,$4)`, episodeID, seriesID, batchIndex+1, "FLO-100 "+strings.ToUpper(string(rune('A'+batchIndex)))); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	episodeHash, err := digest(map[string]any{"productInputHash": batch.product.ContentHash, "episodeContext": batch.product.Contexts.Episode})
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal episode revision", `INSERT INTO video_pipeline.episode_revisions
		(id,episode_id,revision,status,target_duration_ms,content_hash,created_by)
		VALUES ($1,$2,1,'DRAFT',$3,$4,$5)`, episodeRevisionID, episodeID, int(batch.manifest.TargetDuration*1000), episodeHash, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal scene", `INSERT INTO video_pipeline.scenes
		(id,episode_id,ordinal) VALUES ($1,$2,1)`, sceneID, episodeID); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	sceneHash, err := digest(map[string]any{"productInputHash": batch.product.ContentHash, "sceneContext": batch.product.Contexts.Scene})
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal scene revision", `INSERT INTO video_pipeline.scene_revisions
		(id,scene_id,episode_revision_id,revision,status,content_hash,payload,created_by)
		VALUES ($1,$2,$3,1,'DRAFT',$4,$5,$6)`, sceneRevisionID, sceneID, episodeRevisionID, sceneHash, batch.product.Contexts.Scene, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	scriptHash, err := digest(map[string]any{"dialogue": batch.product.Dialogue, "shots": batch.product.Shots, "productInputHash": batch.product.ContentHash})
	if err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf("hash %s script: %w", batchID, err)
	}
	storyboardHash, err := digest(map[string]any{"scriptHash": scriptHash, "shots": batch.product.Shots, "productInputHash": batch.product.ContentHash})
	if err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf("hash %s storyboard: %w", batchID, err)
	}
	if err := exec("formal script", `INSERT INTO video_pipeline.episode_script_revisions
		(id,episode_id,revision,status,schema_version,payload,content_hash,created_by)
		VALUES ($1,$2,1,'DRAFT','flo100.script.v1',$3,$4,$5)`, scriptID, episodeID,
		map[string]any{"dialogue": batch.product.Dialogue, "shots": batch.product.Shots}, scriptHash, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal storyboard", `INSERT INTO video_pipeline.storyboard_revisions
		(id,episode_id,script_revision_id,revision,status,content_hash,created_by)
		VALUES ($1,$2,$3,1,'DRAFT',$4,$5)`, storyboardID, episodeID, scriptID, storyboardHash, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	var episodeContext, sceneContext formalContextIdentity
	if err := json.Unmarshal(batch.product.Contexts.Episode, &episodeContext); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := json.Unmarshal(batch.product.Contexts.Scene, &sceneContext); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	for _, contextRow := range []struct {
		id      uuid.UUID
		scope   string
		scopeID uuid.UUID
		payload json.RawMessage
		hash    string
	}{
		{episodeContextID, "EPISODE", episodeID, batch.product.Contexts.Episode, episodeContext.ContentHash},
		{sceneContextID, "SCENE", sceneID, batch.product.Contexts.Scene, sceneContext.ContentHash},
	} {
		if err := exec("formal context", `INSERT INTO video_pipeline.context_revisions
			(id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by)
			VALUES ($1,$2,$3,$4,1,'APPROVED','flo100.context.v1','flo100-offline-v1',$5,$6,$7)`,
			contextRow.id, seriesID, contextRow.scope, contextRow.scopeID, contextRow.payload, contextRow.hash, createdBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	for _, decision := range []struct {
		id                        uuid.UUID
		gate, reason, explanation string
	}{
		{g2ID, "G2", "PENDING_INDEPENDENT_QA", "Exact product input, shots and plan await independent G2 QA."},
		{safetyID, "SAFETY", "PENDING_INDEPENDENT_QA", "Frozen safety evidence awaits independent QA; live submission denied."},
	} {
		if err := exec("formal pending decision", `INSERT INTO video_pipeline.approval_decisions
			(id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id)
			VALUES ($1,$2,$3,$4,'RETURNED',$5,$6,$7,'QA_PENDING',$8,$9)`,
			decision.id, seriesID, episodeID, decision.gate, decision.reason, decision.explanation, approval.ActorID, now, traceID); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	bind := func(decision uuid.UUID, typ string, id uuid.UUID, hash string) error {
		return exec("formal approval binding", `INSERT INTO video_pipeline.approval_bindings
			(decision_id,object_type,revision_id,content_hash) VALUES ($1,$2,$3,$4)`, decision, typ, id, hash)
	}
	for _, assetID := range batch.assetVersionIDs {
		if err := bind(safetyID, "ASSET_VERSION", mustUUID(assetID), objects["asset:"+assetID].Digest); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	if err := exec("formal safety artifact binding", `INSERT INTO video_pipeline.approval_bindings
		(decision_id,object_type,revision_id,content_hash) VALUES ($1,'ARTIFACT',$2,$3)`,
		safetyID, safetyArtifactID, formalSafetyHash); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	for _, binding := range []struct {
		typ  string
		id   uuid.UUID
		hash string
	}{
		{"EPISODE_REVISION", episodeRevisionID, episodeHash}, {"STORYBOARD_REVISION", storyboardID, storyboardHash},
	} {
		if err := bind(g2ID, binding.typ, binding.id, binding.hash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	jobs := make([]stage1.FrozenJob, 0, 10)
	runIDs := make([]string, 0, 10)
	for shotIndex, shot := range batch.product.Shots {
		intentJob := batch.intent.PrimaryJobs[shotIndex]
		shotID := formalUUID("shot:" + shot.ShotSpecRevisionID)
		shotContextID := formalUUID("context:shot:" + shot.ShotSpecRevisionID)
		effectiveContextID := formalUUID("effective-context:" + shot.ShotSpecRevisionID)
		shotContextHash, err := digest(map[string]any{"shot": shot, "productInputHash": batch.product.ContentHash})
		if err != nil {
			return stage1.ExecutionPackage{}, fmt.Errorf("hash %s context: %w", shot.ShotID, err)
		}
		contexts := []uuid.UUID{seriesContextID, episodeContextID, sceneContextID, shotContextID}
		contextHashes := map[string]string{
			"context:series": shot.ContextHashes["series"], "context:episode": shot.ContextHashes["episode"],
			"context:scene": shot.ContextHashes["scene"], "context:shot": shotContextHash,
		}
		effectiveHash, err := digest(map[string]any{"contextRevisionIds": contexts, "contextHashes": contextHashes, "productInputHash": batch.product.ContentHash})
		if err != nil {
			return stage1.ExecutionPackage{}, fmt.Errorf("hash %s effective context: %w", shot.ShotID, err)
		}
		assetIDs := make([]uuid.UUID, len(shot.AssetVersionIDs))
		inputHashes := map[string]string{
			"shot_spec": shot.ContentHash, "generation_profile": profileHash,
			"context:series": shot.ContextHashes["series"], "context:episode": shot.ContextHashes["episode"],
			"context:scene": shot.ContextHashes["scene"], "context:shot": shotContextHash,
		}
		for assetIndex, assetID := range shot.AssetVersionIDs {
			assetIDs[assetIndex] = mustUUID(assetID)
			inputHashes["asset:"+assetID] = shot.AssetVersionHashes[assetIndex]
		}
		if err := exec("formal shot", `INSERT INTO video_pipeline.shots
			(id,scene_id,ordinal) VALUES ($1,$2,$3)`, shotID, sceneID, shot.Ordinal); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal shot context", `INSERT INTO video_pipeline.context_revisions
			(id,series_id,scope_type,scope_id,revision,status,schema_version,resolver_version,payload,content_hash,created_by)
			VALUES ($1,$2,'SHOT',$3,1,'APPROVED','flo100.context.v1','flo100-offline-v1',$4,$5,$6)`,
			shotContextID, seriesID, shotID, map[string]any{
				"shotId": shot.ShotID, "previousShotId": shot.PreviousShotID,
				"continuityAnchor": shot.ContinuityAnchor, "expectedVisibleFacts": shot.ExpectedVisibleFacts,
			}, shotContextHash, createdBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		castCount := len(shot.Characters)
		if err := exec("formal shot spec", `INSERT INTO video_pipeline.shot_spec_revisions
			(id,shot_id,storyboard_revision_id,revision,lifecycle_state,freshness,duration_ms,aspect_profile,fps,width,height,
			 cast_count,primary_action_count,narrative,asset_version_refs,context_revision_ids,effective_context_hash,
			 continuity,cinematography,generation_profile_id,gate2_decision_id,content_hash,created_by)
			VALUES ($1,$2,$3,1,'DRAFT','FRESH',$4,'16:9_720P24',24,1280,720,$5,1,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			mustUUID(shot.ShotSpecRevisionID), shotID, storyboardID, int(shot.DurationSeconds*1000), castCount,
			map[string]any{"shotId": shot.ShotID, "sourceSpanId": shot.SourceSpanID, "dialogue": dialogueForShot(shot)},
			assetIDs, contexts, effectiveHash,
			map[string]any{"anchor": shot.ContinuityAnchor, "previousShotId": shot.PreviousShotID},
			map[string]any{"camera": shot.Camera, "primaryAction": shot.PrimaryAction},
			profileID, g2ID, shot.ContentHash, createdBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := bind(g2ID, "SHOT_SPEC_REVISION", mustUUID(shot.ShotSpecRevisionID), shot.ContentHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := bind(safetyID, "SHOT_SPEC_REVISION", mustUUID(shot.ShotSpecRevisionID), shot.ContentHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal effective context", `INSERT INTO video_pipeline.effective_context_snapshots
			(id,shot_spec_revision_id,schema_version,resolver_version,context_revision_ids,normalized_payload,content_hash)
			VALUES ($1,$2,'v1','flo100-offline-v1',$3,$4,$5)`, effectiveContextID,
			mustUUID(shot.ShotSpecRevisionID), contexts, map[string]any{"contextHashes": contextHashes, "productInputHash": batch.product.ContentHash}, effectiveHash); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		output := providercontract.OutputSpec{Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9", FPS: 24, DurationMillis: int(shot.DurationSeconds * 1000), Format: "mp4"}
		promptHash, err := repository.ImportedPromptHash(shot.ShotSpecRevisionID, profileID.String(), effectiveHash,
			assetIDs, shot.DeterministicPrompt, shot.ForbiddenFailures, output, inputHashes, batch.product.ContentHash)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal prompt", `INSERT INTO video_pipeline.prompt_snapshots
			(id,shot_spec_revision_id,schema_version,compiler_version,prompt_template_ref,effective_context_snapshot_id,
			 asset_version_refs,positive_prompt,negative_prompt,model_payload,normalized_input_hash,content_hash,output_spec,input_revision_hashes)
			VALUES ($1,$2,'v1','stage1-product-input-v1','flo100.product-input.v1',$3,$4,$5,$6,$7,$8,$8,$9,$10)`,
			mustUUID(shot.PromptSnapshotID), mustUUID(shot.ShotSpecRevisionID), effectiveContextID, assetIDs,
			shot.DeterministicPrompt, shot.ForbiddenFailures,
			map[string]any{"generationProfileRevisionId": profileID, "productInputHash": batch.product.ContentHash,
				"intentInputHash": shot.InputHash, "providerPostAuthorized": false}, promptHash, output, inputHashes); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		for _, input := range []struct {
			typ        string
			id         uuid.UUID
			hash, role string
		}{
			{"SHOT_SPEC", mustUUID(shot.ShotSpecRevisionID), shot.ContentHash, "primary-shot"},
			{"GENERATION_PROFILE", profileID, profileHash, "generation-profile"},
			{"CONTEXT", seriesContextID, shot.ContextHashes["series"], "context:series"},
			{"CONTEXT", episodeContextID, shot.ContextHashes["episode"], "context:episode"},
			{"CONTEXT", sceneContextID, shot.ContextHashes["scene"], "context:scene"},
			{"CONTEXT", shotContextID, shotContextHash, "context:shot"},
		} {
			if err := exec("formal prompt input", `INSERT INTO video_pipeline.prompt_snapshot_inputs
				(prompt_snapshot_id,input_type,input_revision_id,input_hash,dependency_role) VALUES ($1,$2,$3,$4,$5)`,
				mustUUID(shot.PromptSnapshotID), input.typ, input.id, input.hash, input.role); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		for assetIndex, assetID := range shot.AssetVersionIDs {
			if err := exec("formal prompt asset", `INSERT INTO video_pipeline.prompt_snapshot_assets
				(prompt_snapshot_id,alias,asset_version_id,asset_hash,provider_role) VALUES ($1,$2,$3,$4,'reference_image')`,
				mustUUID(shot.PromptSnapshotID), fmt.Sprintf("asset-%03d", assetIndex+1), mustUUID(assetID), shot.AssetVersionHashes[assetIndex]); err != nil {
				return stage1.ExecutionPackage{}, err
			}
		}
		if err := exec("formal prompt audit", `INSERT INTO video_pipeline.audit_events
			(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
			VALUES ($1,$2,$3,'ADMIN','prompt_snapshot.imported','PROMPT_SNAPSHOT',$4,'FLO100_FORMAL_OFFLINE_V1',$5,$6)`,
			formalUUID("audit:prompt:"+shot.PromptSnapshotID), now, approval.ActorID, mustUUID(shot.PromptSnapshotID), intentJob.TraceID,
			map[string]any{"inputPackageHash": batch.product.ContentHash, "originalPromptHash": shot.PromptHash,
				"derivedPromptHash": promptHash, "intentInputHash": shot.InputHash, "approvalCommentId": approval.CommentID}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		runDigest, err := repository.GenerationRunSpecDigest(shot.ShotSpecRevisionID, shot.PromptSnapshotID,
			promptHash, profileID.String(), batch.plan.GenerationPlanID, videoRoute, 1)
		if err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal generation run", `INSERT INTO video_pipeline.generation_runs
			(id,shot_spec_revision_id,prompt_snapshot_id,generation_profile_id,temporal_workflow_id,run_spec_digest,
			 creative_attempt,state,dry_run,budget_approval_id,trace_id,created_by)
			VALUES ($1,$2,$3,$4,$5,$6,1,'VALIDATED',true,$7,$8,$9)`,
			mustUUID(intentJob.RunID), mustUUID(shot.ShotSpecRevisionID), mustUUID(shot.PromptSnapshotID), profileID,
			intentJob.WorkflowID, runDigest, videoBudgetID.String(), intentJob.TraceID, createdBy); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal generation attempt", `INSERT INTO video_pipeline.generation_attempts
			(id,generation_run_id,sequence,attempt_kind,state,input_hash,model_snapshot,parameter_diff)
			VALUES ($1,$2,1,'PROVIDER_REQUEST','VALIDATED',$3,$4,$5)`,
			formalUUID("attempt:"+intentJob.RunID), mustUUID(intentJob.RunID), runDigest, videoRoute,
			map[string]any{"intentIdempotencyKey": intentJob.IdempotencyKey, "intentInputHash": shot.InputHash,
				"offlinePackageHash": prepared.manifest.ContentHash, "providerPostAuthorized": false}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		if err := exec("formal run audit", `INSERT INTO video_pipeline.audit_events
			(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
			VALUES ($1,$2,$3,'ADMIN','generation_run.created','GENERATION_RUN',$4,'FLO100_FORMAL_OFFLINE_V1',$5,$6)`,
			formalUUID("audit:run:"+intentJob.RunID), now, approval.ActorID, mustUUID(intentJob.RunID), intentJob.TraceID,
			map[string]any{"workflowId": intentJob.WorkflowID, "shotSpecRevisionId": shot.ShotSpecRevisionID,
				"promptSnapshotId": shot.PromptSnapshotID, "runSpecDigest": runDigest, "creativeAttempt": 1,
				"generationPlanId": batch.plan.GenerationPlanID, "inputPackageHash": batch.product.ContentHash,
				"intentIdempotencyKey": intentJob.IdempotencyKey, "approvalCommentId": approval.CommentID}); err != nil {
			return stage1.ExecutionPackage{}, err
		}
		jobs = append(jobs, stage1.FrozenJob{
			ShotID: shot.ShotID, ShotSpecRevisionID: shot.ShotSpecRevisionID,
			AttemptID: intentJob.AttemptID, IdempotencyKey: "provider-job-" + intentJob.RunID,
			Run:              orchestration.GenerationRunRef{RunID: intentJob.RunID, RunSpecDigest: runDigest, Attempt: 1},
			PromptSnapshotID: shot.PromptSnapshotID, PromptSnapshotHash: promptHash,
			GenerationPlanID: batch.plan.GenerationPlanID, BudgetApprovalID: videoBudgetID.String(),
			BudgetMaximumMicros: 0, BudgetCurrency: "CNY", ProviderProfileID: videoProfileID.String(),
			Route: videoRoute, EstimatedVideoTokens: intentJob.EstimatedVideoTokens,
			PredictedAFPMilli: intentJob.PredictedAFPMilli, EstimatedNonSubscriptionCashMicros: 0,
			WorkflowID: intentJob.WorkflowID, ActivityID: intentJob.ActivityID, TraceID: intentJob.TraceID,
		})
		runIDs = append(runIDs, intentJob.RunID)
	}
	executionPolicy := controlplane.ExecutionPolicy{
		TargetTerritory: "CN", ProductForm: "INTERNAL_POC_ACCEPTANCE",
		ContentSafetyPolicyVersion: "flo100-original-fiction-safety-v1", ContentSafetyDecisionID: safetyID.String(),
	}
	planRecord := controlplane.GenerationPlan{
		GenerationPlanID: batch.plan.GenerationPlanID, State: "BLOCKED", DryRun: true,
		ShotCount: 10, ProviderCallCount: 0,
		RouteSnapshot: controlplane.ModelRouteSnapshot{CapabilityAlias: "video.primary", ProviderProfileID: videoProfileID.String(),
			Provider: videoRoute.Provider, ModelID: videoRoute.ModelID, RouteVersion: videoRoute.RouteVersion, CapabilityHash: videoRoute.CapabilityHash},
		ExecutionPolicy: executionPolicy,
		Estimate: controlplane.CostEstimate{UnitsMinimum: batch.manifest.TargetDuration, UnitsMaximum: batch.manifest.TargetDuration,
			Unit: "video_seconds", Currency: "CNY", PricingRuleVersion: "agent-plan-subscription-v1", ValidUntil: approval.ValidUntil},
		SpeechBudgetLimit: &controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
		BudgetDecision:    "PENDING_CURRENT_QUOTA_AND_QA", PlanHash: batch.plan.ContentHash,
	}
	if err := exec("formal plan operation", `INSERT INTO video_pipeline.operation_requests
		(id,operation_type,aggregate_type,aggregate_id,state,trace_id,requested_by)
		VALUES ($1,'CREATE_GENERATION_PLAN','SERIES',$2,'SUCCEEDED',$3,$4)`,
		mustUUID(batch.plan.GenerationPlanID), seriesID, traceID, createdBy); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := exec("formal plan idempotency", `INSERT INTO video_pipeline.idempotency_records
		(scope,idempotency_key,request_hash,operation_id,response_status,response_body,expires_at)
		VALUES ('flo100-formal-materialize',$1,$2,$3,201,$4,$5)`,
		batch.product.ContentHash, batch.plan.ContentHash, mustUUID(batch.plan.GenerationPlanID), planRecord, approval.ValidUntil); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	shotSpecIDs := make([]string, len(batch.product.Shots))
	for i, shot := range batch.product.Shots {
		shotSpecIDs[i] = shot.ShotSpecRevisionID
	}
	if err := exec("formal plan audit", `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','generation_plan.created','GENERATION_PLAN',$4,'FLO100_FORMAL_OFFLINE_V1',$5,$6)`,
		formalUUID("audit:plan:"+batchID), now, approval.ActorID, mustUUID(batch.plan.GenerationPlanID), traceID,
		map[string]any{"seriesId": seriesID, "episodeRevisionId": episodeRevisionID,
			"shotSpecRevisionIds": shotSpecIDs, "candidatesPerShot": 1,
			"pricingRuleVersion": "agent-plan-subscription-v1", "planHash": batch.plan.ContentHash,
			"state": "BLOCKED", "budgetDecision": "PENDING_CURRENT_QUOTA_AND_QA",
			"budgetLimit":       controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
			"speechBudgetLimit": controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
			"executionPolicy":   executionPolicy, "inputPackageHash": batch.product.ContentHash,
			"approvalCommentId": approval.CommentID, "providerPostAuthorized": false}); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	for _, review := range []struct {
		id    uuid.UUID
		scope string
	}{{videoBudgetID, "VIDEO"}, {speechBudgetID, "SPEECH"}} {
		if err := exec("formal pending budget review", `INSERT INTO video_pipeline.review_tasks
			(id,series_id,episode_id,review_type,state,reason_codes,assigned_role,generation_plan_id,budget_scope,budget_limit_micros,budget_currency)
			VALUES ($1,$2,$3,'BUDGET','OPEN',ARRAY['CURRENT_QUOTA_AND_INDEPENDENT_QA_REQUIRED'],'QA',$4,$5,0,'CNY')`,
			review.id, seriesID, episodeID, mustUUID(batch.plan.GenerationPlanID), review.scope); err != nil {
			return stage1.ExecutionPackage{}, err
		}
	}
	postConfig := orchestration.PostProductionConfig{
		Enabled:            batch.product.Dialogue.UnicodeCharacters > 0,
		Evidence:           postproduction.EvidencePendingKey,
		SubtitleLanguage:   batch.intent.PostProduction.SubtitleLanguage,
		BurnSubtitles:      batch.intent.PostProduction.BurnSubtitles,
		EnforcePoCDuration: true,
	}
	if postConfig.Enabled {
		postConfig.SpeechRoute = speechRoute
		postConfig.SpeechProviderProfileID = speechProfileID.String()
		postConfig.SpeechBudgetApprovalID = speechBudgetID.String()
		postConfig.SpeechBudgetMaximumMicros = 0
		postConfig.SpeechBudgetCurrency = "CNY"
	}
	package_, err := stage1.SealExecutionPackage(stage1.ExecutionPackage{
		SchemaVersion: stage1.ExecutionPackageSchemaVersion, BatchID: batchID,
		PrimaryJobs: jobs,
		PostProduction: orchestration.FinalizeEpisodeInput{
			EpisodeRevisionID: episodeRevisionID.String(), RunIDs: runIDs,
			GenerationPlanID: batch.plan.GenerationPlanID, TraceID: traceID,
			PersistProductTruth: true, Config: postConfig,
		},
	})
	if err != nil {
		return stage1.ExecutionPackage{}, err
	}
	if err := package_.Validate(batch.readiness); err != nil {
		return stage1.ExecutionPackage{}, fmt.Errorf("validate %s sealed package: %w", batchID, err)
	}
	previousBatch := ""
	if batchIndex > 0 {
		previousBatch = formalBatchIDs[batchIndex-1]
	}
	if err := exec("formal materialization audit", `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','flo100.execution_package.materialized','GENERATION_PLAN',$4,'FLO100_FORMAL_OFFLINE_V1',$5,$6)`,
		formalUUID("audit:materialize:"+batchID), now, approval.ActorID, mustUUID(batch.plan.GenerationPlanID), traceID,
		map[string]any{"batchOrder": batchIndex + 1, "previousBatch": previousBatch,
			"inputPackageHash": batch.product.ContentHash, "generationPlanHash": batch.plan.ContentHash,
			"executionIntentHash": batch.intent.ContentHash, "executionPackageHash": package_.ContentHash,
			"executionPackage": package_, "approvalCommentId": approval.CommentID,
			"approvalValidUntil": approval.ValidUntil, "offlinePackageHash": prepared.manifest.ContentHash,
			"g1": "PENDING_INDEPENDENT_QA", "g2": "PENDING_INDEPENDENT_QA",
			"currentQuotaSnapshot": "MISSING_EXPECTED_BLOCKER", "providerCalls": 0,
			"providerPostAuthorized": false}); err != nil {
		return stage1.ExecutionPackage{}, err
	}
	return package_, nil
}

func loadFormalReplay(ctx context.Context, tx pgx.Tx, prepared preparedFormal) ([]stage1.ExecutionPackage, error) {
	packages := make([]stage1.ExecutionPackage, 0, 3)
	for index, batch := range prepared.batches {
		var payload struct {
			BatchOrder          int                     `json:"batchOrder"`
			InputPackageHash    string                  `json:"inputPackageHash"`
			GenerationPlanHash  string                  `json:"generationPlanHash"`
			ExecutionIntentHash string                  `json:"executionIntentHash"`
			OfflinePackageHash  string                  `json:"offlinePackageHash"`
			ApprovalCommentID   string                  `json:"approvalCommentId"`
			ExecutionPackage    stage1.ExecutionPackage `json:"executionPackage"`
		}
		if err := tx.QueryRow(ctx, `SELECT payload FROM video_pipeline.audit_events
			WHERE action='flo100.execution_package.materialized' AND aggregate_id=$1
			  AND reason_code='FLO100_FORMAL_OFFLINE_V1'`, mustUUID(batch.plan.GenerationPlanID)).Scan(&payload); err != nil {
			return nil, err
		}
		if payload.BatchOrder != index+1 || payload.InputPackageHash != batch.product.ContentHash ||
			payload.GenerationPlanHash != batch.plan.ContentHash || payload.ExecutionIntentHash != batch.intent.ContentHash ||
			payload.OfflinePackageHash != prepared.manifest.ContentHash ||
			payload.ApprovalCommentID != prepared.manifest.ScopeAuthorizationCommentID ||
			payload.ExecutionPackage.Validate(batch.readiness) != nil {
			return nil, fmt.Errorf("existing %s materialization differs from the frozen package", batch.product.BatchID)
		}
		packages = append(packages, payload.ExecutionPackage)
	}
	return packages, nil
}

type formalProjectionSection struct {
	Name string            `json:"name"`
	Rows []json.RawMessage `json:"rows"`
}

type formalProjectionSnapshot struct {
	SchemaVersion      string                    `json:"schemaVersion"`
	OfflinePackageHash string                    `json:"offlinePackageHash"`
	Sections           []formalProjectionSection `json:"sections"`
}

type formalProjectionSectionSpec struct {
	name     string
	expected int
	query    string
	args     []any
}

func sealFormalProjection(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedFormal,
	approval Approval,
	objects map[string]artifactstore.Artifact,
	now time.Time,
) error {
	projectionHash, err := formalProjectionHash(ctx, tx, prepared, objects)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','flo100.formal_projection.sealed','SERIES',$4,
			'FLO100_FORMAL_PROJECTION_V1','flo100-formal-projection',$5)`,
		formalUUID("audit:projection-seal"), now, approval.ActorID, formalUUID("series"), map[string]any{
			"schemaVersion":      "flo100.formal-projection.v1",
			"sourceCodeCommit":   formalSourceCommit,
			"offlinePackageHash": prepared.manifest.ContentHash,
			"approvalCommentId":  approval.CommentID,
			"projectionHash":     projectionHash,
		}); err != nil {
		return fmt.Errorf("seal FLO-100 formal projection: %w", err)
	}
	return nil
}

func verifyFormalProjectionSeal(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedFormal,
	objects map[string]artifactstore.Artifact,
) error {
	var sealCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.audit_events
		WHERE action='flo100.formal_projection.sealed'
		  AND aggregate_type='SERIES' AND aggregate_id=$1
		  AND reason_code='FLO100_FORMAL_PROJECTION_V1'`, formalUUID("series")).Scan(&sealCount); err != nil {
		return fmt.Errorf("count FLO-100 formal projection seals: %w", err)
	}
	if sealCount != 1 {
		return fmt.Errorf("formal projection drift: expected exactly one immutable projection seal, got %d", sealCount)
	}
	var actorID, actorRole, traceID string
	var payload struct {
		SchemaVersion      string `json:"schemaVersion"`
		SourceCodeCommit   string `json:"sourceCodeCommit"`
		OfflinePackageHash string `json:"offlinePackageHash"`
		ApprovalCommentID  string `json:"approvalCommentId"`
		ProjectionHash     string `json:"projectionHash"`
	}
	if err := tx.QueryRow(ctx, `SELECT actor_id,actor_role,trace_id,payload
		FROM video_pipeline.audit_events
		WHERE id=$1 AND action='flo100.formal_projection.sealed'
		  AND aggregate_type='SERIES' AND aggregate_id=$2
		  AND reason_code='FLO100_FORMAL_PROJECTION_V1'`,
		formalUUID("audit:projection-seal"), formalUUID("series")).Scan(&actorID, &actorRole, &traceID, &payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("formal projection drift: deterministic projection seal is missing or rebound")
		}
		return fmt.Errorf("load FLO-100 formal projection seal: %w", err)
	}
	if actorID != prepared.manifest.ScopeAuthorizationActorID || actorRole != "ADMIN" ||
		traceID != "flo100-formal-projection" || payload.SchemaVersion != "flo100.formal-projection.v1" ||
		payload.SourceCodeCommit != formalSourceCommit || payload.OfflinePackageHash != prepared.manifest.ContentHash ||
		payload.ApprovalCommentID != prepared.manifest.ScopeAuthorizationCommentID || !validFormalDigest(payload.ProjectionHash) {
		return errors.New("formal projection drift: projection seal identity differs from the frozen authorization")
	}
	projectionHash, err := formalProjectionHash(ctx, tx, prepared, objects)
	if err != nil {
		return err
	}
	if projectionHash != payload.ProjectionHash {
		return fmt.Errorf("formal projection drift: current PostgreSQL projection hash %s differs from sealed %s", projectionHash, payload.ProjectionHash)
	}
	return nil
}

func formalProjectionHash(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedFormal,
	objects map[string]artifactstore.Artifact,
) (string, error) {
	seriesID := formalUUID("series")
	providerProfileIDs := []uuid.UUID{
		formalUUID("provider-profile:video"),
		formalUUID("provider-profile:speech"),
		formalUUID("provider-profile:offline-renderer"),
	}
	capabilityIDs := []uuid.UUID{
		formalUUID("capability:video:" + formalVideoHash),
		formalUUID("capability:speech:" + formalSpeechHash),
		formalUUID("capability:image:" + formalRendererHash),
	}
	generationProfileIDs := make([]uuid.UUID, 0, len(prepared.batches))
	decisionIDs := []uuid.UUID{formalUUID("gate:g1:all-assets")}
	licenseIDs := make([]uuid.UUID, 0, len(prepared.assets.Versions))
	expectedApprovalBindings := len(prepared.assets.Versions)
	expectedPromptAssets := 0
	for _, asset := range prepared.assets.Versions {
		licenseIDs = append(licenseIDs, mustUUID(asset.License.SnapshotID))
	}
	for _, batch := range prepared.batches {
		generationProfileIDs = append(generationProfileIDs, formalUUID("generation-profile:"+batch.product.BatchID))
		decisionIDs = append(decisionIDs,
			formalUUID("gate:g2:"+batch.product.BatchID),
			formalUUID("gate:safety:"+batch.product.BatchID),
		)
		expectedApprovalBindings += len(batch.assetVersionIDs) + 23
		for _, shot := range batch.product.Shots {
			expectedPromptAssets += len(shot.AssetVersionIDs)
		}
	}
	artifactHashSet := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		artifactHashSet[object.Digest] = struct{}{}
	}
	artifactHashes := make([]string, 0, len(artifactHashSet))
	for digest := range artifactHashSet {
		artifactHashes = append(artifactHashes, digest)
	}
	sort.Strings(artifactHashes)

	sections := []formalProjectionSectionSpec{
		{"artifacts", len(artifactHashes), `SELECT to_jsonb(a) FROM video_pipeline.artifacts a WHERE content_hash=ANY($1) ORDER BY content_hash`, []any{artifactHashes}},
		{"provider_profiles", 3, `SELECT to_jsonb(p) FROM video_pipeline.provider_profiles p WHERE id=ANY($1) ORDER BY id`, []any{providerProfileIDs}},
		{"provider_capabilities", 3, `SELECT to_jsonb(c) FROM video_pipeline.provider_capability_snapshots c WHERE id=ANY($1) ORDER BY id`, []any{capabilityIDs}},
		{"generation_profiles", 3, `SELECT to_jsonb(g) FROM video_pipeline.generation_profiles g WHERE id=ANY($1) ORDER BY id`, []any{generationProfileIDs}},
		{"series", 1, `SELECT to_jsonb(s) FROM video_pipeline.series s WHERE id=$1 ORDER BY id`, []any{seriesID}},
		{"source_revisions", 3, `SELECT to_jsonb(s) FROM video_pipeline.source_revisions s WHERE series_id=$1 ORDER BY id`, []any{seriesID}},
		{"episodes", 3, `SELECT to_jsonb(e) FROM video_pipeline.episodes e WHERE series_id=$1 ORDER BY id`, []any{seriesID}},
		{"episode_revisions", 3, `SELECT to_jsonb(er) FROM video_pipeline.episode_revisions er JOIN video_pipeline.episodes e ON e.id=er.episode_id WHERE e.series_id=$1 ORDER BY er.id`, []any{seriesID}},
		{"scenes", 3, `SELECT to_jsonb(s) FROM video_pipeline.scenes s JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY s.id`, []any{seriesID}},
		{"scene_revisions", 3, `SELECT to_jsonb(sr) FROM video_pipeline.scene_revisions sr JOIN video_pipeline.scenes s ON s.id=sr.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY sr.id`, []any{seriesID}},
		{"scripts", 3, `SELECT to_jsonb(esr) FROM video_pipeline.episode_script_revisions esr JOIN video_pipeline.episodes e ON e.id=esr.episode_id WHERE e.series_id=$1 ORDER BY esr.id`, []any{seriesID}},
		{"storyboards", 3, `SELECT to_jsonb(sr) FROM video_pipeline.storyboard_revisions sr JOIN video_pipeline.episodes e ON e.id=sr.episode_id WHERE e.series_id=$1 ORDER BY sr.id`, []any{seriesID}},
		{"contexts", 37, `SELECT to_jsonb(c) FROM video_pipeline.context_revisions c WHERE series_id=$1 ORDER BY id`, []any{seriesID}},
		{"approval_decisions", 7, `SELECT to_jsonb(d) FROM video_pipeline.approval_decisions d WHERE series_id=$1 ORDER BY id`, []any{seriesID}},
		{"approval_bindings", expectedApprovalBindings, `SELECT to_jsonb(b) FROM video_pipeline.approval_bindings b WHERE decision_id=ANY($1) ORDER BY decision_id,object_type,revision_id`, []any{decisionIDs}},
		{"license_snapshots", 8, `SELECT to_jsonb(l) FROM video_pipeline.license_snapshots l WHERE id=ANY($1) ORDER BY id`, []any{licenseIDs}},
		{"assets", 8, `SELECT to_jsonb(a) FROM video_pipeline.assets a WHERE series_id=$1 ORDER BY id`, []any{seriesID}},
		{"asset_versions", 8, `SELECT to_jsonb(av) FROM video_pipeline.asset_versions av JOIN video_pipeline.assets a ON a.id=av.asset_id WHERE a.series_id=$1 ORDER BY av.id`, []any{seriesID}},
		{"shots", 30, `SELECT to_jsonb(sh) FROM video_pipeline.shots sh JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY sh.id`, []any{seriesID}},
		{"shot_specs", 30, `SELECT to_jsonb(ssr) FROM video_pipeline.shot_spec_revisions ssr JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY ssr.id`, []any{seriesID}},
		{"effective_contexts", 30, `SELECT to_jsonb(ecs) FROM video_pipeline.effective_context_snapshots ecs JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ecs.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY ecs.id`, []any{seriesID}},
		{"prompts", 30, `SELECT to_jsonb(ps) FROM video_pipeline.prompt_snapshots ps JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ps.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY ps.id`, []any{seriesID}},
		{"prompt_inputs", 180, `SELECT to_jsonb(psi) FROM video_pipeline.prompt_snapshot_inputs psi JOIN video_pipeline.prompt_snapshots ps ON ps.id=psi.prompt_snapshot_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ps.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY psi.prompt_snapshot_id,psi.input_type,psi.dependency_role`, []any{seriesID}},
		{"prompt_assets", expectedPromptAssets, `SELECT to_jsonb(psa) FROM video_pipeline.prompt_snapshot_assets psa JOIN video_pipeline.prompt_snapshots ps ON ps.id=psa.prompt_snapshot_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ps.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY psa.prompt_snapshot_id,psa.alias`, []any{seriesID}},
		{"runs", 30, `SELECT to_jsonb(gr) FROM video_pipeline.generation_runs gr JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY gr.id`, []any{seriesID}},
		{"attempts", 30, `SELECT to_jsonb(ga) FROM video_pipeline.generation_attempts ga JOIN video_pipeline.generation_runs gr ON gr.id=ga.generation_run_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY ga.id`, []any{seriesID}},
		{"operation_requests", 3, `SELECT to_jsonb(o) FROM video_pipeline.operation_requests o WHERE aggregate_id=$1 AND operation_type='CREATE_GENERATION_PLAN' ORDER BY id`, []any{seriesID}},
		{"idempotency_records", 3, `SELECT to_jsonb(i) FROM video_pipeline.idempotency_records i WHERE scope='flo100-formal-materialize' ORDER BY idempotency_key`, nil},
		{"budget_reviews", 6, `SELECT to_jsonb(r) FROM video_pipeline.review_tasks r WHERE series_id=$1 AND review_type='BUDGET' ORDER BY id`, []any{seriesID}},
		{"formal_audits", 66, `SELECT to_jsonb(a) FROM video_pipeline.audit_events a WHERE reason_code='FLO100_FORMAL_OFFLINE_V1' ORDER BY id`, nil},
		{"provider_jobs", 0, `SELECT to_jsonb(pj) FROM video_pipeline.provider_jobs pj JOIN video_pipeline.generation_attempts ga ON ga.id=pj.generation_attempt_id JOIN video_pipeline.generation_runs gr ON gr.id=ga.generation_run_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY pj.id`, []any{seriesID}},
		{"budget_reservations", 0, `SELECT to_jsonb(br) FROM video_pipeline.budget_reservations br JOIN video_pipeline.generation_runs gr ON gr.id=br.generation_run_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY br.id`, []any{seriesID}},
		{"cost_ledger", 0, `SELECT to_jsonb(cl) FROM video_pipeline.cost_ledger cl JOIN video_pipeline.provider_jobs pj ON pj.id=cl.provider_job_id JOIN video_pipeline.generation_attempts ga ON ga.id=pj.generation_attempt_id JOIN video_pipeline.generation_runs gr ON gr.id=ga.generation_run_id JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes s ON s.id=sh.scene_id JOIN video_pipeline.episodes e ON e.id=s.episode_id WHERE e.series_id=$1 ORDER BY cl.id`, []any{seriesID}},
	}
	snapshot := formalProjectionSnapshot{
		SchemaVersion: "flo100.formal-projection.v1", OfflinePackageHash: prepared.manifest.ContentHash,
		Sections: make([]formalProjectionSection, 0, len(sections)),
	}
	for _, section := range sections {
		rows, err := queryFormalProjectionRows(ctx, tx, section.query, section.args...)
		if err != nil {
			return "", fmt.Errorf("read formal projection section %s: %w", section.name, err)
		}
		if len(rows) != section.expected {
			return "", fmt.Errorf("formal projection drift: section %s has %d rows, expected %d", section.name, len(rows), section.expected)
		}
		snapshot.Sections = append(snapshot.Sections, formalProjectionSection{Name: section.name, Rows: rows})
	}
	projectionHash, err := digest(snapshot)
	if err != nil {
		return "", fmt.Errorf("hash FLO-100 formal projection: %w", err)
	}
	return projectionHash, nil
}

func queryFormalProjectionRows(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]json.RawMessage, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	payloads := make([]json.RawMessage, 0)
	for rows.Next() {
		var payload json.RawMessage
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		payloads = append(payloads, append(json.RawMessage(nil), payload...))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return payloads, nil
}

func verifyFormal(
	ctx context.Context,
	pool *pgxpool.Pool,
	prepared preparedFormal,
	approval Approval,
	packages []stage1.ExecutionPackage,
	objects map[string]artifactstore.Artifact,
) (FormalReport, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return FormalReport{}, err
	}
	defer tx.Rollback(ctx)
	if err := verifyFormalProjectionSeal(ctx, tx, prepared, objects); err != nil {
		return FormalReport{}, err
	}
	if len(packages) != 3 {
		return FormalReport{}, errors.New("formal materialization did not seal all three packages")
	}
	for index := range packages {
		if err := packages[index].Validate(prepared.batches[index].readiness); err != nil {
			return FormalReport{}, fmt.Errorf("verify %s package: %w", prepared.batches[index].product.BatchID, err)
		}
	}
	seriesID := formalUUID("series")
	counts := map[string]int64{}
	queries := map[string]string{
		"shots":      `SELECT count(*) FROM video_pipeline.shot_spec_revisions ssr JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes sc ON sc.id=sh.scene_id JOIN video_pipeline.episodes ep ON ep.id=sc.episode_id WHERE ep.series_id=$1`,
		"prompts":    `SELECT count(*) FROM video_pipeline.prompt_snapshots ps JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=ps.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes sc ON sc.id=sh.scene_id JOIN video_pipeline.episodes ep ON ep.id=sc.episode_id WHERE ep.series_id=$1`,
		"runs":       `SELECT count(*) FROM video_pipeline.generation_runs gr JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id JOIN video_pipeline.scenes sc ON sc.id=sh.scene_id JOIN video_pipeline.episodes ep ON ep.id=sc.episode_id WHERE ep.series_id=$1`,
		"assets":     `SELECT count(*) FROM video_pipeline.asset_versions av JOIN video_pipeline.assets a ON a.id=av.asset_id WHERE a.series_id=$1`,
		"pending_g1": `SELECT count(*) FROM video_pipeline.approval_decisions WHERE series_id=$1 AND gate='G1' AND decision='RETURNED'`,
		"pending_g2": `SELECT count(*) FROM video_pipeline.approval_decisions WHERE series_id=$1 AND gate='G2' AND decision='RETURNED'`,
	}
	for name, query := range queries {
		var count int64
		if err := tx.QueryRow(ctx, query, seriesID).Scan(&count); err != nil {
			return FormalReport{}, err
		}
		counts[name] = count
	}
	if counts["shots"] != 30 || counts["prompts"] != 30 || counts["runs"] != 30 ||
		counts["assets"] != 8 || counts["pending_g1"] != 1 || counts["pending_g2"] != 3 {
		return FormalReport{}, fmt.Errorf("formal database counts are incomplete: %v", counts)
	}
	runIDs := make([]uuid.UUID, 0, 30)
	for _, package_ := range packages {
		for _, job := range package_.PrimaryJobs {
			runIDs = append(runIDs, mustUUID(job.Run.RunID))
		}
	}
	var providerJobs, reservations, cost int64
	if err := tx.QueryRow(ctx, `
		WITH package_attempts AS (
			SELECT id FROM video_pipeline.generation_attempts WHERE generation_run_id=ANY($1)
		), package_jobs AS (
			SELECT pj.id FROM video_pipeline.provider_jobs pj JOIN package_attempts pa ON pa.id=pj.generation_attempt_id
		)
		SELECT
			(SELECT count(*) FROM package_jobs),
			(SELECT count(*) FROM video_pipeline.budget_reservations WHERE generation_run_id=ANY($1)),
			(SELECT count(*) FROM video_pipeline.cost_ledger cl JOIN package_jobs pj ON pj.id=cl.provider_job_id)`, runIDs).Scan(&providerJobs, &reservations, &cost); err != nil {
		return FormalReport{}, err
	}
	if providerJobs != 0 || reservations != 0 || cost != 0 {
		return FormalReport{}, errors.New("formal offline materialization crossed a paid Provider boundary")
	}
	casMap := make(map[string]string, len(objects))
	for name, object := range objects {
		casMap[name] = object.Digest
	}
	report := FormalReport{
		SchemaVersion: "flo100.formal-materialization-report.v1", IssueID: formalIssueID,
		SourceCodeCommit: formalSourceCommit, OfflinePackageHash: prepared.manifest.ContentHash,
		Counts: counts, CAS: casMap, ProviderCalls: 0, ProviderJobs: providerJobs,
		BudgetReservations: reservations, CostLedgerEntries: cost, NonSubscriptionCashMicros: 0,
		G1Status: "PENDING_INDEPENDENT_QA", G2Status: "PENDING_INDEPENDENT_QA",
		CurrentQuotaSnapshot: "MISSING_EXPECTED_BLOCKER", LiveExecutionAuthorized: false,
		SerialBatchOrder: append([]string(nil), formalBatchIDs...), ApprovalCommentID: approval.CommentID,
		ApprovalValidUntil: approval.ValidUntil,
	}
	for index, batch := range prepared.batches {
		intentKeys := make([]string, 0, 10)
		for _, key := range batch.plan.Idempotency.Keys {
			intentKeys = append(intentKeys, key.Key)
			report.IntentIdempotencyKeys = append(report.IntentIdempotencyKeys, key.Key)
		}
		report.ExecutionPackages = append(report.ExecutionPackages, FormalBatchReport{
			BatchID: batch.product.BatchID, ProductInputHash: batch.product.ContentHash,
			GenerationPlanHash: batch.plan.ContentHash, ExecutionIntentHash: batch.intent.ContentHash,
			ExecutionPackageHash: packages[index].ContentHash, ShotCount: len(batch.product.Shots),
			AssetVersionIDs:       append([]string(nil), batch.assetVersionIDs...),
			IntentIdempotencyKeys: intentKeys, LiveProviderSubmit: "DENIED",
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return FormalReport{}, err
	}
	return report, nil
}

func dialogueForShot(shot formalShot) []map[string]any {
	if shot.SubtitleText == "" {
		return []map[string]any{}
	}
	return []map[string]any{{
		"id": formalUUID("dialogue:" + shot.ShotSpecRevisionID).String(), "speaker": "narrator",
		"text": shot.SubtitleText, "startMillis": 0, "endMillis": int(shot.DurationSeconds * 1000),
	}}
}

func formalUUID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("flo100-formal-v1:"+name))
}

func formalVideoRoute() providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{
		CapabilityAlias: "video.primary", Provider: "VOLCENGINE", ModelID: stage1.FormalVideoModel,
		RouteVersion: "agent-plan-large-v1", CapabilityHash: formalVideoHash, Verification: providercontract.PendingKey,
	}
}

func formalSpeechModelRoute() providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{
		CapabilityAlias: "speech.primary", Provider: "VOLCENGINE", ModelID: "doubao-seed-tts-2.0",
		RouteVersion: "agent-plan-large-tts-v2", CapabilityHash: formalSpeechHash, Verification: providercontract.PendingKey,
	}
}

func validFormalDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}

// FormalExpectedPackageHash exposes the pinned identity for CLI help, tests,
// and independent QA without duplicating it in callers.
func FormalExpectedPackageHash() string { return formalManifestHash }

// FormalBatchOutputName is the deterministic prompt-free artifact name.
func FormalBatchOutputName(batchID string) string {
	return batchID + ".execution-package.json"
}

// FormalReportOutputName is the deterministic materialization report name.
func FormalReportOutputName() string { return "flo100.formal-materialization-report.json" }
