// Package nativepreflight proves that one FLO-154 package is bound to the
// exact code, build, product inputs, analyzer installation, offline fixture,
// and zero-paid-boundary PostgreSQL state. It contains no Provider client.
package nativepreflight

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/analyzerseal"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/jackc/pgx/v5/pgxpool"
)

const SchemaVersion = "flo154.native-preflight-report.v1"

type Files struct {
	Plan, Package, Product, Source, Safety, Visual string
	AnalyzerRoot, AnalyzerSeal, RepoRoot, Build    string
	FixtureInput                                   string
}

type Report struct {
	SchemaVersion string            `json:"schemaVersion"`
	BatchID       string            `json:"batchId"`
	Hashes        map[string]string `json:"hashes"`
	Counts        map[string]int64  `json:"counts"`
	Fixture       FixtureReport     `json:"fixture"`
}

type FixtureReport struct {
	InputSHA256    string `json:"inputSha256"`
	AnalysisSHA256 string `json:"analysisSha256"`
	Transcript     string `json:"transcript"`
	CueCount       int    `json:"cueCount"`
	RunCount       int    `json:"runCount"`
}

type fixtureMedia struct {
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	DurationMillis int64  `json:"durationMillis"`
}

type fixtureCue struct {
	CueID              string `json:"cueId"`
	StartMillis        int64  `json:"startMillis"`
	EndMillis          int64  `json:"endMillis"`
	LipSyncRunID       string `json:"lipSyncRunId"`
	LipSyncStartMillis int64  `json:"lipSyncStartMillis"`
	LipSyncEndMillis   int64  `json:"lipSyncEndMillis"`
	LipSyncRequired    bool   `json:"lipSyncRequired"`
}

type fixtureRun struct {
	RunID               string `json:"runId"`
	StartMillis         int64  `json:"startMillis"`
	EndMillis           int64  `json:"endMillis"`
	ContextSnapshotID   string `json:"contextSnapshotId"`
	ContextSnapshotHash string `json:"contextSnapshotHash"`
	AmbienceIdentity    string `json:"ambienceIdentity"`
	AmbienceVersion     string `json:"ambienceVersion"`
	ContinuityIntoNext  bool   `json:"continuityIntoNext"`
	LipSyncRequired     bool   `json:"lipSyncRequired"`
}

type fixtureInput struct {
	SchemaVersion string                  `json:"schemaVersion"`
	Evidence      string                  `json:"evidence"`
	ASR           cerevaluation.ASRConfig `json:"asr"`
	FinalMix      fixtureMedia            `json:"finalMix"`
	FinalVideo    fixtureMedia            `json:"finalVideo"`
	NativeMixes   []fixtureMedia          `json:"nativeMixes"`
	Dialogue      *fixtureMedia           `json:"dialogue,omitempty"`
	CueWindows    []fixtureCue            `json:"cueWindows"`
	RunWindows    []fixtureRun            `json:"runWindows"`
}

func Verify(ctx context.Context, pool *pgxpool.Pool, files Files) (Report, error) {
	if pool == nil {
		return Report{}, errors.New("PostgreSQL pool is required")
	}
	var plan stage1.Plan
	if err := decodeStrictFile(files.Plan, 4<<20, &plan); err != nil {
		return Report{}, fmt.Errorf("read plan: %w", err)
	}
	var package_ stage1.ExecutionPackage
	if err := decodeStrictFile(files.Package, 8<<20, &package_); err != nil {
		return Report{}, fmt.Errorf("read execution package: %w", err)
	}
	if !plan.IsNativeOnly() || package_.NativeEvidence == nil {
		return Report{}, errors.New("preflight accepts only a FLO-154 native package")
	}
	if err := package_.Validate(plan); err != nil {
		return Report{}, fmt.Errorf("validate execution package: %w", err)
	}
	evidence := package_.NativeEvidence

	commit, err := gitCommit(ctx, files.RepoRoot)
	if err != nil {
		return Report{}, err
	}
	if commit != evidence.CodeCommitSHA {
		return Report{}, errors.New("checked-out code commit differs from native package evidence")
	}
	buildHash, err := fileDigest(files.Build, 1<<30)
	if err != nil {
		return Report{}, fmt.Errorf("hash build: %w", err)
	}
	if buildHash != evidence.BuildSHA256 {
		return Report{}, errors.New("materializer build differs from native package evidence")
	}
	inputPaths := map[string]string{
		"product_input": files.Product,
		"source":        files.Source,
		"safety":        files.Safety,
		"visual":        files.Visual,
		"analyzer_seal": files.AnalyzerSeal,
	}
	hashes := map[string]string{
		"code_commit":       commit,
		"build":             buildHash,
		"execution_package": package_.ContentHash,
	}
	for name, path := range inputPaths {
		hash, hashErr := fileDigest(path, 64<<20)
		if hashErr != nil {
			return Report{}, fmt.Errorf("hash %s: %w", name, hashErr)
		}
		hashes[name] = hash
		if name == "product_input" {
			if hash != evidence.ProductInputSHA256 {
				return Report{}, errors.New("product input differs from native package evidence")
			}
			continue
		}
		if hash != evidence.AssetSHA256[name] {
			return Report{}, fmt.Errorf("%s differs from native package evidence", name)
		}
	}
	manifest, analyzerEvidence, err := analyzerseal.Verify(files.AnalyzerRoot, files.AnalyzerSeal)
	if err != nil {
		return Report{}, fmt.Errorf("verify analyzer seal: %w", err)
	}
	if analyzerEvidence.SealSHA256 != evidence.AnalyzerSealSHA256 ||
		analyzerEvidence.ExecutableSHA256 != evidence.AnalyzerExecutableSHA256 ||
		analyzerEvidence.ConfigSHA256 != evidence.AnalyzerConfigSHA256 ||
		!reflect.DeepEqual(analyzerEvidence.Components, evidence.AnalyzerComponentSHA256) {
		return Report{}, errors.New("analyzer seal or component hashes differ from native package evidence")
	}
	hashes["analyzer_executable"] = analyzerEvidence.ExecutableSHA256
	hashes["analyzer_config"] = analyzerEvidence.ConfigSHA256
	for kind, hash := range analyzerEvidence.Components {
		hashes["analyzer_"+kind] = hash
	}
	program := filepath.Join(files.AnalyzerRoot, filepath.Clean(manifest.Analyzer.Path))
	fixture, err := verifyFixture(ctx, program, manifest.Analyzer.Version, files.FixtureInput)
	if err != nil {
		return Report{}, err
	}
	counts, err := verifyZeroPaidBoundary(ctx, pool, package_)
	if err != nil {
		return Report{}, err
	}
	return Report{
		SchemaVersion: SchemaVersion, BatchID: package_.BatchID,
		Hashes: hashes, Counts: counts, Fixture: fixture,
	}, nil
}

func gitCommit(ctx context.Context, root string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve code commit: %w", err)
	}
	commit := strings.TrimSpace(string(output))
	if len(commit) != 40 || !validLowerHex(commit) {
		return "", errors.New("git returned an invalid code commit")
	}
	// A dirty tracked tree can produce a binary that is correctly hash-bound to
	// the package while falsely naming an older source revision. Untracked
	// generated evidence is allowed; tracked source drift is not.
	status := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain=v1", "--untracked-files=no")
	dirty, err := status.Output()
	if err != nil {
		return "", fmt.Errorf("verify clean code commit: %w", err)
	}
	if len(bytes.TrimSpace(dirty)) != 0 {
		return "", errors.New("checked-out code has uncommitted tracked changes")
	}
	return commit, nil
}

func verifyFixture(ctx context.Context, program, analyzerVersion, inputPath string) (FixtureReport, error) {
	var input fixtureInput
	inputBytes, err := decodeStrictFileBytes(inputPath, 4<<20, &input)
	if err != nil {
		return FixtureReport{}, fmt.Errorf("read offline fixture: %w", err)
	}
	if input.SchemaVersion != "flo154.audio-analyzer-command.v1" ||
		strings.TrimSpace(input.Evidence) == "" ||
		!reflect.DeepEqual(input.ASR, cerevaluation.FrozenASRConfig()) ||
		len(input.CueWindows) == 0 || len(input.RunWindows) == 0 ||
		len(input.NativeMixes) != len(input.RunWindows) {
		return FixtureReport{}, errors.New("offline fixture schema, ASR, cue, or run boundary is invalid")
	}
	var generic any
	if err := json.Unmarshal(inputBytes, &generic); err != nil {
		return FixtureReport{}, err
	}
	if containsReferenceText(generic) {
		return FixtureReport{}, errors.New("offline fixture supplies reference subtitle text to the analyzer")
	}
	media := []fixtureMedia{input.FinalMix, input.FinalVideo}
	media = append(media, input.NativeMixes...)
	if input.Dialogue != nil {
		media = append(media, *input.Dialogue)
	}
	wantSources := make([]string, 0, len(media))
	for index, item := range media {
		actual, hashErr := fileDigest(item.Path, 1<<30)
		if hashErr != nil || actual != item.SHA256 || item.DurationMillis <= 0 {
			return FixtureReport{}, fmt.Errorf("offline fixture media %d source hash or duration is invalid", index)
		}
		wantSources = append(wantSources, item.SHA256)
	}
	outputDir, err := os.MkdirTemp("", "flo154-native-preflight-*")
	if err != nil {
		return FixtureReport{}, err
	}
	defer os.RemoveAll(outputDir)
	absoluteInput, err := filepath.Abs(inputPath)
	if err != nil {
		return FixtureReport{}, err
	}
	outputPath := filepath.Join(outputDir, "analysis.json")
	command := exec.CommandContext(ctx, program, absoluteInput, outputPath)
	command.Dir = outputDir
	combined, err := command.CombinedOutput()
	if err != nil {
		return FixtureReport{}, fmt.Errorf("run offline analyzer fixture: %w: %s", err, strings.TrimSpace(string(combined)))
	}
	var analysis postproduction.AudioAnalysis
	analysisBytes, err := decodeStrictFileBytes(outputPath, 4<<20, &analysis)
	if err != nil {
		return FixtureReport{}, fmt.Errorf("decode offline analyzer fixture: %w", err)
	}
	if analysis.SchemaVersion != postproduction.AudioAnalysisSchemaVersion ||
		strings.TrimSpace(analysis.AnalysisID) == "" || strings.TrimSpace(analysis.Analyzer) == "" ||
		analysis.AnalyzerVersion != analyzerVersion || analysis.Evidence != input.Evidence ||
		strings.TrimSpace(analysis.Transcript) == "" ||
		!reflect.DeepEqual(analysis.ASR, cerevaluation.FrozenASRConfig()) ||
		len(analysis.CueTimings) != len(input.CueWindows) ||
		len(analysis.LipSync) != len(input.CueWindows) ||
		len(analysis.AmbienceTransitions) != len(input.RunWindows)-1 ||
		len(analysis.AudioVideoStartMillis) != len(input.RunWindows) ||
		math.IsNaN(analysis.IntegratedLUFS) || math.IsInf(analysis.IntegratedLUFS, 0) ||
		math.IsNaN(analysis.TruePeakDBTP) || math.IsInf(analysis.TruePeakDBTP, 0) {
		return FixtureReport{}, errors.New("offline analyzer did not return every strict production field")
	}
	sort.Strings(wantSources)
	gotSources := append([]string(nil), analysis.SourceHashes...)
	sort.Strings(gotSources)
	if !slices.Equal(gotSources, wantSources) {
		return FixtureReport{}, errors.New("offline analyzer output source hashes drifted")
	}
	return FixtureReport{
		InputSHA256: sum(inputBytes), AnalysisSHA256: sum(analysisBytes),
		Transcript: analysis.Transcript, CueCount: len(input.CueWindows),
		RunCount: len(input.RunWindows),
	}, nil
}

func containsReferenceText(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(key)
			if normalized == "text" || normalized == "subtitle" || normalized == "referencetext" ||
				(normalized == "referenceprompt" && child != nil) {
				return true
			}
			if containsReferenceText(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsReferenceText(child) {
				return true
			}
		}
	}
	return false
}

func verifyZeroPaidBoundary(ctx context.Context, pool *pgxpool.Pool, package_ stage1.ExecutionPackage) (map[string]int64, error) {
	var providerJobs, reservations, costEntries, speechJobs, auditProviderCalls, auditTTSCalls int64
	var approvedVideoBudgetReviews, openVideoBudgetReviews int64
	err := pool.QueryRow(ctx, `
		WITH package_runs AS (
			SELECT id FROM video_pipeline.generation_runs WHERE trace_id=$1
		), package_attempts AS (
			SELECT ga.id FROM video_pipeline.generation_attempts ga
			JOIN package_runs pr ON pr.id=ga.generation_run_id
		), package_jobs AS (
			SELECT pj.id, pj.capability_snapshot_id FROM video_pipeline.provider_jobs pj
			JOIN package_attempts pa ON pa.id=pj.generation_attempt_id
		)
		SELECT
			(SELECT count(*) FROM package_jobs),
			(SELECT count(*) FROM video_pipeline.budget_reservations br JOIN package_runs pr ON pr.id=br.generation_run_id),
			(SELECT count(*) FROM video_pipeline.cost_ledger cl JOIN package_jobs pj ON pj.id=cl.provider_job_id),
			(SELECT count(*) FROM package_jobs pj JOIN video_pipeline.provider_capability_snapshots pcs ON pcs.id=pj.capability_snapshot_id WHERE pcs.capability_alias='speech.primary'),
			COALESCE((SELECT (payload->>'providerCalls')::bigint FROM video_pipeline.audit_events WHERE action='stage1.execution_package.materialized' AND aggregate_id=$2 ORDER BY occurred_at DESC LIMIT 1), -1),
			COALESCE((SELECT (payload->>'ttsCalls')::bigint FROM video_pipeline.audit_events WHERE action='stage1.execution_package.materialized' AND aggregate_id=$2 ORDER BY occurred_at DESC LIMIT 1), -1),
			(SELECT count(*) FROM video_pipeline.review_tasks WHERE generation_plan_id=$2 AND budget_scope='VIDEO' AND state='APPROVED'),
			(SELECT count(*) FROM video_pipeline.review_tasks WHERE generation_plan_id=$2 AND budget_scope='VIDEO' AND state='OPEN')`,
		package_.PostProduction.TraceID, package_.PostProduction.GenerationPlanID,
	).Scan(&providerJobs, &reservations, &costEntries, &speechJobs, &auditProviderCalls, &auditTTSCalls, &approvedVideoBudgetReviews, &openVideoBudgetReviews)
	if err != nil {
		return nil, fmt.Errorf("verify zero paid boundary: %w", err)
	}
	counts := map[string]int64{
		"provider_calls": auditProviderCalls, "provider_jobs": providerJobs,
		"budget_reservations": reservations, "cost_ledger_entries": costEntries,
		"tts_calls": auditTTSCalls, "tts_provider_jobs": speechJobs,
		"approved_video_budget_reviews": approvedVideoBudgetReviews,
	}
	for name, count := range counts {
		if count != 0 {
			return nil, fmt.Errorf("zero paid boundary failed: %s=%d", name, count)
		}
	}
	if openVideoBudgetReviews != 1 {
		return nil, fmt.Errorf("revoked live authorization boundary failed: open_video_budget_reviews=%d", openVideoBudgetReviews)
	}
	counts["open_video_budget_reviews"] = openVideoBudgetReviews
	return counts, nil
}

func decodeStrictFile(path string, maximum int64, destination any) error {
	_, err := decodeStrictFileBytes(path, maximum, destination)
	return err
}

func decodeStrictFileBytes(path string, maximum int64, destination any) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("JSON file must be a bounded regular non-symlink file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("JSON file must contain exactly one value")
	}
	return data, nil
}

func fileDigest(path string, maximum int64) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maximum {
		return "", errors.New("file must be a bounded regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func sum(data []byte) string {
	value := sha256.Sum256(data)
	return hex.EncodeToString(value[:])
}

func validLowerHex(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return value != ""
}
