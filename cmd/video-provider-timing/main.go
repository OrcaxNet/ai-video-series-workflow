// Command video-provider-timing creates a sanitized, read-only timing snapshot
// for existing Volcengine video jobs. It never submits, cancels, downloads, or
// prints prompts, credentials, or transport URLs.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

type input struct {
	SchemaVersion string     `json:"schemaVersion"`
	Jobs          []inputJob `json:"jobs"`
}

type inputJob struct {
	ShotID string `json:"shotId"`
	TaskID string `json:"taskId"`
}

type registryRecord struct {
	Response struct {
		JobID          string `json:"job_id"`
		UpstreamTaskID string `json:"upstream_task_id"`
		RequestID      string `json:"request_id"`
		Artifacts      []struct {
			Role   string `json:"role"`
			Digest string `json:"sha256"`
		} `json:"artifacts"`
	} `json:"response"`
}

type timingJob struct {
	ShotID                  string `json:"shotId"`
	TaskID                  string `json:"taskId"`
	ProviderJobID           string `json:"providerJobId"`
	ProviderCreatedAt       string `json:"providerCreatedAt"`
	ProviderUpdatedAt       string `json:"providerUpdatedAt"`
	SubmitAcknowledgedAt    string `json:"submitAcknowledgedAt"`
	TerminalObservedAt      string `json:"terminalObservedAt"`
	TerminalRecordPersisted string `json:"terminalRecordPersistedAt"`
	QueueMillis             int64  `json:"queueMillis"`
	GenerationMillis        int64  `json:"generationMillis"`
	PollMillis              int64  `json:"pollMillis"`
	EndToEndMillis          int64  `json:"endToEndMillis"`
	DownloadPersistMillis   int64  `json:"downloadPersistMillis"`
	RegistrySHA256          string `json:"registrySha256"`
	VideoSHA256             string `json:"videoSha256"`
}

type metricSummary struct {
	P50Millis int64 `json:"p50Millis"`
	P95Millis int64 `json:"p95Millis"`
}

type output struct {
	SchemaVersion string                   `json:"schemaVersion"`
	Semantics     map[string]string        `json:"semantics"`
	Jobs          []timingJob              `json:"jobs"`
	Summary       map[string]metricSummary `json:"summary"`
}

func main() {
	if err := run(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Fatalf("video provider timing failed: %v", err)
	}
}

func run(ctx context.Context, reader io.Reader, writer io.Writer) error {
	var request input
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode timing request: %w", err)
	}
	if request.SchemaVersion != "video-provider-timing-request-v1" || len(request.Jobs) == 0 {
		return errors.New("timing request schemaVersion and jobs are required")
	}
	cfg, err := runtimeconfig.LoadVolcengineProvider()
	if err != nil {
		return fmt.Errorf("load live provider configuration: %w", err)
	}
	provider, err := providercontract.NewVolcengineProvider(providercontract.VolcengineConfig{
		BaseURL: cfg.BaseURL, APIKey: cfg.APIKey, Region: cfg.Region,
		Models:     providercontract.VolcengineModels{Video: cfg.VideoModel},
		HTTPClient: &http.Client{Timeout: cfg.RequestTimeout},
	})
	if err != nil {
		return err
	}
	records, err := loadRegistryRecords(cfg.ArtifactRoot)
	if err != nil {
		return err
	}
	result := output{
		SchemaVersion: "video-provider-timing-evidence-v1",
		Semantics: map[string]string{
			"queueMillis":      "provider created_at minus the Agent Plan request timestamp carried by the submit acknowledgment; negative sub-second skew is clamped to zero because created_at has one-second resolution",
			"generationMillis": "provider updated_at minus provider created_at",
			"pollMillis":       "immutable video CAS persistence minus provider updated_at; includes terminal poll cadence and video download",
			"endToEndMillis":   "terminal registry persistence minus the submit acknowledgment timestamp",
		},
		Jobs: make([]timingJob, 0, len(request.Jobs)),
	}
	seen := make(map[string]struct{}, len(request.Jobs))
	for _, requested := range request.Jobs {
		if strings.TrimSpace(requested.ShotID) == "" || strings.TrimSpace(requested.TaskID) == "" {
			return errors.New("every timing job requires shotId and taskId")
		}
		if _, duplicate := seen[requested.TaskID]; duplicate {
			return fmt.Errorf("duplicate taskId %q", requested.TaskID)
		}
		seen[requested.TaskID] = struct{}{}
		record, ok := records[requested.TaskID]
		if !ok {
			return fmt.Errorf("no terminal registry record for task %q", requested.TaskID)
		}
		job, err := provider.Poll(ctx, requested.TaskID)
		if err != nil {
			return fmt.Errorf("poll existing task %q: %w", requested.TaskID, err)
		}
		if job.Status != providercontract.StatusSucceeded || job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
			return fmt.Errorf("task %q is not a timestamped success", requested.TaskID)
		}
		acknowledged, err := agentPlanRequestTime(record.record.Response.RequestID, job.CreatedAt)
		if err != nil {
			return fmt.Errorf("task %q submit acknowledgment: %w", requested.TaskID, err)
		}
		videoDigest := ""
		for _, artifact := range record.record.Response.Artifacts {
			if artifact.Role == "output" {
				videoDigest = artifact.Digest
				break
			}
		}
		if len(videoDigest) != sha256.Size*2 {
			return fmt.Errorf("task %q has no immutable video digest", requested.TaskID)
		}
		videoPath := filepath.Join(cfg.ArtifactRoot, "sha256", videoDigest[:2], videoDigest)
		videoStat, err := os.Stat(videoPath)
		if err != nil {
			return fmt.Errorf("stat task %q video CAS: %w", requested.TaskID, err)
		}
		registryStat, err := os.Stat(record.path)
		if err != nil {
			return fmt.Errorf("stat task %q registry: %w", requested.TaskID, err)
		}
		queueMillis := job.CreatedAt.Sub(acknowledged).Milliseconds()
		if queueMillis < 0 && queueMillis >= -1_000 {
			queueMillis = 0
		}
		if queueMillis < 0 {
			return fmt.Errorf("task %q acknowledgment predates creation beyond timestamp precision", requested.TaskID)
		}
		pollMillis := videoStat.ModTime().UTC().Sub(job.UpdatedAt).Milliseconds()
		endToEndMillis := registryStat.ModTime().UTC().Sub(acknowledged).Milliseconds()
		if pollMillis < 0 || endToEndMillis < 0 {
			return fmt.Errorf("task %q persistence timestamps predate Provider state", requested.TaskID)
		}
		result.Jobs = append(result.Jobs, timingJob{
			ShotID: requested.ShotID, TaskID: requested.TaskID,
			ProviderJobID:           record.record.Response.JobID,
			ProviderCreatedAt:       job.CreatedAt.UTC().Format(time.RFC3339Nano),
			ProviderUpdatedAt:       job.UpdatedAt.UTC().Format(time.RFC3339Nano),
			SubmitAcknowledgedAt:    acknowledged.UTC().Format(time.RFC3339Nano),
			TerminalObservedAt:      videoStat.ModTime().UTC().Format(time.RFC3339Nano),
			TerminalRecordPersisted: registryStat.ModTime().UTC().Format(time.RFC3339Nano),
			QueueMillis:             queueMillis,
			GenerationMillis:        job.UpdatedAt.Sub(job.CreatedAt).Milliseconds(),
			PollMillis:              pollMillis, EndToEndMillis: endToEndMillis,
			DownloadPersistMillis: registryStat.ModTime().Sub(videoStat.ModTime()).Milliseconds(),
			RegistrySHA256:        record.digest, VideoSHA256: videoDigest,
		})
	}
	result.Summary = summarizeTimings(result.Jobs)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

type loadedRecord struct {
	path   string
	digest string
	record registryRecord
}

func loadRegistryRecords(root string) (map[string]loadedRecord, error) {
	directory := filepath.Join(root, "provider-jobs")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read provider registry: %w", err)
	}
	records := make(map[string]loadedRecord)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || strings.Contains(entry.Name(), ".speech-") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var record registryRecord
		if json.Unmarshal(data, &record) != nil || record.Response.UpstreamTaskID == "" {
			continue
		}
		digest := sha256.Sum256(data)
		records[record.Response.UpstreamTaskID] = loadedRecord{
			path: path, digest: hex.EncodeToString(digest[:]), record: record,
		}
	}
	return records, nil
}

func agentPlanRequestTime(requestID string, createdAt time.Time) (time.Time, error) {
	if len(requestID) < 15 || requestID[:2] != "02" {
		return time.Time{}, errors.New("requestId does not contain the Agent Plan millisecond timestamp prefix")
	}
	milliseconds, err := strconv.ParseInt(requestID[2:15], 10, 64)
	if err != nil {
		return time.Time{}, errors.New("requestId timestamp prefix is invalid")
	}
	value := time.UnixMilli(milliseconds).UTC()
	if value.Before(createdAt.Add(-5*time.Second)) || value.After(createdAt.Add(time.Second)) {
		return time.Time{}, fmt.Errorf(
			"requestId timestamp %s is outside Provider creation window %s",
			value.Format(time.RFC3339Nano), createdAt.Format(time.RFC3339Nano),
		)
	}
	return value, nil
}

func summarizeTimings(jobs []timingJob) map[string]metricSummary {
	metrics := map[string][]int64{
		"queueMillis": {}, "generationMillis": {}, "pollMillis": {}, "endToEndMillis": {},
	}
	for _, job := range jobs {
		metrics["queueMillis"] = append(metrics["queueMillis"], job.QueueMillis)
		metrics["generationMillis"] = append(metrics["generationMillis"], job.GenerationMillis)
		metrics["pollMillis"] = append(metrics["pollMillis"], job.PollMillis)
		metrics["endToEndMillis"] = append(metrics["endToEndMillis"], job.EndToEndMillis)
	}
	result := make(map[string]metricSummary, len(metrics))
	for name, values := range metrics {
		result[name] = metricSummary{
			P50Millis: percentileLinear(values, 0.50),
			P95Millis: percentileLinear(values, 0.95),
		}
	}
	return result
}

func percentileLinear(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := float64(len(sorted)-1) * percentile
	lower := int(rank)
	upper := lower
	if rank > float64(lower) {
		upper++
	}
	if lower == upper {
		return sorted[lower]
	}
	value := float64(sorted[lower]) + (rank-float64(lower))*float64(sorted[upper]-sorted[lower])
	return int64(value + 0.5)
}
