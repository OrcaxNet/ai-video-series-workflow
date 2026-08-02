// Command video-live-probe runs exactly one five-second live video pipeline
// probe and writes sanitized evidence. The output directory is single-use.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/liveprobe"
)

func main() {
	result, err := liveprobe.Run(context.Background(), liveprobe.Config{
		AdapterURL:        env("VIDEO_PROVIDER_ADAPTER_URL", "http://127.0.0.1:8091"),
		ServiceAuthSecret: env("VIDEO_PROVIDER_SERVICE_AUTH_SECRET", ""),
		ArtifactRoot:      env("VIDEO_ARTIFACT_ROOT", "artifacts/flo104-live-cas"),
		OutputDir:         env("VIDEO_LIVE_PROBE_OUTPUT_DIR", "artifacts/flo104-live-probe"),
		BuildVersion:      env("VIDEO_BUILD_VERSION", "development"),
		Region:            env("VIDEO_VOLCENGINE_REGION", "cn-beijing"),
		PlanName:          env("VIDEO_VOLCENGINE_PLAN", "agent-plan-large"),
		PollInterval:      3 * time.Second,
		Timeout:           10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("single live provider probe failed: %v", err)
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("encode sanitized probe result: %v", err)
	}
	_, _ = os.Stdout.Write(append(encoded, '\n'))
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
