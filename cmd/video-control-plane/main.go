// Command video-control-plane starts the standalone AI video control-plane
// bootstrap service.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/temporalcontrol"
	"go.temporal.io/sdk/client"
)

func main() {
	cfg, err := runtimeconfig.LoadControlPlane()
	if err != nil {
		log.Fatalf("invalid video control-plane configuration: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := repository.Open(ctx, cfg.PostgresDSN, repository.PoolConfig{})
	if err != nil {
		log.Fatalf("connect video product store: %v", err)
	}
	defer store.Close()
	artifacts, err := artifactstore.New(cfg.ArtifactRoot)
	if err != nil {
		log.Fatalf("open video artifact store: %v", err)
	}
	temporalClient, err := client.Dial(client.Options{HostPort: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace})
	if err != nil {
		log.Fatalf("connect to Temporal: %v", err)
	}
	defer temporalClient.Close()
	workflows, err := temporalcontrol.New(temporalClient, cfg.TemporalTaskQueue, store)
	if err != nil {
		log.Fatalf("configure Temporal controller: %v", err)
	}
	dependencies := []controlplane.Dependency{
		{Name: "postgresql", Critical: true, Probe: controlplane.ProbeFunc(store.Ping)},
		{Name: "temporal", Critical: true, Probe: controlplane.TCPProbe(cfg.TemporalAddress)},
		{Name: "artifact_store", Critical: true, Probe: controlplane.DirectoryProbe(cfg.ArtifactRoot)},
		{Name: "provider_adapter", Critical: true, Probe: controlplane.HTTPProbe(cfg.ProviderAdapterURL + "/health/ready")},
	}
	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           controlplane.NewWithRuntime(cfg, dependencies, store, workflows, artifacts).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("video control plane listening on %s (version=%s)", cfg.HTTPAddress, cfg.Version)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve video control plane: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("video control-plane shutdown: %v", err)
	}
}
