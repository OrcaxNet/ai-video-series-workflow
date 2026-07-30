// Command video-mock-provider starts the deterministic provider fixture used
// for no-key development and QA. It performs no generative inference.
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
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

func main() {
	cfg, err := runtimeconfig.LoadMockProvider()
	if err != nil {
		log.Fatalf("invalid video mock-provider configuration: %v", err)
	}
	store, err := artifactstore.New(cfg.ArtifactRoot)
	if err != nil {
		log.Fatalf("open video artifact store: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           mockprovider.New(cfg, store).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("video mock provider %s listening on %s", cfg.ProviderID, cfg.HTTPAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve video mock provider: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("video mock-provider shutdown: %v", err)
	}
}
