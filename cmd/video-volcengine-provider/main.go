// Command video-volcengine-provider starts the credential-isolated Agent Plan
// video adapter. It never logs credentials, prompts, or signed transport URLs.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
)

func main() {
	cfg, err := runtimeconfig.LoadVolcengineProvider()
	if err != nil {
		log.Fatalf("invalid live video provider configuration: %v", err)
	}
	store, err := artifactstore.New(cfg.ArtifactRoot)
	if err != nil {
		log.Fatalf("open video artifact store: %v", err)
	}
	upstream, err := providercontract.NewVolcengineProvider(providercontract.VolcengineConfig{
		BaseURL:    cfg.BaseURL,
		APIKey:     cfg.APIKey,
		Region:     cfg.Region,
		Models:     providercontract.VolcengineModels{Video: cfg.VideoModel},
		HTTPClient: &http.Client{Timeout: cfg.RequestTimeout},
	})
	if err != nil {
		log.Fatalf("configure live video provider: %v", err)
	}
	adapter, err := volcengineprovider.New(cfg, upstream, store, volcengineprovider.Options{})
	if err != nil {
		log.Fatalf("configure live video adapter: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	server := &http.Server{
		Addr: cfg.HTTPAddress, Handler: adapter.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      3 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("live video provider %s listening on %s (plan=%s model=%s region=%s)",
			cfg.ProviderID, cfg.HTTPAddress, cfg.PlanName, cfg.VideoModel, cfg.Region)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve live video provider: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("live video provider shutdown: %v", err)
	}
}
