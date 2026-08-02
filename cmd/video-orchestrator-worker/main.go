// Command video-orchestrator-worker registers the episode production Workflow
// and versioned Activities with Temporal.
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	cfg, err := runtimeconfig.LoadOrchestratorWorker()
	if err != nil {
		log.Fatalf("invalid video orchestrator configuration: %v", err)
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		log.Fatalf("connect to Temporal: %v", err)
	}
	defer temporalClient.Close()
	store, err := repository.Open(context.Background(), cfg.PostgresDSN, repository.PoolConfig{})
	if err != nil {
		log.Fatalf("connect to video PostgreSQL: %v", err)
	}
	defer store.Close()
	if err := store.ValidateWorkerUpgradeReadiness(context.Background()); err != nil {
		log.Fatalf("video PostgreSQL upgrade gate failed: %v", err)
	}
	artifacts, err := artifactstore.New(cfg.ArtifactRoot)
	if err != nil {
		log.Fatalf("open video artifact store: %v", err)
	}

	temporalWorker := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		orchestration.EpisodeProductionWorkflow,
		workflow.RegisterOptions{Name: orchestration.WorkflowName},
	)
	temporalWorker.RegisterWorkflowWithOptions(
		orchestration.ShotProductionWorkflow,
		workflow.RegisterOptions{Name: orchestration.ShotWorkflowName},
	)
	temporalWorker.RegisterWorkflowWithOptions(
		orchestration.ShotReconciliationWorkflow,
		workflow.RegisterOptions{Name: orchestration.ShotReconciliationWorkflowName},
	)
	temporalWorker.RegisterWorkflowWithOptions(
		orchestration.Stage1FinalizationWorkflow,
		workflow.RegisterOptions{Name: orchestration.Stage1FinalizationWorkflowName},
	)
	activities := orchestration.NewProductionActivities(cfg.ProviderAdapterURL, store, store, artifacts)
	providerClient, err := workerProviderHTTPClient(
		mockprovider.DefaultHTTPClient(), cfg.ProviderServiceAuthSecret,
	)
	if err != nil {
		log.Fatalf("configure provider service authentication: %v", err)
	}
	activities.HTTPClient = providerClient
	speech, err := postproduction.NewHTTPSpeechProvider(
		cfg.SpeechProviderAdapterURL,
		providerClient,
	)
	if err != nil {
		log.Fatalf("configure speech provider adapter: %v", err)
	}
	media, err := postproduction.NewFFmpegProcessor(artifacts)
	if err != nil {
		log.Fatalf("configure FFmpeg post-production: %v", err)
	}
	var analyzers []postproduction.AudioAnalyzer
	if cfg.AudioAnalyzerCommand != "" {
		analyzer, analyzerErr := postproduction.NewCommandAudioAnalyzer(cfg.AudioAnalyzerCommand, artifacts)
		if analyzerErr != nil {
			log.Fatalf("configure native audio analyzer: %v", analyzerErr)
		}
		analyzers = append(analyzers, analyzer)
	}
	postProduction, err := postproduction.NewService(speech, media, artifacts, analyzers...)
	if err != nil {
		log.Fatalf("configure episode post-production: %v", err)
	}
	activities.ConfigurePostProduction(postProduction, store)
	temporalWorker.RegisterActivityWithOptions(activities.ValidateBatch, activity.RegisterOptions{Name: orchestration.ActivityValidateBatch})
	temporalWorker.RegisterActivityWithOptions(activities.CompilePrompt, activity.RegisterOptions{Name: orchestration.ActivityCompilePrompt})
	temporalWorker.RegisterActivityWithOptions(activities.CreateRun, activity.RegisterOptions{Name: orchestration.ActivityCreateRun})
	temporalWorker.RegisterActivityWithOptions(activities.ExecuteProviderJob, activity.RegisterOptions{Name: orchestration.ActivityExecuteProviderJob})
	temporalWorker.RegisterActivityWithOptions(activities.RunAutomaticQC, activity.RegisterOptions{Name: orchestration.ActivityRunAutomaticQC})
	temporalWorker.RegisterActivityWithOptions(activities.CreateShotReview, activity.RegisterOptions{Name: orchestration.ActivityCreateShotReview})
	temporalWorker.RegisterActivityWithOptions(activities.EscalateShot, activity.RegisterOptions{Name: orchestration.ActivityEscalateShot})
	temporalWorker.RegisterActivityWithOptions(activities.FinalizeEpisode, activity.RegisterOptions{Name: orchestration.ActivityFinalizeEpisode})
	temporalWorker.RegisterActivityWithOptions(activities.CreateGate3, activity.RegisterOptions{Name: orchestration.ActivityCreateGate3})
	temporalWorker.RegisterActivityWithOptions(activities.CancelProviderJob, activity.RegisterOptions{Name: orchestration.ActivityCancelProviderJob})
	temporalWorker.RegisterActivityWithOptions(activities.FinalizeShotRun, activity.RegisterOptions{Name: orchestration.ActivityFinalizeShotRun})

	log.Printf("video Temporal worker listening (namespace=%s task_queue=%s)", cfg.Namespace, cfg.TaskQueue)
	if err := temporalWorker.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run video Temporal worker: %v", err)
	}
}

// workerProviderHTTPClient returns the one adapter client shared by video
// Activities and the post-production speech provider. A Live Adapter protects
// submit, poll, and cancel uniformly, so using an unsigned speech client would
// fail only after all paid Stage 1 videos had completed.
func workerProviderHTTPClient(base *http.Client, serviceAuthSecret string) (*http.Client, error) {
	if serviceAuthSecret == "" {
		return base, nil
	}
	return volcengineprovider.AuthenticatedHTTPClient(base, serviceAuthSecret)
}
