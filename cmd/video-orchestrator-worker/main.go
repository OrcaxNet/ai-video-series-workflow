// Command video-orchestrator-worker registers the episode production Workflow
// and versioned Activities with Temporal.
package main

import (
	"context"
	"log"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
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
	artifacts, err := artifactstore.New(cfg.ArtifactRoot)
	if err != nil {
		log.Fatalf("open video artifact store: %v", err)
	}

	temporalWorker := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		orchestration.EpisodeProductionWorkflow,
		workflow.RegisterOptions{Name: orchestration.WorkflowName},
	)
	activities := orchestration.NewProductionActivities(cfg.ProviderAdapterURL, store, store, artifacts)
	temporalWorker.RegisterActivityWithOptions(activities.ValidateBatch, activity.RegisterOptions{Name: orchestration.ActivityValidateBatch})
	temporalWorker.RegisterActivityWithOptions(activities.CompilePrompt, activity.RegisterOptions{Name: orchestration.ActivityCompilePrompt})
	temporalWorker.RegisterActivityWithOptions(activities.CreateRun, activity.RegisterOptions{Name: orchestration.ActivityCreateRun})
	temporalWorker.RegisterActivityWithOptions(activities.ExecuteProviderJob, activity.RegisterOptions{Name: orchestration.ActivityExecuteProviderJob})
	temporalWorker.RegisterActivityWithOptions(activities.RunAutomaticQC, activity.RegisterOptions{Name: orchestration.ActivityRunAutomaticQC})
	temporalWorker.RegisterActivityWithOptions(activities.CreateShotReview, activity.RegisterOptions{Name: orchestration.ActivityCreateShotReview})
	temporalWorker.RegisterActivityWithOptions(activities.EscalateShot, activity.RegisterOptions{Name: orchestration.ActivityEscalateShot})
	temporalWorker.RegisterActivityWithOptions(activities.CreateGate3, activity.RegisterOptions{Name: orchestration.ActivityCreateGate3})

	log.Printf("video Temporal worker listening (namespace=%s task_queue=%s)", cfg.Namespace, cfg.TaskQueue)
	if err := temporalWorker.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("run video Temporal worker: %v", err)
	}
}
