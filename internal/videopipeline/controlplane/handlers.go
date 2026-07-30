package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const maxCommandBody = 1 << 20

type problem struct {
	Type            string           `json:"type"`
	Title           string           `json:"title"`
	Status          int              `json:"status"`
	Detail          string           `json:"detail,omitempty"`
	ErrorCode       ErrorCode        `json:"errorCode"`
	Retryable       bool             `json:"retryable"`
	AffectedObjects []map[string]any `json:"affectedObjects,omitempty"`
	TraceID         string           `json:"traceId"`
	SuggestedAction string           `json:"suggestedAction"`
}

func (s *Server) createSeries(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command CreateSeriesCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	if err := s.authorizeCommandActor(r, command.Actor); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if err := validateCreateSeries(command); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "series:create:"+command.Actor.ActorID, command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.CreateSeries(r.Context(), command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) createSourceRevision(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command CreateSourceRevisionCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	seriesID := r.PathValue("seriesId")
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = validateUUID("seriesId", seriesID)
	}
	if err == nil {
		err = validateCreateSource(command)
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.artifacts == nil {
		writeProblem(w, traceID, domainError(
			CodeDependency, http.StatusServiceUnavailable, "artifact store is not configured", "restore CAS and retry", nil,
		))
		return
	}
	reader, err := s.artifacts.Open(command.ArtifactHash)
	if err != nil {
		writeProblem(w, traceID, domainError(
			CodeDependency, http.StatusUnprocessableEntity, "artifactHash does not exist in CAS", "commit the artifact before creating its revision", err,
		))
		return
	}
	_ = reader.Close()
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "series:"+seriesID+":source", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.CreateSourceRevision(r.Context(), seriesID, expected, command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) createGenerationPlan(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command CreateGenerationPlanCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	if err := s.authorizeCommandActor(r, command.Actor); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if err := validateCreatePlan(command); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "generation-plan:"+command.SeriesID, command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.CreateGenerationPlan(r.Context(), command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeJSON(w, http.StatusCreated, stored.Value)
}

func (s *Server) startEpisodeProduction(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command StartProductionCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	episodeID := r.PathValue("episodeId")
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = validateUUID("episodeId", episodeID)
	}
	if err == nil {
		err = validateStartProduction(command)
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "episode:"+episodeID+":production", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.PrepareProduction(r.Context(), episodeID, expected, command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.workflows == nil {
		writeProblem(w, traceID, domainError(
			CodeTemporal, http.StatusServiceUnavailable, "Temporal controller is not configured", "restore Temporal and retry with the same idempotency key", nil,
		))
		return
	}
	started, err := s.workflows.StartEpisode(r.Context(), stored.Value, command)
	if err != nil {
		writeProblem(w, traceID, &DomainError{
			Code:            CodeTemporal,
			Status:          http.StatusServiceUnavailable,
			Detail:          "Temporal did not acknowledge the persisted production operation",
			Retryable:       true,
			SuggestedAction: "retry with the same idempotency key; no second operation will be created",
			Cause:           err,
		})
		return
	}
	if err := s.store.MarkOperationStarted(r.Context(), stored.Value.OperationID, started.WorkflowID, started.RunID); err == nil {
		stored.Value.State = "RUNNING"
		stored.Value.TemporalWorkflowID = started.WorkflowID
		stored.Value.TemporalRunID = started.RunID
	} else if current, getErr := s.store.GetOperation(r.Context(), stored.Value.OperationID); getErr == nil {
		stored.Value = current
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) createGenerationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command CreateGenerationRunCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	shotID := r.PathValue("shotId")
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = validateUUID("shotId", shotID)
	}
	if err == nil {
		err = validateCreateRun(command)
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "shot:"+shotID+":run", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.CreateGenerationRun(r.Context(), shotID, expected, command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.workflows == nil {
		_ = s.store.MarkOperationFailed(r.Context(), stored.Value.OperationID, string(CodeTemporal))
		writeProblem(w, traceID, domainError(
			CodeTemporal, http.StatusServiceUnavailable, "Temporal controller is not configured",
			"restore Temporal and retry with the same idempotency key", nil,
		))
		return
	}
	started, err := s.workflows.StartShot(r.Context(), stored.Value)
	if err != nil {
		writeProblem(w, traceID, &DomainError{
			Code: CodeTemporal, Status: http.StatusServiceUnavailable, Retryable: true,
			Detail:          "Temporal did not acknowledge the persisted shot workflow",
			SuggestedAction: "retry with the same idempotency key; the stable workflow ID prevents duplicate provider work",
			Cause:           err,
		})
		return
	}
	if err := s.store.MarkOperationStarted(r.Context(), stored.Value.OperationID, started.WorkflowID, started.RunID); err == nil {
		stored.Value.State = "RUNNING"
		stored.Value.TemporalWorkflowID = started.WorkflowID
		stored.Value.TemporalRunID = started.RunID
	} else if current, getErr := s.store.GetOperation(r.Context(), stored.Value.OperationID); getErr == nil {
		stored.Value = current
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) getGenerationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	traceID := traceID(r)
	run, err := s.store.GetGenerationRun(r.Context(), r.PathValue("runId"))
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, run.CreativeAttempt))
	writeJSON(w, http.StatusOK, run)
}

type cancelRunCommand struct {
	Actor      Actor  `json:"actor"`
	ReasonCode string `json:"reasonCode"`
}

type pauseRunCommand struct {
	Actor      Actor  `json:"actor"`
	ReasonCode string `json:"reasonCode"`
}

func (s *Server) pauseGenerationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command pauseRunCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = authorize(ActionPauseRun, command.Actor)
	}
	if err == nil && strings.TrimSpace(command.ReasonCode) == "" {
		err = validationError("reasonCode is required")
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	runID := r.PathValue("runId")
	idempotency, err := commandIdempotency(r, "run:"+runID+":pause", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.RequestRunPause(
		r.Context(), runID, expected, command.Actor, command.ReasonCode, idempotency, traceID,
	)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.workflows == nil || stored.Value.TemporalWorkflowID == "" {
		writeProblem(w, traceID, domainError(
			CodeTemporal, http.StatusServiceUnavailable, "run has no active Temporal workflow",
			"retry after workflow reconciliation", nil,
		))
		return
	}
	if err := s.workflows.Pause(
		r.Context(), stored.Value.TemporalWorkflowID, stored.Value.OperationID, command.ReasonCode,
	); err != nil {
		writeProblem(w, traceID, &DomainError{
			Code: CodeTemporal, Status: http.StatusServiceUnavailable, Retryable: true,
			Detail:          "pause was persisted but Temporal has not acknowledged it",
			SuggestedAction: "retry with the same idempotency key",
			Cause:           err,
		})
		return
	}
	_ = s.store.MarkOperationSucceeded(r.Context(), stored.Value.OperationID)
	if current, getErr := s.store.GetOperation(r.Context(), stored.Value.OperationID); getErr == nil {
		stored.Value = current
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) cancelGenerationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command cancelRunCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = authorize(ActionCancelRun, command.Actor)
	}
	if err == nil && strings.TrimSpace(command.ReasonCode) == "" {
		err = validationError("reasonCode is required")
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	runID := r.PathValue("runId")
	idempotency, err := commandIdempotency(r, "run:"+runID+":cancel", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.RequestRunCancellation(
		r.Context(), runID, expected, command.Actor, command.ReasonCode, idempotency, traceID,
	)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.workflows != nil && stored.Value.TemporalWorkflowID != "" {
		if err := s.workflows.Cancel(r.Context(), stored.Value.TemporalWorkflowID, command.ReasonCode); err != nil {
			writeProblem(w, traceID, &DomainError{
				Code: CodeTemporal, Status: http.StatusServiceUnavailable, Retryable: true,
				Detail:          "cancellation was persisted but Temporal has not acknowledged it",
				SuggestedAction: "retry with the same idempotency key; reconciliation will not create a new provider job",
				Cause:           err,
			})
			return
		}
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

type resumeRunCommand struct {
	Actor        Actor  `json:"actor"`
	RecoveryMode string `json:"recoveryMode"`
}

func (s *Server) resumeGenerationRun(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command resumeRunCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	expected, err := expectedRevision(r)
	if err == nil {
		err = s.authorizeCommandActor(r, command.Actor)
	}
	if err == nil {
		err = authorize(ActionResumeRun, command.Actor)
	}
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Actor = normalizedActor(command.Actor)
	runID := r.PathValue("runId")
	idempotency, err := commandIdempotency(r, "run:"+runID+":resume", command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.RequestRunResume(
		r.Context(), runID, expected, command.Actor, command.RecoveryMode, idempotency, traceID,
	)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if s.workflows != nil && stored.Value.TemporalWorkflowID != "" {
		if err := s.workflows.Resume(
			r.Context(), stored.Value.TemporalWorkflowID, stored.Value.OperationID, command.RecoveryMode,
		); err != nil {
			writeProblem(w, traceID, &DomainError{
				Code: CodeTemporal, Status: http.StatusServiceUnavailable, Retryable: true,
				Detail:          "recovery was persisted but Temporal has not acknowledged it",
				SuggestedAction: "retry with the same idempotency key",
				Cause:           err,
			})
			return
		}
		_ = s.store.MarkOperationSucceeded(r.Context(), stored.Value.OperationID)
		if current, getErr := s.store.GetOperation(r.Context(), stored.Value.OperationID); getErr == nil {
			stored.Value = current
		}
	}
	writeOperation(w, http.StatusAccepted, stored.Value)
}

func (s *Server) createApprovalDecision(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	var command CreateApprovalDecisionCommand
	if !decodeCommand(w, r, &command) {
		return
	}
	traceID := traceID(r)
	if err := s.authorizeCommandActor(r, command.Actor); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	if err := validateApproval(command); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	command.Gate = strings.ToUpper(strings.TrimSpace(command.Gate))
	for index := range command.Bindings {
		command.Bindings[index].ObjectType = strings.ToUpper(strings.TrimSpace(command.Bindings[index].ObjectType))
	}
	command.Actor = normalizedActor(command.Actor)
	idempotency, err := commandIdempotency(r, "approval:"+command.SeriesID+":"+command.Gate, command)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	stored, err := s.store.CreateApprovalDecision(r.Context(), command, idempotency, traceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	// Signal delivery is deliberately retried on an idempotent API replay.
	// Temporal signals are safe to duplicate and this closes the commit/signal
	// crash window without creating a second approval decision.
	if s.workflows != nil {
		if err := s.workflows.RecordApproval(r.Context(), stored.Value); err != nil {
			writeProblem(w, traceID, &DomainError{
				Code: CodeTemporal, Status: http.StatusServiceUnavailable, Retryable: true,
				Detail:          "approval is durable but workflow signal delivery is pending",
				SuggestedAction: "retry with the same idempotency key or let the outbox reconciler deliver it",
				Cause:           err,
			})
			return
		}
	}
	writeJSON(w, http.StatusCreated, stored.Value)
}

func (s *Server) listRevisionImpacts(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	traceID := traceID(r)
	sourceID := r.URL.Query().Get("sourceRevisionId")
	if err := validateUUID("sourceRevisionId", sourceID); err != nil {
		writeProblem(w, traceID, err)
		return
	}
	impacts, err := s.store.ListRevisionImpacts(r.Context(), r.PathValue("seriesId"), sourceID)
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sourceRevisionId": sourceID, "impacts": impacts})
}

func (s *Server) getGenerationManifest(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	traceID := traceID(r)
	scope := strings.ToUpper(r.PathValue("scopeType"))
	if scope != "SHOT" && scope != "EPISODE" {
		writeProblem(w, traceID, validationError("scopeType must be SHOT or EPISODE"))
		return
	}
	manifest, err := s.store.GetManifest(r.Context(), scope, r.PathValue("revisionId"))
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	if !s.requireStore(w, r) {
		return
	}
	traceID := traceID(r)
	operation, err := s.store.GetOperation(r.Context(), r.PathValue("operationId"))
	if err != nil {
		writeProblem(w, traceID, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func validateCreateSeries(command CreateSeriesCommand) error {
	if command.SchemaVersion != "v1" {
		return validationError("schemaVersion must be v1")
	}
	title := strings.TrimSpace(command.Title)
	if title == "" || len(title) > 200 {
		return validationError("title must contain 1 to 200 characters")
	}
	if err := validateUUID("generationProfileRevisionId", command.GenerationProfileRevisionID); err != nil {
		return err
	}
	if strings.TrimSpace(command.RightsDeclaration.Basis) == "" {
		return validationError("rightsDeclaration.basis is required")
	}
	if err := validateSHA256("rightsDeclaration.evidenceArtifactHash", command.RightsDeclaration.EvidenceArtifactHash); err != nil {
		return err
	}
	return authorize(ActionCreateSeries, command.Actor)
}

func validateCreateSource(command CreateSourceRevisionCommand) error {
	if command.SchemaVersion != "v1" || strings.TrimSpace(command.Language) == "" {
		return validationError("schemaVersion v1 and language are required")
	}
	if err := validateArtifact(command.ArtifactHash, command.ArtifactURI); err != nil {
		return err
	}
	if err := validateUUID("rightsSnapshotId", command.RightsSnapshotID); err != nil {
		return err
	}
	if command.ParentRevisionID != "" {
		if err := validateUUID("parentRevisionId", command.ParentRevisionID); err != nil {
			return err
		}
	}
	return authorize(ActionCreateRevision, command.Actor)
}

func validateCreatePlan(command CreateGenerationPlanCommand) error {
	if command.SchemaVersion != "v1" {
		return validationError("schemaVersion must be v1")
	}
	if err := validateUUID("seriesId", command.SeriesID); err != nil {
		return err
	}
	if command.EpisodeRevisionID != "" {
		if err := validateUUID("episodeRevisionId", command.EpisodeRevisionID); err != nil {
			return err
		}
	}
	if err := uniqueUUIDs("shotSpecRevisionIds", command.ShotSpecRevisionIDs); err != nil {
		return err
	}
	if command.CandidatesPerShot < 1 {
		return validationError("candidatesPerShot must be positive")
	}
	if command.BudgetLimit.AmountMicros < 0 || !currencyPattern.MatchString(command.BudgetLimit.Currency) {
		return validationError("budgetLimit requires a non-negative amountMicros and ISO currency")
	}
	if err := validateVideoRoute(command.RouteSnapshot); err != nil {
		return err
	}
	if err := validateExecutionPolicy(command.ExecutionPolicy); err != nil {
		return err
	}
	return authorize(ActionCreatePlan, command.Actor)
}

func validateStartProduction(command StartProductionCommand) error {
	if command.SchemaVersion != "v1" {
		return validationError("schemaVersion must be v1")
	}
	for name, value := range map[string]string{
		"episodeRevisionId":           command.EpisodeRevisionID,
		"generationProfileRevisionId": command.GenerationProfileRevisionID,
		"gate2DecisionId":             command.Gate2DecisionID,
		"generationPlanId":            command.GenerationPlanID,
		"budgetApprovalId":            command.BudgetApprovalID,
	} {
		if err := validateUUID(name, value); err != nil {
			return err
		}
	}
	if err := uniqueUUIDs("shotSpecRevisionIds", command.ShotSpecRevisionIDs); err != nil {
		return err
	}
	if err := validateVideoRoute(command.RouteSnapshot); err != nil {
		return err
	}
	if err := validateExecutionPolicy(command.ExecutionPolicy); err != nil {
		return err
	}
	return authorize(ActionStartProduction, command.Actor)
}

func validateCreateRun(command CreateGenerationRunCommand) error {
	if command.SchemaVersion != "v1" {
		return validationError("schemaVersion must be v1")
	}
	for name, value := range map[string]string{
		"shotSpecRevisionId":          command.ShotSpecRevisionID,
		"promptSnapshotId":            command.PromptSnapshotID,
		"generationProfileRevisionId": command.GenerationProfileRevisionID,
		"generationPlanId":            command.GenerationPlanID,
		"budgetApprovalId":            command.BudgetApprovalID,
	} {
		if err := validateUUID(name, value); err != nil {
			return err
		}
	}
	if command.CreativeAttempt < 1 || command.CreativeAttempt > 2 {
		return validationError("creativeAttempt must be 1 or 2")
	}
	if err := validateVideoRoute(command.RouteSnapshot); err != nil {
		return err
	}
	if err := validateExecutionPolicy(command.ExecutionPolicy); err != nil {
		return err
	}
	return authorize(ActionCreateRun, command.Actor)
}

func validateExecutionPolicy(policy ExecutionPolicy) error {
	if !territoryPattern.MatchString(policy.TargetTerritory) {
		return validationError("executionPolicy.targetTerritory must be an ISO alpha-2 uppercase code")
	}
	if policy.ProductForm != "INTERNAL_PREVIEW" && policy.ProductForm != "COMMERCIAL_RELEASE" {
		return validationError("executionPolicy.productForm must be INTERNAL_PREVIEW or COMMERCIAL_RELEASE")
	}
	if strings.TrimSpace(policy.ContentSafetyPolicyVersion) == "" || !policy.ContentSafetyApproved {
		return validationError("executionPolicy requires an approved contentSafetyPolicyVersion")
	}
	return nil
}

func validateApproval(command CreateApprovalDecisionCommand) error {
	if command.SchemaVersion != "v1" {
		return validationError("schemaVersion must be v1")
	}
	if err := validateUUID("seriesId", command.SeriesID); err != nil {
		return err
	}
	if command.EpisodeID != "" {
		if err := validateUUID("episodeId", command.EpisodeID); err != nil {
			return err
		}
	}
	action, err := approvalAction(command.Gate)
	if err != nil {
		return err
	}
	if command.Decision != "APPROVED" && command.Decision != "REJECTED" && command.Decision != "RETURNED" {
		return validationError("decision must be APPROVED, REJECTED, or RETURNED")
	}
	if strings.TrimSpace(command.ReasonCode) == "" || len(command.Bindings) == 0 {
		return validationError("reasonCode and at least one immutable binding are required")
	}
	gate := strings.ToUpper(strings.TrimSpace(command.Gate))
	if command.EpisodeID == "" {
		return validationError("episodeId is required for G1, G2, Q1, and G3")
	}
	bindingTypes := make(map[string]int, len(command.Bindings))
	bindingKeys := make(map[string]struct{}, len(command.Bindings))
	for _, binding := range command.Bindings {
		objectType := strings.ToUpper(strings.TrimSpace(binding.ObjectType))
		if objectType == "" {
			return validationError("binding.objectType is required")
		}
		if err := validateUUID("binding.revisionId", binding.RevisionID); err != nil {
			return err
		}
		if err := validateSHA256("binding.contentHash", binding.ContentHash); err != nil {
			return err
		}
		key := objectType + "\x00" + binding.RevisionID
		if _, exists := bindingKeys[key]; exists {
			return validationError("approval bindings must not contain duplicates")
		}
		bindingKeys[key] = struct{}{}
		bindingTypes[objectType]++
	}
	if command.Decision == "APPROVED" {
		switch gate {
		case "G1", "G2":
			if bindingTypes["EPISODE_REVISION"] == 0 {
				return validationError(gate + " approval requires an EPISODE_REVISION binding")
			}
		case "Q1":
			if bindingTypes["SHOT_SPEC_REVISION"] != 1 || bindingTypes["GENERATION_RUN"] != 1 {
				return validationError("Q1 approval requires exactly one SHOT_SPEC_REVISION and one GENERATION_RUN binding")
			}
		case "G3":
			if bindingTypes["EPISODE_REVISION"] == 0 || bindingTypes["MANIFEST"] == 0 {
				return validationError("G3 approval requires EPISODE_REVISION and MANIFEST bindings")
			}
		}
	}
	return authorize(action, command.Actor)
}

func decodeCommand(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxCommandBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeProblem(w, traceID(r), validationError("invalid JSON request: "+safeJSONError(err)))
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, traceID(r), validationError("request body must contain one JSON object"))
		return false
	}
	return true
}

func safeJSONError(err error) string {
	var syntax *json.SyntaxError
	var typeError *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntax):
		return fmt.Sprintf("syntax error near byte %d", syntax.Offset)
	case errors.As(err, &typeError):
		return "field " + typeError.Field + " has an invalid type"
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return err.Error()
	default:
		return "body does not match the command schema"
	}
}

func commandIdempotency(r *http.Request, scope string, command any) (Idempotency, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if _, err := uuid.Parse(key); err != nil {
		return Idempotency{}, validationError("Idempotency-Key must be a UUID")
	}
	hash, err := requestDigest(command)
	if err != nil {
		return Idempotency{}, err
	}
	return Idempotency{Scope: scope, Key: key, RequestHash: hash}, nil
}

func expectedRevision(r *http.Request) (int, error) {
	value := strings.TrimSpace(r.Header.Get("If-Match"))
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return 0, validationError(`If-Match must be a quoted positive integer, for example "1"`)
	}
	revision, err := strconv.Atoi(value[1 : len(value)-1])
	if err != nil || revision < 1 {
		return 0, validationError(`If-Match must be a quoted positive integer, for example "1"`)
	}
	return revision, nil
}

func normalizedActor(actor Actor) Actor {
	return Actor{ActorID: strings.TrimSpace(actor.ActorID), Role: strings.ToUpper(strings.TrimSpace(actor.Role))}
}

func traceID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("X-Trace-Id")); value != "" && len(value) <= 128 {
		return value
	}
	return uuid.NewString()
}

func (s *Server) requireStore(w http.ResponseWriter, r *http.Request) bool {
	if s.store != nil {
		return true
	}
	writeProblem(w, traceID(r), domainError(
		CodeDependency, http.StatusServiceUnavailable, "PostgreSQL product store is not configured", "restore PostgreSQL and retry", nil,
	))
	return false
}

func writeOperation(w http.ResponseWriter, status int, operation Operation) {
	w.Header().Set("Location", APIBase+"/operations/"+operation.OperationID)
	if operation.State == "SUCCEEDED" {
		w.Header().Set("ETag", `"1"`)
	}
	writeJSON(w, status, operation)
}

func writeProblem(w http.ResponseWriter, trace string, err error) {
	domain := asDomainError(err)
	w.Header().Set("Content-Type", "application/problem+json")
	writeJSON(w, domain.Status, problem{
		Type:            "https://ai-video-series.local/problems/" + strings.ToLower(string(domain.Code)),
		Title:           string(domain.Code),
		Status:          domain.Status,
		Detail:          domain.Detail,
		ErrorCode:       domain.Code,
		Retryable:       domain.Retryable,
		TraceID:         trace,
		SuggestedAction: domain.SuggestedAction,
	})
}
