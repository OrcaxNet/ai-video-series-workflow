package volcengineprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const (
	speechInspectionReceiptSchema = "speech-inspection-receipt-v1"
	speechInspectionResultSchema  = "speech-inspection-result-v1"
	speechInspectionPending       = "inspection_pending"
	speechInspectionSucceeded     = "succeeded"
)

// speechInspectionCheckpoint keeps an acquired Provider output out of the
// public artifact list until its immutable CAS bytes pass media inspection.
// The exact CAS identity remains durable and auditable while reconciliation is
// pending or fail-closed.
type speechInspectionCheckpoint struct {
	State    string                    `json:"state"`
	Artifact providercontract.AssetRef `json:"artifact"`
}

// speechInspectionReceipt is published atomically before the CAS write. It is
// immutable, keyed by job ID, and contains enough sanitized Provider evidence
// to recover even when the mutable job registry update fails after synthesis.
type speechInspectionReceipt struct {
	SchemaVersion string                       `json:"schema_version"`
	RequestHash   string                       `json:"request_hash"`
	Response      providercontract.JobResponse `json:"response"`
	Artifact      providercontract.AssetRef    `json:"artifact"`
}

// speechInspectionResult is the monotonic completion point shared by Adapter
// processes. Once this immutable marker exists, no stale inspection failure is
// allowed to turn the job back into requires_action.
type speechInspectionResult struct {
	SchemaVersion string                       `json:"schema_version"`
	RequestHash   string                       `json:"request_hash"`
	Response      providercontract.JobResponse `json:"response"`
	Duration      speechDurationQC             `json:"speech_duration_qc"`
}

func speechInspectionError() *providercontract.Error {
	return safeError(
		providercontract.CodeUnavailable,
		"Agent Plan TTS audio is committed to CAS and requires media inspection reconciliation",
		false,
	)
}

func provisionalSpeechArtifact(
	request providercontract.JobRequest,
	audio []byte,
	mediaType string,
) providercontract.AssetRef {
	digestBytes := sha256.Sum256(audio)
	digest := hex.EncodeToString(digestBytes[:])
	return providercontract.AssetRef{
		ID: request.JobID + "-audio", Revision: digest,
		Kind: providercontract.ModalityAudio, Role: providercontract.AssetRoleOutput,
		URI: "cas://sha256/" + digest, SHA256: digest,
		LicenseReference: "request-license-manifest", MediaType: mediaType,
		SizeBytes: int64(len(audio)),
	}
}

func (s *Server) createSpeechInspectionReceipt(
	jobID string,
	receipt speechInspectionReceipt,
) error {
	created, err := s.createImmutableSpeechState(
		s.speechInspectionReceiptPath(jobID),
		receipt,
		"speech inspection receipt could not be committed",
	)
	if err != nil {
		return err
	}
	if !created {
		return safeError(
			providercontract.CodeUnavailable,
			"speech inspection receipt already exists and requires reconciliation",
			false,
		)
	}
	return nil
}

func (s *Server) createSpeechInspectionResult(
	jobID string,
	result speechInspectionResult,
) (speechInspectionResult, error) {
	created, err := s.createImmutableSpeechState(
		s.speechInspectionResultPath(jobID),
		result,
		"speech inspection result could not be committed",
	)
	if err != nil {
		return speechInspectionResult{}, err
	}
	if created {
		return result, nil
	}
	existing, ok, err := s.loadSpeechInspectionResult(jobID)
	if err != nil {
		return speechInspectionResult{}, err
	}
	if !ok || existing.RequestHash != result.RequestHash {
		return speechInspectionResult{}, safeError(
			providercontract.CodeUnavailable,
			"speech inspection result is inconsistent",
			false,
		)
	}
	return existing, nil
}

func (s *Server) createImmutableSpeechState(
	path string,
	value any,
	safeMessage string,
) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, safeError(providercontract.CodeUnavailable, safeMessage, false)
	}
	file, err := os.CreateTemp(s.stateDir, ".speech-inspection-*.tmp")
	if err != nil {
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if err := file.Chmod(0o440); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if err := file.Close(); err != nil {
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if err := os.Link(temporaryPath, path); errors.Is(err, os.ErrExist) {
		return false, nil
	} else if err != nil {
		return false, safeError(providercontract.CodeUnavailable, safeMessage, true)
	}
	if err := s.syncStateDirectory(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) loadSpeechInspectionReceipt(jobID string) (speechInspectionReceipt, bool, error) {
	var receipt speechInspectionReceipt
	ok, err := decodeSpeechState(s.speechInspectionReceiptPath(jobID), &receipt)
	if err != nil {
		return speechInspectionReceipt{}, false, safeError(
			providercontract.CodeUnavailable,
			"speech inspection receipt is invalid",
			false,
		)
	}
	if !ok {
		return speechInspectionReceipt{}, false, nil
	}
	if err := validateSpeechInspectionReceipt(jobID, receipt); err != nil {
		return speechInspectionReceipt{}, false, safeError(
			providercontract.CodeUnavailable,
			"speech inspection receipt is invalid",
			false,
		)
	}
	return receipt, true, nil
}

func (s *Server) loadSpeechInspectionResult(jobID string) (speechInspectionResult, bool, error) {
	var result speechInspectionResult
	ok, err := decodeSpeechState(s.speechInspectionResultPath(jobID), &result)
	if err != nil {
		return speechInspectionResult{}, false, safeError(
			providercontract.CodeUnavailable,
			"speech inspection result is invalid",
			false,
		)
	}
	if !ok {
		return speechInspectionResult{}, false, nil
	}
	if err := validateSpeechInspectionResult(jobID, result); err != nil {
		return speechInspectionResult{}, false, safeError(
			providercontract.CodeUnavailable,
			"speech inspection result is invalid",
			false,
		)
	}
	return result, true, nil
}

func decodeSpeechState(path string, target any) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return false, errors.New("speech inspection state has trailing content")
	}
	return true, nil
}

func validateSpeechInspectionReceipt(jobID string, receipt speechInspectionReceipt) error {
	if receipt.SchemaVersion != speechInspectionReceiptSchema ||
		!validLowerSHA256(receipt.RequestHash) ||
		receipt.Response.JobID != jobID ||
		receipt.Response.State != providercontract.StatusRequiresAction ||
		receipt.Response.Error == nil || receipt.Response.Error.Retryable ||
		len(receipt.Response.Artifacts) != 0 {
		return errors.New("invalid speech inspection receipt envelope")
	}
	return validateSpeechInspectionArtifact(jobID, receipt.Artifact, false)
}

func validateSpeechInspectionResult(jobID string, result speechInspectionResult) error {
	if result.SchemaVersion != speechInspectionResultSchema ||
		!validLowerSHA256(result.RequestHash) ||
		result.Response.JobID != jobID ||
		result.Response.State != providercontract.StatusSucceeded ||
		result.Response.Error != nil || len(result.Response.Artifacts) != 1 {
		return errors.New("invalid speech inspection result envelope")
	}
	artifact := result.Response.Artifacts[0]
	if err := validateSpeechInspectionArtifact(jobID, artifact, true); err != nil {
		return err
	}
	delta := result.Duration.MeasuredMillis - result.Duration.RequestedMillis
	if delta < 0 {
		delta = -delta
	}
	wantState := "passed"
	if delta > speechDurationToleranceMillis {
		wantState = "requires_adjustment"
	}
	if result.Duration.RequestedMillis <= 0 ||
		result.Duration.MeasuredMillis != artifact.DurationMillis ||
		result.Duration.DeltaMillis != delta ||
		result.Duration.ToleranceMillis != speechDurationToleranceMillis ||
		result.Duration.State != wantState {
		return errors.New("invalid speech inspection duration result")
	}
	return nil
}

func validateSpeechInspectionResultBinding(
	result speechInspectionResult,
	receipt speechInspectionReceipt,
) error {
	if result.RequestHash != receipt.RequestHash {
		return errors.New("speech inspection result request hash differs from receipt")
	}
	artifact := result.Response.Artifacts[0]
	artifact.DurationMillis = 0
	if !reflect.DeepEqual(artifact, receipt.Artifact) {
		return errors.New("speech inspection result artifact differs from receipt")
	}
	normalized := result.Response
	normalized.State = receipt.Response.State
	normalized.Progress = receipt.Response.Progress
	normalized.Artifacts = receipt.Response.Artifacts
	normalized.Error = receipt.Response.Error
	if !reflect.DeepEqual(normalized, receipt.Response) {
		return errors.New("speech inspection result Provider evidence differs from receipt")
	}
	return nil
}

func validateSpeechInspectionArtifact(
	jobID string,
	artifact providercontract.AssetRef,
	measured bool,
) error {
	if artifact.ID != jobID+"-audio" || artifact.Revision != artifact.SHA256 ||
		!validLowerSHA256(artifact.SHA256) ||
		artifact.Kind != providercontract.ModalityAudio ||
		artifact.Role != providercontract.AssetRoleOutput ||
		artifact.URI != "cas://sha256/"+artifact.SHA256 ||
		artifact.LicenseReference != "request-license-manifest" ||
		strings.TrimSpace(artifact.MediaType) == "" || artifact.SizeBytes <= 0 ||
		artifact.Width != 0 || artifact.Height != 0 || artifact.FPS != 0 {
		return errors.New("invalid speech inspection CAS artifact")
	}
	if (!measured && artifact.DurationMillis != 0) || (measured && artifact.DurationMillis <= 0) {
		return errors.New("invalid speech inspection artifact duration")
	}
	return nil
}

func (s *Server) reconcileSpeechInspection(
	ctx context.Context,
	jobID string,
	requestHash string,
	record *jobRecord,
) (providercontract.JobResponse, bool, error) {
	if result, ok, err := s.loadSpeechInspectionResult(jobID); err != nil {
		return providercontract.JobResponse{}, true, err
	} else if ok {
		receipt, receiptOK, receiptErr := s.loadSpeechInspectionReceipt(jobID)
		if receiptErr != nil {
			return providercontract.JobResponse{}, true, receiptErr
		}
		if result.RequestHash != requestHash ||
			result.Duration.RequestedMillis != int64(record.Expected.DurationMillis) ||
			!receiptOK || validateSpeechInspectionResultBinding(result, receipt) != nil {
			return providercontract.JobResponse{}, true, safeError(
				providercontract.CodeUnavailable,
				"speech inspection result does not match the durable job intent",
				false,
			)
		}
		applySpeechInspectionResult(record, result)
		if err := s.updateRecord(jobID, record); err != nil {
			return providercontract.JobResponse{}, true, err
		}
		return record.Response, true, nil
	}

	receipt, ok, err := s.loadSpeechInspectionReceipt(jobID)
	if err != nil {
		return providercontract.JobResponse{}, true, err
	}
	if !ok {
		return providercontract.JobResponse{}, false, nil
	}
	if receipt.RequestHash != requestHash {
		return providercontract.JobResponse{}, true, safeError(
			providercontract.CodeConflict,
			"speech inspection receipt does not match the durable job intent",
			false,
		)
	}

	record.Response = receipt.Response
	record.SpeechInspection = &speechInspectionCheckpoint{
		State: speechInspectionPending, Artifact: receipt.Artifact,
	}
	// Repair the mutable registry from the immutable receipt before touching
	// ffprobe. The receipt remains authoritative if this update is interrupted.
	if err := s.updateRecord(jobID, record); err != nil {
		return providercontract.JobResponse{}, true, err
	}
	// A competing Adapter may have published the immutable success marker
	// between our first read and checkpoint repair. Re-read it before media
	// inspection so a stale failure can never overwrite completed truth.
	if result, ok, err := s.loadSpeechInspectionResult(jobID); err != nil {
		return providercontract.JobResponse{}, true, err
	} else if ok {
		completedReceipt, receiptOK, receiptErr := s.loadSpeechInspectionReceipt(jobID)
		if receiptErr != nil {
			return providercontract.JobResponse{}, true, receiptErr
		}
		if result.RequestHash != requestHash ||
			result.Duration.RequestedMillis != int64(record.Expected.DurationMillis) ||
			!receiptOK || validateSpeechInspectionResultBinding(result, completedReceipt) != nil {
			return providercontract.JobResponse{}, true, safeError(
				providercontract.CodeUnavailable,
				"speech inspection result does not match the durable job intent",
				false,
			)
		}
		applySpeechInspectionResult(record, result)
		if err := s.updateRecord(jobID, record); err != nil {
			return providercontract.JobResponse{}, true, err
		}
		return record.Response, true, nil
	}

	committed, err := s.store.Resolve(ctx, receipt.Artifact.SHA256)
	if err != nil || committed.Digest != receipt.Artifact.SHA256 ||
		committed.URI != receipt.Artifact.URI || committed.Size != receipt.Artifact.SizeBytes {
		record.SpeechInspection.State = "requires_action"
		return record.Response, true, nil
	}
	measured, err := s.inspector.Inspect(ctx, committed.Path)
	if err != nil || receipt.Artifact.MediaType != "audio/mpeg" ||
		validateMeasuredSpeech(measured, record.Expected) != nil {
		record.SpeechInspection.State = "requires_action"
		return record.Response, true, nil
	}

	artifact := receipt.Artifact
	artifact.DurationMillis = measured.DurationMillis
	response := receipt.Response
	response.State = providercontract.StatusSucceeded
	response.Progress = 100
	response.Error = nil
	response.Artifacts = []providercontract.AssetRef{artifact}
	result := speechInspectionResult{
		SchemaVersion: speechInspectionResultSchema,
		RequestHash:   requestHash,
		Response:      response,
		Duration:      *newSpeechDurationQC(int64(record.Expected.DurationMillis), measured.DurationMillis),
	}
	result, err = s.createSpeechInspectionResult(jobID, result)
	if err != nil {
		return providercontract.JobResponse{}, true, err
	}
	if err := validateSpeechInspectionResultBinding(result, receipt); err != nil {
		return providercontract.JobResponse{}, true, safeError(
			providercontract.CodeUnavailable,
			"speech inspection result does not match its immutable receipt",
			false,
		)
	}
	applySpeechInspectionResult(record, result)
	if err := s.updateRecord(jobID, record); err != nil {
		return providercontract.JobResponse{}, true, err
	}
	return record.Response, true, nil
}

func applySpeechInspectionResult(record *jobRecord, result speechInspectionResult) {
	record.Response = result.Response
	record.SpeechDuration = &result.Duration
	record.SpeechInspection = &speechInspectionCheckpoint{
		State:    speechInspectionSucceeded,
		Artifact: result.Response.Artifacts[0],
	}
}

func (s *Server) speechInspectionReceiptPath(jobID string) string {
	sum := sha256.Sum256([]byte(jobID))
	return filepath.Join(s.stateDir, hex.EncodeToString(sum[:])+".speech-inspection.receipt.json")
}

func (s *Server) speechInspectionResultPath(jobID string) string {
	sum := sha256.Sum256([]byte(jobID))
	return filepath.Join(s.stateDir, hex.EncodeToString(sum[:])+".speech-inspection.result.json")
}
