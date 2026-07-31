package production

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

// ProviderContentGenerator adapts any configured text provider to the
// ContentGenerator boundary. It uses the stable GenerationRequest contract,
// preserves the same idempotency key across uncertain submits, and accepts
// only strict CompilationDraft JSON.
type ProviderContentGenerator struct {
	Provider  providercontract.Provider
	ModelHint string
	Budget    providercontract.BudgetEnvelope
	MaxPolls  int
	Wait      func(context.Context, time.Duration) error
}

func (g ProviderContentGenerator) Generate(ctx context.Context, input ContentGenerationRequest) (CompilationDraft, error) {
	if g.Provider == nil {
		return CompilationDraft{}, validationf("text provider is required")
	}
	if err := input.SourceRevision.Validate(); err != nil {
		return CompilationDraft{}, err
	}
	if input.SourceRevision.Kind != KindSource || input.SourceHash != input.SourceRevision.ContentHash ||
		!validSHA256(input.SourceHash) || !nonEmpty(input.Title, input.Language, input.Text) {
		return CompilationDraft{}, validationf("structured generation must pin the authorized source revision")
	}
	if g.Budget.MaxAttempts < 1 {
		return CompilationDraft{}, validationf("positive content-generation attempt budget is required")
	}
	capabilities, err := g.Provider.Discover(ctx)
	if err != nil {
		return CompilationDraft{}, err
	}
	if !slices.ContainsFunc(capabilities, func(capability providercontract.Capability) bool {
		return capability.OutputModality == providercontract.ModalityText
	}) {
		return CompilationDraft{}, policyf("configured provider has no discovered text-generation capability")
	}
	wait := g.Wait
	if wait == nil {
		wait = waitContext
	}
	idempotencyKey := derivedID("content-job", hashString(input.SourceRevision.ID+"\x00"+input.SourceHash+"\x00"+g.ModelHint))
	request := providercontract.GenerationRequest{
		RequestID:        idempotencyKey,
		IdempotencyKey:   idempotencyKey,
		Modality:         providercontract.ModalityText,
		Prompt:           structuredCompilationPrompt(input),
		PromptSnapshotID: derivedID("content-prompt", hashString(input.SourceHash+"\x00structured-compilation-v1")),
		Context: providercontract.ContextRefs{
			SeriesSnapshotID:  derivedID("content-context", hashString("series\x00"+input.SourceRevision.AggregateID)),
			EpisodeSnapshotID: derivedID("content-context", hashString("episode\x00"+input.SourceRevision.ID)),
			SceneSnapshotID:   derivedID("content-context", hashString("scene\x00"+input.SourceRevision.ID)),
			ShotSnapshotID:    derivedID("content-context", hashString("shot\x00"+input.SourceRevision.ID)),
		},
		Output:    providercontract.OutputSpec{Format: "application/json"},
		ModelHint: g.ModelHint,
		Budget:    g.Budget,
	}
	if err := request.Validate(); err != nil {
		return CompilationDraft{}, err
	}
	var job providercontract.Job
	for attempt := 1; attempt <= g.Budget.MaxAttempts; attempt++ {
		submitted, err := g.Provider.Submit(ctx, request)
		if err == nil {
			job = submitted
			break
		}
		var providerErr *providercontract.Error
		if !errors.As(err, &providerErr) || !providerErr.Retryable || attempt == g.Budget.MaxAttempts {
			return CompilationDraft{}, err
		}
		if err := wait(ctx, retryDelay(providerErr)); err != nil {
			return CompilationDraft{}, err
		}
	}
	if job.ID == "" {
		return CompilationDraft{}, &providercontract.Error{
			Code: providercontract.CodeUnavailable, Retryable: true,
			SafeMessage: "structured content submission outcome is unknown",
		}
	}
	maxPolls := g.MaxPolls
	if maxPolls <= 0 {
		maxPolls = 60
	}
	for poll := 0; !job.Status.Terminal() && poll < maxPolls; poll++ {
		polled, err := g.Provider.Poll(ctx, job.ID)
		if err != nil {
			var providerErr *providercontract.Error
			if !errors.As(err, &providerErr) || !providerErr.Retryable {
				return CompilationDraft{}, err
			}
			if err := wait(ctx, retryDelay(providerErr)); err != nil {
				return CompilationDraft{}, err
			}
			continue
		}
		job = polled
		if !job.Status.Terminal() {
			if err := wait(ctx, 100*time.Millisecond); err != nil {
				return CompilationDraft{}, err
			}
		}
	}
	if job.Status != providercontract.StatusSucceeded {
		if job.Error != nil {
			return CompilationDraft{}, job.Error
		}
		return CompilationDraft{}, &providercontract.Error{
			Code: providercontract.CodeTimeout, Retryable: true,
			SafeMessage: "structured content job did not complete successfully",
		}
	}
	if g.ModelHint != "" && job.ProviderModel != g.ModelHint {
		return CompilationDraft{}, conflictf("text result does not match the frozen model hint")
	}
	if job.Output == nil || strings.TrimSpace(job.Output.Text) == "" {
		return CompilationDraft{}, &providercontract.Error{
			Code:        providercontract.CodeInvalidRequest,
			SafeMessage: "text provider returned no structured content",
		}
	}
	decoder := json.NewDecoder(bytes.NewBufferString(job.Output.Text))
	decoder.DisallowUnknownFields()
	var draft CompilationDraft
	if err := decoder.Decode(&draft); err != nil {
		return CompilationDraft{}, &providercontract.Error{
			Code:        providercontract.CodeInvalidRequest,
			SafeMessage: "text provider returned invalid structured content",
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return CompilationDraft{}, &providercontract.Error{
			Code:        providercontract.CodeInvalidRequest,
			SafeMessage: "text provider returned multiple structured values",
		}
	}
	return draft, nil
}

func structuredCompilationPrompt(input ContentGenerationRequest) string {
	return fmt.Sprintf(`Return exactly one JSON object matching the versioned CompilationDraft v1 contract.
Do not use markdown fences and do not add fields.
Required top-level fields: story, episodes.
story requires world, characters, relationships, locations, props, evidence.
Each evidence span must use byte offsets into the exact authorized source and the SHA-256 of that exact substring.
Each episode requires 45-60 seconds total; every shot requires 4-6 seconds, at most two characters, exactly one primary action, expressions, camera, and entry/exit continuity.
Source revision: %s
Source SHA-256: %s
Title: %s
Language: %s
AUTHORIZED SOURCE START
%s
AUTHORIZED SOURCE END`, input.SourceRevision.ID, input.SourceHash, input.Title, input.Language, input.Text)
}
