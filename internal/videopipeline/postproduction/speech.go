package postproduction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
)

type SpeechRequest struct {
	EpisodeRevisionID string
	SubtitleRevision  SubtitleRevision
	Cue               Cue
	Config            SpeechConfig
	Evidence          string
	TraceID           string
	BudgetMicros      int64
}

type SpeechProvider interface {
	Synthesize(context.Context, SpeechRequest) (ProviderAttempt, error)
}

type SpeechJobIdentity struct {
	JobID     string `json:"jobId"`
	InputHash string `json:"inputHash"`
}

// DeriveSpeechJobIdentity keeps the legacy identity readable while assigning
// speech-v2 a semantic identity that changes with every provider-facing voice
// revision. The canonical v2 tuple deliberately excludes transient request and
// trace identifiers.
func DeriveSpeechJobIdentity(input SpeechRequest) (SpeechJobIdentity, error) {
	if input.Config.IdentityVersion == SpeechIdentityV2 {
		if input.Config.Voice == nil {
			return SpeechJobIdentity{}, errors.New("speech-v2 requires an immutable VOICE binding")
		}
		inputHash, err := digestJSON(map[string]any{
			"cueId":               input.Cue.ID,
			"episodeRevisionId":   input.EpisodeRevisionID,
			"resourceId":          input.Config.Voice.ResourceID,
			"routeVersion":        input.Config.Route.RouteVersion,
			"speaker":             input.Config.Voice.Speaker,
			"subtitleContentHash": input.SubtitleRevision.ContentHash,
			"voiceAssetVersionId": input.Config.Voice.AssetVersionID,
		})
		if err != nil {
			return SpeechJobIdentity{}, err
		}
		return SpeechJobIdentity{
			JobID: "speech-v2-" + inputHash[:32], InputHash: inputHash,
		}, nil
	}
	inputHash, err := digestJSON(map[string]any{
		"episodeRevisionId": input.EpisodeRevisionID,
		"subtitleRevision":  input.SubtitleRevision.ID,
		"subtitleHash":      input.SubtitleRevision.ContentHash,
		"cue":               input.Cue,
		"route":             input.Config.Route,
	})
	if err != nil {
		return SpeechJobIdentity{}, err
	}
	jobDigest := sha256.Sum256([]byte(
		"speech\x00" + input.EpisodeRevisionID + "\x00" +
			input.SubtitleRevision.ContentHash + "\x00" + input.Cue.ID,
	))
	return SpeechJobIdentity{
		JobID: "speech-" + hex.EncodeToString(jobDigest[:16]), InputHash: inputHash,
	}, nil
}

type HTTPSpeechProvider struct {
	Endpoint     string
	Client       *http.Client
	PollInterval time.Duration
}

func NewHTTPSpeechProvider(endpoint string, client *http.Client) (*HTTPSpeechProvider, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, errors.New("speech provider adapter endpoint is required")
	}
	if client == nil {
		client = mockprovider.DefaultHTTPClient()
	}
	return &HTTPSpeechProvider{
		Endpoint:     strings.TrimRight(endpoint, "/"),
		Client:       client,
		PollInterval: 500 * time.Millisecond,
	}, nil
}

func (p *HTTPSpeechProvider) Synthesize(
	ctx context.Context,
	input SpeechRequest,
) (ProviderAttempt, error) {
	if err := input.Cue.Validate(); err != nil {
		return ProviderAttempt{}, err
	}
	if err := input.SubtitleRevision.Validate(0); err != nil {
		return ProviderAttempt{}, err
	}
	if err := input.Config.Validate(); err != nil {
		return ProviderAttempt{}, err
	}
	if input.BudgetMicros <= 0 || input.BudgetMicros > input.Config.BudgetMaximumMicros {
		return ProviderAttempt{}, errors.New("cue speech budget is outside the approved envelope")
	}
	if input.Evidence != EvidenceMockOnly && input.Evidence != EvidenceLive {
		return ProviderAttempt{}, errors.New("speech submission requires mock_only or live_provider_call evidence")
	}
	var cueAuthorization speechcontract.CueAuthorization
	if input.Config.IdentityVersion == SpeechIdentityV2 {
		if input.Config.BatchAuthorization != nil {
			var ok bool
			cueAuthorization, _, ok = input.Config.BatchAuthorization.Cue(input.Cue.ID)
			if !ok {
				return ProviderAttempt{}, errors.New("speech-v2 cue is outside the ordered batch authorization")
			}
		} else if input.Cue.ID != input.Config.AuthorizedCueID {
			return ProviderAttempt{}, errors.New("speech-v2 cue is outside the single authorized canary")
		}
	}
	identity, err := DeriveSpeechJobIdentity(input)
	if err != nil {
		return ProviderAttempt{}, err
	}
	jobID := identity.JobID
	inputHash := identity.InputHash
	runID := "episode-" + input.EpisodeRevisionID
	modelHint := input.Cue.VoiceRef
	maxAttempts := 2
	format := "wav"
	var assets []providercontract.AssetRef
	if input.Config.IdentityVersion == SpeechIdentityV2 {
		modelHint = input.Config.Voice.Speaker
		maximumAFPMilli := input.Config.MaximumAFPMilli
		maxAttempts = input.Config.MaxAttempts
		if input.Config.BatchAuthorization != nil {
			maximumAFPMilli = cueAuthorization.MaximumAFPMilli
			maxAttempts = cueAuthorization.MaxAttempts
			if identity.JobID != cueAuthorization.JobID || identity.InputHash != cueAuthorization.InputHash {
				return ProviderAttempt{}, errors.New("speech-v2 cue identity drifted from the ordered batch authorization")
			}
		}
		format = "mp3"
		predictedAFPMilli := int64(len([]rune(strings.TrimSpace(input.Cue.Text)))) * 135
		if predictedAFPMilli <= 0 || predictedAFPMilli > maximumAFPMilli {
			return ProviderAttempt{}, errors.New("speech-v2 cue exceeds the frozen AFP ceiling")
		}
		assets = []providercontract.AssetRef{{
			ID: input.Config.Voice.AssetID, Revision: input.Config.Voice.AssetVersionID,
			Kind: providercontract.ModalityAudio, Role: providercontract.AssetRoleReferenceAudio,
			URI:    "cas://sha256/" + input.Config.Voice.AssetVersionHash,
			SHA256: input.Config.Voice.AssetVersionHash,
			LicenseReference: input.Config.Voice.LicenseSnapshotID + ":" +
				input.Config.Voice.LicenseSnapshotHash,
			MediaType: "audio/x-voice-profile+json",
		}}
	}
	generationRequest := providercontract.GenerationRequest{
		RequestID:        jobID,
		IdempotencyKey:   jobID,
		Modality:         providercontract.ModalityAudio,
		Prompt:           input.Cue.Text,
		PromptSnapshotID: input.SubtitleRevision.ID + ":" + input.Cue.ID,
		ModelHint:        modelHint,
		Assets:           assets,
		Output: providercontract.OutputSpec{
			DurationMillis: int(input.Cue.EndMillis - input.Cue.StartMillis),
			Format:         format,
		},
		Budget: providercontract.BudgetEnvelope{
			EstimatedCostMicros: input.BudgetMicros,
			MaxCostMicros:       input.BudgetMicros,
			MaxAttempts:         maxAttempts,
		},
	}
	reservation, err := providercontract.BindBudgetReservation(
		providercontract.BudgetReservation{
			ReservationID:  input.Config.BudgetApprovalID + ":" + input.Cue.ID,
			Currency:       input.Config.BudgetCurrency,
			AmountMicros:   input.BudgetMicros,
			PricingVersion: "post-production-approved-v1",
			ConfirmedBy:    input.Config.BudgetApprovalID,
		},
		providercontract.BudgetBindingInput{
			RunID: runID, InputHash: inputHash, Model: input.Config.Route,
			Budget: generationRequest.Budget,
		},
	)
	if err != nil {
		return ProviderAttempt{}, fmt.Errorf("bind speech budget reservation: %w", err)
	}
	jobRequest := providercontract.JobRequest{
		SchemaVersion:     "v1",
		JobID:             jobID,
		RunID:             runID,
		Capability:        providercontract.CapabilitySpeech,
		InputHash:         inputHash,
		Model:             input.Config.Route,
		Request:           generationRequest,
		BudgetReservation: reservation,
		TraceID:           input.TraceID,
	}
	var response providercontract.JobResponse
	submitted := false
	if input.Config.BatchAuthorization != nil {
		// A restarted Activity reconciles the immutable job with GET first. It
		// must never issue another POST after an ambiguous or consumed submit.
		response, err = mockprovider.Get(ctx, p.Client, p.Endpoint, jobID)
		if err != nil && providercontract.ErrorCodeOf(err) != providercontract.CodeNotFound {
			return ProviderAttempt{}, err
		}
	}
	if input.Config.BatchAuthorization == nil || providercontract.ErrorCodeOf(err) == providercontract.CodeNotFound {
		response, err = mockprovider.Submit(ctx, p.Client, p.Endpoint, jobRequest)
		if err != nil {
			return ProviderAttempt{}, err
		}
		submitted = true
	}
	defer func() {
		if submitted && input.Config.BatchAuthorization == nil && ctx.Err() != nil {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = mockprovider.Cancel(cancelCtx, p.Client, p.Endpoint, jobID)
		}
	}()
	if err := stopOnAmbiguousSpeechState(response); err != nil {
		return ProviderAttempt{}, err
	}
	for !providercontract.Terminal(response.State) {
		timer := time.NewTimer(p.pollInterval())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ProviderAttempt{}, ctx.Err()
		case <-timer.C:
		}
		response, err = mockprovider.Get(ctx, p.Client, p.Endpoint, jobID)
		if err != nil {
			return ProviderAttempt{}, err
		}
		if err := stopOnAmbiguousSpeechState(response); err != nil {
			return ProviderAttempt{}, err
		}
	}
	submitted = false
	if response.State != providercontract.StatusSucceeded {
		if response.Error != nil {
			return ProviderAttempt{}, response.Error
		}
		return ProviderAttempt{}, fmt.Errorf("speech provider job ended in %s", response.State)
	}
	if len(response.Artifacts) != 1 {
		return ProviderAttempt{}, errors.New("speech provider must return exactly one audio artifact")
	}
	if response.JobID != jobID ||
		response.RunID != "episode-"+input.EpisodeRevisionID ||
		response.Model != input.Config.Route {
		return ProviderAttempt{}, errors.New("speech provider response drifted from the frozen job or route")
	}
	output := response.Artifacts[0]
	if output.Kind != providercontract.ModalityAudio ||
		output.Role != providercontract.AssetRoleOutput ||
		strings.TrimSpace(output.LicenseReference) == "" ||
		!strings.HasPrefix(strings.ToLower(output.MediaType), "audio/") {
		return ProviderAttempt{}, errors.New("speech provider returned an invalid audio output contract")
	}
	artifact := Artifact{
		Kind:           "dialogue_segment",
		Digest:         output.SHA256,
		URI:            output.URI,
		MediaType:      output.MediaType,
		SizeBytes:      output.SizeBytes,
		DurationMillis: output.DurationMillis,
	}
	if err := artifact.Validate(); err != nil {
		return ProviderAttempt{}, fmt.Errorf("speech artifact: %w", err)
	}
	attempt := ProviderAttempt{
		CueID:          input.Cue.ID,
		JobID:          response.JobID,
		RequestID:      response.RequestID,
		UpstreamTaskID: response.UpstreamTaskID,
		ConnectID:      response.ConnectID,
		LogID:          response.LogID,
		Model:          response.Model,
		Usage:          response.Usage,
		Cost:           response.Cost,
		Artifact:       artifact,
		Evidence:       input.Evidence,
	}
	if err := validateProviderAttempt(input, attempt); err != nil {
		return ProviderAttempt{}, err
	}
	return attempt, nil
}

func stopOnAmbiguousSpeechState(response providercontract.JobResponse) error {
	if response.State != providercontract.StatusUnknown &&
		response.State != providercontract.StatusRequiresAction {
		return nil
	}
	return &providercontract.Error{
		Code: providercontract.CodeUnavailable, Retryable: false,
		SafeMessage:     "speech provider result requires read-only reconciliation",
		RequiresAction:  true,
		SuggestedAction: "inspect the immutable job receipt without submitting again",
	}
}

func (p *HTTPSpeechProvider) pollInterval() time.Duration {
	if p.PollInterval <= 0 {
		return 500 * time.Millisecond
	}
	return p.PollInterval
}

func validateProviderAttempt(input SpeechRequest, attempt ProviderAttempt) error {
	var expectedJobID string
	maximumAFPMilli := input.Config.MaximumAFPMilli
	if input.Config.IdentityVersion == SpeechIdentityV2 {
		identity, err := DeriveSpeechJobIdentity(input)
		if err != nil {
			return err
		}
		expectedJobID = identity.JobID
		if input.Config.BatchAuthorization != nil {
			authorization, _, ok := input.Config.BatchAuthorization.Cue(input.Cue.ID)
			if !ok || authorization.JobID != identity.JobID || authorization.InputHash != identity.InputHash {
				return errors.New("speech-v2 attempt is outside the ordered batch authorization")
			}
			maximumAFPMilli = authorization.MaximumAFPMilli
		}
	}
	switch {
	case attempt.CueID != input.Cue.ID:
		return errors.New("speech attempt cue does not match the requested cue")
	case expectedJobID != "" && attempt.JobID != expectedJobID:
		return errors.New("speech attempt job identity does not match the frozen voice input")
	case strings.TrimSpace(attempt.JobID) == "" ||
		strings.TrimSpace(attempt.RequestID) == "" ||
		strings.TrimSpace(attempt.UpstreamTaskID) == "":
		return errors.New("speech attempt requires job, request, and upstream task identifiers")
	case attempt.Model != input.Config.Route:
		return errors.New("speech attempt model does not match the frozen route")
	case attempt.Evidence != input.Evidence:
		return errors.New("speech attempt evidence does not match the request")
	case attempt.Cost.EstimatedMicros < 0 ||
		attempt.Cost.EstimatedMicros > input.BudgetMicros:
		return errors.New("speech attempt estimate exceeds the cue budget")
	case attempt.Cost.ActualMicros != nil &&
		(*attempt.Cost.ActualMicros < 0 || *attempt.Cost.ActualMicros > input.BudgetMicros):
		return errors.New("speech attempt actual cost exceeds the cue budget")
	case attempt.Cost.Currency != input.Config.BudgetCurrency ||
		strings.TrimSpace(attempt.Cost.PricingVersion) == "":
		return errors.New("speech attempt cost currency or pricing version is invalid")
	case negativeUsage(attempt.Usage):
		return errors.New("speech attempt usage cannot be negative")
	case attempt.Artifact.Kind != "dialogue_segment" ||
		attempt.Artifact.DurationMillis <= 0:
		return errors.New("speech attempt requires a positive-duration dialogue artifact")
	}
	if err := attempt.Artifact.Validate(); err != nil {
		return fmt.Errorf("speech attempt artifact: %w", err)
	}
	if sameSpeechProvider(input.Config.Route.Provider, "volcengine_ark") &&
		input.Config.Route.ModelID == "doubao-seed-tts-2.0" {
		if strings.TrimSpace(attempt.ConnectID) == "" || strings.TrimSpace(attempt.LogID) == "" {
			return errors.New("Agent Plan TTS attempt requires connect and X-Tt-Logid evidence")
		}
		if attempt.Usage.GeneratedChars <= 0 || attempt.Usage.GeneratedChars > 600 ||
			attempt.Usage.Unit != "milli_afp" ||
			attempt.Usage.OutputUnits != attempt.Usage.GeneratedChars*135 {
			return errors.New("Agent Plan TTS attempt requires returned usage tokens and exact per-request AFP attribution")
		}
	}
	if input.Config.IdentityVersion == SpeechIdentityV2 {
		if attempt.Usage.OutputUnits > maximumAFPMilli {
			return errors.New("speech-v2 attempt exceeds the frozen AFP ceiling")
		}
		if attempt.Cost.ActualMicros == nil || *attempt.Cost.ActualMicros != 0 ||
			attempt.Cost.BillingMode != "subscription_included" ||
			input.Config.MaximumNonSubscriptionCashMicros != 0 {
			return errors.New("speech-v2 attempt lacks zero-cash Agent Plan evidence")
		}
	}
	return nil
}

func validateSpeechAuthorizationCoverage(request Request) error {
	batch := request.Speech.BatchAuthorization
	if batch == nil {
		if len(request.Speech.CompletedAttempts) != 0 {
			return errors.New("completed speech attempts require an ordered batch authorization")
		}
		return nil
	}
	completed := make(map[string]ProviderAttempt, len(request.Speech.CompletedAttempts))
	for index, attempt := range request.Speech.CompletedAttempts {
		if _, duplicate := completed[attempt.CueID]; duplicate {
			return fmt.Errorf("duplicate completed speech cue %q", attempt.CueID)
		}
		completed[attempt.CueID] = attempt
		if err := validateCompletedAttempt(request, attempt); err != nil {
			return fmt.Errorf("completed speech attempt %d: %w", index, err)
		}
	}
	authorizedIndex := 0
	seenCompleted := 0
	for _, cue := range request.Subtitle.Cues {
		if _, ok := completed[cue.ID]; ok {
			seenCompleted++
			continue
		}
		if authorizedIndex >= len(batch.Cues) || batch.Cues[authorizedIndex].CueID != cue.ID {
			return errors.New("speech batch cues must exactly match remaining subtitle cues in timeline order")
		}
		authorization := batch.Cues[authorizedIndex]
		if authorization.UnicodeCharacters != len([]rune(strings.TrimSpace(cue.Text))) {
			return fmt.Errorf("speech batch cue %q character count drifted", cue.ID)
		}
		identity, err := DeriveSpeechJobIdentity(SpeechRequest{
			EpisodeRevisionID: request.EpisodeRevisionID,
			SubtitleRevision:  request.Subtitle,
			Cue:               cue,
			Config:            request.Speech,
		})
		if err != nil {
			return err
		}
		if authorization.JobID != identity.JobID || authorization.InputHash != identity.InputHash {
			return fmt.Errorf("speech batch cue %q identity drifted", cue.ID)
		}
		authorizedIndex++
	}
	if authorizedIndex != len(batch.Cues) || seenCompleted != len(completed) {
		return errors.New("speech batch contains cues outside the frozen subtitle revision")
	}
	return nil
}

func validateCompletedAttempt(request Request, attempt ProviderAttempt) error {
	var cue *Cue
	for index := range request.Subtitle.Cues {
		if request.Subtitle.Cues[index].ID == attempt.CueID {
			cue = &request.Subtitle.Cues[index]
			break
		}
	}
	if cue == nil {
		return errors.New("completed speech cue is absent from the subtitle revision")
	}
	identity, err := DeriveSpeechJobIdentity(SpeechRequest{
		EpisodeRevisionID: request.EpisodeRevisionID,
		SubtitleRevision:  request.Subtitle,
		Cue:               *cue,
		Config:            request.Speech,
	})
	if err != nil {
		return err
	}
	switch {
	case attempt.JobID != identity.JobID:
		return errors.New("completed speech job identity drifted")
	case attempt.CueID != cue.ID || strings.TrimSpace(attempt.RequestID) == "" ||
		strings.TrimSpace(attempt.UpstreamTaskID) == "" || strings.TrimSpace(attempt.ConnectID) == "" ||
		strings.TrimSpace(attempt.LogID) == "":
		return errors.New("completed speech attempt identity is incomplete")
	case attempt.Model != request.Speech.Route || attempt.Evidence != request.Evidence:
		return errors.New("completed speech attempt route or evidence drifted")
	case attempt.Usage.GeneratedChars != int64(len([]rune(strings.TrimSpace(cue.Text)))) ||
		attempt.Usage.OutputUnits != attempt.Usage.GeneratedChars*135 ||
		attempt.Usage.Unit != "milli_afp":
		return errors.New("completed speech attempt usage is invalid")
	case attempt.Cost.ActualMicros == nil || *attempt.Cost.ActualMicros != 0 ||
		attempt.Cost.BillingMode != "subscription_included" ||
		attempt.Cost.Currency != request.Speech.BudgetCurrency:
		return errors.New("completed speech attempt lacks zero-cash evidence")
	case attempt.Artifact.Kind != "dialogue_segment" || attempt.Artifact.DurationMillis <= 0:
		return errors.New("completed speech attempt artifact is invalid")
	}
	return attempt.Artifact.Validate()
}

func negativeUsage(usage providercontract.Usage) bool {
	return usage.InputTokens < 0 ||
		usage.OutputTokens < 0 ||
		usage.VideoTokens < 0 ||
		usage.GeneratedImages < 0 ||
		usage.GeneratedChars < 0 ||
		usage.GeneratedMillis < 0 ||
		usage.ProviderCostMicros < 0 ||
		usage.InputUnits < 0 ||
		usage.OutputUnits < 0
}
