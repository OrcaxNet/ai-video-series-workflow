package postproduction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

var ErrPendingKey = errors.New("live post-production is pending runtime provider credentials")

type Executor interface {
	Finalize(context.Context, Request) (Result, error)
}

type Service struct {
	Speech SpeechProvider
	Media  MediaProcessor
	Store  *artifactstore.Store
}

func NewService(
	speech SpeechProvider,
	media MediaProcessor,
	store *artifactstore.Store,
) (*Service, error) {
	if speech == nil || media == nil || store == nil {
		return nil, errors.New("speech provider, media processor, and artifact store are required")
	}
	return &Service{Speech: speech, Media: media, Store: store}, nil
}

func (s *Service) Finalize(ctx context.Context, request Request) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	if request.Evidence == EvidencePendingKey {
		return Result{}, ErrPendingKey
	}
	subtitleBytes, err := RenderSRT(request.Subtitle, request.DurationMillis())
	if err != nil {
		return Result{}, err
	}
	subtitleArtifact, err := s.commitBytes(ctx, subtitleBytes, Artifact{
		Kind:           "subtitle_srt",
		MediaType:      "application/x-subrip; charset=utf-8",
		DurationMillis: request.DurationMillis(),
	})
	if err != nil {
		return Result{}, err
	}

	attempts := make([]ProviderAttempt, 0, len(request.Subtitle.Cues))
	if len(request.Subtitle.Cues) > 0 {
		completed := make(map[string]ProviderAttempt, len(request.Speech.CompletedAttempts))
		for _, attempt := range request.Speech.CompletedAttempts {
			completed[attempt.CueID] = attempt
		}
		paidCueCount := len(request.Subtitle.Cues) - len(completed)
		if paidCueCount == 0 {
			paidCueCount = 1
		}
		baseBudget := request.Speech.BudgetMaximumMicros / int64(paidCueCount)
		remainder := request.Speech.BudgetMaximumMicros % int64(paidCueCount)
		if baseBudget == 0 {
			return Result{}, errors.New("speech budget cannot allocate a positive amount to every cue")
		}
		paidIndex := 0
		var batchAFPMilli int64
		for _, cue := range request.Subtitle.Cues {
			if attempt, ok := completed[cue.ID]; ok {
				attempts = append(attempts, attempt)
				continue
			}
			if request.Speech.IdentityVersion == SpeechIdentityV2 &&
				request.Speech.BatchAuthorization == nil && cue.ID != request.Speech.AuthorizedCueID {
				return Result{}, &providercontract.Error{
					Code: providercontract.CodeConflict, Retryable: false,
					SafeMessage:     "speech canary is limited to the frozen authorized cue",
					RequiresAction:  true,
					SuggestedAction: "authorize a new frozen speech package before another cue",
				}
			}
			cueBudget := baseBudget
			if int64(paidIndex) < remainder {
				cueBudget++
			}
			paidIndex++
			if request.AuthorizePaidSubmit != nil {
				if err := request.AuthorizePaidSubmit(ctx, cue); err != nil {
					return Result{}, fmt.Errorf(
						"authorize paid speech submission for cue %q: %w", cue.ID, err,
					)
				}
			}
			attempt, synthErr := s.Speech.Synthesize(ctx, SpeechRequest{
				EpisodeRevisionID: request.EpisodeRevisionID,
				SubtitleRevision:  request.Subtitle,
				Cue:               cue,
				Config:            request.Speech,
				Evidence:          request.Evidence,
				TraceID:           request.TraceID,
				BudgetMicros:      cueBudget,
			})
			if synthErr != nil {
				return Result{}, fmt.Errorf("synthesize cue %q: %w", cue.ID, synthErr)
			}
			if err := validateProviderAttempt(SpeechRequest{
				EpisodeRevisionID: request.EpisodeRevisionID,
				SubtitleRevision:  request.Subtitle,
				Cue:               cue,
				Config:            request.Speech,
				Evidence:          request.Evidence,
				TraceID:           request.TraceID,
				BudgetMicros:      cueBudget,
			}, attempt); err != nil {
				return Result{}, fmt.Errorf("validate cue %q speech evidence: %w", cue.ID, err)
			}
			attempts = append(attempts, attempt)
			if request.Speech.BatchAuthorization != nil {
				batchAFPMilli += attempt.Usage.OutputUnits
				if batchAFPMilli > request.Speech.BatchAuthorization.MaximumAFPMilli {
					return Result{}, &providercontract.Error{
						Code: providercontract.CodeBudgetExceeded, Retryable: false,
						SafeMessage:     "speech batch exceeded its cumulative AFP ceiling",
						RequiresAction:  true,
						SuggestedAction: "stop the batch and inspect the immutable usage evidence",
					}
				}
			} else if request.Speech.IdentityVersion == SpeechIdentityV2 {
				return Result{}, &providercontract.Error{
					Code: providercontract.CodeConflict, Retryable: false,
					SafeMessage:     "speech canary completed and remaining cues are not authorized",
					RequiresAction:  true,
					SuggestedAction: "inspect canary evidence before authorizing additional speech jobs",
				}
			}
		}
	}
	rendered, err := s.Media.Render(ctx, request, subtitleBytes, attempts)
	if err != nil {
		return Result{}, err
	}
	components, err := buildComponents(request, rendered)
	if err != nil {
		return Result{}, err
	}
	serviceBOMPayload := map[string]any{
		"schemaVersion":     SchemaVersion,
		"episodeRevisionId": request.EpisodeRevisionID,
		"evidence":          request.Evidence,
		"components":        components,
	}
	serviceBOMBytes, err := canonicalJSON(serviceBOMPayload)
	if err != nil {
		return Result{}, fmt.Errorf("encode service BOM: %w", err)
	}
	serviceBOM, err := s.commitBytes(ctx, serviceBOMBytes, Artifact{
		Kind:      "service_bom",
		MediaType: "application/vnd.video-series.service-bom+json",
	})
	if err != nil {
		return Result{}, err
	}

	manifestPayload := map[string]any{
		"schemaVersion":       SchemaVersion,
		"evidence":            request.Evidence,
		"episodeRevisionId":   request.EpisodeRevisionID,
		"episodeRevisionHash": request.EpisodeRevisionHash,
		"runIds":              request.RunIDs,
		"clips":               request.Clips,
		"subtitleRevision":    request.Subtitle,
		"speechConfig":        request.Speech,
		"speechAttempts":      attempts,
		"outputPolicy":        request.Output.withDefaults(),
		"outputs": []Artifact{
			subtitleArtifact, rendered.Dialogue, rendered.FinalVideo, serviceBOM,
		},
		"gates":           request.Gates,
		"qc":              rendered.QC,
		"commandPlanHash": rendered.CommandPlanHash,
		"aiContentMarker": true,
		"traceId":         request.TraceID,
	}
	manifestBytes, err := canonicalJSON(manifestPayload)
	if err != nil {
		return Result{}, fmt.Errorf("encode post-production manifest: %w", err)
	}
	manifest, err := s.commitBytes(ctx, manifestBytes, Artifact{
		Kind:      "postproduction_manifest",
		MediaType: "application/vnd.video-series.postproduction-manifest+json",
	})
	if err != nil {
		return Result{}, err
	}
	result := Result{
		SchemaVersion:     SchemaVersion,
		Evidence:          request.Evidence,
		EpisodeRevisionID: request.EpisodeRevisionID,
		Subtitle:          subtitleArtifact,
		Dialogue:          rendered.Dialogue,
		FinalVideo:        rendered.FinalVideo,
		Manifest:          manifest,
		ServiceBOM:        serviceBOM,
		SpeechAttempts:    attempts,
		QC:                rendered.QC,
		CommandPlanHash:   rendered.CommandPlanHash,
		ManifestHash:      manifest.Digest,
		ServiceBOMHash:    serviceBOM.Digest,
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) commitBytes(
	ctx context.Context,
	data []byte,
	template Artifact,
) (Artifact, error) {
	committed, err := s.Store.Put(ctx, bytes.NewReader(data))
	if err != nil {
		return Artifact{}, fmt.Errorf("commit %s to CAS: %w", template.Kind, err)
	}
	template.Digest = committed.Digest
	template.URI = committed.URI
	template.SizeBytes = committed.Size
	if err := template.Validate(); err != nil {
		return Artifact{}, err
	}
	return template, nil
}

func buildComponents(request Request, rendered RenderResult) ([]ServiceComponent, error) {
	speechHash, err := digestJSON(request.Speech)
	if err != nil {
		return nil, err
	}
	ffmpegHash, err := digestJSON(map[string]any{
		"version":         rendered.FFmpegVersion,
		"commandPlanHash": rendered.CommandPlanHash,
		"outputPolicy":    request.Output.withDefaults(),
	})
	if err != nil {
		return nil, err
	}
	ffprobeHash, err := digestJSON(map[string]any{
		"version": rendered.FFprobeVersion,
	})
	if err != nil {
		return nil, err
	}
	components := []ServiceComponent{
		{
			Name:       "speech-provider",
			Kind:       "provider",
			Provider:   request.Speech.Route.Provider,
			Model:      request.Speech.Route.ModelID,
			Version:    request.Speech.Route.RouteVersion,
			Evidence:   request.Evidence,
			ConfigHash: speechHash,
		},
		{
			Name:       "ffmpeg",
			Kind:       "media-tool",
			Version:    rendered.FFmpegVersion,
			Evidence:   "runtime_binary",
			ConfigHash: ffmpegHash,
		},
		{
			Name:       "ffprobe",
			Kind:       "media-tool",
			Version:    rendered.FFprobeVersion,
			Evidence:   "runtime_binary",
			ConfigHash: ffprobeHash,
		},
	}
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })
	return components, nil
}
