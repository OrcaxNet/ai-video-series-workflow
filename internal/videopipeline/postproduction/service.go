package postproduction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

var ErrPendingKey = errors.New("live post-production is pending runtime provider credentials")

type Executor interface {
	Finalize(context.Context, Request) (Result, error)
}

type Service struct {
	Speech   SpeechProvider
	Media    MediaProcessor
	Store    *artifactstore.Store
	Analyzer AudioAnalyzer
}

func NewService(
	speech SpeechProvider,
	media MediaProcessor,
	store *artifactstore.Store,
	analyzers ...AudioAnalyzer,
) (*Service, error) {
	if media == nil || store == nil {
		return nil, errors.New("media processor and artifact store are required")
	}
	if len(analyzers) > 1 {
		return nil, errors.New("at most one audio analyzer may be configured")
	}
	service := &Service{Speech: speech, Media: media, Store: store}
	if len(analyzers) == 1 {
		service.Analyzer = analyzers[0]
	}
	return service, nil
}

func (s *Service) Finalize(ctx context.Context, request Request) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	// A hybrid replacement is a second pass over already-extracted native
	// evidence. Reject a missing source hash before any paid TTS authorization.
	for _, fallback := range request.CueFallbacks {
		exists, err := s.Store.Exists(fallback.OriginalNativeMixSHA256)
		if err != nil {
			return Result{}, fmt.Errorf("verify cue %q native mix: %w", fallback.CueID, err)
		}
		if !exists {
			return Result{}, &providercontract.Error{
				Code: providercontract.CodeConflict, Retryable: false, RequiresAction: true,
				SafeMessage:     "cue fallback references a missing original native mix",
				SuggestedAction: "run native audio extraction/QC first and freeze its CAS hash",
			}
		}
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

	selectedCues := request.SpeechCueIDs()
	attempts := make([]ProviderAttempt, 0, len(selectedCues))
	if len(selectedCues) > 0 {
		if s.Speech == nil {
			return Result{}, errors.New("selected TTS cues require a configured speech provider")
		}
		completed := make(map[string]ProviderAttempt, len(request.Speech.CompletedAttempts))
		for _, attempt := range request.Speech.CompletedAttempts {
			completed[attempt.CueID] = attempt
		}
		paidCueCount := len(selectedCues) - len(completed)
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
			if _, selected := selectedCues[cue.ID]; !selected {
				continue
			}
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
	if err := validateRenderedFallbackLineage(request, rendered.NativeMixes); err != nil {
		return Result{}, err
	}
	if request.ResolvedAudioStrategy().RequiresNativeAudio() {
		if s.Analyzer == nil {
			return Result{}, &providercontract.Error{
				Code: providercontract.CodeUnavailable, Retryable: false, RequiresAction: true,
				SafeMessage:     "native audio requires the approved ASR/lip/ambience analyzer",
				SuggestedAction: "configure the frozen analyzer before opening G3",
			}
		}
		if request.AnalyzerSealSHA256 != "" {
			sealed, ok := s.Analyzer.(sealedAudioAnalyzer)
			if !ok {
				return Result{}, &providercontract.Error{
					Code: providercontract.CodeUnavailable, Retryable: false, RequiresAction: true,
					SafeMessage:     "native audio analyzer differs from the frozen execution package",
					SuggestedAction: "configure the exact sealed analyzer before opening G3",
				}
			}
			if err := sealed.VerifyAnalyzerSeal(request.AnalyzerSealSHA256); err != nil {
				return Result{}, analyzerIntegrityError()
			}
		}
		analysisRequest := AudioAnalysisRequest{
			Request: request, NativeMixes: rendered.NativeMixes,
			FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
		}
		if rendered.Dialogue.Kind != "" {
			analysisRequest.Dialogue = &rendered.Dialogue
		}
		analysis, err := s.Analyzer.Analyze(ctx, analysisRequest)
		if err != nil {
			return Result{}, fmt.Errorf("analyze native audio: %w", err)
		}
		rendered.QC, err = EvaluateAudioQuality(request, rendered, analysis)
		qcEvidenceBytes, encodeErr := canonicalJSON(map[string]any{
			"schemaVersion": AudioAnalysisSchemaVersion,
			"revision":      AudioQCRevision,
			"analysis":      analysis,
			"qc":            rendered.QC,
		})
		if encodeErr != nil {
			return Result{}, fmt.Errorf("encode audio QC evidence: %w", encodeErr)
		}
		rendered.AudioQC, encodeErr = s.commitBytes(ctx, qcEvidenceBytes, Artifact{
			Kind: "audio_qc_report", MediaType: "application/vnd.video-series.audio-qc+json",
		})
		if encodeErr != nil {
			return Result{}, encodeErr
		}
		if err != nil {
			return Result{}, &AudioQualityError{
				Report: rendered.QC, Evidence: rendered.AudioQC, Cause: err,
			}
		}
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

	outputs := []Artifact{subtitleArtifact, rendered.FinalMix, rendered.FinalVideo, serviceBOM}
	if rendered.AudioQC.Kind != "" {
		outputs = append(outputs, rendered.AudioQC)
	}
	if rendered.Dialogue.Kind != "" {
		outputs = append(outputs, rendered.Dialogue)
	}
	outputs = append(outputs, rendered.NativeMixes...)
	manifestPayload := map[string]any{
		"schemaVersion":                   SchemaVersion,
		"evidence":                        request.Evidence,
		"episodeRevisionId":               request.EpisodeRevisionID,
		"episodeRevisionHash":             request.EpisodeRevisionHash,
		"runIds":                          request.RunIDs,
		"clips":                           request.Clips,
		"backgroundAudio":                 request.BackgroundAudio,
		"audioStrategy":                   request.ResolvedAudioStrategy(),
		"cueFallbacks":                    request.CueFallbacks,
		"subtitleRevision":                request.Subtitle,
		"speechConfig":                    request.Speech,
		"speechAttempts":                  attempts,
		"outputPolicy":                    request.Output.withDefaults(),
		"outputs":                         outputs,
		"gates":                           request.Gates,
		"qc":                              rendered.QC,
		"audioTimingCorrections":          rendered.AudioTimingCorrections,
		"postProductionAlgorithmRevision": AlgorithmRevision,
		"commandPlanHash":                 rendered.CommandPlanHash,
		"aiContentMarker":                 true,
		"traceId":                         request.TraceID,
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
		NativeMixes:       rendered.NativeMixes,
		FinalMix:          rendered.FinalMix,
		AudioQC:           rendered.AudioQC,
		AudioStrategy:     request.ResolvedAudioStrategy(),
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

func validateRenderedFallbackLineage(request Request, mixes []Artifact) error {
	if len(request.CueFallbacks) == 0 {
		return nil
	}
	known := make(map[string]struct{}, len(mixes))
	for _, mix := range mixes {
		known[mix.Digest] = struct{}{}
	}
	for _, fallback := range request.CueFallbacks {
		if _, ok := known[fallback.OriginalNativeMixSHA256]; !ok {
			return &providercontract.Error{
				Code: providercontract.CodeConflict, Retryable: false, RequiresAction: true,
				SafeMessage:     "cue fallback native mix differs from the rendered Provider source",
				SuggestedAction: "freeze a new fallback revision against the current native mix hash",
			}
		}
	}
	return nil
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
			Name:       "audio-strategy",
			Kind:       "policy",
			Version:    string(request.ResolvedAudioStrategy()),
			Evidence:   request.Evidence,
			ConfigHash: rendered.CommandPlanHash,
		},
		{
			Name:       "speech-provider",
			Kind:       "provider",
			Provider:   request.Speech.Route.Provider,
			Model:      request.Speech.Route.ModelID,
			Version:    request.Speech.Route.RouteVersion,
			Evidence:   request.Evidence,
			ConfigHash: "",
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
	if !request.RequiresSpeech() {
		components = slices.DeleteFunc(components, func(component ServiceComponent) bool {
			return component.Name == "speech-provider"
		})
	} else {
		speechHash, err := digestJSON(request.Speech)
		if err != nil {
			return nil, err
		}
		for index := range components {
			if components[index].Name == "speech-provider" {
				components[index].ConfigHash = speechHash
			}
		}
	}
	if request.ResolvedAudioStrategy().RequiresNativeAudio() {
		components = append(components, ServiceComponent{
			Name: "audio-qc", Kind: "quality-gate", Version: AudioQCRevision,
			Evidence: request.Evidence, ConfigHash: rendered.QC.AnalysisHash,
		})
		for index, clip := range request.Clips {
			providerHash, err := digestJSON(clip.ProviderVideo)
			if err != nil {
				return nil, err
			}
			components = append(components, ServiceComponent{
				Name: fmt.Sprintf("provider-native-audio-%03d", index+1), Kind: "provider",
				Provider: clip.ProviderVideo.Provider, Model: clip.ProviderVideo.Model,
				Version: clip.ProviderVideo.Version, Evidence: request.Evidence,
				ConfigHash: providerHash,
			})
		}
	}
	if request.BackgroundAudio != nil {
		backgroundHash, err := digestJSON(request.BackgroundAudio)
		if err != nil {
			return nil, err
		}
		components = append(components, ServiceComponent{
			Name: "background-audio", Kind: "licensed-asset", Version: request.BackgroundAudio.Digest,
			Evidence: request.Evidence, ConfigHash: backgroundHash,
		})
	}
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })
	return components, nil
}
