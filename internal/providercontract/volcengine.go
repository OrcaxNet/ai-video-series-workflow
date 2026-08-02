package providercontract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultVolcengineBaseURL = "https://ark.cn-beijing.volces.com/api/v3"
	maxProviderResponseBytes = 2 << 20
)

type VolcengineModels struct {
	Text  string
	Image string
	Video string
}

type VolcengineConfig struct {
	BaseURL    string
	APIKey     string
	Region     string
	Models     VolcengineModels
	HTTPClient *http.Client
	Now        func() time.Time
}

// VolcengineProvider implements the Ark wire mapping. The API key is retained
// only in memory, is never exposed by a method, and is never included in an
// error.
type VolcengineProvider struct {
	baseURL string
	apiKey  string
	region  string
	models  VolcengineModels
	client  *http.Client
	now     func() time.Time
}

func NewVolcengineProvider(config VolcengineConfig) (*VolcengineProvider, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, &Error{Code: CodeUnauthenticated, SafeMessage: "ARK_API_KEY is not configured"}
	}
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultVolcengineBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, &Error{Code: CodeInvalidRequest, SafeMessage: "invalid provider base URL"}
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Minute}
	}
	region := strings.TrimSpace(config.Region)
	if region == "" && baseURL == defaultVolcengineBaseURL {
		region = "cn-beijing"
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &VolcengineProvider{
		baseURL: baseURL,
		apiKey:  config.APIKey,
		region:  region,
		models:  config.Models,
		client:  client,
		now:     now,
	}, nil
}

// Discover returns only public-document capability evidence. Account-specific
// model IDs, quota, and availability remain pending until a live preflight.
func (p *VolcengineProvider) Discover(context.Context) ([]Capability, error) {
	return []Capability{
		{
			Provider:        "volcengine_ark",
			ModelFamily:     "doubao-seed-2.1",
			InputModalities: []Modality{ModalityText, ModalityImage, ModalityAudio, ModalityVideo},
			OutputModality:  ModalityText,
			Verification:    "official_docs_pending_key",
		},
		{
			Provider:               "volcengine_ark",
			ModelFamily:            "doubao-seedream-5.0-lite",
			InputModalities:        []Modality{ModalityText, ModalityImage},
			OutputModality:         ModalityImage,
			SupportsReferenceImage: true,
			Verification:           "official_docs_pending_key",
		},
		{
			Provider:               "volcengine_ark",
			ModelFamily:            "doubao-seedance-2.0",
			InputModalities:        []Modality{ModalityText, ModalityImage, ModalityAudio, ModalityVideo},
			OutputModality:         ModalityVideo,
			Async:                  true,
			SupportsPolling:        true,
			SupportsCallback:       true,
			SupportsCancel:         true,
			SupportsReferenceImage: true,
			SupportsLastFrame:      true,
			Resolutions:            []string{"480p", "720p", "1080p"},
			AspectRatios:           []string{"16:9", "9:16", "4:3", "3:4", "21:9"},
			MinDurationMillis:      4_000,
			MaxDurationMillis:      15_000,
			NativeFPS:              []int{24},
			Verification:           "official_docs_pending_key",
		},
	}, nil
}

func (p *VolcengineProvider) Submit(ctx context.Context, request GenerationRequest) (Job, error) {
	if err := request.Validate(); err != nil {
		return Job{}, &Error{Code: CodeInvalidRequest, SafeMessage: err.Error()}
	}
	switch request.Modality {
	case ModalityText:
		return p.submitText(ctx, request)
	case ModalityImage:
		return p.submitImage(ctx, request)
	case ModalityVideo:
		return p.submitVideo(ctx, request)
	default:
		return Job{}, &Error{
			Code:        CodeModelUnavailable,
			SafeMessage: "modality is not available through the Ark adapter",
		}
	}
}

func (p *VolcengineProvider) Poll(ctx context.Context, id string) (Job, error) {
	if strings.TrimSpace(id) == "" {
		return Job{}, &Error{Code: CodeInvalidRequest, SafeMessage: "job id is required"}
	}
	var response volcVideoTask
	requestID, err := p.do(ctx, http.MethodGet, "/contents/generations/tasks/"+url.PathEscape(id), nil, &response)
	if err != nil {
		return Job{}, err
	}
	job := response.toJob(requestID)
	job.ProviderRegion = p.region
	return job, nil
}

func (p *VolcengineProvider) Cancel(ctx context.Context, id string) (Job, error) {
	if strings.TrimSpace(id) == "" {
		return Job{}, &Error{Code: CodeInvalidRequest, SafeMessage: "job id is required"}
	}
	var response volcVideoTask
	requestID, err := p.do(ctx, http.MethodDelete, "/contents/generations/tasks/"+url.PathEscape(id), nil, &response)
	if err != nil {
		return Job{}, err
	}
	job := response.toJob(requestID)
	job.ProviderRegion = p.region
	if job.ID == "" {
		job.ID = id
		job.Provider = "volcengine_ark"
		job.Status = StatusCancelled
		job.ProviderRequestID = requestID
	}
	return job, nil
}

func (p *VolcengineProvider) submitText(ctx context.Context, request GenerationRequest) (Job, error) {
	model := chooseModel(p.models.Text, request.ModelHint)
	if model == "" {
		return Job{}, missingModelError(ModalityText)
	}
	payload := map[string]any{
		"model": model,
		"input": request.Prompt,
		"store": false,
	}
	var response volcResponse
	providerRequestID, err := p.do(ctx, http.MethodPost, "/responses", payload, &response)
	if err != nil {
		return Job{}, err
	}
	now := p.now().UTC()
	return Job{
		ID:                response.ID,
		RequestID:         request.RequestID,
		IdempotencyKey:    request.IdempotencyKey,
		Status:            mapStatus(response.Status),
		Provider:          "volcengine_ark",
		ProviderModel:     firstNonEmpty(response.Model, model),
		ProviderRegion:    p.region,
		ProviderRequestID: providerRequestID,
		CreatedAt:         now,
		UpdatedAt:         now,
		Output: &Output{Usage: Usage{
			InputTokens:  response.Usage.InputTokens,
			OutputTokens: response.Usage.OutputTokens,
		}, Text: response.outputText()},
	}, nil
}

func (p *VolcengineProvider) submitImage(ctx context.Context, request GenerationRequest) (Job, error) {
	model := chooseModel(p.models.Image, request.ModelHint)
	if model == "" {
		return Job{}, missingModelError(ModalityImage)
	}
	payload := map[string]any{
		"model":                       model,
		"prompt":                      request.Prompt,
		"size":                        imageSize(request.Output),
		"sequential_image_generation": "disabled",
		"response_format":             "url",
		"watermark":                   true,
	}
	images := assetURIs(request.Assets, ModalityImage)
	if len(images) > 0 {
		payload["image"] = images
	}
	var response volcImageResponse
	providerRequestID, err := p.do(ctx, http.MethodPost, "/images/generations", payload, &response)
	if err != nil {
		return Job{}, err
	}
	now := p.now().UTC()
	output := &Output{
		Usage: Usage{
			OutputTokens:    response.Usage.OutputTokens,
			GeneratedImages: response.Usage.GeneratedImages,
		},
	}
	for index, item := range response.Data {
		output.Assets = append(output.Assets, AssetRef{
			ID:               fmt.Sprintf("%s-output-%d", request.RequestID, index+1),
			Revision:         "provider-result",
			Kind:             ModalityImage,
			Role:             AssetRoleOutput,
			URI:              item.URL,
			SHA256:           "pending_download",
			LicenseReference: "request-license-manifest",
		})
	}
	return Job{
		ID:                fmt.Sprintf("image-%s", request.RequestID),
		RequestID:         request.RequestID,
		IdempotencyKey:    request.IdempotencyKey,
		Status:            StatusSucceeded,
		Provider:          "volcengine_ark",
		ProviderModel:     firstNonEmpty(response.Model, model),
		ProviderRegion:    p.region,
		ProviderRequestID: providerRequestID,
		CreatedAt:         now,
		UpdatedAt:         now,
		Output:            output,
	}, nil
}

func (p *VolcengineProvider) submitVideo(ctx context.Context, request GenerationRequest) (Job, error) {
	model := chooseModel(p.models.Video, request.ModelHint)
	if model == "" {
		return Job{}, missingModelError(ModalityVideo)
	}
	payload := buildVolcVideoPayload(request, model)
	var response struct {
		ID string `json:"id"`
	}
	providerRequestID, err := p.doWithIdempotency(
		ctx, http.MethodPost, "/contents/generations/tasks", payload, &response,
		request.IdempotencyKey,
	)
	if err != nil {
		return Job{}, err
	}
	now := p.now().UTC()
	return Job{
		ID:                response.ID,
		RequestID:         request.RequestID,
		IdempotencyKey:    request.IdempotencyKey,
		Status:            StatusQueued,
		Provider:          "volcengine_ark",
		ProviderModel:     model,
		ProviderRegion:    p.region,
		ProviderRequestID: providerRequestID,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func buildVolcVideoPayload(request GenerationRequest, model string) map[string]any {
	content := []map[string]any{{"type": "text", "text": strings.TrimSpace(request.Prompt)}}
	for _, asset := range request.Assets {
		if asset.URI == "" {
			continue
		}
		item := map[string]any{
			"type":                      string(asset.Kind) + "_url",
			string(asset.Kind) + "_url": map[string]string{"url": asset.URI},
		}
		if asset.Role != "" {
			item["role"] = string(asset.Role)
		}
		content = append(content, item)
	}
	payload := map[string]any{
		"model":             model,
		"content":           content,
		"return_last_frame": true,
		"generate_audio":    request.Output.GenerateAudio,
	}
	if request.Output.AspectRatio != "" {
		payload["ratio"] = request.Output.AspectRatio
	}
	if resolution := videoResolution(request.Output); resolution != "" {
		payload["resolution"] = resolution
	}
	if request.Output.DurationMillis > 0 {
		payload["duration"] = (request.Output.DurationMillis + 500) / 1000
	}
	if request.CallbackURL != "" {
		payload["callback_url"] = request.CallbackURL
	}
	return payload
}

func (p *VolcengineProvider) do(
	ctx context.Context,
	method, path string,
	payload any,
	output any,
) (string, error) {
	return p.doWithIdempotency(ctx, method, path, payload, output, "")
}

func (p *VolcengineProvider) doWithIdempotency(
	ctx context.Context,
	method, path string,
	payload any,
	output any,
	idempotencyKey string,
) (string, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return "", &Error{Code: CodeInvalidRequest, SafeMessage: "provider request could not be encoded"}
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return "", &Error{Code: CodeInvalidRequest, SafeMessage: "provider request could not be created"}
	}
	request.Header.Set("Authorization", "Bearer "+p.apiKey)
	request.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(idempotencyKey) != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}

	response, err := p.client.Do(request)
	if err != nil {
		return "", MapContextError(err)
	}
	defer response.Body.Close()
	providerRequestID := firstNonEmpty(
		response.Header.Get("X-Request-Id"),
		response.Header.Get("X-Tt-Logid"),
	)
	data, readErr := io.ReadAll(io.LimitReader(response.Body, maxProviderResponseBytes))
	if readErr != nil {
		return providerRequestID, &Error{
			Code:        CodeUnavailable,
			Retryable:   true,
			SafeMessage: "provider response could not be read",
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var errorBody struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &errorBody)
		code := firstNonEmpty(errorBody.Error.Code, errorBody.Code)
		message := firstNonEmpty(errorBody.Error.Message, errorBody.Message)
		mapped := MapHTTPError(response.StatusCode, code, providerRequestID, message)
		if mapped.Retryable {
			mapped.RetryAfter = ParseRetryAfter(response.Header.Get("Retry-After"), p.now())
		}
		return providerRequestID, mapped
	}
	if output != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, output); err != nil {
			return providerRequestID, &Error{
				Code:        CodeUnavailable,
				Retryable:   true,
				SafeMessage: "provider returned an invalid response",
			}
		}
	}
	return providerRequestID, nil
}

type volcResponse struct {
	ID         string `json:"id"`
	Model      string `json:"model"`
	Status     string `json:"status"`
	OutputText string `json:"output_text"`
	Output     []struct {
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
	Usage struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

func (r volcResponse) outputText() string {
	if strings.TrimSpace(r.OutputText) != "" {
		return strings.TrimSpace(r.OutputText)
	}
	var parts []string
	for _, item := range r.Output {
		for _, block := range item.Content {
			if block.Type == "output_text" && strings.TrimSpace(block.Text) != "" {
				parts = append(parts, strings.TrimSpace(block.Text))
			}
		}
	}
	return strings.Join(parts, "\n")
}

type volcImageResponse struct {
	Model string `json:"model"`
	Data  []struct {
		URL string `json:"url"`
	} `json:"data"`
	Usage struct {
		OutputTokens    int64 `json:"output_tokens"`
		GeneratedImages int64 `json:"generated_images"`
	} `json:"usage"`
}

type volcVideoTask struct {
	ID        string `json:"id"`
	Model     string `json:"model"`
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	Content   struct {
		VideoURL     string `json:"video_url"`
		LastFrameURL string `json:"last_frame_url"`
	} `json:"content"`
	// Agent Plan exposes transport URLs at the top level, while the platform
	// endpoint historically nested them below content. Both are transient and
	// are consumed by the adapter before any durable response is produced.
	OutputURL       string        `json:"output_url"`
	LastFrameURL    string        `json:"last_frame_url"`
	Duration        flexibleInt64 `json:"duration"`
	Resolution      string        `json:"resolution"`
	Ratio           string        `json:"ratio"`
	Frames          int           `json:"frames"`
	FramesPerSecond int           `json:"framespersecond"`
	FileFormat      string        `json:"fileformat"`
	Usage           struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		TotalTokens      int64 `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// flexibleInt64 accepts the string duration returned by older Ark endpoints
// and the numeric duration returned by Agent Plan without weakening the rest
// of the response decoder.
type flexibleInt64 int64

func (value *flexibleInt64) UnmarshalJSON(data []byte) error {
	raw := strings.Trim(strings.TrimSpace(string(data)), `"`)
	if raw == "" || raw == "null" {
		*value = 0
		return nil
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || parsed < 0 {
		return errors.New("invalid non-negative integer value")
	}
	*value = flexibleInt64(parsed)
	return nil
}

func (t volcVideoTask) toJob(providerRequestID string) Job {
	created := time.Unix(t.CreatedAt, 0).UTC()
	updated := time.Unix(t.UpdatedAt, 0).UTC()
	job := Job{
		ID:                t.ID,
		Status:            mapStatus(t.Status),
		Provider:          "volcengine_ark",
		ProviderModel:     t.Model,
		ProviderRequestID: providerRequestID,
		CreatedAt:         created,
		UpdatedAt:         updated,
	}
	if t.Error != nil {
		job.Error = MapHTTPError(http.StatusBadRequest, t.Error.Code, providerRequestID, t.Error.Message)
	}
	if job.Status.Terminal() {
		durationSeconds := int64(t.Duration)
		output := &Output{
			Usage: Usage{
				InputTokens:     t.Usage.PromptTokens,
				OutputTokens:    t.Usage.CompletionTokens,
				VideoTokens:     t.Usage.TotalTokens,
				GeneratedMillis: durationSeconds * 1000,
			},
		}
		// Providers may meter work before a terminal cancellation or failure.
		// Preserve that usage even though those outcomes have no output asset.
		if job.Status != StatusSucceeded {
			job.Output = output
			return job
		}
		fps := t.FramesPerSecond
		if fps == 0 && t.Frames > 0 && durationSeconds > 0 {
			fps = int((int64(t.Frames) + durationSeconds/2) / durationSeconds)
		}
		format := strings.TrimSpace(t.FileFormat)
		if format == "" {
			format = "mp4"
		}
		output.Actual = OutputSpec{
			Resolution:     t.Resolution,
			AspectRatio:    t.Ratio,
			FPS:            fps,
			DurationMillis: int(durationSeconds * 1000),
			Format:         format,
		}
		videoURL := firstNonEmpty(t.OutputURL, t.Content.VideoURL)
		lastFrameURL := firstNonEmpty(t.LastFrameURL, t.Content.LastFrameURL)
		if videoURL != "" {
			output.Assets = append(output.Assets, AssetRef{
				ID:               t.ID + "-video",
				Revision:         "provider-result",
				Kind:             ModalityVideo,
				Role:             AssetRoleOutput,
				URI:              videoURL,
				SHA256:           "pending_download",
				LicenseReference: "request-license-manifest",
			})
		}
		if lastFrameURL != "" {
			output.Assets = append(output.Assets, AssetRef{
				ID:               t.ID + "-last-frame",
				Revision:         "provider-result",
				Kind:             ModalityImage,
				Role:             AssetRoleLastFrame,
				URI:              lastFrameURL,
				SHA256:           "pending_download",
				LicenseReference: "request-license-manifest",
			})
		}
		job.Output = output
	}
	return job
}

func mapStatus(status string) JobStatus {
	switch strings.ToLower(status) {
	case "queued", "in_queue":
		return StatusQueued
	case "running", "in_progress":
		return StatusRunning
	case "succeeded", "completed":
		return StatusSucceeded
	case "cancelled", "canceled":
		return StatusCancelled
	default:
		return StatusFailed
	}
}

// chooseModel keeps runtime configuration authoritative. A request hint is
// used only when the operator has not pinned a model for that modality.
func chooseModel(configured, hint string) string {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured)
	}
	return strings.TrimSpace(hint)
}

func missingModelError(modality Modality) error {
	return &Error{
		Code:        CodeModelUnavailable,
		SafeMessage: fmt.Sprintf("runtime model mapping is missing for %s", modality),
	}
}

func imageSize(spec OutputSpec) string {
	if spec.Width > 0 && spec.Height > 0 {
		return fmt.Sprintf("%dx%d", spec.Width, spec.Height)
	}
	return "2K"
}

func videoResolution(spec OutputSpec) string {
	switch spec.Height {
	case 480:
		return "480p"
	case 720:
		return "720p"
	case 1080:
		return "1080p"
	default:
		return ""
	}
}

func assetURIs(assets []AssetRef, kind Modality) []string {
	var values []string
	for _, asset := range assets {
		if asset.Kind == kind && asset.URI != "" {
			values = append(values, asset.URI)
		}
	}
	return values
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func IsProviderError(err error, code ErrorCode) bool {
	var providerErr *Error
	return errors.As(err, &providerErr) && providerErr.Code == code
}
