package volcengineprovider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/google/uuid"
)

const (
	AgentPlanTTSResourceID   = "seed-tts-2.0"
	AgentPlanTTSModelID      = "doubao-seed-tts-2.0"
	AgentPlanTTSEndpoint     = runtimeconfig.AgentPlanTTSEndpoint
	AgentPlanTTSSpeakerID    = runtimeconfig.AgentPlanTTSSpeakerID
	AgentPlanTTSRouteVersion = "agent-plan-large-tts-v2"
	AgentPlanTTSMaxChars     = 600
	ttsAFPMilliPerChar       = 135
	defaultMaxSpeechBytes    = 32 << 20
)

// AgentPlanTTSCapabilityHash binds the public route identity, including the
// exact Plan endpoint and speaker. It intentionally excludes credentials.
func AgentPlanTTSCapabilityHash(config runtimeconfig.VolcengineProvider) string {
	material := strings.Join([]string{
		config.ProviderID, config.Region, AgentPlanTTSEndpoint,
		AgentPlanTTSResourceID, config.SpeechModel, config.PlanName,
		config.PricingVersion, config.SpeechSpeaker,
	}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return hex.EncodeToString(digest[:])
}

// SpeechSynthesisResult is the secret-free result of one Agent Plan TTS call.
// RequestID, ConnectID, and LogID are all retained so manifests can be audited
// without persisting the API key, prompt, or any transport URL.
type SpeechSynthesisResult struct {
	Audio       []byte
	MediaType   string
	RequestID   string
	ConnectID   string
	LogID       string
	UsageTokens int64
}

type SpeechSynthesisRequest struct {
	Text    string
	Speaker string
}

type SpeechSynthesizer interface {
	Synthesize(context.Context, SpeechSynthesisRequest) (SpeechSynthesisResult, error)
}

type AgentPlanTTSConfig struct {
	Endpoint      string
	APIKey        string
	HTTPClient    *http.Client
	MaxAudioBytes int64
	NewRequestID  func() string
	NewConnectID  func() string
}

// AgentPlanTTS implements the documented Agent Plan HTTP TTS boundary. The
// resource ID is deliberately not configurable: live speech always uses the
// prepaid seed-tts-2.0 route and the same runtime ARK_API_KEY as Agent Plan.
type AgentPlanTTS struct {
	endpoint      string
	apiKey        string
	client        *http.Client
	maxAudioBytes int64
	newRequestID  func() string
	newConnectID  func() string
}

func NewAgentPlanTTS(config AgentPlanTTSConfig) (*AgentPlanTTS, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, safeError(providercontract.CodeUnauthenticated, "ARK_API_KEY is not configured", false)
	}
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = AgentPlanTTSEndpoint
	}
	if endpoint != AgentPlanTTSEndpoint {
		return nil, safeError(
			providercontract.CodeInvalidRequest,
			"Agent Plan TTS requires the exact subscription endpoint",
			false,
		)
	}
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	maxAudioBytes := config.MaxAudioBytes
	if maxAudioBytes <= 0 {
		maxAudioBytes = defaultMaxSpeechBytes
	}
	newRequestID := config.NewRequestID
	if newRequestID == nil {
		newRequestID = uuid.NewString
	}
	newConnectID := config.NewConnectID
	if newConnectID == nil {
		newConnectID = uuid.NewString
	}
	return &AgentPlanTTS{
		endpoint: endpoint, apiKey: config.APIKey, client: client,
		maxAudioBytes: maxAudioBytes, newRequestID: newRequestID, newConnectID: newConnectID,
	}, nil
}

func (p *AgentPlanTTS) Synthesize(
	ctx context.Context,
	input SpeechSynthesisRequest,
) (SpeechSynthesisResult, error) {
	text := strings.TrimSpace(input.Text)
	chars := int64(len([]rune(text)))
	if text == "" || chars > AgentPlanTTSMaxChars {
		return SpeechSynthesisResult{}, safeError(
			providercontract.CodeInvalidRequest,
			"Agent Plan TTS text must contain between 1 and 600 Unicode characters",
			false,
		)
	}
	if strings.TrimSpace(input.Speaker) == "" {
		return SpeechSynthesisResult{}, safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS speaker is required", false)
	}
	requestID := p.newRequestID()
	connectID := p.newConnectID()
	if requestID == "" || connectID == "" || requestID == connectID {
		return SpeechSynthesisResult{}, safeError(providercontract.CodeUnavailable, "unique TTS request and connect IDs could not be allocated", false)
	}
	result := SpeechSynthesisResult{
		MediaType: "audio/mpeg", RequestID: requestID, ConnectID: connectID,
	}
	payload := map[string]any{
		"req_params": map[string]any{
			"text":    text,
			"speaker": input.Speaker,
			"audio_params": map[string]any{
				"format": "mp3", "sample_rate": 24_000,
			},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return result, safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS request could not be encoded", false)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return result, safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS request could not be created", false)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", p.apiKey)
	request.Header.Set("X-Api-Resource-Id", AgentPlanTTSResourceID)
	request.Header.Set("X-Api-Request-Id", requestID)
	request.Header.Set("X-Api-Connect-Id", connectID)
	request.Header.Set("X-Control-Require-Usage-Tokens-Return", "*")

	response, err := p.client.Do(request)
	if err != nil {
		return result, providerErrorOrGeneric(providercontract.MapContextError(err))
	}
	defer response.Body.Close()
	result.LogID = strings.TrimSpace(response.Header.Get("X-Tt-Logid"))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var errorFrame map[string]any
		decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
		decoder.UseNumber()
		_ = decoder.Decode(&errorFrame)
		providerCode := ttsProviderCode(
			response.Header.Get("X-Api-Status-Code"), errorFrame["code"],
		)
		message := response.Header.Get("X-Api-Message")
		if strings.TrimSpace(message) == "" {
			message = fmt.Sprint(errorFrame["message"])
		}
		return result, mapTTSError(
			response.StatusCode, providerCode, classifyTTSAPIMessage(message),
		)
	}

	scanner := bufio.NewScanner(io.LimitReader(response.Body, p.maxAudioBytes*2))
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	terminal := false
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var frame map[string]any
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&frame); err != nil {
			return result, safeError(providercontract.CodeUnavailable, "Agent Plan TTS returned an invalid stream frame", true)
		}
		code := int64Value(frame["code"])
		if code != 0 && code != 20_000_000 {
			return result, mapTTSError(
				response.StatusCode, fmt.Sprint(code),
				classifyTTSAPIMessage(fmt.Sprint(frame["message"])),
			)
		}
		if audio, _ := frame["data"].(string); audio != "" {
			chunk, err := base64.StdEncoding.DecodeString(audio)
			if err != nil {
				return result, safeError(providercontract.CodeUnavailable, "Agent Plan TTS returned invalid audio data", true)
			}
			if int64(len(result.Audio)+len(chunk)) > p.maxAudioBytes {
				return result, safeError(providercontract.CodeUnavailable, "Agent Plan TTS audio exceeds the configured size limit", false)
			}
			result.Audio = append(result.Audio, chunk...)
		}
		if usage := findUsageTokens(frame); usage > 0 {
			result.UsageTokens = usage
		}
		if code == 20_000_000 {
			terminal = true
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return result, safeError(providercontract.CodeUnavailable, "Agent Plan TTS stream could not be read", true)
	}
	if !terminal || len(result.Audio) == 0 || result.UsageTokens <= 0 || result.LogID == "" {
		return result, safeError(
			providercontract.CodeUnavailable,
			"Agent Plan TTS response lacks terminal audio, usage tokens, or log ID evidence",
			true,
		)
	}
	return result, nil
}

// TTSUsageAttributes records both provider-returned character tokens and the
// exact Agent Plan attribution: 1350 AFP / 10000 chars = 135 milli-AFP/char.
func TTSUsageAttributes(generatedChars int64) (providercontract.Usage, error) {
	if generatedChars <= 0 || generatedChars > AgentPlanTTSMaxChars {
		return providercontract.Usage{}, errors.New("TTS generated character usage must be between 1 and 600")
	}
	return providercontract.Usage{
		GeneratedChars: generatedChars,
		OutputUnits:    generatedChars * ttsAFPMilliPerChar,
		Unit:           "milli_afp",
	}, nil
}

func findUsageTokens(value any) int64 {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{"text_words", "usage_tokens", "generated_chars"} {
			if count := int64Value(typed[key]); count > 0 {
				return count
			}
		}
		if count := int64Value(typed["usage"]); count > 0 {
			return count
		}
		for _, child := range typed {
			if count := findUsageTokens(child); count > 0 {
				return count
			}
		}
	case []any:
		for _, child := range typed {
			if count := findUsageTokens(child); count > 0 {
				return count
			}
		}
	case string:
		if strings.HasPrefix(strings.TrimSpace(typed), "{") {
			var decoded any
			decoder := json.NewDecoder(strings.NewReader(typed))
			decoder.UseNumber()
			if decoder.Decode(&decoded) == nil {
				return findUsageTokens(decoded)
			}
		}
	}
	return 0
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	case string:
		var number json.Number = json.Number(typed)
		parsed, _ := number.Int64()
		return parsed
	default:
		return 0
	}
}

func mapTTSError(status int, providerCode, messageClass string) error {
	providerError := func(
		code providercontract.ErrorCode,
		message string,
		retryable bool,
	) error {
		return &providercontract.Error{
			Code: code, HTTPStatus: status, ProviderCode: providerCode,
			ProviderMessageClass: messageClass,
			Retryable:            retryable, SafeMessage: message,
			RequiresAction:  !retryable,
			SuggestedAction: "inspect the sanitized TTS status before authorizing another job",
		}
	}
	if status == http.StatusUnauthorized {
		return providerError(providercontract.CodeUnauthenticated, "Agent Plan TTS authentication failed", false)
	}
	if status == http.StatusForbidden {
		return providerError(providercontract.CodeForbidden, "Agent Plan TTS access is forbidden", false)
	}
	if strings.HasPrefix(providerCode, "45") {
		return providerError(providercontract.CodeInvalidRequest, "Agent Plan TTS rejected the request contract", false)
	}
	if providerCode == "55000000" {
		return providerError(providercontract.CodeModelUnavailable, "Agent Plan TTS resource or speaker is unavailable", false)
	}
	if strings.HasPrefix(providerCode, "55") {
		return providerError(providercontract.CodeUnavailable, "Agent Plan TTS returned an unclassified 55-series status", false)
	}
	if status == http.StatusTooManyRequests {
		return providerError(providercontract.CodeQuotaExceeded, "Agent Plan TTS quota is unavailable", false)
	}
	if status == http.StatusBadRequest || status == http.StatusUnprocessableEntity {
		return providerError(providercontract.CodeInvalidRequest, "Agent Plan TTS rejected the request contract", false)
	}
	return providerError(providercontract.CodeUnavailable, "Agent Plan TTS is unavailable", status >= 500 || status == 0)
}

func ttsProviderCode(header string, body any) string {
	for _, candidate := range []string{strings.TrimSpace(header), strings.TrimSpace(fmt.Sprint(body))} {
		if candidate == "" || candidate == "<nil>" || candidate == "0" {
			continue
		}
		valid := true
		for _, character := range candidate {
			if character < '0' || character > '9' {
				valid = false
				break
			}
		}
		if valid && len(candidate) <= 32 {
			return candidate
		}
	}
	return ""
}

func classifyTTSAPIMessage(message string) string {
	lower := strings.ToLower(strings.TrimSpace(message))
	if lower == "" || lower == "<nil>" {
		return "not_returned"
	}
	containsAny := func(values ...string) bool {
		for _, value := range values {
			if strings.Contains(lower, value) {
				return true
			}
		}
		return false
	}
	switch {
	case containsAny("unauth", "authentication", "token", "api key", "鉴权", "认证"):
		return "authentication"
	case containsAny("forbidden", "permission", "denied", "权限"):
		return "authorization"
	case containsAny("quota", "limit", "额度", "配额"):
		return "quota"
	case containsAny("speaker", "voice", "音色", "声音"):
		return "speaker"
	case containsAny("resource", "model", "资源", "模型"):
		return "resource_or_model"
	case containsAny("parameter", "request", "invalid", "参数", "请求"):
		return "request_contract"
	case containsAny("content", "sensitive", "risk", "文本", "内容", "敏感"):
		return "content_policy"
	default:
		return "unclassified"
	}
}
