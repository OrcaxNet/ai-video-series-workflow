package volcengineprovider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/google/uuid"
)

const (
	AgentPlanTTSResourceID = "seed-tts-2.0"
	AgentPlanTTSModelID    = "doubao-seed-tts-2.0"
	AgentPlanTTSEndpoint   = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
	AgentPlanTTSMaxChars   = 600
	ttsAFPMilliPerChar     = 135
	defaultMaxSpeechBytes  = 32 << 20
)

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
	endpoint := strings.TrimSpace(config.Endpoint)
	if endpoint == "" {
		endpoint = AgentPlanTTSEndpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, safeError(providercontract.CodeInvalidRequest, "invalid Agent Plan TTS endpoint", false)
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
		return SpeechSynthesisResult{}, safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS request could not be encoded", false)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return SpeechSynthesisResult{}, safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS request could not be created", false)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", p.apiKey)
	request.Header.Set("X-Api-Resource-Id", AgentPlanTTSResourceID)
	request.Header.Set("X-Api-Request-Id", requestID)
	request.Header.Set("X-Api-Connect-Id", connectID)
	request.Header.Set("X-Control-Require-Usage-Tokens-Return", "*")

	response, err := p.client.Do(request)
	if err != nil {
		return SpeechSynthesisResult{}, providerErrorOrGeneric(providercontract.MapContextError(err))
	}
	defer response.Body.Close()
	logID := strings.TrimSpace(response.Header.Get("X-Tt-Logid"))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var errorFrame map[string]any
		decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
		decoder.UseNumber()
		_ = decoder.Decode(&errorFrame)
		return SpeechSynthesisResult{}, mapTTSError(
			response.StatusCode, fmt.Sprint(int64Value(errorFrame["code"])), logID,
		)
	}

	result := SpeechSynthesisResult{
		MediaType: "audio/mpeg", RequestID: requestID, ConnectID: connectID, LogID: logID,
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
			return SpeechSynthesisResult{}, safeError(providercontract.CodeUnavailable, "Agent Plan TTS returned an invalid stream frame", true)
		}
		code := int64Value(frame["code"])
		if code != 0 && code != 20_000_000 {
			return SpeechSynthesisResult{}, mapTTSError(response.StatusCode, fmt.Sprint(code), logID)
		}
		if audio, _ := frame["data"].(string); audio != "" {
			chunk, err := base64.StdEncoding.DecodeString(audio)
			if err != nil {
				return SpeechSynthesisResult{}, safeError(providercontract.CodeUnavailable, "Agent Plan TTS returned invalid audio data", true)
			}
			if int64(len(result.Audio)+len(chunk)) > p.maxAudioBytes {
				return SpeechSynthesisResult{}, safeError(providercontract.CodeUnavailable, "Agent Plan TTS audio exceeds the configured size limit", false)
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
		return SpeechSynthesisResult{}, safeError(providercontract.CodeUnavailable, "Agent Plan TTS stream could not be read", true)
	}
	if !terminal || len(result.Audio) == 0 || result.UsageTokens <= 0 || result.LogID == "" {
		return SpeechSynthesisResult{}, safeError(
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

func mapTTSError(status int, providerCode, _ string) error {
	if status == http.StatusUnauthorized {
		return safeError(providercontract.CodeUnauthenticated, "Agent Plan TTS authentication failed", false)
	}
	if status == http.StatusForbidden {
		return safeError(providercontract.CodeForbidden, "Agent Plan TTS access is forbidden", false)
	}
	if status == http.StatusTooManyRequests {
		return safeError(providercontract.CodeQuotaExceeded, "Agent Plan TTS quota is unavailable", false)
	}
	if status == http.StatusBadRequest || status == http.StatusUnprocessableEntity {
		return safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS rejected the request contract", false)
	}
	if strings.HasPrefix(providerCode, "45") {
		return safeError(providercontract.CodeInvalidRequest, "Agent Plan TTS rejected the request contract", false)
	}
	if strings.HasPrefix(providerCode, "55") {
		return safeError(providercontract.CodeModelUnavailable, "Agent Plan TTS resource and speaker are incompatible", false)
	}
	return safeError(providercontract.CodeUnavailable, "Agent Plan TTS is unavailable", status >= 500 || status == 0)
}
