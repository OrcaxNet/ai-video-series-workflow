package providercontract

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter supports both RFC 9110 Retry-After forms. Invalid or stale
// values are ignored; extremely large values saturate instead of overflowing.
func ParseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds > math.MaxInt64/int64(time.Second) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(seconds) * time.Second
	}
	deadline, err := http.ParseTime(value)
	if err != nil || !deadline.After(now) {
		return 0
	}
	return deadline.Sub(now)
}

type ErrorCode string

const (
	CodeInvalidRequest    ErrorCode = "invalid_request"
	CodeUnauthenticated   ErrorCode = "unauthenticated"
	CodeForbidden         ErrorCode = "forbidden"
	CodeRateLimited       ErrorCode = "rate_limited"
	CodeQuotaExceeded     ErrorCode = "quota_exceeded"
	CodeContentBlocked    ErrorCode = "content_blocked"
	CodeModelUnavailable  ErrorCode = "model_unavailable"
	CodeRegionUnavailable ErrorCode = "region_unavailable"
	CodeTimeout           ErrorCode = "timeout"
	CodeUnavailable       ErrorCode = "provider_unavailable"
	CodeBudgetExceeded    ErrorCode = "budget_exceeded"
	CodeConflict          ErrorCode = "conflict"
	CodeNotFound          ErrorCode = "not_found"
)

// Error intentionally stores only a safe summary. Raw provider response bodies
// may contain prompts, URLs, or credentials and must not cross this boundary.
type Error struct {
	Code                 ErrorCode     `json:"code"`
	HTTPStatus           int           `json:"http_status,omitempty"`
	ProviderCode         string        `json:"provider_code,omitempty"`
	ProviderReqID        string        `json:"provider_request_id,omitempty"`
	ProviderMessageClass string        `json:"provider_message_class,omitempty"`
	Retryable            bool          `json:"retryable"`
	RetryAfter           time.Duration `json:"retry_after,omitempty"`
	SafeMessage          string        `json:"message"`
	RequiresAction       bool          `json:"requires_action,omitempty"`
	SuggestedAction      string        `json:"suggested_action,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.SafeMessage)
}

func ErrorCodeOf(err error) ErrorCode {
	var providerErr *Error
	if errors.As(err, &providerErr) {
		return providerErr.Code
	}
	return ""
}

// MapHTTPError normalizes HTTP and provider error codes. rawMessage is never
// retained or returned.
func MapHTTPError(status int, providerCode, providerRequestID, rawMessage string) *Error {
	code := CodeInvalidRequest
	retryable := false
	requiresAction := false
	suggestedAction := ""
	safe := "provider rejected the request"
	lowerCode := strings.ToLower(providerCode)

	switch {
	case status == http.StatusUnauthorized:
		code, safe, requiresAction, suggestedAction = CodeUnauthenticated, "provider authentication failed", true, "replace_or_revalidate_credential"
	case status == http.StatusForbidden:
		code, safe, requiresAction, suggestedAction = CodeForbidden, "provider authorization failed", true, "request_model_or_endpoint_access"
	case status == http.StatusTooManyRequests && strings.Contains(lowerCode, "quota"):
		code, safe, requiresAction, suggestedAction = CodeQuotaExceeded, "provider quota is exhausted", true, "increase_quota_or_select_another_route"
	case status == http.StatusTooManyRequests:
		code, safe, retryable = CodeRateLimited, "provider rate limit exceeded", true
	case strings.Contains(lowerCode, "sensitive") ||
		strings.Contains(lowerCode, "risk") ||
		strings.Contains(lowerCode, "moderation"):
		code, safe, requiresAction, suggestedAction = CodeContentBlocked, "provider content policy blocked the request", true, "revise_input_and_create_a_new_attempt"
	case status == http.StatusNotFound && strings.Contains(lowerCode, "model"):
		code, safe, requiresAction, suggestedAction = CodeModelUnavailable, "provider model is unavailable", true, "refresh_discovery_and_select_an_available_model"
	case status == http.StatusNotFound:
		code, safe = CodeNotFound, "provider resource was not found"
	case status >= 500:
		code, safe, retryable = CodeUnavailable, "provider service is unavailable", true
	}

	_ = rawMessage
	return &Error{
		Code:            code,
		HTTPStatus:      status,
		ProviderCode:    providerCode,
		ProviderReqID:   providerRequestID,
		Retryable:       retryable,
		RequiresAction:  requiresAction,
		SuggestedAction: suggestedAction,
		SafeMessage:     safe,
	}
}

func MapContextError(err error) *Error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Code: CodeTimeout, Retryable: true, SafeMessage: "provider request timed out"}
	case errors.Is(err, context.Canceled):
		return &Error{Code: CodeConflict, SafeMessage: "provider request was cancelled"}
	default:
		return &Error{Code: CodeUnavailable, Retryable: true, SafeMessage: "provider connection failed"}
	}
}
