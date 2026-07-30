package providercontract

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
	Code          ErrorCode     `json:"code"`
	HTTPStatus    int           `json:"http_status,omitempty"`
	ProviderCode  string        `json:"provider_code,omitempty"`
	ProviderReqID string        `json:"provider_request_id,omitempty"`
	Retryable     bool          `json:"retryable"`
	RetryAfter    time.Duration `json:"retry_after,omitempty"`
	SafeMessage   string        `json:"message"`
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
	safe := "provider rejected the request"
	lowerCode := strings.ToLower(providerCode)

	switch {
	case status == http.StatusUnauthorized:
		code, safe = CodeUnauthenticated, "provider authentication failed"
	case status == http.StatusForbidden:
		code, safe = CodeForbidden, "provider authorization failed"
	case status == http.StatusTooManyRequests && strings.Contains(lowerCode, "quota"):
		code, safe = CodeQuotaExceeded, "provider quota is exhausted"
	case status == http.StatusTooManyRequests:
		code, safe, retryable = CodeRateLimited, "provider rate limit exceeded", true
	case strings.Contains(lowerCode, "sensitive") ||
		strings.Contains(lowerCode, "risk") ||
		strings.Contains(lowerCode, "moderation"):
		code, safe = CodeContentBlocked, "provider content policy blocked the request"
	case status == http.StatusNotFound && strings.Contains(lowerCode, "model"):
		code, safe = CodeModelUnavailable, "provider model is unavailable"
	case status == http.StatusNotFound:
		code, safe = CodeNotFound, "provider resource was not found"
	case status >= 500:
		code, safe, retryable = CodeUnavailable, "provider service is unavailable", true
	}

	_ = rawMessage
	return &Error{
		Code:          code,
		HTTPStatus:    status,
		ProviderCode:  providerCode,
		ProviderReqID: providerRequestID,
		Retryable:     retryable,
		SafeMessage:   safe,
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
