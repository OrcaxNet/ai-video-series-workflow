package controlplane

import (
	"errors"
	"fmt"
	"net/http"
)

type ErrorCode string

const (
	CodeValidation        ErrorCode = "VALIDATION_ERROR"
	CodeAuthentication    ErrorCode = "AUTHENTICATION_REQUIRED"
	CodeForbidden         ErrorCode = "FORBIDDEN"
	CodeNotFound          ErrorCode = "NOT_FOUND"
	CodeConflict          ErrorCode = "conflict"
	CodeRevisionConflict  ErrorCode = "REVISION_CONFLICT"
	CodeGateRequired      ErrorCode = "GATE_REQUIRED"
	CodeStaleDependency   ErrorCode = "STALE_DEPENDENCY"
	CodeProfileInactive   ErrorCode = "PROFILE_INACTIVE"
	CodeLicenseBlocked    ErrorCode = "LICENSE_BLOCKED"
	CodeConsentRequired   ErrorCode = "CONSENT_REQUIRED"
	CodeBudgetExceeded    ErrorCode = "budget_exceeded"
	CodeQuotaExceeded     ErrorCode = "quota_exceeded"
	CodeContentBlocked    ErrorCode = "content_blocked"
	CodeRegionUnavailable ErrorCode = "region_unavailable"
	CodeCapability        ErrorCode = "CAPABILITY_UNAVAILABLE"
	CodeRunActive         ErrorCode = "RUN_ALREADY_ACTIVE"
	CodeRunTerminal       ErrorCode = "RUN_TERMINAL"
	CodeRecoveryActive    ErrorCode = "RECOVERY_IN_PROGRESS"
	CodeDependency        ErrorCode = "DEPENDENCY_UNAVAILABLE"
	CodeTemporal          ErrorCode = "TEMPORAL_UNAVAILABLE"
	CodeInternal          ErrorCode = "INTERNAL_ERROR"
)

// DomainError is safe to return to an API caller. Cause stays server-side.
type DomainError struct {
	Code            ErrorCode
	Status          int
	Detail          string
	Retryable       bool
	SuggestedAction string
	Cause           error
}

func (e *DomainError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return string(e.Code)
}

func (e *DomainError) Unwrap() error { return e.Cause }

func domainError(code ErrorCode, status int, detail, action string, cause error) error {
	return &DomainError{
		Code:            code,
		Status:          status,
		Detail:          detail,
		SuggestedAction: action,
		Cause:           cause,
	}
}

func validationError(detail string) error {
	return domainError(CodeValidation, http.StatusBadRequest, detail, "fix the request and retry", nil)
}

func conflictError(code ErrorCode, detail string) error {
	return domainError(code, http.StatusConflict, detail, "refresh current state and retry with a new idempotency key", nil)
}

func policyError(code ErrorCode, detail, action string) error {
	return domainError(code, http.StatusUnprocessableEntity, detail, action, nil)
}

func forbiddenError(detail string) error {
	return domainError(CodeForbidden, http.StatusForbidden, detail, "use an actor with the required role", nil)
}

func notFoundError(object, id string) error {
	return domainError(CodeNotFound, http.StatusNotFound, fmt.Sprintf("%s %q was not found", object, id), "verify the identifier and access scope", nil)
}

// NewConflictError lets persistence adapters translate database concurrency
// outcomes without exposing PostgreSQL errors to HTTP callers.
func NewConflictError(code ErrorCode, detail string) error {
	return conflictError(code, detail)
}

// NewPolicyError reports a pre-dispatch policy failure.
func NewPolicyError(code ErrorCode, detail, action string) error {
	return policyError(code, detail, action)
}

// NewNotFoundError reports a missing product object.
func NewNotFoundError(object, id string) error {
	return notFoundError(object, id)
}

// NewInternalError retains the cause for server logs while returning a stable,
// secret-free response to callers.
func NewInternalError(detail string, cause error) error {
	return domainError(CodeInternal, http.StatusInternalServerError, detail, "retry with the same idempotency key or contact an operator", cause)
}

func asDomainError(err error) *DomainError {
	var domain *DomainError
	if errors.As(err, &domain) {
		return domain
	}
	return &DomainError{
		Code:            CodeInternal,
		Status:          http.StatusInternalServerError,
		Detail:          "the control plane could not complete the request",
		SuggestedAction: "retry with the same idempotency key or contact an operator",
		Cause:           err,
	}
}
