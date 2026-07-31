// Package production implements the provider-neutral content-to-generation
// mainline. It deliberately contains no provider credentials or SDK-specific
// request fields.
package production

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrValidation     = errors.New("production validation failed")
	ErrConflict       = errors.New("production conflict")
	ErrNotFound       = errors.New("production object not found")
	ErrPolicyBlocked  = errors.New("production policy blocked")
	ErrStaleReference = errors.New("production reference is stale")
)

func validationf(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, values...))
}

func conflictf(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrConflict, fmt.Sprintf(format, values...))
}

func policyf(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrPolicyBlocked, fmt.Sprintf(format, values...))
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	return data, nil
}

func contentHash(value any) (string, error) {
	data, err := canonicalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func derivedID(prefix, digest string) string {
	return prefix + "-" + digest[:24]
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') &&
			(character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func nonEmpty(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}
