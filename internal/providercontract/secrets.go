package providercontract

import (
	"regexp"
	"strings"
)

const RedactionMarker = "[REDACTED]"

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9][A-Za-z0-9._-]{15,}`),
	regexp.MustCompile(`\bsk-ant-[A-Za-z0-9_-]{12,}`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`(?i)\b(?:ARK_API_KEY|ANTHROPIC_API_KEY|ANTHROPIC_AUTH_TOKEN)\s*[:=]\s*["']?[A-Za-z0-9][A-Za-z0-9._-]{15,}`),
	regexp.MustCompile(`(?i)(?:api[_-]?key|token|secret)=([A-Za-z0-9][A-Za-z0-9._-]{15,})`),
}

// Redact removes configured runtime secret values and common credential
// patterns from provider errors, logs, and traces.
func Redact(value string, runtimeSecrets ...string) string {
	redacted := value
	for _, secret := range runtimeSecrets {
		if secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, RedactionMarker)
		}
	}
	for _, pattern := range secretPatterns {
		redacted = pattern.ReplaceAllString(redacted, RedactionMarker)
	}
	return redacted
}

// ContainsPotentialSecret supports a repository/fixture secret gate. Runtime
// variable references such as "$ARK_API_KEY" are intentionally not matched.
func ContainsPotentialSecret(value string) bool {
	for _, pattern := range secretPatterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return false
}
