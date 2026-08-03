package repository

import (
	"errors"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
)

func TestCreatorCapabilityFailsClosed(t *testing.T) {
	t.Parallel()
	valid := providercontract.CapabilitySnapshot{
		Alias: providercontract.CapabilityVideo,
		Capability: providercontract.Capability{
			Provider: "volcengine_ark", ModelFamily: "doubao-seedance-2.0",
			OutputModality: providercontract.ModalityVideo, Resolutions: []string{"720p"},
			AspectRatios: []string{"16:9", "9:16"}, MinDurationMillis: 4_000,
			MaxDurationMillis: 15_000, NativeFPS: []int{24},
		},
		Configured: true, Enabled: true, Mode: "live", RouteVersion: "agent-plan-large-v1",
		SnapshotHash: strings.Repeat("a", 64), SupportedInputs: []string{"text"},
		Limits: map[string]any{"billingMode": "subscription", "maximumConcurrency": float64(1)},
	}
	if err := creatorCapabilityError(valid); err != nil {
		t.Fatalf("valid capability: %v", err)
	}
	tests := []struct {
		name string
		edit func(*providercontract.CapabilitySnapshot)
		code controlplane.ErrorCode
	}{
		{name: "paygo", edit: func(value *providercontract.CapabilitySnapshot) { value.Limits["billingMode"] = "paygo" }, code: controlplane.CodeSubscriptionRouteRequired},
		{name: "positive cash", edit: func(value *providercontract.CapabilitySnapshot) { value.Limits["cashAmountMaximum"] = float64(1) }, code: controlplane.CodeCashChargeNotAllowed},
		{name: "missing text", edit: func(value *providercontract.CapabilitySnapshot) { value.SupportedInputs = []string{"image-reference"} }, code: controlplane.CodeCapability},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Limits = map[string]any{"billingMode": "subscription", "maximumConcurrency": float64(1)}
			test.edit(&candidate)
			if err := creatorCapabilityError(candidate); creatorErrorCode(err) != test.code {
				t.Fatalf("error=%v code=%s want=%s", err, creatorErrorCode(err), test.code)
			}
		})
	}
}

func creatorErrorCode(err error) controlplane.ErrorCode {
	var domain *controlplane.DomainError
	if errors.As(err, &domain) {
		return domain.Code
	}
	return ""
}
