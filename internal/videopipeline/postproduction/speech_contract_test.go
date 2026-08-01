package postproduction

import (
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

func TestValidateProviderAttemptRequiresAgentPlanTTSUsageAndTraceEvidence(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	input := SpeechRequest{
		Cue: Cue{ID: "cue-1"}, Evidence: EvidenceLive, BudgetMicros: 100,
		Config: SpeechConfig{
			Route: providercontract.ModelSnapshot{
				CapabilityAlias: string(providercontract.CapabilitySpeech),
				Provider:        "volcengine_ark", ModelID: "doubao-seed-tts-2.0",
				RouteVersion: "agent-plan-large-tts-v1", CapabilityHash: digest,
				Verification: providercontract.PendingKey,
			},
			BudgetCurrency: "CNY",
		},
	}
	zero := int64(0)
	valid := ProviderAttempt{
		CueID: "cue-1", JobID: "job-1", RequestID: "request-1", UpstreamTaskID: "connect-1",
		ConnectID: "connect-1", LogID: "log-1", Model: input.Config.Route,
		Usage: providercontract.Usage{GeneratedChars: 5, OutputUnits: 675, Unit: "milli_afp"},
		Cost: providercontract.Cost{
			EstimatedMicros: 0, ActualMicros: &zero, Currency: "CNY",
			PricingVersion: "agent-plan-large-included-v1",
		},
		Artifact: Artifact{
			Kind: "dialogue_segment", Digest: digest, URI: "cas://sha256/" + digest,
			MediaType: "audio/mpeg", SizeBytes: 10, DurationMillis: 1_000,
		},
		Evidence: EvidenceLive,
	}
	if err := validateProviderAttempt(input, valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ProviderAttempt)
	}{
		{name: "missing connect", mutate: func(value *ProviderAttempt) { value.ConnectID = "" }},
		{name: "missing log ID", mutate: func(value *ProviderAttempt) { value.LogID = "" }},
		{name: "missing provider usage", mutate: func(value *ProviderAttempt) { value.Usage.GeneratedChars = 0 }},
		{name: "wrong AFP attribution", mutate: func(value *ProviderAttempt) { value.Usage.OutputUnits++ }},
		{name: "wrong unit", mutate: func(value *ProviderAttempt) { value.Usage.Unit = "afp" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempt := valid
			tt.mutate(&attempt)
			if err := validateProviderAttempt(input, attempt); err == nil {
				t.Fatal("invalid Agent Plan TTS evidence unexpectedly passed")
			}
		})
	}
}
