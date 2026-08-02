package stage1materialize

import (
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
)

func TestSpeechVoiceRevisionInputAllowsOnlyExactSingleCallPlanRoute(t *testing.T) {
	t.Parallel()
	parent := stage1.ExecutionPackage{BatchID: "flo104-sample-1", ContentHash: "parent-package-hash"}
	valid := validSpeechVoiceRevisionInput(parent)
	if err := valid.Validate(parent); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*SpeechVoiceRevisionInput)
	}{
		{name: "non Plan endpoint", mutate: func(value *SpeechVoiceRevisionInput) {
			value.Endpoint = "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
		}},
		{name: "route drift", mutate: func(value *SpeechVoiceRevisionInput) { value.RouteVersion = "agent-plan-large-tts-v1" }},
		{name: "resource drift", mutate: func(value *SpeechVoiceRevisionInput) { value.ResourceID = "unknown" }},
		{name: "legacy Mars speaker", mutate: func(value *SpeechVoiceRevisionInput) {
			value.Speaker = "zh_female_tianmeitaozi_mars_bigtts"
		}},
		{name: "cash enabled", mutate: func(value *SpeechVoiceRevisionInput) { value.MaximumNonSubscriptionCashMicros = 1 }},
		{name: "retry enabled", mutate: func(value *SpeechVoiceRevisionInput) { value.MaxAttempts = 2 }},
		{name: "unbounded AFP", mutate: func(value *SpeechVoiceRevisionInput) { value.MaximumAFPMilli = 0 }},
		{name: "not internal", mutate: func(value *SpeechVoiceRevisionInput) { value.InternalMVPOnly = false }},
		{name: "parent drift", mutate: func(value *SpeechVoiceRevisionInput) { value.ParentExecutionPackageHash = "other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := valid
			tt.mutate(&candidate)
			if err := candidate.Validate(parent); err == nil {
				t.Fatal("invalid speech VOICE revision unexpectedly passed")
			}
		})
	}
}

func validSpeechVoiceRevisionInput(parent stage1.ExecutionPackage) SpeechVoiceRevisionInput {
	return SpeechVoiceRevisionInput{
		SchemaVersion: SpeechVoiceRevisionSchemaVersion,
		BatchID:       parent.BatchID, ParentExecutionPackageHash: parent.ContentHash,
		ParentVoiceAssetVersionID: "10400000-0000-4000-8000-000000000010",
		Provider:                  "volcengine_ark", ProviderID: "volcengine-agent-plan-large",
		Region: "cn-beijing", Endpoint: runtimeconfig.AgentPlanTTSEndpoint,
		ModelID:      volcengineprovider.AgentPlanTTSModelID,
		RouteVersion: volcengineprovider.AgentPlanTTSRouteVersion,
		ResourceID:   volcengineprovider.AgentPlanTTSResourceID,
		Speaker:      volcengineprovider.AgentPlanTTSSpeakerID,
		PlanName:     "agent-plan-large", PricingVersion: "agent-plan-large-included-v1",
		AuthorizedCueID: "cue-001", MaximumAFPMilli: 2_228,
		MaximumNonSubscriptionCashMicros: 0, MaxAttempts: 1,
		LicenseID:        "agent-plan-account-entitlement-internal-mvp-v2",
		LicenseSourceURI: "https://www.volcengine.com/docs/6561/1257544",
		Territories:      []string{"CN"}, InternalMVPOnly: true,
	}
}

func TestSpeechVoiceRevisionInputMustExtendCurrentPackageVoice(t *testing.T) {
	t.Parallel()
	parent := stage1.ExecutionPackage{BatchID: "flo104-sample-1", ContentHash: "parent-package-hash"}
	parent.PostProduction.Config.SpeechVoice = &postproduction.SpeechVoiceBinding{
		AssetVersionID: "10400000-0000-4000-8000-000000000020",
	}
	input := validSpeechVoiceRevisionInput(parent)
	input.ParentVoiceAssetVersionID = "10400000-0000-4000-8000-000000000010"
	if err := input.Validate(parent); err == nil {
		t.Fatal("speech voice revision skipped the current package voice")
	}
}
