package providercontract

import (
	"strings"
	"testing"
)

func TestOutputSpecValidateAudio(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		modality Modality
		output   OutputSpec
		wantErr  string
	}{
		{
			name:     "native mix",
			modality: ModalityVideo,
			output:   OutputSpec{AudioStrategy: AudioStrategyNativePreferred, GenerateAudio: true, AudioDelivery: NativeAudioMix},
		},
		{
			name:     "hybrid",
			modality: ModalityVideo,
			output:   OutputSpec{AudioStrategy: AudioStrategyHybrid, GenerateAudio: true, AudioDelivery: NativeAudioMix},
		},
		{
			name:     "legacy tts",
			modality: ModalityVideo,
			output:   OutputSpec{AudioStrategy: AudioStrategyTTSRequired},
		},
		{
			name:     "native without provider audio",
			modality: ModalityVideo,
			output:   OutputSpec{AudioStrategy: AudioStrategyNativePreferred},
			wantErr:  "generate_audio=true",
		},
		{
			name:     "tts cannot generate native audio",
			modality: ModalityVideo,
			output:   OutputSpec{AudioStrategy: AudioStrategyTTSRequired, GenerateAudio: true},
			wantErr:  "cannot request",
		},
		{
			name:     "image rejects audio strategy",
			modality: ModalityImage,
			output:   OutputSpec{AudioStrategy: AudioStrategyNativePreferred, GenerateAudio: true},
			wantErr:  "only for video",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.output.ValidateAudio(test.modality)
			if test.wantErr == "" && err != nil {
				t.Fatalf("ValidateAudio() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("ValidateAudio() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestCapabilitySupportsOutputAudioBeforePaidSubmit(t *testing.T) {
	t.Parallel()
	output := OutputSpec{
		AudioStrategy: AudioStrategyNativePreferred,
		GenerateAudio: true,
		AudioDelivery: NativeAudioMix,
	}
	if err := (Capability{}).SupportsOutputAudio(output); err == nil {
		t.Fatal("capability without native audio support passed preflight")
	}
	capability := Capability{NativeAudioDelivery: NativeAudioMix}
	if err := capability.SupportsOutputAudio(output); err != nil {
		t.Fatalf("native-mix capability rejected: %v", err)
	}
	output.AudioDelivery = NativeAudioStems
	if err := capability.SupportsOutputAudio(output); err == nil {
		t.Fatal("native-mix capability was promoted to native stems")
	}
}

func TestWithNativeAudioDefaultPreservesFrozenLegacyChoice(t *testing.T) {
	t.Parallel()
	compiled := (OutputSpec{}).WithNativeAudioDefault()
	if compiled.AudioStrategy != AudioStrategyNativePreferred || !compiled.GenerateAudio ||
		compiled.AudioDelivery != NativeAudioMix {
		t.Fatalf("compiled default = %#v", compiled)
	}
	explicit := OutputSpec{AudioStrategy: AudioStrategyTTSRequired}
	if got := explicit.WithNativeAudioDefault(); got != explicit {
		t.Fatalf("explicit frozen strategy changed: got %#v want %#v", got, explicit)
	}
}
