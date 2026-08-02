package providercontract

import (
	"errors"
	"fmt"
)

// AudioStrategy is frozen with each shot/episode revision. The empty value is
// retained only for pre-FLO-154 snapshots: GenerateAudio=false means the
// legacy TTS-required path, while GenerateAudio=true means native-preferred.
type AudioStrategy string

const (
	AudioStrategyNativePreferred AudioStrategy = "native_preferred"
	AudioStrategyTTSRequired     AudioStrategy = "tts_required"
	AudioStrategyHybrid          AudioStrategy = "hybrid"
)

func (s AudioStrategy) Valid() bool {
	switch s {
	case AudioStrategyNativePreferred, AudioStrategyTTSRequired, AudioStrategyHybrid:
		return true
	default:
		return false
	}
}

func (s AudioStrategy) RequiresNativeAudio() bool {
	return s == AudioStrategyNativePreferred || s == AudioStrategyHybrid
}

// NativeAudioDelivery describes what the Provider really returned. A mixed
// track is never promoted to an independently editable dialogue stem.
type NativeAudioDelivery string

const (
	NativeAudioNone  NativeAudioDelivery = "none"
	NativeAudioMix   NativeAudioDelivery = "native_mix"
	NativeAudioStems NativeAudioDelivery = "native_stems"
)

func (d NativeAudioDelivery) Valid() bool {
	switch d {
	case NativeAudioNone, NativeAudioMix, NativeAudioStems:
		return true
	default:
		return false
	}
}

// ResolvedAudioStrategy preserves immutable legacy requests while making the
// new explicit generateAudio path unambiguous.
func (o OutputSpec) ResolvedAudioStrategy() AudioStrategy {
	if o.AudioStrategy.Valid() {
		return o.AudioStrategy
	}
	if o.GenerateAudio {
		return AudioStrategyNativePreferred
	}
	return AudioStrategyTTSRequired
}

// WithNativeAudioDefault is applied only while compiling a new immutable video
// snapshot. It must not be applied while reading an older frozen snapshot.
func (o OutputSpec) WithNativeAudioDefault() OutputSpec {
	if o.AudioStrategy == "" {
		o.AudioStrategy = AudioStrategyNativePreferred
		o.GenerateAudio = true
		o.AudioDelivery = NativeAudioMix
	}
	return o
}

func (o OutputSpec) ValidateAudio(modality Modality) error {
	if o.AudioStrategy != "" && !o.AudioStrategy.Valid() {
		return fmt.Errorf("unsupported audio_strategy %q", o.AudioStrategy)
	}
	if o.AudioDelivery != "" && !o.AudioDelivery.Valid() {
		return fmt.Errorf("unsupported audio_delivery %q", o.AudioDelivery)
	}
	if modality != ModalityVideo {
		if o.AudioStrategy != "" || o.GenerateAudio || o.AudioDelivery != "" {
			return errors.New("audio strategy is supported only for video generation")
		}
		return nil
	}
	strategy := o.ResolvedAudioStrategy()
	if strategy.RequiresNativeAudio() && !o.GenerateAudio {
		return fmt.Errorf("%s requires generate_audio=true", strategy)
	}
	if strategy == AudioStrategyTTSRequired && o.GenerateAudio {
		return errors.New("tts_required cannot request Provider-native audio")
	}
	if o.GenerateAudio && o.AudioDelivery == NativeAudioNone {
		return errors.New("generate_audio=true cannot declare audio_delivery=none")
	}
	if !o.GenerateAudio && o.AudioDelivery != "" && o.AudioDelivery != NativeAudioNone {
		return errors.New("audio delivery requires generate_audio=true")
	}
	return nil
}

// SupportsOutputAudio is the no-cost preflight used before a paid submit.
func (c Capability) SupportsOutputAudio(output OutputSpec) error {
	if err := output.ValidateAudio(ModalityVideo); err != nil {
		return err
	}
	if !output.ResolvedAudioStrategy().RequiresNativeAudio() {
		return nil
	}
	if !c.NativeAudioDelivery.Valid() || c.NativeAudioDelivery == NativeAudioNone {
		return errors.New("frozen model capability does not support native audio")
	}
	if output.AudioDelivery == NativeAudioStems && c.NativeAudioDelivery != NativeAudioStems {
		return errors.New("frozen model capability does not return native audio stems")
	}
	return nil
}
