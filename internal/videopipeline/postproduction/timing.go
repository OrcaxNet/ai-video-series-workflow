package postproduction

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	// AlgorithmRevision is part of the formal Stage 1 finalization identity.
	// Bump it whenever post-production bytes or QC semantics change.
	AlgorithmRevision = "native-audio-postproduction-v5"

	timingSilenceThresholdDB = -40
	timingMinimumSilenceMS   = int64(20)
	timingHeadroomMS         = int64(20)
)

var (
	silenceStartPattern = regexp.MustCompile(`silence_start:\s*([0-9]+(?:\.[0-9]+)?)`)
	silenceEndPattern   = regexp.MustCompile(`silence_end:\s*([0-9]+(?:\.[0-9]+)?)`)
)

type silenceWindow struct {
	OnsetMillis        int64
	AudibleEndMillis   int64
	HasTrailingSilence bool
}

// AudioTimingCorrection records both the immutable source measurement and the
// exact non-destructive FFmpeg transformation applied to one subtitle cue.
// EndTrimApplied is intentionally explicit: live correction must fit the whole
// decoded source into the cue window and may never hide an active-speech cut.
type AudioTimingCorrection struct {
	CueID                           string  `json:"cueId"`
	SourceDigest                    string  `json:"sourceDigest"`
	SourceDurationMillis            int64   `json:"sourceDurationMillis"`
	DetectedLeadingSilenceMillis    int64   `json:"detectedLeadingSilenceMillis"`
	TrimStartMillis                 int64   `json:"trimStartMillis"`
	TempoRatio                      float64 `json:"tempoRatio"`
	CorrectedDurationMillis         int64   `json:"correctedDurationMillis"`
	EndTrimApplied                  bool    `json:"endTrimApplied"`
	MeasuredOnsetMillis             int64   `json:"measuredOnsetMillis"`
	MeasuredAudibleEndMillis        int64   `json:"measuredAudibleEndMillis"`
	MeasuredBoundaryDeviationMillis int64   `json:"measuredBoundaryDeviationMillis"`
	HardCutDetected                 bool    `json:"hardCutDetected"`
	MeasurementMethod               string  `json:"measurementMethod"`
}

func parseSilenceWindow(output []byte, durationMillis int64) (silenceWindow, error) {
	if durationMillis <= 0 {
		return silenceWindow{}, errors.New("audio duration must be positive")
	}
	starts, err := silenceMatchesMillis(silenceStartPattern, output)
	if err != nil {
		return silenceWindow{}, err
	}
	ends, err := silenceMatchesMillis(silenceEndPattern, output)
	if err != nil {
		return silenceWindow{}, err
	}
	window := silenceWindow{AudibleEndMillis: durationMillis}
	if len(starts) > 0 && starts[0] <= 1 && len(ends) > 0 {
		window.OnsetMillis = minInt64(ends[0], durationMillis)
	}
	if len(starts) > 0 && len(ends) > 0 {
		lastStart := starts[len(starts)-1]
		lastEnd := ends[len(ends)-1]
		if lastEnd >= durationMillis-timingMinimumSilenceMS && lastStart <= durationMillis {
			window.AudibleEndMillis = lastStart
			window.HasTrailingSilence = true
		}
	}
	if window.OnsetMillis > window.AudibleEndMillis {
		return silenceWindow{}, errors.New("silence measurement has onset after audible end")
	}
	return window, nil
}

func silenceMatchesMillis(pattern *regexp.Regexp, output []byte) ([]int64, error) {
	matches := pattern.FindAllSubmatch(output, -1)
	values := make([]int64, 0, len(matches))
	for _, match := range matches {
		seconds, err := strconv.ParseFloat(string(match[1]), 64)
		if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
			return nil, fmt.Errorf("parse FFmpeg silence timestamp %q", match[1])
		}
		values = append(values, int64(math.Round(seconds*1_000)))
	}
	return values, nil
}

func buildAudioCorrection(
	cue Cue,
	sourceDurationMillis int64,
	measurement silenceWindow,
) (AudioTimingCorrection, error) {
	windowMillis := cue.EndMillis - cue.StartMillis
	if cue.ID == "" || windowMillis <= timingHeadroomMS || sourceDurationMillis <= 0 {
		return AudioTimingCorrection{}, errors.New("cue and source durations must be positive")
	}
	trimStart := measurement.OnsetMillis - timingHeadroomMS
	if trimStart < 0 {
		trimStart = 0
	}
	if trimStart >= sourceDurationMillis {
		return AudioTimingCorrection{}, errors.New("leading-silence trim consumes the audio source")
	}
	targetDuration := windowMillis - timingHeadroomMS
	tempo := float64(sourceDurationMillis-trimStart) / float64(targetDuration)
	if tempo < 0.5 || tempo > 2.0 {
		return AudioTimingCorrection{}, fmt.Errorf(
			"cue %q requires unsupported atempo ratio %.6f", cue.ID, tempo,
		)
	}
	correctedDuration := int64(math.Ceil(float64(sourceDurationMillis-trimStart) / tempo))
	return AudioTimingCorrection{
		CueID:                        cue.ID,
		SourceDurationMillis:         sourceDurationMillis,
		DetectedLeadingSilenceMillis: measurement.OnsetMillis,
		TrimStartMillis:              trimStart,
		TempoRatio:                   tempo,
		CorrectedDurationMillis:      correctedDuration,
		EndTrimApplied:               false,
		MeasurementMethod:            timingMeasurementMethod(),
	}, nil
}

func measuredTimingQC(
	durationMillis int64,
	evidence string,
	corrections []AudioTimingCorrection,
) QCReport {
	onsets := make([]int64, 0, len(corrections))
	boundaries := make([]int64, 0, len(corrections))
	failed := len(corrections) == 0
	for _, correction := range corrections {
		onsets = append(onsets, absolute(correction.MeasuredOnsetMillis))
		boundaries = append(boundaries, absolute(correction.MeasuredBoundaryDeviationMillis))
		failed = failed || correction.HardCutDetected
	}
	onsetP95 := percentileLinearMillis(onsets, 0.95)
	boundaryP95 := percentileLinearMillis(boundaries, 0.95)
	failed = failed || onsetP95 > 120 || boundaryP95 > 250
	state := "TIMING_PASSED_CER_PENDING"
	if failed {
		state = "TIMING_FAILED"
	}
	return QCReport{
		State:                     state,
		SubtitleBoundaryP95Millis: &boundaryP95,
		AudioVideoStartP95Millis:  &onsetP95,
		ActualDurationMillis:      durationMillis,
		ManualTimingRequired:      failed,
		MeasurementEvidence:       evidence,
	}
}

func percentileLinearMillis(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := float64(len(sorted)-1) * percentile
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sorted[lower]
	}
	value := float64(sorted[lower]) + (rank-float64(lower))*float64(sorted[upper]-sorted[lower])
	return int64(math.Round(value))
}

func timingMeasurementMethod() string {
	return fmt.Sprintf(
		"ffmpeg.silencedetect.noise_%ddB.min_%dms.linear_p95",
		timingSilenceThresholdDB,
		timingMinimumSilenceMS,
	)
}

func silenceDetectFilter() string {
	return fmt.Sprintf(
		"silencedetect=noise=%ddB:d=%s",
		timingSilenceThresholdDB,
		strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", float64(timingMinimumSilenceMS)/1_000), "0"), "."),
	)
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
