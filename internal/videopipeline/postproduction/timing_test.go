package postproduction

import (
	"math"
	"testing"
)

func TestParseSilenceWindows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		output            string
		durationMillis    int64
		wantOnsetMillis   int64
		wantEndMillis     int64
		wantTrailingQuiet bool
	}{
		{
			name: "leading and trailing silence",
			output: "[silencedetect] silence_start: 0\n" +
				"[silencedetect] silence_end: 0.298583 | silence_duration: 0.298583\n" +
				"[silencedetect] silence_start: 3.831917\n" +
				"[silencedetect] silence_end: 4.056 | silence_duration: 0.224083\n",
			durationMillis: 4_056, wantOnsetMillis: 299, wantEndMillis: 3_832,
			wantTrailingQuiet: true,
		},
		{
			name:           "audible to boundary",
			output:         "",
			durationMillis: 600, wantOnsetMillis: 0, wantEndMillis: 600,
			wantTrailingQuiet: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			window, err := parseSilenceWindow([]byte(test.output), test.durationMillis)
			if err != nil {
				t.Fatal(err)
			}
			if window.OnsetMillis != test.wantOnsetMillis ||
				window.AudibleEndMillis != test.wantEndMillis ||
				window.HasTrailingSilence != test.wantTrailingQuiet {
				t.Fatalf("parseSilenceWindow() = %#v", window)
			}
		})
	}
}

func TestBuildAudioCorrectionFitsWholeSourceWithoutHardCut(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		cue             Cue
		sourceMillis    int64
		leadingMillis   int64
		wantTrimMillis  int64
		wantTempo       float64
		wantOutputLimit int64
	}{
		{
			name:         "short cue is accelerated after leading silence",
			cue:          Cue{ID: "cue-006", StartMillis: 26_000, EndMillis: 27_700},
			sourceMillis: 2_256, leadingMillis: 290, wantTrimMillis: 270,
			wantTempo: 1.1821428571, wantOutputLimit: 1_680,
		},
		{
			name:         "long cue is slowed while retaining the entire source",
			cue:          Cue{ID: "cue-002", StartMillis: 5_300, EndMillis: 9_700},
			sourceMillis: 4_272, leadingMillis: 276, wantTrimMillis: 256,
			wantTempo: 0.9168949772, wantOutputLimit: 4_380,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			correction, err := buildAudioCorrection(
				test.cue,
				test.sourceMillis,
				silenceWindow{OnsetMillis: test.leadingMillis, AudibleEndMillis: test.sourceMillis},
			)
			if err != nil {
				t.Fatal(err)
			}
			if correction.TrimStartMillis != test.wantTrimMillis {
				t.Fatalf("trim = %d, want %d", correction.TrimStartMillis, test.wantTrimMillis)
			}
			if math.Abs(correction.TempoRatio-test.wantTempo) > 0.000001 {
				t.Fatalf("tempo = %.10f, want %.10f", correction.TempoRatio, test.wantTempo)
			}
			if correction.CorrectedDurationMillis > test.wantOutputLimit {
				t.Fatalf("corrected duration = %d, must be <= %d", correction.CorrectedDurationMillis, test.wantOutputLimit)
			}
			if correction.EndTrimApplied {
				t.Fatal("correction must not trim the source tail")
			}
		})
	}
}

func TestMeasuredTimingQCUsesLinearP95AndRejectsHardCuts(t *testing.T) {
	t.Parallel()
	corrections := []AudioTimingCorrection{
		{CueID: "1", MeasuredOnsetMillis: 18, MeasuredBoundaryDeviationMillis: 40},
		{CueID: "2", MeasuredOnsetMillis: 20, MeasuredBoundaryDeviationMillis: 50},
		{CueID: "3", MeasuredOnsetMillis: 22, MeasuredBoundaryDeviationMillis: 60},
		{CueID: "4", MeasuredOnsetMillis: 24, MeasuredBoundaryDeviationMillis: 70},
		{CueID: "5", MeasuredOnsetMillis: 26, MeasuredBoundaryDeviationMillis: 80},
		{CueID: "6", MeasuredOnsetMillis: 28, MeasuredBoundaryDeviationMillis: 90},
	}
	qc := measuredTimingQC(50_000, EvidenceLive, corrections)
	if qc.AudioVideoStartP95Millis == nil || *qc.AudioVideoStartP95Millis != 28 {
		t.Fatalf("onset p95 = %v, want 28", qc.AudioVideoStartP95Millis)
	}
	if qc.SubtitleBoundaryP95Millis == nil || *qc.SubtitleBoundaryP95Millis != 88 {
		t.Fatalf("boundary p95 = %v, want 88", qc.SubtitleBoundaryP95Millis)
	}
	if qc.State != "TIMING_PASSED_CER_PENDING" || qc.ManualTimingRequired {
		t.Fatalf("passing measured QC = %#v", qc)
	}

	corrections[5].HardCutDetected = true
	qc = measuredTimingQC(50_000, EvidenceLive, corrections)
	if qc.State != "TIMING_FAILED" || !qc.ManualTimingRequired {
		t.Fatalf("hard-cut QC = %#v", qc)
	}
}
