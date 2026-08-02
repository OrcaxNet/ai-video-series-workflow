package postproduction

import (
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

func TestEvaluateAudioQualityRequiresCompleteAbsoluteAudioVideoStartEvidence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mutate        func(*AudioAnalysis)
		wantError     string
		wantStartP95  int64
		wantQCFailure bool
	}{
		{
			name: "complete measurements pass",
		},
		{
			name:      "missing measurements fail closed",
			mutate:    func(analysis *AudioAnalysis) { analysis.AudioVideoStartMillis = nil },
			wantError: "audio/video start",
		},
		{
			name: "partial clip coverage fails closed",
			mutate: func(analysis *AudioAnalysis) {
				analysis.AudioVideoStartMillis = []int64{0}
			},
			wantError: "audio/video start",
		},
		{
			name: "signed offsets use absolute magnitude",
			mutate: func(analysis *AudioAnalysis) {
				analysis.AudioVideoStartMillis = []int64{-1_000, -1_000}
			},
			wantError:     "native audio quality failed",
			wantStartP95:  1_000,
			wantQCFailure: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, rendered, analysis := completeAudioQCFixture(t)
			if test.mutate != nil {
				test.mutate(&analysis)
				rehashAudioAnalysis(t, &analysis)
			}
			qc, err := EvaluateAudioQuality(request, rendered, analysis)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("EvaluateAudioQuality() error = %v", err)
				}
				if qc.AudioVideoStartP95Millis == nil || *qc.AudioVideoStartP95Millis != 0 {
					t.Fatalf("audio/video start p95 = %v, want 0", qc.AudioVideoStartP95Millis)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("EvaluateAudioQuality() error = %v, want containing %q", err, test.wantError)
			}
			if test.wantQCFailure {
				if qc.State != "AUDIO_QC_FAILED" || qc.AudioVideoStartP95Millis == nil ||
					*qc.AudioVideoStartP95Millis != test.wantStartP95 {
					t.Fatalf("failed audio/video start QC = %#v", qc)
				}
			}
		})
	}
}

func TestEvaluateAudioQualityBindsAnalysisToFinalVideo(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	analysis, err := (&fakeAudioAnalyzer{}).Analyze(t.Context(), AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	analysis.AudioVideoStartMillis = make([]int64, len(request.Clips))
	rehashAudioAnalysis(t, &analysis)

	drifted := rendered
	drifted.FinalVideo.Digest = strings.Repeat("e", 64)
	drifted.FinalVideo.URI = "cas://sha256/" + drifted.FinalVideo.Digest
	if _, err := EvaluateAudioQuality(request, drifted, analysis); err == nil ||
		!strings.Contains(err.Error(), "different source artifacts") {
		t.Fatalf("Final Video drift error = %v, want immutable source binding", err)
	}
}

func completeAudioQCFixture(t *testing.T) (Request, RenderResult, AudioAnalysis) {
	t.Helper()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := nativeAudioRequest(t)
	rendered, err := (&fakeMedia{store: store}).Render(t.Context(), request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	analysis, err := (&fakeAudioAnalyzer{}).Analyze(t.Context(), AudioAnalysisRequest{
		Request: request, NativeMixes: rendered.NativeMixes,
		FinalMix: rendered.FinalMix, FinalVideo: rendered.FinalVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	analysis.AudioVideoStartMillis = make([]int64, len(request.Clips))
	rehashAudioAnalysis(t, &analysis)
	return request, rendered, analysis
}

func rehashAudioAnalysis(t *testing.T, analysis *AudioAnalysis) {
	t.Helper()
	var err error
	analysis.ContentHash, err = digestJSON(analysis.digestInput())
	if err != nil {
		t.Fatal(err)
	}
}
