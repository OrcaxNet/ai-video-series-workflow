package volcengineprovider

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

// TestQASpeechInspectionFailureRecoversCommittedCASWithoutProviderReplay
// covers the crash/restart boundary after paid synthesis and CAS commit. The
// immutable audio must remain recoverable without a second Provider call.
func TestQASpeechInspectionFailureRecoversCommittedCASWithoutProviderReplay(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	firstSpeech := &fakeSpeechSynthesizer{}
	first, err := New(cfg, &fakeProvider{}, store, Options{
		Speech:    firstSpeech,
		Inspector: fixedInspector{err: errors.New("fixture transient ffprobe failure")},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstHTTP := httptest.NewServer(first.Handler())
	request := testSpeechJobRequest(t)
	if status := submitSpeechJobStatus(t, firstHTTP.URL, request); status != http.StatusServiceUnavailable {
		firstHTTP.Close()
		t.Fatalf("initial status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	firstHTTP.Close()
	if firstSpeech.callCount() != 1 {
		t.Fatalf("initial TTS calls = %d, want 1", firstSpeech.callCount())
	}
	digest := sha256.Sum256([]byte("adapter-speech-fixture"))
	audioSHA := hex.EncodeToString(digest[:])
	if exists, err := store.Exists(audioSHA); err != nil || !exists {
		t.Fatalf("committed CAS audio exists = %v, err = %v", exists, err)
	}

	restartedSpeech := &fakeSpeechSynthesizer{}
	restarted, err := New(cfg, &fakeProvider{}, store, Options{
		Speech:    restartedSpeech,
		Inspector: fakeSpeechInspector(),
	})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	recovered := postJob(t, restartedHTTP.URL, request)
	if recovered.State != providercontract.StatusSucceeded || len(recovered.Artifacts) != 1 ||
		recovered.Artifacts[0].SHA256 != audioSHA || recovered.Artifacts[0].DurationMillis != 2_000 {
		t.Fatalf("restart recovery = %#v, want succeeded artifact from committed CAS", recovered)
	}
	if restartedSpeech.callCount() != 0 {
		t.Fatalf("restart recovery made %d new TTS calls, want 0", restartedSpeech.callCount())
	}
}
