package volcengineprovider

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

// TestQASpeechInspectionUnrecoverableCASPersistsRequiresAction verifies that
// an unrecoverable immutable inspection receipt is reflected in the durable
// audit state, rather than being left indefinitely as inspection_pending.
func TestQASpeechInspectionUnrecoverableCASPersistsRequiresAction(t *testing.T) {
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	speech := &fakeSpeechSynthesizer{}
	first, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech, Inspector: fixedInspector{err: errors.New("fixture inspection failure")},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstHTTP := httptest.NewServer(first.Handler())
	request := testSpeechJobRequest(t)
	if status := submitSpeechJobStatus(t, firstHTTP.URL, request); status != http.StatusServiceUnavailable {
		firstHTTP.Close()
		t.Fatalf("first status = %d, want 503", status)
	}
	firstHTTP.Close()
	record, ok, err := first.loadRecordFromDisk(request.JobID)
	if err != nil || !ok || record.SpeechInspection == nil {
		t.Fatalf("inspection checkpoint = %#v, ok=%v err=%v", record, ok, err)
	}
	artifact, err := store.Resolve(t.Context(), record.SpeechInspection.Artifact.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(artifact.Path); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech, Inspector: fakeSpeechInspector(),
	})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	replayed := postJob(t, restartedHTTP.URL, request)
	if replayed.State != providercontract.StatusRequiresAction || len(replayed.Artifacts) != 0 || speech.callCount() != 1 {
		t.Fatalf("fail-closed replay = %#v, TTS calls = %d", replayed, speech.callCount())
	}
	persisted, ok, err := restarted.loadRecordFromDisk(request.JobID)
	if err != nil || !ok || persisted.SpeechInspection == nil {
		t.Fatalf("persisted record = %#v, ok=%v err=%v", persisted, ok, err)
	}
	if persisted.SpeechInspection.State != string(providercontract.StatusRequiresAction) {
		t.Fatalf("persisted inspection state = %q, want %q", persisted.SpeechInspection.State, providercontract.StatusRequiresAction)
	}
}
