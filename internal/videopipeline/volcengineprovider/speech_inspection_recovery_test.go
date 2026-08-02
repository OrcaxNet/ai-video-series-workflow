package volcengineprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

type failOnceSpeechInspector struct {
	mu    sync.Mutex
	calls int
	spec  MediaSpec
}

func (i *failOnceSpeechInspector) Inspect(context.Context, string) (MediaSpec, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	if i.calls == 1 {
		return MediaSpec{}, errors.New("fixture temporary ffprobe failure")
	}
	return i.spec, nil
}

func TestSpeechInspectionFailureRecoversInProcessWithoutProviderReplay(t *testing.T) {
	t.Parallel()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLiveConfig()
	cfg.SpeechModel = AgentPlanTTSModelID
	speech := &fakeSpeechSynthesizer{}
	inspector := &failOnceSpeechInspector{spec: MediaSpec{DurationMillis: 2_000, Format: "mp3"}}
	adapter, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: inspector})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()
	request := testSpeechJobRequest(t)

	if status := submitSpeechJobStatus(t, server.URL, request); status != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d, want 503", status)
	}
	httpResponse, err := authenticatedTestClient(t).Get(server.URL + "/v1/jobs/" + request.JobID)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET recovery status = %d, want 200", httpResponse.StatusCode)
	}
	var recovered providercontract.JobResponse
	if err := json.NewDecoder(httpResponse.Body).Decode(&recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.State != providercontract.StatusSucceeded || len(recovered.Artifacts) != 1 ||
		recovered.Artifacts[0].DurationMillis != 2_000 || speech.callCount() != 1 {
		t.Fatalf("recovered = %#v, TTS calls = %d", recovered, speech.callCount())
	}
	if replayed := postJob(t, server.URL, request); replayed.State != providercontract.StatusSucceeded ||
		speech.callCount() != 1 {
		t.Fatalf("POST after GET recovery = %#v, TTS calls = %d", replayed, speech.callCount())
	}
	record, ok, err := adapter.loadRecordFromDisk(request.JobID)
	if err != nil || !ok || record.SpeechInspection == nil ||
		record.SpeechInspection.State != speechInspectionSucceeded ||
		record.SpeechInspection.Artifact.SHA256 != recovered.Artifacts[0].SHA256 ||
		record.SpeechDuration == nil || record.SpeechDuration.MeasuredMillis != 2_000 {
		t.Fatalf("durable recovered record = %#v, ok=%v err=%v", record, ok, err)
	}
	for _, path := range []string{
		adapter.speechInspectionReceiptPath(request.JobID),
		adapter.speechInspectionResultPath(request.JobID),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o440 {
			t.Fatalf("%s mode = %o, want 440", filepath.Base(path), info.Mode().Perm())
		}
	}
}

func TestSpeechInspectionReceiptRecoversWhenMutableCheckpointWasNotPersisted(t *testing.T) {
	t.Parallel()
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

	// Model a crash or failed mutable-registry rename by restoring only the
	// original pre-submit intent. The immutable receipt and CAS object survive.
	record, ok, err := first.loadRecordFromDisk(request.JobID)
	if err != nil || !ok {
		t.Fatalf("load record: ok=%v err=%v", ok, err)
	}
	record.Response = first.pendingResponse(request)
	record.SpeechInspection = nil
	record.SpeechDuration = nil
	if err := first.updateRecord(request.JobID, record); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: fakeSpeechInspector()})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	recovered := postJob(t, restartedHTTP.URL, request)
	if recovered.State != providercontract.StatusSucceeded || len(recovered.Artifacts) != 1 ||
		speech.callCount() != 1 {
		t.Fatalf("receipt recovery = %#v, TTS calls = %d", recovered, speech.callCount())
	}
}

func TestSpeechInspectionReceiptPreventsProviderReplayWhenMutableRegistryIsMissing(t *testing.T) {
	t.Parallel()
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
	if err := os.Remove(first.recordPath(request.JobID)); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: fakeSpeechInspector()})
	if err != nil {
		t.Fatal(err)
	}
	restartedHTTP := httptest.NewServer(restarted.Handler())
	defer restartedHTTP.Close()
	recovered := postJob(t, restartedHTTP.URL, request)
	if recovered.State != providercontract.StatusSucceeded || len(recovered.Artifacts) != 1 ||
		speech.callCount() != 1 {
		t.Fatalf("orphan receipt recovery = %#v, TTS calls = %d", recovered, speech.callCount())
	}
}

func TestSpeechInspectionRecoveryFailsClosedForUnavailableOrNoncompliantCAS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mediaType string
		mutate    func(*testing.T, artifactstore.Artifact)
		inspector MediaInspector
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, artifact artifactstore.Artifact) {
				t.Helper()
				if err := os.Remove(artifact.Path); err != nil {
					t.Fatal(err)
				}
			},
			inspector: fakeSpeechInspector(),
		},
		{
			name: "corrupted",
			mutate: func(t *testing.T, artifact artifactstore.Artifact) {
				t.Helper()
				if err := os.Chmod(artifact.Path, 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(artifact.Path, []byte("corrupted"), 0o440); err != nil {
					t.Fatal(err)
				}
			},
			inspector: fakeSpeechInspector(),
		},
		{
			name:      "declared media type mismatch",
			mediaType: "audio/wav",
			inspector: fakeSpeechInspector(),
		},
		{
			name:      "measured format mismatch",
			inspector: fixedInspector{spec: MediaSpec{DurationMillis: 2_000, Format: "wav"}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			cfg := testLiveConfig()
			cfg.SpeechModel = AgentPlanTTSModelID
			result := SpeechSynthesisResult{
				Audio: []byte("adapter-speech-fixture"), MediaType: test.mediaType,
				RequestID: "tts-request-id", ConnectID: "tts-connect-id", LogID: "tts-log-id",
				UsageTokens: 5,
			}
			if result.MediaType == "" {
				result.MediaType = "audio/mpeg"
			}
			speech := &fakeSpeechSynthesizer{result: result}
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
			if test.mutate != nil {
				test.mutate(t, artifact)
			}

			restarted, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: test.inspector})
			if err != nil {
				t.Fatal(err)
			}
			restartedHTTP := httptest.NewServer(restarted.Handler())
			defer restartedHTTP.Close()
			replayed := postJob(t, restartedHTTP.URL, request)
			if replayed.State != providercontract.StatusRequiresAction ||
				len(replayed.Artifacts) != 0 || speech.callCount() != 1 {
				t.Fatalf("fail-closed replay = %#v, TTS calls = %d", replayed, speech.callCount())
			}
			persisted, ok, err := restarted.loadRecordFromDisk(request.JobID)
			if err != nil || !ok || persisted.SpeechInspection == nil ||
				persisted.SpeechInspection.Artifact.SHA256 != artifact.Digest {
				t.Fatalf("auditable CAS reference = %#v, ok=%v err=%v", persisted, ok, err)
			}
		})
	}
}

func TestSpeechInspectionRecoveryAcrossCompetingAdaptersIsMonotonic(t *testing.T) {
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

	good, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: fakeSpeechInspector()})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := New(cfg, &fakeProvider{}, store, Options{
		Speech: speech, Inspector: fixedInspector{err: errors.New("fixture competing failure")},
	})
	if err != nil {
		t.Fatal(err)
	}
	goodHTTP := httptest.NewServer(good.Handler())
	defer goodHTTP.Close()
	badHTTP := httptest.NewServer(bad.Handler())
	defer badHTTP.Close()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	client := authenticatedTestClient(t)
	start := make(chan struct{})
	results := make(chan providercontract.JobResponse, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for _, endpoint := range []string{goodHTTP.URL, badHTTP.URL} {
		endpoint := endpoint
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			response, err := postSpeechRecovery(client, endpoint, request.JobID, body)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- response
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	succeeded := 0
	for result := range results {
		if result.State == providercontract.StatusSucceeded {
			succeeded++
		}
	}
	if succeeded < 1 || speech.callCount() != 1 {
		t.Fatalf("successful recoveries = %d, TTS calls = %d", succeeded, speech.callCount())
	}

	third, err := New(cfg, &fakeProvider{}, store, Options{Speech: speech, Inspector: fixedInspector{err: errors.New("must not regress")}})
	if err != nil {
		t.Fatal(err)
	}
	thirdHTTP := httptest.NewServer(third.Handler())
	defer thirdHTTP.Close()
	replayed := postJob(t, thirdHTTP.URL, request)
	if replayed.State != providercontract.StatusSucceeded || len(replayed.Artifacts) != 1 ||
		speech.callCount() != 1 {
		t.Fatalf("monotonic replay = %#v, TTS calls = %d", replayed, speech.callCount())
	}
	persisted, ok, err := third.loadRecordFromDisk(request.JobID)
	if err != nil || !ok || persisted.Response.State != providercontract.StatusSucceeded {
		t.Fatalf("persisted monotonic record = %#v, ok=%v err=%v", persisted, ok, err)
	}
}

func postSpeechRecovery(
	client *http.Client,
	endpoint string,
	jobID string,
	body []byte,
) (providercontract.JobResponse, error) {
	request, err := http.NewRequest(http.MethodPost, endpoint+"/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return providercontract.JobResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", jobID)
	response, err := client.Do(request)
	if err != nil {
		return providercontract.JobResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return providercontract.JobResponse{}, errors.New("speech recovery did not return HTTP 200")
	}
	var result providercontract.JobResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return providercontract.JobResponse{}, err
	}
	return result, nil
}
