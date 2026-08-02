package volcengineprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

func TestAppendSpeechDurationCorrectionIsBoundImmutableAndIdempotent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	audio := []byte("immutable audio fixture")
	audioPath, audioSHA := writeCorrectionFixture(t, root, "audio.cas", audio)
	ledgerPath, ledgerSHA := writeCorrectionFixture(t, root, "ledger.json", []byte("{}\n"))
	sbomPath, sbomSHA := writeCorrectionFixture(t, root, "runtime-sbom.json", []byte(`{
  "components": [
    {"name":"ai-video-series-volcengine-provider","hashes":[{"alg":"SHA-256","content":"`+strings.Repeat("1", 64)+`"}]},
    {"name":"ai-video-series-orchestrator-worker","hashes":[{"alg":"SHA-256","content":"`+strings.Repeat("2", 64)+`"}]}
  ]
}
`))
	record := jobRecord{
		RequestHash: strings.Repeat("3", 64),
		Expected: providercontract.OutputSpec{
			DurationMillis: 3_700,
			Format:         "mp3",
		},
		Response: providercontract.JobResponse{
			JobID: "speech-job", RunID: "episode-1",
			UpstreamTaskID: "connect-id", RequestID: "request-id",
			ConnectID: "connect-id", LogID: "log-id",
			State: providercontract.StatusSucceeded,
			Artifacts: []providercontract.AssetRef{{
				ID: "speech-job-audio", Revision: audioSHA,
				Kind: providercontract.ModalityAudio, Role: providercontract.AssetRoleOutput,
				URI: "cas://sha256/" + audioSHA, SHA256: audioSHA,
				LicenseReference: "license", MediaType: "audio/mpeg",
				SizeBytes: int64(len(audio)), DurationMillis: 3_700,
			}},
		},
	}
	registryBytes, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	registryPath, registrySHA := writeCorrectionFixture(t, root, "provider-registry.json", append(registryBytes, '\n'))
	outputPath := filepath.Join(root, "corrections", "speech-job.duration.json")
	options := SpeechDurationCorrectionOptions{
		IssueID:                "43552081-0557-4875-a12a-6acb8ec2ce09",
		ProviderRegistryPath:   registryPath,
		ProviderRegistrySHA256: registrySHA,
		Stage1LedgerPath:       ledgerPath,
		Stage1LedgerSHA256:     ledgerSHA,
		AudioPath:              audioPath,
		AudioSHA256:            audioSHA,
		RuntimeSBOMPath:        sbomPath,
		RuntimeSBOMSHA256:      sbomSHA,
		FixedGitSHA:            strings.Repeat("a", 40),
		CreatedAt:              time.Unix(1_800_000_000, 123).UTC(),
		OutputPath:             outputPath,
		Inspector:              fixedInspector{spec: MediaSpec{DurationMillis: 4_056, Format: "mp3"}},
	}

	first, err := AppendSpeechDurationCorrection(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if first.SchemaVersion != SpeechDurationCorrectionSchema ||
		first.Provider.ProviderCalls != 0 ||
		first.Duration.RequestedMillis != 3_700 ||
		first.Duration.PreviouslyRecordedMS != 3_700 ||
		first.Duration.MeasuredMillis != 4_056 ||
		first.Duration.AbsoluteDeltaMillis != 356 ||
		first.Duration.ToleranceMillis != 250 ||
		first.Duration.QCState != "requires_adjustment" ||
		first.Duration.DownstreamAuthority != "measuredMillis" ||
		first.RuntimeProvenance.InstanceBinding != "unverifiable" ||
		first.RuntimeProvenance.Classification != "model_availability_only" ||
		first.RuntimeProvenance.ListedImageSHA256["adapter"] != strings.Repeat("1", 64) ||
		first.RuntimeProvenance.ListedImageSHA256["worker"] != strings.Repeat("2", 64) {
		t.Fatalf("correction = %#v", first)
	}
	before, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendSpeechDurationCorrection(context.Background(), options); err != nil {
		t.Fatalf("exact idempotent replay: %v", err)
	}
	options.FixedGitSHA = strings.Repeat("b", 40)
	if _, err := AppendSpeechDurationCorrection(context.Background(), options); err == nil ||
		!strings.Contains(err.Error(), "conflicting correction") {
		t.Fatalf("conflicting append error = %v", err)
	}
	after, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("conflicting append changed the immutable correction")
	}
}

func writeCorrectionFixture(t *testing.T, root, name string, data []byte) (string, string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return path, hex.EncodeToString(digest[:])
}
