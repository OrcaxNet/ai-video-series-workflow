// Command video-speech-evidence-correction creates an offline, append-only
// duration/provenance correction. It has no Provider client or credential.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
)

func main() {
	var options volcengineprovider.SpeechDurationCorrectionOptions
	var createdAt string
	flag.StringVar(&options.IssueID, "issue-id", "", "Multica issue UUID")
	flag.StringVar(&options.ProviderRegistryPath, "provider-registry", "", "historical provider registry path")
	flag.StringVar(&options.ProviderRegistrySHA256, "provider-registry-sha256", "", "expected registry SHA-256")
	flag.StringVar(&options.Stage1LedgerPath, "stage1-ledger", "", "historical Stage 1 ledger path")
	flag.StringVar(&options.Stage1LedgerSHA256, "stage1-ledger-sha256", "", "expected ledger SHA-256")
	flag.StringVar(&options.AudioPath, "audio", "", "immutable audio CAS object path")
	flag.StringVar(&options.AudioSHA256, "audio-sha256", "", "expected audio SHA-256")
	flag.StringVar(&options.RuntimeSBOMPath, "runtime-sbom", "", "historical runtime SBOM path")
	flag.StringVar(&options.RuntimeSBOMSHA256, "runtime-sbom-sha256", "", "expected runtime SBOM SHA-256")
	flag.StringVar(&options.FixedGitSHA, "fixed-git-sha", "", "40-character fixed Git SHA")
	flag.StringVar(&createdAt, "created-at", "", "RFC3339 correction timestamp")
	flag.StringVar(&options.OutputPath, "output", "", "single-use correction output path")
	flag.Parse()

	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		log.Fatalf("invalid correction creation time: %v", err)
	}
	options.CreatedAt = parsed
	correction, err := volcengineprovider.AppendSpeechDurationCorrection(context.Background(), options)
	if err != nil {
		log.Fatalf("append speech evidence correction: %v", err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n", correction.SchemaVersion); err != nil {
		log.Fatalf("write result: %v", err)
	}
}
