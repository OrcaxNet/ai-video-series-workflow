// Command video-cer-evidence emits one immutable, Provider-free FLO-104 dual
// CER evidence artifact. It binds the evidence to the exact Dialogue bytes.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/cerevaluation"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) != 3 {
		return errors.New("usage: video-cer-evidence <input.json> <dialogue.wav> <new-evidence.json>")
	}
	inputFile, err := os.Open(args[0])
	if err != nil {
		return fmt.Errorf("open CER input: %w", err)
	}
	defer inputFile.Close()
	var input cerevaluation.Input
	decoder := json.NewDecoder(io.LimitReader(inputFile, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return fmt.Errorf("decode CER input: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("CER input must contain exactly one JSON value")
	}
	evidence, err := cerevaluation.EvaluateFile(input, args[1])
	if err != nil {
		return err
	}
	output, err := os.OpenFile(args[2], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable CER evidence: %w", err)
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(evidence); err != nil {
		_ = output.Close()
		return fmt.Errorf("write CER evidence: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("close CER evidence: %w", err)
	}
	return nil
}
