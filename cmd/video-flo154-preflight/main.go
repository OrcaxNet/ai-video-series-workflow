// Command video-flo154-preflight verifies one native package without any
// Provider Adapter URL, credential, client, submit, poll, or cancel operation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/nativepreflight"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr, os.LookupEnv); err != nil {
		log.Fatalf("FLO-154 native preflight failed: %v", err)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer, lookup func(string) (string, bool)) error {
	flags := flag.NewFlagSet("video-flo154-preflight", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var files nativepreflight.Files
	var output string
	flags.StringVar(&files.Plan, "plan", "", "FLO-154 readiness plan")
	flags.StringVar(&files.Package, "package", "", "FLO-154 execution package")
	flags.StringVar(&files.Product, "product", "", "FLO-154 product input")
	flags.StringVar(&files.Source, "source", "", "fixed source text")
	flags.StringVar(&files.Safety, "safety", "", "fixed safety evidence")
	flags.StringVar(&files.Visual, "visual", "", "fixed visual input")
	flags.StringVar(&files.AnalyzerRoot, "analyzer-root", "", "sealed analyzer root")
	flags.StringVar(&files.AnalyzerSeal, "analyzer-seal", "", "analyzer seal JSON")
	flags.StringVar(&files.RepoRoot, "repo-root", "", "checked-out repository root")
	flags.StringVar(&files.Build, "build", "", "materializer executable")
	flags.StringVar(&files.FixtureInput, "fixture-input", "", "reference-free offline fixture input")
	flags.StringVar(&output, "output", "", "preflight report output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || files.Plan == "" || files.Package == "" || files.Product == "" ||
		files.Source == "" || files.Safety == "" || files.Visual == "" || files.AnalyzerRoot == "" ||
		files.AnalyzerSeal == "" || files.RepoRoot == "" || files.Build == "" ||
		files.FixtureInput == "" || output == "" {
		return errors.New("all fixed FLO-154 package, analyzer, build, fixture, and output flags are required")
	}
	dsn, ok := lookup("VIDEO_POSTGRES_DSN")
	if !ok || strings.TrimSpace(dsn) == "" {
		return errors.New("VIDEO_POSTGRES_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	report, err := nativepreflight.Verify(ctx, pool, files)
	if err != nil {
		return err
	}
	return writeJSONAtomically(output, report)
}

func writeJSONAtomically(path string, value any) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".flo154-preflight-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	encoder := json.NewEncoder(temporary)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
