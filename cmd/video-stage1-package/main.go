// Command video-stage1-package seals or verifies a prompt-free Stage 1
// execution package. It performs no Provider call and accepts no credentials.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatalf("stage 1 package failed: %v", err)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) < 3 {
		return packageUsageError()
	}
	if args[0] == "verify-flo167" {
		if len(args) != 3 {
			return packageUsageError()
		}
		var package_ stage1.FLO167SupersessionPackage
		if err := decodeFile(args[1], &package_); err != nil {
			return fmt.Errorf("read FLO-167 supersession package: %w", err)
		}
		var projection stage1.FLO167CanonicalProjection
		if err := decodeFile(args[2], &projection); err != nil {
			return fmt.Errorf("read FLO-167 canonical projection: %w", err)
		}
		if err := package_.Validate(); err != nil {
			return err
		}
		if err := projection.Validate(); err != nil {
			return err
		}
		if projection.SupersessionPackageHash != package_.ContentHash || !reflect.DeepEqual(projection.Shots, package_.Shots) {
			return errors.New("FLO-167 package and projection bindings differ")
		}
		return encodePackage(output, projection)
	}
	var plan stage1.Plan
	if err := decodeFile(args[1], &plan); err != nil {
		return fmt.Errorf("read stage 1 plan: %w", err)
	}
	var package_ stage1.ExecutionPackage
	if err := decodeFile(args[2], &package_); err != nil {
		return fmt.Errorf("read stage 1 execution package: %w", err)
	}
	if args[0] == "seal" || args[0] == "verify" {
		if len(args) != 3 {
			return packageUsageError()
		}
		if args[0] == "seal" {
			var err error
			package_, err = stage1.SealExecutionPackage(package_)
			if err != nil {
				return err
			}
		}
		if err := package_.Validate(plan); err != nil {
			return err
		}
		return encodePackage(output, package_)
	}
	if args[0] == "verify-revision" && len(args) == 4 {
		var child stage1.ExecutionPackage
		if err := decodeFile(args[3], &child); err != nil {
			return fmt.Errorf("read stage 1 child execution package: %w", err)
		}
		if err := child.ValidateRevision(plan, package_); err != nil {
			return err
		}
		return encodePackage(output, child)
	}
	if (args[0] != "seal-retry" && args[0] != "verify-retry") || len(args) != 4 {
		return packageUsageError()
	}
	var retry stage1.ControlledRetryPackage
	if err := decodeFile(args[3], &retry); err != nil {
		return fmt.Errorf("read stage 1 controlled retry package: %w", err)
	}
	if args[0] == "seal-retry" {
		var err error
		retry, err = stage1.SealControlledRetryPackage(retry)
		if err != nil {
			return err
		}
	}
	if err := retry.Validate(plan, package_); err != nil {
		return err
	}
	return encodePackage(output, retry)
}

func encodePackage(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func packageUsageError() error {
	return errors.New("usage: video-stage1-package <seal|verify> <plan.json> <package.json> OR verify-flo167 <supersession-package.json> <projection.json> OR verify-revision <plan.json> <parent-package.json> <child-package.json> OR <seal-retry|verify-retry> <plan.json> <package.json> <retry-package.json>")
}

func decodeFile(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("file must contain exactly one JSON value")
	}
	return nil
}
