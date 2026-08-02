// Command video-stage1-readiness validates the immutable, no-cost stage 1
// execution envelope. It has no provider client and cannot make a paid call.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: video-stage1-readiness <plan.json>")
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("open stage 1 readiness plan: %v", err)
	}
	defer file.Close()
	var plan stage1.Plan
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		log.Fatalf("decode stage 1 readiness plan: %v", err)
	}
	if err := plan.Validate(); err != nil {
		log.Fatalf("stage 1 readiness failed: %v", err)
	}
	result := map[string]any{
		"status": "ready_no_cost", "batchId": plan.BatchID,
		"videoModel": plan.VideoModel, "primaryJobs": len(plan.PrimaryShotIDs),
		"maximumNewProviderJobs":           plan.MaximumNewJobs,
		"maximumVideoTokens":               plan.MaximumVideoTokens,
		"maximumMonthlyAfpMilli":           plan.MonthlyMaximumAFPMilli,
		"maximumNonSubscriptionCashMicros": plan.MaximumCashMicros,
		"maximumDialogueCharacters":        plan.MaximumDialogueCharacters,
		"maximumTtsAfpMilli":               plan.MaximumTTSAFPMilli,
		"providerCalls":                    0,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("encode stage 1 readiness result: %v", err)
	}
	fmt.Println(string(encoded))
}
