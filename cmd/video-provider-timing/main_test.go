package main

import (
	"testing"
	"time"
)

func TestAgentPlanRequestTime(t *testing.T) {
	t.Parallel()
	created := time.UnixMilli(1_785_594_163_000).UTC()
	got, err := agentPlanRequestTime(
		"021785594163974a1a935ddbf9b19ff5adfcf4dcb0854f6942351",
		created,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.UnixMilli() != 1_785_594_163_974 {
		t.Fatalf("timestamp = %s", got)
	}
}

func TestSummarizeTimingsUsesLinearPercentiles(t *testing.T) {
	t.Parallel()
	jobs := []timingJob{
		{QueueMillis: 10, GenerationMillis: 100, PollMillis: 1, EndToEndMillis: 111},
		{QueueMillis: 20, GenerationMillis: 200, PollMillis: 2, EndToEndMillis: 222},
		{QueueMillis: 30, GenerationMillis: 300, PollMillis: 3, EndToEndMillis: 333},
		{QueueMillis: 40, GenerationMillis: 400, PollMillis: 4, EndToEndMillis: 444},
		{QueueMillis: 50, GenerationMillis: 500, PollMillis: 5, EndToEndMillis: 555},
		{QueueMillis: 60, GenerationMillis: 600, PollMillis: 6, EndToEndMillis: 666},
	}
	summary := summarizeTimings(jobs)
	if summary["queueMillis"].P50Millis != 35 || summary["queueMillis"].P95Millis != 58 {
		t.Fatalf("queue summary = %#v", summary["queueMillis"])
	}
	if summary["generationMillis"].P50Millis != 350 || summary["generationMillis"].P95Millis != 575 {
		t.Fatalf("generation summary = %#v", summary["generationMillis"])
	}
}
