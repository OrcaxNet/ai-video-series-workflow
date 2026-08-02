package speechcontract

import (
	"strings"
	"testing"
)

func TestBatchAuthorizationValidate(t *testing.T) {
	t.Parallel()
	valid := testBatchAuthorization()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*BatchAuthorization)
	}{
		{name: "duplicate cue", mutate: func(value *BatchAuthorization) {
			value.Cues = append(value.Cues, value.Cues[0])
			value.MaximumSubmits++
			value.EstimatedAFPMilli += value.Cues[0].EstimatedAFPMilli
			value.MaximumAFPMilli += value.Cues[0].MaximumAFPMilli
		}},
		{name: "job input mismatch", mutate: func(value *BatchAuthorization) {
			value.Cues[0].JobID = "speech-v2-" + strings.Repeat("b", 32)
		}},
		{name: "retry enabled", mutate: func(value *BatchAuthorization) {
			value.Cues[0].MaxAttempts = 2
		}},
		{name: "aggregate drift", mutate: func(value *BatchAuthorization) {
			value.MaximumAFPMilli++
		}},
		{name: "cash enabled", mutate: func(value *BatchAuthorization) {
			value.MaximumNonSubscriptionCashMicros = 1
		}},
		{name: "non-canonical expiry", mutate: func(value *BatchAuthorization) {
			value.ValidUntil = "2026-08-31T23:59:59+08:00"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			changed := valid
			changed.Cues = append([]CueAuthorization(nil), valid.Cues...)
			tt.mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("invalid authorization unexpectedly passed")
			}
		})
	}
}

func testBatchAuthorization() BatchAuthorization {
	inputHash := strings.Repeat("a", 64)
	return BatchAuthorization{
		SchemaVersion:              SchemaVersion,
		ParentExecutionPackageHash: strings.Repeat("d", 64),
		ApprovalCommentID:          "10400000-0000-4000-8000-000000000030",
		ApprovalActorID:            "10400000-0000-4000-8000-000000000031",
		ValidUntil:                 "2026-08-31T15:59:59Z",
		Provider:                   "volcengine_ark", ModelID: "doubao-seed-tts-2.0",
		RouteVersion: "agent-plan-large-tts-v2", ResourceID: "seed-tts-2.0",
		Speaker:                   "zh_female_vv_uranus_bigtts",
		VoiceAssetID:              "10400000-0000-4000-8000-00000000000f",
		ParentVoiceAssetVersionID: "10400000-0000-4000-8000-000000000010",
		VoiceAssetVersionID:       "10400000-0000-4000-8000-000000000011",
		VoiceAssetVersionHash:     strings.Repeat("b", 64),
		LicenseSnapshotID:         "10400000-0000-4000-8000-000000000012",
		LicenseSnapshotHash:       strings.Repeat("c", 64),
		MaximumSubmits:            1, EstimatedAFPMilli: 945, MaximumAFPMilli: 1040,
		Cues: []CueAuthorization{{
			CueID: "cue-006", JobID: "speech-v2-" + inputHash[:32], InputHash: inputHash,
			UnicodeCharacters: 7, EstimatedAFPMilli: 945, MaximumAFPMilli: 1040,
			MaxAttempts: 1,
		}},
	}
}
