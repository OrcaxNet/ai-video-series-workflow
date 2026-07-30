package providercontract

import (
	"encoding/json"
	"os"
	"testing"
)

func TestCommittedLivePlan(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../docs/flo-110/live-test-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan LiveTestPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("committed live plan is invalid: %v", err)
	}
}
