package providercontract

import "testing"

func TestBudgetPolicy_Evaluate(t *testing.T) {
	t.Parallel()
	policy := BudgetPolicy{
		SoftLimitMicros: 80_000_000,
		HardLimitMicros: 100_000_000,
		MaxAttempts:     2,
	}
	tests := []struct {
		name     string
		spent    int64
		reserved int64
		envelope BudgetEnvelope
		wantWarn bool
		wantErr  ErrorCode
	}{
		{
			name:     "within limits",
			spent:    10_000_000,
			reserved: 20_000_000,
			envelope: BudgetEnvelope{MaxCostMicros: 30_000_000, MaxAttempts: 2},
		},
		{
			name:     "soft warning",
			spent:    50_000_000,
			reserved: 20_000_000,
			envelope: BudgetEnvelope{MaxCostMicros: 20_000_000, MaxAttempts: 2},
			wantWarn: true,
		},
		{
			name:     "hard block",
			spent:    80_000_000,
			reserved: 10_000_000,
			envelope: BudgetEnvelope{MaxCostMicros: 20_000_000, MaxAttempts: 2},
			wantErr:  CodeBudgetExceeded,
		},
		{
			name:     "retry block",
			envelope: BudgetEnvelope{MaxCostMicros: 1, MaxAttempts: 3},
			wantErr:  CodeBudgetExceeded,
		},
		{
			name:     "negative rejected",
			spent:    -1,
			envelope: BudgetEnvelope{MaxCostMicros: 1, MaxAttempts: 1},
			wantErr:  CodeInvalidRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := policy.Evaluate(tt.spent, tt.reserved, tt.envelope)
			if ErrorCodeOf(err) != tt.wantErr {
				t.Fatalf("Evaluate() error code = %q, want %q; err=%v", ErrorCodeOf(err), tt.wantErr, err)
			}
			if got.SoftWarning != tt.wantWarn {
				t.Fatalf("Evaluate() warning = %t, want %t", got.SoftWarning, tt.wantWarn)
			}
		})
	}
}

func TestCostFunctions(t *testing.T) {
	t.Parallel()
	if got := CostPerMillion(250_000, 28_000_000); got != 7_000_000 {
		t.Fatalf("CostPerMillion() = %d, want 7000000", got)
	}
	if got := CostPerTenThousand(5_001, 5_000_000); got != 2_500_500 {
		t.Fatalf("CostPerTenThousand() = %d, want 2500500", got)
	}
	if got := CNY(2_500_500); got != "2.500500" {
		t.Fatalf("CNY() = %q, want 2.500500", got)
	}
}
