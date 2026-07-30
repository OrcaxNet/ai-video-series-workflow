package providercontract

import (
	"fmt"
)

const microsPerCNY int64 = 1_000_000

type BudgetEnvelope struct {
	EstimatedCostMicros int64 `json:"estimated_cost_micros"`
	MaxCostMicros       int64 `json:"max_cost_micros"`
	MaxAttempts         int   `json:"max_attempts"`
}

type BudgetPolicy struct {
	SoftLimitMicros int64
	HardLimitMicros int64
	MaxAttempts     int
}

type BudgetDecision struct {
	ProjectedMicros int64 `json:"projected_micros"`
	SoftWarning     bool  `json:"soft_warning"`
}

func (p BudgetPolicy) Evaluate(spentMicros, reservedMicros int64, envelope BudgetEnvelope) (BudgetDecision, error) {
	if spentMicros < 0 || reservedMicros < 0 || envelope.EstimatedCostMicros < 0 ||
		envelope.MaxCostMicros < 0 {
		return BudgetDecision{}, &Error{
			Code:        CodeInvalidRequest,
			SafeMessage: "budget values must be non-negative",
		}
	}
	if p.MaxAttempts > 0 && envelope.MaxAttempts > p.MaxAttempts {
		return BudgetDecision{}, &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "request retry limit exceeds policy",
		}
	}
	projected := spentMicros + reservedMicros + envelope.MaxCostMicros
	if p.HardLimitMicros > 0 && projected > p.HardLimitMicros {
		return BudgetDecision{ProjectedMicros: projected}, &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "hard budget would be exceeded",
		}
	}
	return BudgetDecision{
		ProjectedMicros: projected,
		SoftWarning:     p.SoftLimitMicros > 0 && projected > p.SoftLimitMicros,
	}, nil
}

// CostPerMillion returns a rounded-up cost in CNY micros. Integer arithmetic
// avoids floating point drift in budget gates.
func CostPerMillion(units, cnyMicrosPerMillion int64) int64 {
	if units <= 0 || cnyMicrosPerMillion <= 0 {
		return 0
	}
	const million int64 = 1_000_000
	return (units*cnyMicrosPerMillion + million - 1) / million
}

func CostPerTenThousand(units, cnyMicrosPerTenThousand int64) int64 {
	if units <= 0 || cnyMicrosPerTenThousand <= 0 {
		return 0
	}
	const tenThousand int64 = 10_000
	return (units*cnyMicrosPerTenThousand + tenThousand - 1) / tenThousand
}

func CNY(micros int64) string {
	return fmt.Sprintf("%.6f", float64(micros)/float64(microsPerCNY))
}
