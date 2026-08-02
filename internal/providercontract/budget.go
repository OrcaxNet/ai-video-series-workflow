package providercontract

import (
	"fmt"
	"math"
	"math/bits"
)

const microsPerCNY int64 = 1_000_000

type BudgetEnvelope struct {
	EstimatedCostMicros int64 `json:"estimated_cost_micros"`
	MaxCostMicros       int64 `json:"max_cost_micros"`
	MaxAttempts         int   `json:"max_attempts"`
	// BillingMode is empty for the legacy metered-cash contract. The explicit
	// subscription value is the only contract allowed to carry a zero CNY
	// maximum; ReservedAFPMilli then becomes the paid-boundary resource limit.
	BillingMode      string `json:"billing_mode,omitempty"`
	ReservedAFPMilli int64  `json:"reserved_afp_milli,omitempty"`
}

const BillingModeSubscriptionIncludedOnly = "subscription_included_only"

// Validate rejects ambiguous zero-price envelopes. A Provider request can be
// cash-free only when it is explicitly covered by a subscription and carries
// a positive AFP reservation; metered requests retain the positive cash cap.
func (b BudgetEnvelope) Validate() error {
	if b.EstimatedCostMicros < 0 {
		return fmt.Errorf("estimated_cost_micros must be non-negative")
	}
	if b.MaxCostMicros < 0 {
		return fmt.Errorf("max_cost_micros must be non-negative")
	}
	if b.EstimatedCostMicros > b.MaxCostMicros {
		return fmt.Errorf("estimated_cost_micros must not exceed max_cost_micros")
	}
	if b.MaxAttempts < 1 {
		return fmt.Errorf("max_attempts must be positive")
	}
	if b.ReservedAFPMilli < 0 {
		return fmt.Errorf("reserved_afp_milli must be non-negative")
	}
	if b.BillingMode == BillingModeSubscriptionIncludedOnly {
		if b.EstimatedCostMicros != 0 || b.MaxCostMicros != 0 || b.ReservedAFPMilli <= 0 {
			return fmt.Errorf("subscription-included budget requires zero cash and a positive AFP reservation")
		}
		return nil
	}
	if b.BillingMode != "" {
		return fmt.Errorf("unsupported budget billing mode %q", b.BillingMode)
	}
	if b.MaxCostMicros <= 0 || b.ReservedAFPMilli != 0 {
		return fmt.Errorf("max_cost_micros must be positive for metered billing and reserved_afp_milli must be zero")
	}
	return nil
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
		envelope.MaxCostMicros < 0 || p.SoftLimitMicros < 0 || p.HardLimitMicros < 0 {
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
	projected, ok := checkedAddNonNegative(spentMicros, reservedMicros, envelope.MaxCostMicros)
	if !ok {
		return BudgetDecision{ProjectedMicros: math.MaxInt64}, &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "budget projection exceeds the supported range",
		}
	}
	if p.HardLimitMicros > 0 && projected > p.HardLimitMicros {
		return BudgetDecision{ProjectedMicros: projected}, &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "hard budget would be exceeded",
		}
	}
	return BudgetDecision{
		ProjectedMicros: projected,
		SoftWarning:     p.SoftLimitMicros > 0 && projected >= p.SoftLimitMicros,
	}, nil
}

func checkedAddNonNegative(values ...int64) (int64, bool) {
	var total int64
	for _, value := range values {
		if value < 0 || value > math.MaxInt64-total {
			return 0, false
		}
		total += value
	}
	return total, true
}

// CostPerMillion returns a rounded-up cost in CNY micros. Integer arithmetic
// avoids floating point drift in budget gates.
func CostPerMillion(units, cnyMicrosPerMillion int64) int64 {
	return roundedCost(units, cnyMicrosPerMillion, 1_000_000)
}

func CostPerTenThousand(units, cnyMicrosPerTenThousand int64) int64 {
	return roundedCost(units, cnyMicrosPerTenThousand, 10_000)
}

// roundedCost computes ceil(units*rate/divisor) using a 128-bit intermediate.
// Results that cannot fit in int64 saturate so budget gates fail closed.
func roundedCost(units, rate, divisor int64) int64 {
	if units <= 0 || rate <= 0 || divisor <= 0 {
		return 0
	}
	high, low := bits.Mul64(uint64(units), uint64(rate))
	if high >= uint64(divisor) {
		return math.MaxInt64
	}
	quotient, remainder := bits.Div64(high, low, uint64(divisor))
	if quotient > math.MaxInt64 {
		return math.MaxInt64
	}
	if remainder > 0 {
		if quotient == math.MaxInt64 {
			return math.MaxInt64
		}
		quotient++
	}
	return int64(quotient)
}

func CNY(micros int64) string {
	return fmt.Sprintf("%.6f", float64(micros)/float64(microsPerCNY))
}
