package providercontract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CapabilityAlias is a stable product-level route name. Provider/model
// selection is frozen separately for every paid attempt.
type CapabilityAlias string

const (
	CapabilityText   CapabilityAlias = "text.primary"
	CapabilityImage  CapabilityAlias = "image.primary"
	CapabilityVideo  CapabilityAlias = "video.primary"
	CapabilitySpeech CapabilityAlias = "speech.primary"
)

func (c CapabilityAlias) Valid() bool {
	switch c {
	case CapabilityText, CapabilityImage, CapabilityVideo, CapabilitySpeech:
		return true
	default:
		return false
	}
}

func (c CapabilityAlias) Modality() Modality {
	switch c {
	case CapabilityText:
		return ModalityText
	case CapabilityImage:
		return ModalityImage
	case CapabilityVideo:
		return ModalityVideo
	case CapabilitySpeech:
		return ModalityAudio
	default:
		return ""
	}
}

// ModelSnapshot freezes the route actually used by an attempt. Later routing
// changes affect only new attempts.
type ModelSnapshot struct {
	CapabilityAlias string `json:"capability_alias"`
	Provider        string `json:"provider"`
	ModelID         string `json:"model_id"`
	EndpointID      string `json:"endpoint_id,omitempty"`
	RouteVersion    string `json:"route_version"`
	CapabilityHash  string `json:"capability_hash"`
	Verification    string `json:"verification"`
}

func (m ModelSnapshot) Validate(alias CapabilityAlias) error {
	if !alias.Valid() {
		return errors.New("a valid capability alias is required")
	}
	if m.CapabilityAlias != string(alias) ||
		strings.TrimSpace(m.Provider) == "" ||
		strings.TrimSpace(m.ModelID) == "" ||
		strings.TrimSpace(m.RouteVersion) == "" ||
		!validSHA256(m.CapabilityHash) {
		return errors.New("a matching immutable model snapshot is required")
	}
	if strings.TrimSpace(m.Verification) == "" {
		return errors.New("model snapshot verification evidence is required")
	}
	return nil
}

// CapabilitySnapshot is discovery evidence at one effective time. It is not a
// permanent product rule and must be refreshed before enabling live calls.
type CapabilitySnapshot struct {
	Alias           CapabilityAlias `json:"alias"`
	Capability      Capability      `json:"capability"`
	Configured      bool            `json:"configured"`
	Enabled         bool            `json:"enabled"`
	Mode            string          `json:"mode"`
	RouteVersion    string          `json:"route_version"`
	SnapshotHash    string          `json:"snapshot_hash"`
	EffectiveAt     time.Time       `json:"effective_at"`
	Limits          map[string]any  `json:"limits,omitempty"`
	SupportedInputs []string        `json:"supported_inputs,omitempty"`
}

// EstimateRequest/EstimateResponse run before budget approval. An unknown
// monetary price remains nil rather than being fabricated as zero.
type EstimateRequest struct {
	Capability CapabilityAlias `json:"capability"`
	Model      ModelSnapshot   `json:"model_snapshot"`
	Parameters map[string]any  `json:"parameters"`
	Candidates int             `json:"candidates"`
}

type EstimateResponse struct {
	EstimateID     string `json:"estimate_id"`
	UnitsMinimum   int64  `json:"units_minimum"`
	UnitsMaximum   int64  `json:"units_maximum"`
	Unit           string `json:"unit"`
	AmountMinimum  *int64 `json:"amount_minimum_micros,omitempty"`
	AmountMaximum  *int64 `json:"amount_maximum_micros,omitempty"`
	Currency       string `json:"currency,omitempty"`
	PricingVersion string `json:"pricing_version"`
	ValidUntil     string `json:"valid_until"`
}

// Estimator is an optional provider capability used before budget approval.
// Providers that cannot return a monetary estimate still return unit bounds
// and a nil amount rather than fabricating zero cost.
type Estimator interface {
	Estimate(context.Context, EstimateRequest) (EstimateResponse, error)
}

// BudgetReservation is the explicit, immutable approval attached before a
// billable submit. It complements the request's policy envelope.
type BudgetReservation struct {
	ReservationID  string `json:"reservation_id"`
	Currency       string `json:"currency"`
	AmountMicros   int64  `json:"amount_micros"`
	PricingVersion string `json:"pricing_version"`
	ConfirmedBy    string `json:"confirmed_by"`
	BindingHash    string `json:"binding_hash"`
}

func (b BudgetReservation) Validate() error {
	if strings.TrimSpace(b.ReservationID) == "" ||
		strings.TrimSpace(b.Currency) == "" ||
		b.AmountMicros < 0 ||
		strings.TrimSpace(b.PricingVersion) == "" ||
		strings.TrimSpace(b.ConfirmedBy) == "" ||
		!validSHA256(b.BindingHash) {
		return errors.New("an approved budget reservation is required")
	}
	return nil
}

// BudgetBindingInput pins an approval to one run, immutable generation input,
// model route, and estimate envelope. InputHash is the control-plane run-spec
// digest or, for the production domain runner, the PromptSnapshot content hash.
type BudgetBindingInput struct {
	RunID     string
	InputHash string
	Model     ModelSnapshot
	Budget    BudgetEnvelope
}

// BindBudgetReservation returns a copy carrying the deterministic approval
// binding. The caller persists this value as the immutable approval evidence.
func BindBudgetReservation(reservation BudgetReservation, input BudgetBindingInput) (BudgetReservation, error) {
	if err := validateBudgetBindingInput(input); err != nil {
		return BudgetReservation{}, err
	}
	if strings.TrimSpace(reservation.ReservationID) == "" ||
		strings.TrimSpace(reservation.Currency) == "" ||
		reservation.AmountMicros < 0 ||
		strings.TrimSpace(reservation.PricingVersion) == "" ||
		strings.TrimSpace(reservation.ConfirmedBy) == "" {
		return BudgetReservation{}, errors.New("complete budget approval fields are required before binding")
	}
	digest, err := budgetReservationBindingHash(reservation, input)
	if err != nil {
		return BudgetReservation{}, err
	}
	reservation.BindingHash = digest
	return reservation, nil
}

// ValidateFor fails closed unless the reservation covers the current estimate
// and its persisted binding matches this exact run/input/route/budget tuple.
func (b BudgetReservation) ValidateFor(input BudgetBindingInput) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if err := validateBudgetBindingInput(input); err != nil {
		return err
	}
	if b.AmountMicros < input.Budget.EstimatedCostMicros {
		return &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "budget reservation does not cover the current estimate",
		}
	}
	if b.AmountMicros > input.Budget.MaxCostMicros {
		return &Error{
			Code:        CodeBudgetExceeded,
			SafeMessage: "budget reservation exceeds the request maximum",
		}
	}
	expected, err := budgetReservationBindingHash(b, input)
	if err != nil {
		return err
	}
	if b.BindingHash != expected {
		return &Error{
			Code:        CodeInvalidRequest,
			SafeMessage: "budget reservation is bound to different immutable generation input",
		}
	}
	return nil
}

func validateBudgetBindingInput(input BudgetBindingInput) error {
	if strings.TrimSpace(input.RunID) == "" || !validSHA256(input.InputHash) {
		return errors.New("budget binding requires a run ID and immutable input hash")
	}
	alias := CapabilityAlias(input.Model.CapabilityAlias)
	if err := input.Model.Validate(alias); err != nil {
		return fmt.Errorf("budget binding model: %w", err)
	}
	if input.Budget.EstimatedCostMicros < 0 ||
		input.Budget.MaxCostMicros <= 0 ||
		input.Budget.EstimatedCostMicros > input.Budget.MaxCostMicros ||
		input.Budget.MaxAttempts < 1 {
		return errors.New("budget binding requires a valid estimate envelope")
	}
	return nil
}

func budgetReservationBindingHash(reservation BudgetReservation, input BudgetBindingInput) (string, error) {
	material := struct {
		SchemaVersion  string         `json:"schema_version"`
		RunID          string         `json:"run_id"`
		InputHash      string         `json:"input_hash"`
		Model          ModelSnapshot  `json:"model"`
		Budget         BudgetEnvelope `json:"budget"`
		ReservationID  string         `json:"reservation_id"`
		Currency       string         `json:"currency"`
		AmountMicros   int64          `json:"amount_micros"`
		PricingVersion string         `json:"pricing_version"`
		ConfirmedBy    string         `json:"confirmed_by"`
	}{
		SchemaVersion:  "budget-reservation-binding-v1",
		RunID:          input.RunID,
		InputHash:      input.InputHash,
		Model:          input.Model,
		Budget:         input.Budget,
		ReservationID:  reservation.ReservationID,
		Currency:       reservation.Currency,
		AmountMicros:   reservation.AmountMicros,
		PricingVersion: reservation.PricingVersion,
		ConfirmedBy:    reservation.ConfirmedBy,
	}
	data, err := json.Marshal(material)
	if err != nil {
		return "", fmt.Errorf("encode budget reservation binding: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type Cost struct {
	EstimatedMicros int64  `json:"estimated_micros"`
	ActualMicros    *int64 `json:"actual_micros,omitempty"`
	Currency        string `json:"currency"`
	PricingVersion  string `json:"pricing_version"`
	Verified        bool   `json:"verified"`
	// BillingMode distinguishes a metered charge from a request included in a
	// prepaid subscription. ProviderReported is false when the provider returns
	// usage units but no per-task monetary amount.
	BillingMode      string `json:"billing_mode,omitempty"`
	ProviderReported bool   `json:"provider_reported"`
}

// JobRequest is the secret-free control-plane-to-adapter envelope. Simulation
// is accepted only by the deterministic local Mock implementation.
type JobRequest struct {
	SchemaVersion     string            `json:"schema_version"`
	JobID             string            `json:"job_id"`
	RunID             string            `json:"run_id"`
	Capability        CapabilityAlias   `json:"capability"`
	InputHash         string            `json:"input_hash"`
	Model             ModelSnapshot     `json:"model_snapshot"`
	Request           GenerationRequest `json:"request"`
	BudgetReservation BudgetReservation `json:"budget_reservation"`
	TraceID           string            `json:"trace_id"`
	Simulation        string            `json:"simulation,omitempty"`
}

func (r JobRequest) Validate() error {
	switch {
	case r.SchemaVersion != "v1":
		return errors.New("schema_version must be v1")
	case strings.TrimSpace(r.JobID) == "" || strings.TrimSpace(r.RunID) == "" || strings.TrimSpace(r.TraceID) == "":
		return errors.New("job_id, run_id, and trace_id are required")
	case !r.Capability.Valid():
		return errors.New("a valid capability is required")
	case !validSHA256(r.InputHash):
		return errors.New("input_hash must be a lowercase SHA-256 digest")
	case r.Request.Modality != r.Capability.Modality():
		return fmt.Errorf("request modality %q does not match capability %q", r.Request.Modality, r.Capability)
	}
	if err := r.Model.Validate(r.Capability); err != nil {
		return err
	}
	if err := r.Request.Validate(); err != nil {
		return fmt.Errorf("generation request: %w", err)
	}
	if r.Request.IdempotencyKey != r.JobID {
		return errors.New("generation request idempotency_key must equal job_id")
	}
	if err := r.BudgetReservation.ValidateFor(BudgetBindingInput{
		RunID:     r.RunID,
		InputHash: r.InputHash,
		Model:     r.Model,
		Budget:    r.Request.Budget,
	}); err != nil {
		return err
	}
	return nil
}

// JobResponse is safe to persist in ProviderJob and Generation Manifest. A
// provider's temporary signed URL is consumed before an output is committed.
type JobResponse struct {
	JobID          string        `json:"job_id"`
	RunID          string        `json:"run_id"`
	UpstreamTaskID string        `json:"upstream_task_id"`
	RequestID      string        `json:"request_id"`
	ProviderRegion string        `json:"provider_region,omitempty"`
	ConnectID      string        `json:"connect_id,omitempty"`
	LogID          string        `json:"log_id,omitempty"`
	State          JobStatus     `json:"state"`
	Progress       int           `json:"progress"`
	Model          ModelSnapshot `json:"model_snapshot"`
	Artifacts      []AssetRef    `json:"artifacts"`
	Usage          Usage         `json:"usage"`
	Cost           Cost          `json:"cost"`
	Error          *Error        `json:"error,omitempty"`
}

func Terminal(state JobStatus) bool { return state.Terminal() }

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
