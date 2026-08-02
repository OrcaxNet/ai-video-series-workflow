package stage1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	DurationNormalizationVersion       = "duration-normalized-afp/v1"
	NonnegativeHalfUpVersion           = "half-up-nonnegative-integer/v1"
	FLO167SupersessionSchema           = "flo100.batch-a-supersession.v3"
	FLO167ReferenceAFPMilli      int64 = 2_504_700
	FLO167ReferenceDurationMS    int64 = 5_000
)

var FLO167ShotDurationsMS = map[string]int64{
	"GOLD-A01": 4_000, "GOLD-A02": 4_500, "GOLD-A03": 5_000,
	"GOLD-A04": 5_500, "GOLD-A05": 4_000, "GOLD-A06": 5_000,
	"GOLD-A07": 4_500, "GOLD-A08": 5_500, "GOLD-A09": 4_000,
	"GOLD-A10": 4_500,
}

// DurationPricingBinding is deliberately repeated on every durable paid-boundary
// record. It prevents a package-level value from being silently substituted for
// a shot-specific price after authorization.
type DurationPricingBinding struct {
	DurationMS            int64  `json:"durationMs"`
	PricingSnapshotID     string `json:"pricingSnapshotId"`
	PricingSnapshotDigest string `json:"pricingSnapshotDigest"`
	ReferenceAFPMilli     int64  `json:"referenceAfpMilli"`
	ReferenceDurationMS   int64  `json:"referenceDurationMs"`
	ExpectedAFPMilli      int64  `json:"expectedAfpMilli"`
	NormalizationVersion  string `json:"normalizationVersion"`
	RoundingVersion       string `json:"roundingVersion"`
}

func (b DurationPricingBinding) Validate() error {
	if b.DurationMS <= 0 || b.ReferenceAFPMilli < 0 || b.ReferenceDurationMS <= 0 {
		return errors.New("duration pricing values are outside the nonnegative integer domain")
	}
	if strings.TrimSpace(b.PricingSnapshotID) == "" || !validLowerDigest(b.PricingSnapshotDigest) {
		return errors.New("duration pricing snapshot identity is incomplete")
	}
	if b.NormalizationVersion != DurationNormalizationVersion || b.RoundingVersion != NonnegativeHalfUpVersion {
		return errors.New("unknown duration normalization or rounding version")
	}
	expected, err := NormalizeAFPMilli(b.ReferenceAFPMilli, b.ReferenceDurationMS, b.DurationMS)
	if err != nil {
		return err
	}
	if b.ExpectedAFPMilli != expected {
		return fmt.Errorf("expected AFP drifted: got %d, want %d", b.ExpectedAFPMilli, expected)
	}
	return nil
}

// NormalizeAFPMilli implements nonnegative integer half-up rounding without
// allowing multiplication or rounding additions to wrap.
func NormalizeAFPMilli(referenceAFPMilli, referenceDurationMS, durationMS int64) (int64, error) {
	if referenceAFPMilli < 0 || referenceDurationMS <= 0 || durationMS <= 0 {
		return 0, errors.New("normalization requires nonnegative AFP and positive durations")
	}
	numerator, err := checkedNonnegativeMul(referenceAFPMilli, durationMS)
	if err != nil {
		return 0, fmt.Errorf("normalization numerator: %w", err)
	}
	numerator, err = checkedNonnegativeAdd(numerator, referenceDurationMS/2)
	if err != nil {
		return 0, fmt.Errorf("normalization rounding: %w", err)
	}
	return numerator / referenceDurationMS, nil
}

// AFPWithinDrift returns true at the inclusive boundary. Invalid or overflowing
// comparisons return an error so callers fail closed before reservation.
func AFPWithinDrift(actual, expected int64, maximumBPS int64) (bool, error) {
	if actual < 0 || expected <= 0 || maximumBPS < 0 {
		return false, errors.New("drift comparison values are invalid")
	}
	var delta int64
	if actual >= expected {
		delta = actual - expected
	} else {
		delta = expected - actual
	}
	left, err := checkedNonnegativeMul(delta, 10_000)
	if err != nil {
		return false, fmt.Errorf("drift numerator: %w", err)
	}
	right, err := checkedNonnegativeMul(expected, maximumBPS)
	if err != nil {
		return false, fmt.Errorf("drift threshold: %w", err)
	}
	return left <= right, nil
}

type FLO167ShotBinding struct {
	ShotID             string                 `json:"shotId"`
	Pricing            DurationPricingBinding `json:"pricing"`
	RouteHash          string                 `json:"routeHash"`
	G1Hash             string                 `json:"g1Hash"`
	G2Hash             string                 `json:"g2Hash"`
	SafetyHash         string                 `json:"safetyHash"`
	CanonicalInputHash string                 `json:"canonicalInputHash"`
	SemanticInputHash  string                 `json:"semanticInputHash"`
}

type FLO167AuthorizationBinding struct {
	CompletedSet                     []string `json:"completedSet"`
	AllowedSubmitSet                 []string `json:"allowedSubmitSet"`
	MaximumPrimaryJobs               int      `json:"maximumPrimaryJobs"`
	MaximumControlledRetries         int      `json:"maximumControlledRetries"`
	MaximumVideoTokens               int64    `json:"maximumVideoTokens"`
	MaximumVideoAFPMilli             int64    `json:"maximumVideoAfpMilli"`
	MaximumSpeechAFPMilli            int64    `json:"maximumSpeechAfpMilli"`
	MaximumNonSubscriptionCashMicros int64    `json:"maximumNonSubscriptionCashMicros"`
}

// FLO167SupersessionPackage is a new immutable authority; the v2 activation and
// its A01 terminal ledger are referenced only by hashes and are never updated.
type FLO167SupersessionPackage struct {
	SchemaVersion              string                     `json:"schemaVersion"`
	State                      string                     `json:"state"`
	LegacyAuthorizationHash    string                     `json:"legacyAuthorizationHash"`
	LegacyExecutionPackageHash string                     `json:"legacyExecutionPackageHash"`
	LegacyProjectionHash       string                     `json:"legacyProjectionHash"`
	LegacyTerminalLedgerHash   string                     `json:"legacyTerminalLedgerHash"`
	LegacyStopEvidenceHash     string                     `json:"legacyStopEvidenceHash"`
	Authorization              FLO167AuthorizationBinding `json:"authorization"`
	Shots                      []FLO167ShotBinding        `json:"shots"`
	ContentHash                string                     `json:"contentHash"`
}

func SealFLO167SupersessionPackage(p FLO167SupersessionPackage) (FLO167SupersessionPackage, error) {
	p.ContentHash = ""
	hash, err := canonicalDigest(p)
	if err != nil {
		return FLO167SupersessionPackage{}, err
	}
	p.ContentHash = hash
	return p, p.Validate()
}

func (p FLO167SupersessionPackage) Validate() error {
	if p.SchemaVersion != FLO167SupersessionSchema || p.State != "supersession_package_pending_v3" {
		return errors.New("supersession schema or state is invalid")
	}
	for name, digest := range map[string]string{
		"legacy authorization":   p.LegacyAuthorizationHash,
		"legacy package":         p.LegacyExecutionPackageHash,
		"legacy projection":      p.LegacyProjectionHash,
		"legacy terminal ledger": p.LegacyTerminalLedgerHash,
		"legacy stop evidence":   p.LegacyStopEvidenceHash,
	} {
		if !validLowerDigest(digest) {
			return fmt.Errorf("%s hash is invalid", name)
		}
	}
	wantCompleted := []string{"GOLD-A01"}
	wantAllowed := []string{"GOLD-A02", "GOLD-A03", "GOLD-A04", "GOLD-A05", "GOLD-A06", "GOLD-A07", "GOLD-A08", "GOLD-A09", "GOLD-A10"}
	if !equalOrderedStrings(p.Authorization.CompletedSet, wantCompleted) || !equalOrderedStrings(p.Authorization.AllowedSubmitSet, wantAllowed) {
		return errors.New("supersession completed or allowed-submit set drifted")
	}
	a := p.Authorization
	if a.MaximumPrimaryJobs != 10 || a.MaximumControlledRetries != 1 || a.MaximumVideoTokens != 1_200_000 ||
		a.MaximumVideoAFPMilli != 30_306_870 || a.MaximumSpeechAFPMilli != 1_039 || a.MaximumNonSubscriptionCashMicros != 0 {
		return errors.New("supersession hard limits drifted")
	}
	if len(p.Shots) != 10 {
		return errors.New("supersession requires ten shot bindings")
	}
	seen := make(map[string]struct{}, 10)
	for _, shot := range p.Shots {
		if _, ok := FLO167ShotDurationsMS[shot.ShotID]; !ok {
			return fmt.Errorf("unknown shot %q", shot.ShotID)
		}
		if _, duplicate := seen[shot.ShotID]; duplicate {
			return fmt.Errorf("duplicate shot %q", shot.ShotID)
		}
		seen[shot.ShotID] = struct{}{}
		if shot.Pricing.DurationMS != FLO167ShotDurationsMS[shot.ShotID] {
			return fmt.Errorf("%s duration drifted", shot.ShotID)
		}
		if err := shot.Pricing.Validate(); err != nil {
			return fmt.Errorf("%s: %w", shot.ShotID, err)
		}
		for _, digest := range []string{shot.RouteHash, shot.G1Hash, shot.G2Hash, shot.SafetyHash, shot.CanonicalInputHash, shot.SemanticInputHash} {
			if !validLowerDigest(digest) {
				return fmt.Errorf("%s has an invalid immutable hash", shot.ShotID)
			}
		}
	}
	want, err := canonicalDigest(struct {
		SchemaVersion              string                     `json:"schemaVersion"`
		State                      string                     `json:"state"`
		LegacyAuthorizationHash    string                     `json:"legacyAuthorizationHash"`
		LegacyExecutionPackageHash string                     `json:"legacyExecutionPackageHash"`
		LegacyProjectionHash       string                     `json:"legacyProjectionHash"`
		LegacyTerminalLedgerHash   string                     `json:"legacyTerminalLedgerHash"`
		LegacyStopEvidenceHash     string                     `json:"legacyStopEvidenceHash"`
		Authorization              FLO167AuthorizationBinding `json:"authorization"`
		Shots                      []FLO167ShotBinding        `json:"shots"`
		ContentHash                string                     `json:"contentHash"`
	}{p.SchemaVersion, p.State, p.LegacyAuthorizationHash, p.LegacyExecutionPackageHash, p.LegacyProjectionHash, p.LegacyTerminalLedgerHash, p.LegacyStopEvidenceHash, p.Authorization, p.Shots, ""})
	if err != nil {
		return err
	}
	if p.ContentHash != want {
		return errors.New("supersession contentHash does not match canonical content")
	}
	return nil
}

func (p FLO167SupersessionPackage) AuthorizeSubmit(shotID string, actualAFPMilli int64) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if shotID == "GOLD-A01" {
		return errors.New("GOLD-A01 is terminal and permanently forbidden from resubmission")
	}
	if !contains(p.Authorization.AllowedSubmitSet, shotID) {
		return errors.New("shot is outside the allowed-submit set")
	}
	var binding *FLO167ShotBinding
	for i := range p.Shots {
		if p.Shots[i].ShotID == shotID {
			binding = &p.Shots[i]
			break
		}
	}
	if binding == nil {
		return errors.New("shot pricing binding is missing")
	}
	ok, err := AFPWithinDrift(actualAFPMilli, binding.Pricing.ExpectedAFPMilli, MaximumAFPDriftBPS)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("shot AFP exceeds the inclusive 10 percent duration-normalized limit")
	}
	return nil
}

func canonicalDigest(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func checkedNonnegativeMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, errors.New("nonnegative int64 multiplication overflow")
	}
	return a * b, nil
}

func checkedNonnegativeAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || b > math.MaxInt64-a {
		return 0, errors.New("nonnegative int64 addition overflow")
	}
	return a + b, nil
}

// SortedFLO167ShotIDs is useful to materializers and avoids map-order input to hashes.
func SortedFLO167ShotIDs() []string {
	ids := make([]string, 0, len(FLO167ShotDurationsMS))
	for id := range FLO167ShotDurationsMS {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
