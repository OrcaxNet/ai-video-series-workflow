package stage1

import (
	"math"
	"strings"
	"testing"
)

func TestNormalizeAFPMilli_FLO167Vectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		duration, want int64
	}{
		{"4 seconds", 4_000, 2_003_760}, {"4.5 seconds", 4_500, 2_254_230},
		{"5 seconds", 5_000, 2_504_700}, {"5.5 seconds", 5_500, 2_755_170},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeAFPMilli(FLO167ReferenceAFPMilli, FLO167ReferenceDurationMS, test.duration)
			if err != nil || got != test.want {
				t.Fatalf("NormalizeAFPMilli = %d, %v; want %d", got, err, test.want)
			}
		})
	}
}

func TestNormalizeAFPMilli_FailsClosedOnInvalidOrOverflow(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                   string
		reference, referenceDuration, duration int64
	}{
		{"negative AFP", -1, 5_000, 4_000}, {"zero reference duration", 1, 0, 4_000},
		{"zero duration", 1, 5_000, 0}, {"multiply overflow", math.MaxInt64, 1, 2},
		{"rounding overflow", math.MaxInt64, 2, 1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NormalizeAFPMilli(test.reference, test.referenceDuration, test.duration); err == nil {
				t.Fatal("accepted invalid/overflow input")
			}
		})
	}
}

func TestAFPWithinDrift_InclusiveBoundary(t *testing.T) {
	t.Parallel()
	const expected int64 = 2_003_760
	tests := []struct {
		name   string
		actual int64
		want   bool
	}{
		{"positive boundary", 2_204_136, true}, {"positive one milli outside", 2_204_137, false},
		{"negative boundary", 1_803_384, true}, {"negative one milli outside", 1_803_383, false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := AFPWithinDrift(test.actual, expected, 1_000)
			if err != nil || got != test.want {
				t.Fatalf("AFPWithinDrift = %t, %v; want %t", got, err, test.want)
			}
		})
	}
	if _, err := AFPWithinDrift(math.MaxInt64, 1, 1_000); err == nil {
		t.Fatal("overflow comparison did not fail closed")
	}
}

func TestFLO167SupersessionPackage_ExactSetsAndLegacyA01(t *testing.T) {
	t.Parallel()
	p := validFLO167Package(t)
	if err := p.AuthorizeSubmit("GOLD-A02", 2_254_230); err != nil {
		t.Fatalf("A02: %v", err)
	}
	if err := p.AuthorizeSubmit("GOLD-A01", 2_007_900); err == nil {
		t.Fatal("A01 resubmission was accepted")
	}
	ok, err := AFPWithinDrift(2_007_900, 2_003_760, 1_000)
	if err != nil || !ok {
		t.Fatalf("legacy A01 +0.2066%% must remain valid evidence: %t, %v", ok, err)
	}

	tests := []struct {
		name   string
		mutate func(*FLO167SupersessionPackage)
	}{
		{"duration drift", func(p *FLO167SupersessionPackage) { p.Shots[1].Pricing.DurationMS++ }},
		{"pricing digest drift", func(p *FLO167SupersessionPackage) { p.Shots[1].Pricing.PricingSnapshotDigest = strings.Repeat("f", 64) }},
		{"rounding version drift", func(p *FLO167SupersessionPackage) { p.Shots[1].Pricing.RoundingVersion = "unknown" }},
		{"route drift", func(p *FLO167SupersessionPackage) { p.Shots[1].RouteHash = "bad" }},
		{"G1 drift", func(p *FLO167SupersessionPackage) { p.Shots[1].G1Hash = "bad" }},
		{"G2 drift", func(p *FLO167SupersessionPackage) { p.Shots[1].G2Hash = "bad" }},
		{"safety drift", func(p *FLO167SupersessionPackage) { p.Shots[1].SafetyHash = "bad" }},
		{"canonical drift", func(p *FLO167SupersessionPackage) { p.Shots[1].CanonicalInputHash = "bad" }},
		{"semantic drift", func(p *FLO167SupersessionPackage) { p.Shots[1].SemanticInputHash = "bad" }},
		{"completed set drift", func(p *FLO167SupersessionPackage) { p.Authorization.CompletedSet = nil }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := p
			candidate.Shots = append([]FLO167ShotBinding(nil), p.Shots...)
			test.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("drifted package was accepted")
			}
		})
	}
}

func TestFLO167CanonicalMaterializationIsStable(t *testing.T) {
	t.Parallel()
	first := validFLO167Package(t)
	second := validFLO167Package(t)
	if first.ContentHash != second.ContentHash {
		t.Fatalf("fresh materializations differ: %s != %s", first.ContentHash, second.ContentHash)
	}
	t.Logf("provider-free supersession package SHA-256: %s", first.ContentHash)
	replay, err := SealFLO167SupersessionPackage(first)
	if err != nil || replay.ContentHash != first.ContentHash {
		t.Fatalf("same-package replay drifted: %s %v", replay.ContentHash, err)
	}
}

func validFLO167Package(t *testing.T) FLO167SupersessionPackage {
	t.Helper()
	digest := strings.Repeat("a", 64)
	shots := make([]FLO167ShotBinding, 0, 10)
	for _, id := range SortedFLO167ShotIDs() {
		duration := FLO167ShotDurationsMS[id]
		expected, err := NormalizeAFPMilli(FLO167ReferenceAFPMilli, FLO167ReferenceDurationMS, duration)
		if err != nil {
			t.Fatal(err)
		}
		shots = append(shots, FLO167ShotBinding{ShotID: id, Pricing: DurationPricingBinding{DurationMS: duration, PricingSnapshotID: "ark-agent-plan-20260803", PricingSnapshotDigest: digest, ReferenceAFPMilli: FLO167ReferenceAFPMilli, ReferenceDurationMS: FLO167ReferenceDurationMS, ExpectedAFPMilli: expected, NormalizationVersion: DurationNormalizationVersion, RoundingVersion: NonnegativeHalfUpVersion}, RouteHash: digest, G1Hash: digest, G2Hash: digest, SafetyHash: digest, CanonicalInputHash: digest, SemanticInputHash: digest})
	}
	p, err := SealFLO167SupersessionPackage(FLO167SupersessionPackage{SchemaVersion: FLO167SupersessionSchema, State: "supersession_package_pending_v3", LegacyAuthorizationHash: "7bf55cad2a4f81f54eb6617bbab81fd21f789785ce1176213014f5833ce4ac25", LegacyExecutionPackageHash: digest, LegacyProjectionHash: digest, LegacyTerminalLedgerHash: digest, LegacyStopEvidenceHash: "dd1954608254791425d7574fe7333d1c1d7cd77cb843572f79d84a3dbeadea76", Authorization: FLO167AuthorizationBinding{CompletedSet: []string{"GOLD-A01"}, AllowedSubmitSet: []string{"GOLD-A02", "GOLD-A03", "GOLD-A04", "GOLD-A05", "GOLD-A06", "GOLD-A07", "GOLD-A08", "GOLD-A09", "GOLD-A10"}, MaximumPrimaryJobs: 10, MaximumControlledRetries: 1, MaximumVideoTokens: 1_200_000, MaximumVideoAFPMilli: 30_306_870, MaximumSpeechAFPMilli: 1_039, MaximumNonSubscriptionCashMicros: 0}, Shots: shots})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
