package stage1

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
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
		{"reference AFP drift", func(p *FLO167SupersessionPackage) {
			p.Shots[1].Pricing.ReferenceAFPMilli = 1
			p.Shots[1].Pricing.ExpectedAFPMilli = 1
		}},
		{"reference duration drift", func(p *FLO167SupersessionPackage) {
			p.Shots[1].Pricing.ReferenceDurationMS = 1
			p.Shots[1].Pricing.ExpectedAFPMilli = p.Shots[1].Pricing.ReferenceAFPMilli * p.Shots[1].Pricing.DurationMS
		}},
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
	delivered, err := os.ReadFile("../../../docs/flo-167/provider-free-execution-package.json")
	if err != nil {
		t.Fatal(err)
	}
	var artifact FLO167SupersessionPackage
	if err := json.Unmarshal(delivered, &artifact); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(artifact, first) {
		t.Fatal("delivered execution package differs from independently materialized frozen evidence")
	}
	projection := validFLO167Projection(t, first)
	projectionBytes, err := os.ReadFile("../../../docs/flo-167/canonical-projection.json")
	if err != nil {
		t.Fatal(err)
	}
	var deliveredProjection FLO167CanonicalProjection
	if json.Unmarshal(projectionBytes, &deliveredProjection) != nil || !reflect.DeepEqual(deliveredProjection, projection) {
		t.Fatal("delivered projection differs from independently materialized frozen evidence")
	}
}

func TestValidateFLO167ArtifactsRejectsIndependentlyResealedTerminalHash(t *testing.T) {
	package_ := validFLO167Package(t)
	projection := validFLO167Projection(t, package_)
	package_.LegacyTerminalLedgerHash = strings.Repeat("f", 64)
	var err error
	package_, err = SealFLO167SupersessionPackage(package_)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateFLO167Artifacts(package_, projection); err == nil {
		t.Fatal("independently resealed terminal hash was accepted")
	}
}

func TestGateFLO167UsesDurationBindingForLegacyAndNewAttempts(t *testing.T) {
	plan := testPlan()
	plan.PrimaryShotIDs = SortedFLO167ShotIDs()
	gate, err := Open(plan, filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := Attempt{AttemptID: "legacy-a01", ShotID: "GOLD-A01", IdempotencyKey: "legacy-a01",
		EstimatedVideoTokens: 87_300, PredictedAFPMilli: FLO167ReferenceAFPMilli}
	if _, err := gate.Authorize(legacy); err != nil {
		t.Fatal(err)
	}
	if err := gate.Complete(legacy.IdempotencyKey, Completion{ProviderTaskID: "legacy-task", ActualVideoTokens: 87_300,
		ActualAFPMilli: 2_007_900, EvidenceComplete: true, State: "TERMINAL_SUCCEEDED"}); err != nil {
		t.Fatal(err)
	}
	package_ := validFLO167Package(t)
	if err := gate.BindFLO167Supersession(package_); err != nil {
		t.Fatal(err)
	}
	a02, _ := package_.Shot("GOLD-A02")
	decision, err := gate.Inspect(Attempt{AttemptID: "v3-a02", ShotID: "GOLD-A02", IdempotencyKey: "v3-a02",
		EstimatedVideoTokens: 100_000, PredictedAFPMilli: a02.Pricing.ExpectedAFPMilli})
	if err != nil || decision != DecisionSubmit {
		t.Fatalf("duration-normalized A02 decision=%s err=%v", decision, err)
	}
}

func validFLO167Projection(t *testing.T, p FLO167SupersessionPackage) FLO167CanonicalProjection {
	t.Helper()
	projection, err := SealFLO167CanonicalProjection(FLO167CanonicalProjection{
		SchemaVersion: "flo100.batch-a-duration-projection.v3", SupersessionPackageHash: p.ContentHash,
		CompletedSet: p.Authorization.CompletedSet, AllowedSubmitSet: p.Authorization.AllowedSubmitSet,
		A01Terminal: FLO167LegacyTerminal{ShotID: "GOLD-A01", ProviderTaskID: "cgt-20260803044117-2zwsb",
			ProviderRequestID: "02178570327780286f296e4c58ddf650991f54d763d292afe21e6",
			ArtifactSHA256:    "f791f6914f2fa31c4fecbc8728846b4bbb0a22d45716b83b3845bf05256b125a",
			ActualAFPMilli:    2_007_900, ExpectedAFPMilli: 2_003_760, ActualVideoTokens: 87_300,
			ActualCashMicros: 0, LedgerSHA256: "7ea9cfb63b3c54fa46583cf5abdb0bc67d323eead9ab4f45e9187e9700dcf0e0"},
		Shots: p.Shots, StateMachine: []string{"v2_stopped_after_A01", "supersession_package_pending_v3", "v3_authorized_A02_A10", "quota_reserved", "A02_submitted"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func validFLO167Package(t *testing.T) FLO167SupersessionPackage {
	t.Helper()
	const pricingDigest = "8a24f342be8bb84c95001434ea4622eda28e28c6aa77dd049dff8bc1f7f3c42a"
	const routeHash = "0d1c97d70c7b332940279be334c127fa068069f83d58840fa57b4d3b10166eca"
	const g1Hash = "e458060085c9585366c4c52abe3f7fc4e52110f60bc73b61765bc3edc226c133"
	const safetyHash = "ed8a4852873f8552477a2af1ac5b83900158405286a26fa5f04160de13c3d48f"
	g2 := []string{"37f3c2c5774bc3688723d0e9e8dc05a0d21d71e48d472712182d41d079604425", "ee7493791cfe465cd7402a26afa8caf14d655436feef289877ac78f5354b2d3b", "cfeb896a7dbeb08f77c1fb13e99c8dd448871667032b1433e1835325ef7336d8", "ea9827f441f32360e3546a1fed6837af8fedf825cfdedafeaacf5c1664b035da", "5d0e3fd256211f0ac171215c60520d06dfd0813eeb05032bb8dbc75e22f6b266", "e40fd759a9173b49060eb86eafc49ab408f1d3af6199ceb127716dbb0b00e0cd", "4e2868e6e64c86238204ccb0cdefd4fc5c782826ff87d744f54a6089c2a1db43", "154928813bd0bb3f6e7365a22b8c40c704b7794036e84fc6c77372cc95e07c52", "c6a6ff2a253f0c5ea766f184bf7af9364a6a92fb8f29118c4211c03cc1576547", "fdc27d59ef8ce968cbb732d29ba529adf15e090ed07af8709e2d85e0fb53711a"}
	canonical := []string{"1f36ace7814cc56bf53cd12cd44b0e90d5a2051e4b630672da579d7a63ab787b", "7f38b57d62953b560392d83e2c75eb08077f13db11e0ceff8ae596a4c8c4b3ee", "f5ec411e39ff3c5f19d9c9bcd707e51035d115cbf7123703722d3b944bd4f87c", "dd7155d2c90d05ca132428d7a2cdc6ea08b0b071e7371e3943d56de4a8df5f4d", "8bffdb7eaafd9a5d5146b30f99d68d86fa61f5e385124a6f14267b5abd798301", "1212e33fdbd2b3fda498041b04c5cce93641a3695ef3e89c8241addec3816bee", "1beaa5eacb621e3f13cb25a4e93a3a191f12f31a6abb72c0eefa1f9ce3448011", "50be275911130e5b29a7ab886da894a0a4b35001a7d62a54dca5dc0faba7cbcb", "63e61d529de027ae86eaf8a232525b49ac9cabb9e1a97575d96cd50395d88210", "b1980987f0d9c7581127ce4fde0ba6fae90f92bcc211ec34b968b1c6250fa1e9"}
	semantic := []string{"7f2dae5f402050ad537bfe7514f55e6dd29c69bfbbe8ecf776c60cf3713cdac8", "0a004fb2e3611b6c4a4d8ac477ae6d7ebab7f29cec3a1bce915e866fb33bb697", "75d13dff743d263a1c319621803d4b2435f961fd3c5adb82d663b5bc8a69ba35", "c4b18dc58133f8f96452dbb656deafebe1f75e0744cf31ebd5a5bfd4e29f4bb7", "4b66714e65eedcf7e03444feea52a8a3094eb9a23db02d10432902fd01fe06fe", "7f7e26eb44d10cec1873b02567273fda1afb9e4289e6e3b10abaf5f4f05d7850", "2ddf42d6da999f2b0c16789e2cc0a80249c827b05a4bfc0420411eecf8929a21", "d373bd1130ca52c53293d9620e060a0244d98bec51e9aa26668c71105412319d", "d0b2227d23d087171457b392db151e76be6e487c742c9d651305a403596f6f95", "1c14ef156f7272e6078b3ca5a219b84db3180b22be2c51969c1f0ff5d9ae2f54"}
	shots := make([]FLO167ShotBinding, 0, 10)
	for index, id := range SortedFLO167ShotIDs() {
		duration := FLO167ShotDurationsMS[id]
		expected, err := NormalizeAFPMilli(FLO167ReferenceAFPMilli, FLO167ReferenceDurationMS, duration)
		if err != nil {
			t.Fatal(err)
		}
		shots = append(shots, FLO167ShotBinding{ShotID: id, Pricing: DurationPricingBinding{DurationMS: duration, PricingSnapshotID: "agent-plan-large-v1", PricingSnapshotDigest: pricingDigest, ReferenceAFPMilli: FLO167ReferenceAFPMilli, ReferenceDurationMS: FLO167ReferenceDurationMS, ExpectedAFPMilli: expected, PricingRuleVersion: "agent-plan-subscription-v1", MaximumDriftBPS: MaximumAFPDriftBPS, NormalizationVersion: DurationNormalizationVersion, RoundingVersion: NonnegativeHalfUpVersion}, RouteHash: routeHash, G1Hash: g1Hash, G2Hash: g2[index], SafetyHash: safetyHash, CanonicalInputHash: canonical[index], SemanticInputHash: semantic[index]})
	}
	p, err := SealFLO167SupersessionPackage(FLO167SupersessionPackage{SchemaVersion: FLO167SupersessionSchema, State: "supersession_package_pending_v3", LegacyAuthorizationHash: "7bf55cad2a4f81f54eb6617bbab81fd21f789785ce1176213014f5833ce4ac25", LegacyExecutionPackageHash: "6a7c03ed869c427d23cc6b669e7938ba271c8343b3e4627e85cd93ea50fffd2e", LegacyProjectionHash: "c0d2d316867c79d1ebc419dec3a68fe29c947cd184d21a2b11e45c6224013202", LegacyTerminalLedgerHash: "7ea9cfb63b3c54fa46583cf5abdb0bc67d323eead9ab4f45e9187e9700dcf0e0", LegacyStopEvidenceHash: "dd1954608254791425d7574fe7333d1c1d7cd77cb843572f79d84a3dbeadea76", Authorization: FLO167AuthorizationBinding{CompletedSet: []string{"GOLD-A01"}, AllowedSubmitSet: []string{"GOLD-A02", "GOLD-A03", "GOLD-A04", "GOLD-A05", "GOLD-A06", "GOLD-A07", "GOLD-A08", "GOLD-A09", "GOLD-A10"}, MaximumPrimaryJobs: 10, MaximumControlledRetries: 1, MaximumVideoTokens: 1_200_000, MaximumVideoAFPMilli: 30_306_870, MaximumSpeechAFPMilli: 1_039, MaximumNonSubscriptionCashMicros: 0}, Shots: shots})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
