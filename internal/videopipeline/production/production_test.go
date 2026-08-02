package production

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

type goldenSet struct {
	SchemaVersion string        `json:"schema_version"`
	Groups        []goldenGroup `json:"groups"`
}

type goldenGroup struct {
	Category  string     `json:"category"`
	EpisodeID string     `json:"episode_id"`
	Shots     []ShotSpec `json:"shots"`
}

func loadGolden(t *testing.T) goldenSet {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "golden_30_shots.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture goldenSet
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestRevisionStore_ImmutableOptimisticAppendRollbackAndStaleImpact(t *testing.T) {
	t.Parallel()
	store := NewRevisionStore()
	at := time.Unix(1_800_000_000, 0).UTC()
	payload := map[string]any{"name": "original", "nested": map[string]any{"state": "approved"}}
	first, err := store.CreateNext("character", "char-lin", payload, []string{"evidence-1"}, "editor-1", at)
	if err != nil {
		t.Fatal(err)
	}
	payload["name"] = "mutated outside store"
	var accepted map[string]any
	got, ok := store.Get(first.ID)
	if !ok {
		t.Fatal("first revision not found")
	}
	if err := got.Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted["name"] != "original" {
		t.Fatalf("stored revision was mutated: %#v", accepted)
	}

	second, err := store.CreateNext("character", "char-lin", map[string]any{"name": "second"}, nil, "editor-1", at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(CreateRevisionInput{
		Kind:          "character",
		AggregateID:   "char-lin",
		ParentID:      first.ID,
		SchemaVersion: RevisionSchemaVersion,
		Payload:       map[string]any{"name": "stale write"},
		CreatedBy:     "editor-2",
		CreatedAt:     at.Add(2 * time.Second),
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale append error = %v, want conflict", err)
	}
	rolledBack, err := store.Rollback("character", "char-lin", first.ID, "reviewer-1", at.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Number != 3 || rolledBack.ParentID != second.ID || rolledBack.RollbackOf != first.ID {
		t.Fatalf("rollback revision = %#v", rolledBack)
	}
	if len(store.History("character", "char-lin")) != 3 {
		t.Fatal("rollback mutated history instead of appending")
	}

	graph := NewDependencyGraph()
	prompt := RevisionRef{
		ID: "prompt-revision", Kind: KindPrompt, AggregateID: "shot-1", Number: 1,
		ContentHash: hashString("prompt"),
	}
	run := RevisionRef{
		ID: "run-revision", Kind: "generation_run", AggregateID: "run-1", Number: 1,
		ContentHash: hashString("run"),
	}
	if err := graph.Add(Dependency{Producer: first.Ref(), Consumer: prompt, Role: "character_reference"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(Dependency{Producer: prompt, Consumer: run, Role: "prompt_input"}); err != nil {
		t.Fatal(err)
	}
	impacted := graph.Impacted(first.ID)
	if len(impacted) != 2 || impacted[0].ID != run.ID && impacted[1].ID != run.ID {
		t.Fatalf("stale closure = %#v", impacted)
	}
}

func TestContentCompiler_RequiresAuthorizationAndProducesThirtyImmutableShots(t *testing.T) {
	t.Parallel()
	fixture := loadGolden(t)
	source, draft := goldenCompilationDraft(fixture)
	store := NewRevisionStore()
	compiler := ContentCompiler{
		Store:     store,
		Generator: FixtureContentGenerator{Draft: draft},
	}
	at := time.Unix(1_800_000_000, 0).UTC()
	result, err := compiler.Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Shots) != 30 || len(result.Episodes) != 3 || len(result.Scenes) != 3 {
		t.Fatalf("compiled result counts: episodes=%d scenes=%d shots=%d", len(result.Episodes), len(result.Scenes), len(result.Shots))
	}
	for id, ref := range result.Shots {
		if ref.Kind != KindShotSpec || ref.AggregateID != id || ref.Number != 1 {
			t.Fatalf("shot revision %q = %#v", id, ref)
		}
	}

	source.Rights.Authorized = false
	_, err = compiler.Compile(t.Context(), source, DefaultCompileOptions(at.Add(time.Hour)))
	if !errors.Is(err, ErrPolicyBlocked) {
		t.Fatalf("unauthorized compile error = %v, want policy block", err)
	}
}

func TestProviderContentGenerator_ExecutesTextAPIContract(t *testing.T) {
	t.Parallel()
	fixture := loadGolden(t)
	source, draft := goldenCompilationDraft(fixture)
	data, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	provider := &structuredFixtureProvider{output: string(data)}
	generator := ProviderContentGenerator{
		Provider: provider, ModelHint: "fixture-text-v1",
		Budget: providercontract.BudgetEnvelope{
			EstimatedCostMicros: 100, MaxCostMicros: 200, MaxAttempts: 2,
		},
		Wait: func(context.Context, time.Duration) error { return nil },
	}
	at := time.Unix(1_800_000_000, 0).UTC()
	result, err := (&ContentCompiler{
		Store: NewRevisionStore(), Generator: generator,
	}).Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Shots) != 30 || provider.submits != 1 ||
		provider.request.Modality != providercontract.ModalityText ||
		provider.request.ModelHint != "fixture-text-v1" ||
		!strings.Contains(provider.request.Prompt, "AUTHORIZED SOURCE START") ||
		!strings.Contains(provider.request.Prompt, source.Text) {
		t.Fatalf("provider-backed compile result=%#v request=%#v submits=%d", result, provider.request, provider.submits)
	}
}

func TestContextResolver_WhitelistPriorityAssetConflictAndInvalidReference(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	catalog := NewAssetCatalog()
	lin := fixtureAsset(t, catalog, "lin", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	mara := fixtureAsset(t, catalog, "mara", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	series := mustContext(t, store, ScopeSeries, "series-1", map[string]string{
		"visual.style":        "stylized 2.5D",
		"visual.palette":      "amber and teal",
		"output.aspect_ratio": "16:9",
		"output.fps":          "24",
	}, map[string]string{"character.primary": lin.Revision.ID}, at)
	episode := mustContext(t, store, ScopeEpisode, "episode-1", map[string]string{
		"visual.palette":  "moonlit blue",
		"story.mood":      "suspense",
		"story.pacing":    "measured",
		"camera.language": "restrained",
	}, nil, at)
	scene := mustContext(t, store, ScopeScene, "scene-1", map[string]string{
		"scene.location":            "observatory",
		"scene.time":                "night",
		"lighting":                  "soft rim light",
		"audio.ambience.identity":   "observatory-wind",
		"audio.ambience.version":    "ambience-v2",
		"audio.ambience.continuity": "required",
	}, nil, at)
	shot := mustContext(t, store, ScopeShot, "shot-1", map[string]string{
		"camera.framing":  "medium",
		"camera.angle":    "eye-level",
		"camera.movement": "slow dolly",
		"emotion":         "curiosity",
	}, nil, at)
	resolver := NewContextResolver(catalog)
	effective, err := resolver.Resolve([]ContextLayer{series, episode, scene, shot})
	if err != nil {
		t.Fatal(err)
	}
	if effective.Values["visual.palette"] != "moonlit blue" || effective.Origins["visual.palette"] != ScopeEpisode {
		t.Fatalf("context priority = %#v / %#v", effective.Values, effective.Origins)
	}
	if effective.Values["audio.ambience.identity"] != "observatory-wind" ||
		effective.Origins["audio.ambience.identity"] != ScopeScene {
		t.Fatalf("ambience Scene Context = %#v / %#v", effective.Values, effective.Origins)
	}
	if len(effective.OrderedAssets()) != 1 || effective.OrderedAssets()[0].Revision != lin.Revision.ID {
		t.Fatalf("resolved assets = %#v", effective.Assets)
	}

	forbidden := shot
	forbidden.Values = map[string]string{"visual.style": "forbidden lower-level replacement"}
	if _, err := resolver.Resolve([]ContextLayer{series, episode, scene, forbidden}); !errors.Is(err, ErrPolicyBlocked) {
		t.Fatalf("forbidden override error = %v", err)
	}
	conflictingScene := scene
	conflictingScene.AssetBindings = map[string]string{"character.primary": mara.Revision.ID}
	if _, err := resolver.Resolve([]ContextLayer{series, episode, conflictingScene, shot}); !errors.Is(err, ErrConflict) {
		t.Fatalf("asset conflict error = %v", err)
	}
	invalidShot := shot
	invalidShot.AssetBindings = map[string]string{"continuity.tail": "latest"}
	if _, err := resolver.Resolve([]ContextLayer{series, episode, scene, invalidShot}); !errors.Is(err, ErrStaleReference) {
		t.Fatalf("invalid reference error = %v", err)
	}
}

func TestPromptCompiler_ContinuityDiffAndRegistryImmutability(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	catalog := NewAssetCatalog()
	lin := fixtureAsset(t, catalog, "lin", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	tail := fixtureAsset(t, catalog, "tail", providercontract.ModalityImage, providercontract.AssetRoleLastFrame)
	layers := fixtureContexts(t, store, catalog, lin.Revision.ID, "episode-1", "scene-1", "shot-1", at)
	registry := NewPromptRegistry()
	compiler := PromptCompiler{Resolver: NewContextResolver(catalog), Catalog: catalog, Registry: registry}
	shot := baseShot("shot-1", 1)
	shotRevision := fixtureShotRevision(shot)
	first, err := compiler.Compile(PromptCompileInput{
		ShotRevision:         shotRevision,
		Shot:                 shot,
		ContextLayers:        layers,
		TemplateRef:          "video-shot-v1",
		GenerationProfileRef: "profile-720p24-v1",
		Output:               fixtureOutput(),
		CreatedAt:            at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Output.AudioStrategy != providercontract.AudioStrategyNativePreferred ||
		!first.Output.GenerateAudio || first.Output.AudioDelivery != providercontract.NativeAudioMix ||
		first.ModelPayload["generate_audio"] != true ||
		first.ModelPayload["audio_strategy"] != string(providercontract.AudioStrategyNativePreferred) {
		t.Fatalf("new prompt did not freeze native audio defaults: %#v %#v", first.Output, first.ModelPayload)
	}
	replay, err := compiler.Compile(PromptCompileInput{
		ShotRevision:         shotRevision,
		Shot:                 shot,
		ContextLayers:        layers,
		TemplateRef:          "video-shot-v1",
		GenerationProfileRef: "profile-720p24-v1",
		Output:               fixtureOutput(),
		CreatedAt:            at.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.ID != first.ID || replay.RevisionNumber != 1 || len(registry.History(shot.ID)) != 1 {
		t.Fatalf("identical compilation was not idempotent: first=%#v replay=%#v", first, replay)
	}

	secondShot := baseShot("shot-2", 2)
	secondShot.Continuity.PreviousPromptSnapshotID = first.ID
	secondShot.Continuity.TailFrameAssetRevisionID = tail.Revision.ID
	secondLayers := fixtureContexts(t, store, catalog, lin.Revision.ID, "episode-1b", "scene-1b", "shot-2", at.Add(time.Second))
	secondLayers[1].Values["story.mood"] = "urgent"
	second, err := compiler.Compile(PromptCompileInput{
		ShotRevision:         fixtureShotRevision(secondShot),
		Shot:                 secondShot,
		ContextLayers:        secondLayers,
		Previous:             &first,
		TemplateRef:          "video-shot-v1",
		GenerationProfileRef: "profile-720p24-v1",
		Output:               fixtureOutput(),
		CreatedAt:            at.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.PreviousPromptHash != first.ContentHash || second.TailFrameHash != tail.Revision.ContentHash ||
		!slices.ContainsFunc(second.Assets, func(asset providercontract.AssetRef) bool {
			return asset.Role == providercontract.AssetRoleLastFrame && asset.SHA256 == tail.Revision.ContentHash
		}) {
		t.Fatalf("continuity snapshot = %#v", second)
	}
	diff := DiffPrompts(first, second)
	if !diff.ChangedShot || !slices.Contains(diff.ChangedContextKeys, "story.mood") ||
		!diff.PreviousPromptChanged || !diff.TailFrameChanged {
		t.Fatalf("prompt diff = %#v", diff)
	}
	first.InputRevisionHashes["tamper"] = "tamper"
	stored, ok := registry.Get(first.ID)
	if !ok || stored.InputRevisionHashes["tamper"] != "" {
		t.Fatal("prompt registry returned mutable state")
	}
}

func TestGenerationRunner_GatesBudgetRecoveryIdempotencyManifestAndPublication(t *testing.T) {
	t.Parallel()
	fixture := loadGolden(t)
	source, draft := goldenCompilationDraft(fixture)
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	compilation, err := (&ContentCompiler{
		Store: store, Generator: FixtureContentGenerator{Draft: draft},
	}).Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewAssetCatalog()
	lin := fixtureAsset(t, catalog, "lin", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	layers := fixtureContexts(t, store, catalog, lin.Revision.ID, "episode-single", "scene-single", "single-01", at)
	shot := fixture.Groups[0].Shots[0]
	promptRegistry := NewPromptRegistry()
	prompt, err := (&PromptCompiler{
		Resolver: NewContextResolver(catalog), Catalog: catalog, Registry: promptRegistry,
	}).Compile(PromptCompileInput{
		ShotRevision: compilation.Shots[shot.ID], Shot: shot, ContextLayers: layers,
		TemplateRef: "video-shot-v1", GenerationProfileRef: "profile-720p24-v1",
		Output: fixtureOutput(), CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}

	provider := providercontract.NewFakeProvider(providercontract.FakeRecovery)
	runner := NewGenerationRunner(provider, fixtureCommitter{}, promptRegistry)
	clock := at
	runner.Now = func() time.Time {
		clock = clock.Add(time.Millisecond)
		return clock
	}
	runner.Wait = func(context.Context, time.Duration) error { return nil }
	input := fixtureGenerationInput(t, compilation.Source, compilation.Shots[shot.ID], prompt, at)
	record, err := runner.Execute(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != RunSucceeded || record.ManifestID == "" || !validSHA256(record.ManifestHash) ||
		countSubmitAttempts(record.Attempts) != 2 {
		t.Fatalf("generation record = %#v", record)
	}
	manifestRecord, ok := runner.Manifests.Get(record.ManifestID)
	if !ok || manifestRecord.Manifest.Evidence != providercontract.EvidenceMockOnly ||
		manifestRecord.Manifest.PromptSnapshot != prompt.ID ||
		manifestRecord.Manifest.Provider.RequestID == "" ||
		manifestRecord.Manifest.ModelSnapshot == nil ||
		manifestRecord.Manifest.ModelSnapshot.ModelID != input.Route.ModelID ||
		len(manifestRecord.Manifest.InputRevisions) == 0 ||
		len(manifestRecord.Manifest.Gates) != 2 ||
		len(manifestRecord.Manifest.OutputAssets) != 1 {
		t.Fatalf("manifest record = %#v", manifestRecord)
	}
	replay, err := runner.Execute(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if replay.ManifestID != record.ManifestID || replay.ManifestHash != record.ManifestHash {
		t.Fatalf("idempotent replay created another final record: %#v vs %#v", record, replay)
	}

	changed := input
	changed.Prompt.PositivePrompt += " changed"
	if _, err := runner.Execute(t.Context(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed idempotent request error = %v", err)
	}
	missingGate := input
	missingGate.RunID = "run-missing-gate"
	missingGate.IdempotencyKey = missingGate.RunID
	missingGate.BudgetReservation = fixtureBudgetReservation(t, missingGate)
	missingGate.Gate2.Approved = false
	if _, err := runner.Execute(t.Context(), missingGate); !errors.Is(err, ErrPolicyBlocked) {
		t.Fatalf("missing G2 error = %v", err)
	}
	overBudget := input
	overBudget.RunID = "run-over-budget"
	overBudget.IdempotencyKey = overBudget.RunID
	overBudget.BudgetReservation = fixtureBudgetReservation(t, overBudget)
	overBudget.BudgetPolicy.HardLimitMicros = 1
	if _, err := runner.Execute(t.Context(), overBudget); providercontract.ErrorCodeOf(err) != providercontract.CodeBudgetExceeded {
		t.Fatalf("budget error = %v", err)
	}
	actualCostProvider := &actualCostProvider{
		Provider: providercontract.NewFakeProvider(providercontract.FakeSuccess),
		cost:     input.Budget.EstimatedCostMicros + 1,
	}
	actualCostRunner := NewGenerationRunner(actualCostProvider, fixtureCommitter{}, promptRegistry)
	actualCostRunner.Wait = func(context.Context, time.Duration) error { return nil }
	actualCostInput := input
	actualCostInput.RunID = "run-actual-cost-over-reservation"
	actualCostInput.IdempotencyKey = actualCostInput.RunID
	actualCostInput.BudgetReservation = fixtureBudgetReservationAmount(
		t,
		actualCostInput,
		actualCostInput.Budget.EstimatedCostMicros,
	)
	actualCostRecord, actualCostErr := actualCostRunner.Execute(t.Context(), actualCostInput)
	if providercontract.ErrorCodeOf(actualCostErr) != providercontract.CodeBudgetExceeded ||
		actualCostRecord.State != RunFailed {
		t.Fatalf("actual cost record=%#v error=%v", actualCostRecord, actualCostErr)
	}

	locker := NewPublicationLocker()
	qc := fixtureQuality(record, at.Add(30*time.Minute))
	gate3 := approvedGate(Gate3, record.ManifestID, record.ManifestHash, at.Add(time.Hour))
	lock, err := locker.Lock(record, qc, gate3, at.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if lock.RunID != record.RunID || lock.ManifestHash != record.ManifestHash {
		t.Fatalf("publication lock = %#v", lock)
	}
	rejected := gate3
	rejected.Approved = false
	if _, err := locker.Lock(record, qc, rejected, at.Add(3*time.Hour)); !errors.Is(err, ErrPolicyBlocked) {
		t.Fatalf("rejected G3 error = %v", err)
	}
	failedQC := qc
	failedQC.Passed = false
	if _, err := locker.Lock(record, failedQC, gate3, at.Add(3*time.Hour)); !errors.Is(err, ErrPolicyBlocked) {
		t.Fatalf("failed QC error = %v", err)
	}
}

func TestGenerationRunner_PreflightRejectsPromptTamperingAndUnderfundedReservation(t *testing.T) {
	t.Parallel()
	fixture := loadGolden(t)
	source, draft := goldenCompilationDraft(fixture)
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	compilation, err := (&ContentCompiler{
		Store: store, Generator: FixtureContentGenerator{Draft: draft},
	}).Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewAssetCatalog()
	lin := fixtureAsset(t, catalog, "lin-preflight", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	shot := fixture.Groups[0].Shots[0]
	promptRegistry := NewPromptRegistry()
	prompt, err := (&PromptCompiler{
		Resolver: NewContextResolver(catalog), Catalog: catalog, Registry: promptRegistry,
	}).Compile(PromptCompileInput{
		ShotRevision: compilation.Shots[shot.ID], Shot: shot,
		ContextLayers: fixtureContexts(t, store, catalog, lin.Revision.ID, "episode-preflight", "scene-preflight", "shot-preflight", at),
		TemplateRef:   "video-shot-v1", GenerationProfileRef: "profile-720p24-v1",
		Output: fixtureOutput(), CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		mutate      func(*testing.T, *GenerationInput)
		wantError   error
		wantErrCode providercontract.ErrorCode
	}{
		{
			name: "prompt text changed while retaining frozen identity",
			mutate: func(_ *testing.T, input *GenerationInput) {
				input.Prompt.PositivePrompt += " unapproved mutation"
			},
			wantError: ErrConflict,
		},
		{
			name: "self-consistent prompt is not in persistent registry",
			mutate: func(t *testing.T, input *GenerationInput) {
				t.Helper()
				input.Prompt.PositivePrompt += " unregistered mutation"
				digest, hashErr := promptSnapshotContentHash(input.Prompt)
				if hashErr != nil {
					t.Fatal(hashErr)
				}
				input.Prompt.ContentHash = digest
				input.Prompt.ID = derivedID("prompt", digest)
				input.BudgetReservation = fixtureBudgetReservation(t, *input)
			},
			wantError: ErrStaleReference,
		},
		{
			name: "reservation is one micro below current estimate",
			mutate: func(t *testing.T, input *GenerationInput) {
				t.Helper()
				input.BudgetReservation = fixtureBudgetReservationAmount(
					t,
					*input,
					input.Budget.EstimatedCostMicros-1,
				)
			},
			wantErrCode: providercontract.CodeBudgetExceeded,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &submitCountingProvider{
				Provider: providercontract.NewFakeProvider(providercontract.FakeSuccess),
			}
			runner := NewGenerationRunner(provider, fixtureCommitter{}, promptRegistry)
			runner.Wait = func(context.Context, time.Duration) error { return nil }
			input := fixtureGenerationInput(t, compilation.Source, compilation.Shots[shot.ID], prompt, at)
			input.RunID += "-" + strings.ReplaceAll(test.name, " ", "-")
			input.IdempotencyKey = input.RunID
			input.BudgetReservation = fixtureBudgetReservation(t, input)
			test.mutate(t, &input)

			_, executeErr := runner.Execute(t.Context(), input)
			if test.wantError != nil && !errors.Is(executeErr, test.wantError) {
				t.Fatalf("Execute() error = %v, want %v", executeErr, test.wantError)
			}
			if test.wantErrCode != "" && providercontract.ErrorCodeOf(executeErr) != test.wantErrCode {
				t.Fatalf("Execute() error = %v, want code %s", executeErr, test.wantErrCode)
			}
			if provider.submits != 0 {
				t.Fatalf("provider Submit() calls = %d, want 0", provider.submits)
			}
		})
	}
}

func TestGenerationRunner_ErrorScenariosHaveOneTerminalRecord(t *testing.T) {
	t.Parallel()
	fixture := loadGolden(t)
	source, draft := goldenCompilationDraft(fixture)
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	compilation, err := (&ContentCompiler{
		Store: store, Generator: FixtureContentGenerator{Draft: draft},
	}).Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewAssetCatalog()
	lin := fixtureAsset(t, catalog, "lin-errors", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage)
	shot := fixture.Groups[0].Shots[0]
	promptRegistry := NewPromptRegistry()
	prompt, err := (&PromptCompiler{
		Resolver: NewContextResolver(catalog), Catalog: catalog, Registry: promptRegistry,
	}).Compile(PromptCompileInput{
		ShotRevision: compilation.Shots[shot.ID], Shot: shot,
		ContextLayers: fixtureContexts(t, store, catalog, lin.Revision.ID, "episode-errors", "scene-errors", "shot-errors", at),
		TemplateRef:   "video-shot-v1", GenerationProfileRef: "profile-720p24-v1",
		Output: fixtureOutput(), CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		scenario providercontract.FakeScenario
		code     providercontract.ErrorCode
	}{
		{name: "401", scenario: providercontract.FakeUnauthorized, code: providercontract.CodeUnauthenticated},
		{name: "403", scenario: providercontract.FakeForbidden, code: providercontract.CodeForbidden},
		{name: "429", scenario: providercontract.FakeRateLimited, code: providercontract.CodeRateLimited},
		{name: "5xx", scenario: providercontract.FakeServerError, code: providercontract.CodeUnavailable},
		{name: "timeout", scenario: providercontract.FakeTimeout, code: providercontract.CodeTimeout},
		{name: "quota", scenario: providercontract.FakeQuotaExceeded, code: providercontract.CodeQuotaExceeded},
		{name: "content", scenario: providercontract.FakeContentBlocked, code: providercontract.CodeContentBlocked},
		{name: "region", scenario: providercontract.FakeRegionUnavailable, code: providercontract.CodeRegionUnavailable},
		{name: "model", scenario: providercontract.FakeModelUnavailable, code: providercontract.CodeModelUnavailable},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runner := NewGenerationRunner(providercontract.NewFakeProvider(test.scenario), fixtureCommitter{}, promptRegistry)
			clock := at
			runner.Now = func() time.Time {
				clock = clock.Add(time.Millisecond)
				return clock
			}
			runner.Wait = func(context.Context, time.Duration) error { return nil }
			input := fixtureGenerationInput(t, compilation.Source, compilation.Shots[shot.ID], prompt, at)
			input.RunID = "run-error-" + test.name
			input.IdempotencyKey = input.RunID
			input.BudgetReservation = fixtureBudgetReservation(t, input)
			first, firstErr := runner.Execute(t.Context(), input)
			if providercontract.ErrorCodeOf(firstErr) != test.code || first.State != RunFailed {
				t.Fatalf("first execute record=%#v error=%v", first, firstErr)
			}
			attemptCount := len(first.Attempts)
			second, secondErr := runner.Execute(t.Context(), input)
			if providercontract.ErrorCodeOf(secondErr) != test.code || second.State != RunFailed ||
				len(second.Attempts) != attemptCount {
				t.Fatalf("replay record=%#v error=%v", second, secondErr)
			}
			stored, ok := runner.Ledger.Get(input.RunID)
			if !ok || stored.State != RunFailed || len(stored.Attempts) != attemptCount {
				t.Fatalf("terminal ledger record = %#v", stored)
			}
		})
	}
}

func TestGoldenThirtyShots_CompleteMockLineage(t *testing.T) {
	fixture := loadGolden(t)
	if fixture.SchemaVersion != "v1" || len(fixture.Groups) != 3 {
		t.Fatalf("golden fixture header = %#v", fixture)
	}
	source, draft := goldenCompilationDraft(fixture)
	at := time.Unix(1_800_000_000, 0).UTC()
	store := NewRevisionStore()
	compilation, err := (&ContentCompiler{
		Store: store, Generator: FixtureContentGenerator{Draft: draft},
	}).Compile(t.Context(), source, DefaultCompileOptions(at))
	if err != nil {
		t.Fatal(err)
	}
	catalog := NewAssetCatalog()
	assets := map[string]AssetRevision{
		"char-lin":     fixtureAsset(t, catalog, "lin", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage),
		"char-mara":    fixtureAsset(t, catalog, "mara", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage),
		"prop-compass": fixtureAsset(t, catalog, "compass", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage),
		"prop-chart":   fixtureAsset(t, catalog, "chart", providercontract.ModalityImage, providercontract.AssetRoleReferenceImage),
	}
	promptCompiler := PromptCompiler{
		Resolver: NewContextResolver(catalog), Catalog: catalog, Registry: NewPromptRegistry(),
	}
	runner := NewGenerationRunner(providercontract.NewFakeProvider(providercontract.FakeSuccess), fixtureCommitter{}, promptCompiler.Registry)
	clock := at
	runner.Now = func() time.Time {
		clock = clock.Add(time.Millisecond)
		return clock
	}
	runner.Wait = func(context.Context, time.Duration) error { return nil }
	locker := NewPublicationLocker()

	categoryCount := make(map[string]int)
	total := 0
	for _, group := range fixture.Groups {
		if len(group.Shots) != 10 {
			t.Fatalf("category %q contains %d shots", group.Category, len(group.Shots))
		}
		for _, shot := range group.Shots {
			layers := fixtureContexts(t, store, catalog, assets["char-lin"].Revision.ID, group.EpisodeID, "scene-"+group.Category, shot.ID, at)
			var bindings []PromptAssetBinding
			if slices.Contains(shot.CharacterIDs, "char-mara") {
				bindings = append(bindings, PromptAssetBinding{
					Alias: "character.secondary", RevisionID: assets["char-mara"].Revision.ID,
					Role: providercontract.AssetRoleReferenceImage,
				})
			}
			for _, propID := range shot.PropIDs {
				bindings = append(bindings, PromptAssetBinding{
					Alias: "prop." + propID, RevisionID: assets[propID].Revision.ID,
					Role: providercontract.AssetRoleReferenceImage,
				})
			}
			prompt, compileErr := promptCompiler.Compile(PromptCompileInput{
				ShotRevision:         compilation.Shots[shot.ID],
				Shot:                 shot,
				ContextLayers:        layers,
				Assets:               bindings,
				TemplateRef:          "video-shot-v1",
				GenerationProfileRef: "profile-720p24-v1",
				Output:               fixtureOutput(),
				CreatedAt:            at.Add(time.Duration(total) * time.Second),
			})
			if compileErr != nil {
				t.Fatalf("compile %s: %v", shot.ID, compileErr)
			}
			runInput := fixtureGenerationInput(t, compilation.Source, compilation.Shots[shot.ID], prompt, at)
			runInput.RunID = "run-" + shot.ID
			runInput.IdempotencyKey = runInput.RunID
			runInput.BudgetReservation = fixtureBudgetReservation(t, runInput)
			record, runErr := runner.Execute(t.Context(), runInput)
			if runErr != nil {
				t.Fatalf("execute %s: %v", shot.ID, runErr)
			}
			manifest, ok := runner.Manifests.Get(record.ManifestID)
			if !ok || manifest.Manifest.ShotID != shot.ID ||
				manifest.Manifest.Context.SeriesSnapshotID == "" ||
				len(manifest.Manifest.InputAssets) == 0 ||
				len(manifest.Manifest.OutputAssets) != 1 ||
				manifest.Manifest.Status != providercontract.StatusSucceeded {
				t.Fatalf("incomplete lineage for %s: %#v", shot.ID, manifest)
			}
			qc := fixtureQuality(record, at.Add(3*time.Hour))
			gate3 := approvedGate(Gate3, record.ManifestID, record.ManifestHash, at.Add(4*time.Hour))
			if _, lockErr := locker.Lock(record, qc, gate3, at.Add(5*time.Hour)); lockErr != nil {
				t.Fatalf("G3 lock %s: %v", shot.ID, lockErr)
			}
			categoryCount[group.Category]++
			total++
		}
	}
	if total != 30 || categoryCount["single_character"] != 10 ||
		categoryCount["two_character_prop"] != 10 || categoryCount["motion_camera"] != 10 {
		t.Fatalf("golden coverage total=%d categories=%#v", total, categoryCount)
	}
}

func TestDownloadingCASCommitter_ConsumesTemporaryURLAndVerifiesDigest(t *testing.T) {
	t.Parallel()
	payload := []byte("fixture-video-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	committer := DownloadingCASCommitter{
		Store: store, Client: server.Client(), MaxBytes: 1 << 20,
	}
	asset := providercontract.AssetRef{
		ID: "provider-output", Revision: "provider-result", Kind: providercontract.ModalityVideo,
		Role: providercontract.AssetRoleOutput, URI: server.URL,
		SHA256: "pending_download", LicenseReference: "request-license",
	}
	committed, err := committer.Commit(t.Context(), asset)
	if err != nil {
		t.Fatal(err)
	}
	if committed.URI != "cas://sha256/"+committed.SHA256 || committed.MediaType != "video/mp4" ||
		committed.SizeBytes != int64(len(payload)) {
		t.Fatalf("committed asset = %#v", committed)
	}
	exists, err := store.Exists(committed.SHA256)
	if err != nil || !exists {
		t.Fatalf("CAS object exists=%t err=%v", exists, err)
	}

	asset.SHA256 = hashString("wrong")
	if _, err := committer.Commit(t.Context(), asset); !errors.Is(err, ErrConflict) {
		t.Fatalf("digest mismatch error = %v", err)
	}
}

func goldenCompilationDraft(fixture goldenSet) (NovelImport, CompilationDraft) {
	text := "Lin and Mara discover a compass in the observatory archive and follow its route across the rain bridge."
	evidenceID := "evidence-source-1"
	story := StoryStructure{
		World: World{
			ID: "world-1", Name: "Sky Archive", Premise: "A mechanical archive maps paths through the sky.",
			Rules: []string{"maps react to trusted hands"}, VisualStyle: "stylized 2.5D",
			EvidenceIDs: []string{evidenceID},
		},
		Characters: []Character{
			{ID: "char-lin", Name: "Lin", Role: "protagonist", Appearance: "blue travel coat", Personality: "curious and determined", EvidenceIDs: []string{evidenceID}},
			{ID: "char-mara", Name: "Mara", Role: "ally", Appearance: "amber archive uniform", Personality: "careful and loyal", EvidenceIDs: []string{evidenceID}},
		},
		Relationships: []Relationship{
			{ID: "rel-lin-mara", FromID: "char-lin", ToID: "char-mara", Kind: "trusted allies", Description: "They plan and travel together.", EvidenceIDs: []string{evidenceID}},
		},
		Locations: []Location{
			{ID: "location-observatory", Name: "Observatory", Description: "A moonlit mechanical observatory.", EvidenceIDs: []string{evidenceID}},
			{ID: "location-archive", Name: "Archive", Description: "A brass-lined map archive.", EvidenceIDs: []string{evidenceID}},
			{ID: "location-bridge", Name: "Rain Bridge", Description: "An exposed bridge above the city.", EvidenceIDs: []string{evidenceID}},
		},
		Props: []Prop{
			{ID: "prop-compass", Name: "Brass compass", Description: "A compass with a luminous needle.", OwnerID: "char-lin", EvidenceIDs: []string{evidenceID}},
			{ID: "prop-chart", Name: "Route chart", Description: "A foldable paper route chart.", OwnerID: "char-mara", EvidenceIDs: []string{evidenceID}},
		},
		Evidence: []EvidenceSpan{{
			ID: evidenceID, StartOffset: 0, EndOffset: len(text), ExcerptHash: hashString(text),
		}},
	}
	var episodes []EpisodeDraft
	for index, group := range fixture.Groups {
		locationID := "location-observatory"
		if group.Category == "two_character_prop" {
			locationID = "location-archive"
		}
		if group.Category == "motion_camera" {
			locationID = "location-bridge"
		}
		episodes = append(episodes, EpisodeDraft{
			ID: group.EpisodeID, Ordinal: index + 1, Title: group.Category,
			TargetDurationMillis: 50_000, Summary: "Golden fixture episode for " + group.Category,
			Scenes: []SceneDraft{{
				ID: "scene-" + group.Category, Ordinal: 1, Title: group.Category,
				LocationID: locationID, Summary: "Ten-shot golden scene.", Shots: group.Shots,
			}},
		})
	}
	source := NovelImport{
		SeriesID: "series-golden", SourceID: "source-golden", Title: "Sky Archive",
		Language: "en", Text: text, ImportedBy: "fixture-author",
		Rights: RightsDeclaration{
			Authorized: true, AdaptationAllowed: true, Owner: "fixture-owner",
			LicenseReference: "fixture-source-license",
		},
	}
	return source, CompilationDraft{Story: story, Episodes: episodes}
}

func fixtureAsset(t *testing.T, catalog *AssetCatalog, name string, kind providercontract.Modality, role providercontract.AssetRole) AssetRevision {
	t.Helper()
	digest := hashString("fixture-asset:" + name)
	asset := AssetRevision{
		Revision: RevisionRef{
			ID: "asset-revision-" + name, Kind: "asset", AggregateID: "asset-" + name,
			Number: 1, ContentHash: digest,
		},
		AssetID: "asset-" + name, Kind: kind, DefaultRole: role,
		URI: "cas://sha256/" + digest, MediaType: "image/png", Width: 1024, Height: 1024,
		LicenseReference: "fixture-asset-license", ApprovalID: "asset-approval-" + name,
		Authorized: true, Status: AssetActive,
	}
	if err := catalog.Add(asset); err != nil {
		t.Fatal(err)
	}
	return asset
}

func mustContext(
	t *testing.T,
	store *RevisionStore,
	scope Scope,
	id string,
	values map[string]string,
	assets map[string]string,
	at time.Time,
) ContextLayer {
	t.Helper()
	layer, err := CreateContextLayer(store, scope, id, values, assets, "fixture-editor", at)
	if err != nil {
		t.Fatal(err)
	}
	return layer
}

func fixtureContexts(
	t *testing.T,
	store *RevisionStore,
	catalog *AssetCatalog,
	primaryCharacterRevision string,
	episodeID string,
	sceneID string,
	shotID string,
	at time.Time,
) []ContextLayer {
	t.Helper()
	_ = catalog
	return []ContextLayer{
		mustContext(t, store, ScopeSeries, "series:"+shotID, map[string]string{
			"visual.style":        "stylized cinematic 2.5D",
			"visual.palette":      "amber and teal",
			"world.rules":         "stable identity and material continuity",
			"audience":            "general",
			"output.aspect_ratio": "16:9",
			"output.fps":          "24",
			"negative.global":     "identity drift, extra limbs, text artifacts",
		}, map[string]string{"character.primary": primaryCharacterRevision}, at),
		mustContext(t, store, ScopeEpisode, episodeID+":"+shotID, map[string]string{
			"story.mood":       "adventurous suspense",
			"story.pacing":     "clear five-second beat",
			"camera.language":  "motivated movement",
			"negative.episode": "tone discontinuity",
		}, nil, at),
		mustContext(t, store, ScopeScene, sceneID+":"+shotID, map[string]string{
			"scene.location": "golden fixture location",
			"scene.time":     "blue hour",
			"lighting":       "soft directional rim light",
			"negative.scene": "background discontinuity",
		}, nil, at),
		mustContext(t, store, ScopeShot, shotID, map[string]string{
			"camera.framing":   "match shot specification",
			"camera.angle":     "match shot specification",
			"camera.movement":  "one readable camera move",
			"motion.intensity": "controlled",
			"emotion":          "readable",
			"negative.shot":    "multiple primary actions",
		}, nil, at),
	}
}

func baseShot(id string, ordinal int) ShotSpec {
	return ShotSpec{
		ID: id, Ordinal: ordinal, Narrative: "Lin studies a luminous map.", DurationMillis: 5_000,
		CharacterIDs: []string{"char-lin"},
		Actions:      []Action{{CharacterID: "char-lin", Description: "raises one hand toward the map", Primary: true}},
		Expressions:  map[string]string{"char-lin": "focused curiosity"},
		Camera:       CameraSpec{ShotSize: "medium", Angle: "eye-level", Movement: "slow dolly", Lens: "50mm"},
		Continuity:   ContinuitySpec{EntryState: "hand lowered", ExitState: "hand raised"},
	}
}

func fixtureShotRevision(shot ShotSpec) RevisionRef {
	digest, _ := contentHash(shot)
	return RevisionRef{
		ID: derivedID("shot-revision", digest), Kind: KindShotSpec, AggregateID: shot.ID,
		Number: 1, ContentHash: digest,
	}
}

func fixtureOutput() providercontract.OutputSpec {
	return providercontract.OutputSpec{
		Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
		FPS: 24, DurationMillis: 5_000, Format: "mp4",
	}
}

func fixtureRoute() providercontract.ModelSnapshot {
	return providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilityVideo),
		Provider:        "fake",
		ModelID:         "fake-video-v1",
		RouteVersion:    "mock-routes-v1",
		CapabilityHash:  hashString("fake-video-capability"),
		Verification:    "mock_only",
	}
}

func approvedGate(gate Gate, bindingID, bindingHash string, at time.Time) GateApproval {
	return GateApproval{
		Gate: gate, DecisionID: "decision-" + string(gate) + "-" + bindingID,
		Approved: true, BindingID: bindingID, BindingHash: bindingHash,
		ActorID: "reviewer-" + string(gate), DecidedAt: at,
	}
}

func fixtureQuality(run RunRecord, at time.Time) QualityEvidence {
	reportHash := hashString("qc\x00" + run.RunID + "\x00" + run.ManifestHash)
	return QualityEvidence{
		ReportID: "qc-" + run.RunID, RunID: run.RunID,
		ManifestID: run.ManifestID, ManifestHash: run.ManifestHash,
		ThresholdVersion: "qc-threshold-v1", ReportHash: reportHash,
		Passed: true, CompletedAt: at,
	}
}

func fixtureGenerationInput(t *testing.T, source, shot RevisionRef, prompt PromptSnapshot, at time.Time) GenerationInput {
	t.Helper()
	runID := "run-" + shot.AggregateID
	input := GenerationInput{
		RunID: runID, IdempotencyKey: runID, SourceRevision: source, ShotRevision: shot, Prompt: prompt,
		Authorization: AuthorizationEvidence{
			SourceRevisionID: source.ID, SourceHash: source.ContentHash,
			LicenseReference: "fixture-source-license", Authorized: true, AdaptationAllowed: true,
		},
		Gate1: approvedGate(Gate1, source.ID, source.ContentHash, at),
		Gate2: approvedGate(Gate2, shot.ID, shot.ContentHash, at),
		Route: fixtureRoute(),
		Budget: providercontract.BudgetEnvelope{
			EstimatedCostMicros: 5_000_000, MaxCostMicros: 10_000_000, MaxAttempts: 3,
		},
		BudgetPolicy: providercontract.BudgetPolicy{
			SoftLimitMicros: 10_000_000, HardLimitMicros: 20_000_000, MaxAttempts: 3,
		},
		Evidence: providercontract.EvidenceMockOnly,
		MaxPolls: 3,
	}
	input.BudgetReservation = fixtureBudgetReservation(t, input)
	return input
}

func fixtureBudgetReservation(t *testing.T, input GenerationInput) providercontract.BudgetReservation {
	t.Helper()
	return fixtureBudgetReservationAmount(t, input, 10_000_000)
}

func fixtureBudgetReservationAmount(t *testing.T, input GenerationInput, amountMicros int64) providercontract.BudgetReservation {
	t.Helper()
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID: "budget-" + input.RunID, Currency: "CNY", AmountMicros: amountMicros,
		PricingVersion: "mock-pricing-v1", ConfirmedBy: "budget-reviewer",
	}, providercontract.BudgetBindingInput{
		RunID:     input.RunID,
		InputHash: input.Prompt.ContentHash,
		Model:     input.Route,
		Budget:    input.Budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reservation
}

type submitCountingProvider struct {
	providercontract.Provider
	submits int
}

func (p *submitCountingProvider) Submit(ctx context.Context, request providercontract.GenerationRequest) (providercontract.Job, error) {
	p.submits++
	return p.Provider.Submit(ctx, request)
}

type actualCostProvider struct {
	providercontract.Provider
	cost int64
}

func (p *actualCostProvider) Poll(ctx context.Context, jobID string) (providercontract.Job, error) {
	job, err := p.Provider.Poll(ctx, jobID)
	if err == nil && job.Output != nil {
		job.Output.Usage.ProviderCostMicros = p.cost
	}
	return job, err
}

type fixtureCommitter struct{}

func (fixtureCommitter) Commit(_ context.Context, asset providercontract.AssetRef) (providercontract.AssetRef, error) {
	if !validSHA256(asset.SHA256) {
		return providercontract.AssetRef{}, errors.New("fixture provider output lacks a digest")
	}
	asset.URI = "cas://sha256/" + asset.SHA256
	if asset.MediaType == "" {
		asset.MediaType = "video/mp4"
	}
	if asset.Revision == "" {
		asset.Revision = asset.SHA256
	}
	return RequireCASCommitter{}.Commit(context.Background(), asset)
}

type structuredFixtureProvider struct {
	output  string
	request providercontract.GenerationRequest
	job     providercontract.Job
	submits int
}

func (p *structuredFixtureProvider) Discover(context.Context) ([]providercontract.Capability, error) {
	return []providercontract.Capability{{
		Provider: "fixture-text", ModelFamily: "fixture-text-v1",
		OutputModality: providercontract.ModalityText, Verification: "mock_only",
	}}, nil
}

func (p *structuredFixtureProvider) Submit(_ context.Context, request providercontract.GenerationRequest) (providercontract.Job, error) {
	p.request = request
	p.submits++
	now := time.Unix(1_800_000_000, 0).UTC()
	p.job = providercontract.Job{
		ID: "fixture-text-job", RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		Status: providercontract.StatusSucceeded, Provider: "fixture-text",
		ProviderModel: "fixture-text-v1", ProviderRegion: "local-fixture",
		ProviderRequestID: "fixture-text-request", CreatedAt: now, UpdatedAt: now,
		Output: &providercontract.Output{Text: p.output},
	}
	return p.job, nil
}

func (p *structuredFixtureProvider) Poll(context.Context, string) (providercontract.Job, error) {
	return p.job, nil
}

func (p *structuredFixtureProvider) Cancel(context.Context, string) (providercontract.Job, error) {
	p.job.Status = providercontract.StatusCancelled
	return p.job, nil
}
