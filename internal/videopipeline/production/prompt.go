package production

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const (
	PromptSchemaVersion   = "v1"
	PromptCompilerVersion = "prompt-compiler-v1"
)

type PromptAssetBinding struct {
	Alias      string                     `json:"alias"`
	RevisionID string                     `json:"revision_id"`
	Role       providercontract.AssetRole `json:"role"`
}

type SubtitleCue struct {
	StartMillis int    `json:"start_millis"`
	EndMillis   int    `json:"end_millis"`
	SpeakerID   string `json:"speaker_id"`
	Text        string `json:"text"`
}

// PromptSnapshot freezes every prompt input. Later changes produce a new
// snapshot and never mutate or retarget an accepted provider request.
type PromptSnapshot struct {
	ID                       string                      `json:"id"`
	RevisionNumber           int                         `json:"revision_number"`
	SchemaVersion            string                      `json:"schema_version"`
	CompilerVersion          string                      `json:"compiler_version"`
	TemplateRef              string                      `json:"template_ref"`
	GenerationProfileRef     string                      `json:"generation_profile_ref"`
	ShotRevision             RevisionRef                 `json:"shot_revision"`
	EffectiveContext         EffectiveContext            `json:"effective_context"`
	Assets                   []providercontract.AssetRef `json:"assets"`
	AssetAliases             map[string]string           `json:"asset_aliases"`
	PreviousPromptSnapshotID string                      `json:"previous_prompt_snapshot_id,omitempty"`
	PreviousPromptHash       string                      `json:"previous_prompt_hash,omitempty"`
	TailFrameHash            string                      `json:"tail_frame_hash,omitempty"`
	PositivePrompt           string                      `json:"positive_prompt"`
	NegativePrompt           string                      `json:"negative_prompt"`
	SubtitleTimeline         []SubtitleCue               `json:"subtitle_timeline,omitempty"`
	Output                   providercontract.OutputSpec `json:"output"`
	ModelPayload             map[string]any              `json:"model_payload"`
	InputRevisionHashes      map[string]string           `json:"input_revision_hashes"`
	NormalizedInputHash      string                      `json:"normalized_input_hash"`
	ContentHash              string                      `json:"content_hash"`
	EvidenceIDs              []string                    `json:"evidence_ids,omitempty"`
	CreatedAt                time.Time                   `json:"created_at"`
}

func (p PromptSnapshot) Ref() RevisionRef {
	return RevisionRef{
		ID:          p.ID,
		Kind:        KindPrompt,
		AggregateID: p.ShotRevision.AggregateID,
		Number:      p.RevisionNumber,
		ContentHash: p.ContentHash,
	}
}

type PromptCompileInput struct {
	ShotRevision         RevisionRef
	Shot                 ShotSpec
	ContextLayers        []ContextLayer
	Assets               []PromptAssetBinding
	Previous             *PromptSnapshot
	TemplateRef          string
	GenerationProfileRef string
	Output               providercontract.OutputSpec
	EvidenceIDs          []string
	CreatedAt            time.Time
}

type PromptCompiler struct {
	Resolver *ContextResolver
	Catalog  *AssetCatalog
	Registry *PromptRegistry
}

func (c *PromptCompiler) Compile(input PromptCompileInput) (PromptSnapshot, error) {
	if c.Resolver == nil || c.Catalog == nil || c.Registry == nil {
		return PromptSnapshot{}, validationf("context resolver, asset catalog, and prompt registry are required")
	}
	if err := input.ShotRevision.Validate(); err != nil {
		return PromptSnapshot{}, err
	}
	if input.ShotRevision.Kind != KindShotSpec || input.ShotRevision.AggregateID != input.Shot.ID {
		return PromptSnapshot{}, conflictf("shot payload is not pinned to the supplied shot revision")
	}
	if err := input.Shot.Validate(4_000, 6_000); err != nil {
		return PromptSnapshot{}, err
	}
	if !nonEmpty(input.TemplateRef, input.GenerationProfileRef) || input.CreatedAt.IsZero() {
		return PromptSnapshot{}, validationf("template, generation profile, and creation time are required")
	}
	if err := validateOutput(input.Output, input.Shot.DurationMillis); err != nil {
		return PromptSnapshot{}, err
	}
	effective, err := c.Resolver.Resolve(input.ContextLayers)
	if err != nil {
		return PromptSnapshot{}, err
	}
	if err := validateOutputContext(effective, input.Output); err != nil {
		return PromptSnapshot{}, err
	}

	assetByAlias := maps.Clone(effective.Assets)
	assetAliases := make(map[string]string, len(assetByAlias)+len(input.Assets)+1)
	for alias, asset := range assetByAlias {
		assetAliases[alias] = asset.Revision
	}
	for _, binding := range input.Assets {
		if !nonEmpty(binding.Alias, binding.RevisionID) || binding.Role == "" {
			return PromptSnapshot{}, validationf("prompt assets require alias, revision, and role")
		}
		if existing, duplicate := assetAliases[binding.Alias]; duplicate && existing != binding.RevisionID {
			return PromptSnapshot{}, conflictf("prompt asset alias %q resolves to two revisions", binding.Alias)
		}
		asset, resolveErr := c.Catalog.Resolve(binding.RevisionID)
		if resolveErr != nil {
			return PromptSnapshot{}, resolveErr
		}
		assetByAlias[binding.Alias] = asset.ProviderRef(binding.Role)
		assetAliases[binding.Alias] = binding.RevisionID
	}

	previousID := ""
	previousHash := ""
	if input.Shot.Continuity.PreviousPromptSnapshotID != "" {
		if input.Previous == nil ||
			input.Previous.ID != input.Shot.Continuity.PreviousPromptSnapshotID ||
			!validSHA256(input.Previous.ContentHash) {
			return PromptSnapshot{}, fmt.Errorf("%w: previous prompt snapshot does not match continuity", ErrStaleReference)
		}
		previousID = input.Previous.ID
		previousHash = input.Previous.ContentHash
	} else if input.Previous != nil {
		return PromptSnapshot{}, conflictf("unreferenced previous prompt snapshot was supplied")
	}
	tailFrameHash := ""
	if tailRevisionID := input.Shot.Continuity.TailFrameAssetRevisionID; tailRevisionID != "" {
		tail, resolveErr := c.Catalog.Resolve(tailRevisionID)
		if resolveErr != nil {
			return PromptSnapshot{}, resolveErr
		}
		if tail.Kind != providercontract.ModalityImage {
			return PromptSnapshot{}, validationf("tail frame asset must be an image")
		}
		assetByAlias["continuity.tail_frame"] = tail.ProviderRef(providercontract.AssetRoleLastFrame)
		assetAliases["continuity.tail_frame"] = tailRevisionID
		tailFrameHash = tail.Revision.ContentHash
	}

	assets := orderedPromptAssets(assetByAlias)
	positive, negative := renderPrompt(input.Shot, effective, assetByAlias, previousHash, tailFrameHash, input.Output)
	subtitles := make([]SubtitleCue, 0, len(input.Shot.Dialogue))
	for _, cue := range input.Shot.Dialogue {
		subtitles = append(subtitles, SubtitleCue{
			StartMillis: cue.StartMS,
			EndMillis:   cue.EndMS,
			SpeakerID:   cue.CharacterID,
			Text:        cue.Text,
		})
	}
	inputHashes := map[string]string{
		"shot_spec":         input.ShotRevision.ContentHash,
		"effective_context": effective.ContentHash,
	}
	for scope, digest := range effective.RevisionHashes {
		inputHashes["context."+string(scope)] = digest
	}
	for alias, asset := range assetByAlias {
		inputHashes["asset."+alias] = asset.SHA256
	}
	if previousHash != "" {
		inputHashes["previous_prompt"] = previousHash
	}
	if tailFrameHash != "" {
		inputHashes["tail_frame"] = tailFrameHash
	}
	modelPayload := map[string]any{
		"schema_version":      "provider-neutral-video-v1",
		"positive_prompt":     positive,
		"negative_prompt":     negative,
		"reference_asset_ids": slices.Sorted(maps.Keys(assetAliases)),
		"duration_millis":     input.Output.DurationMillis,
		"aspect_ratio":        input.Output.AspectRatio,
		"resolution":          input.Output.Resolution,
		"fps":                 input.Output.FPS,
		"continuity": map[string]any{
			"previous_prompt_hash": previousHash,
			"tail_frame_hash":      tailFrameHash,
			"entry_state":          input.Shot.Continuity.EntryState,
			"exit_state":           input.Shot.Continuity.ExitState,
		},
	}
	normalizedMaterial := struct {
		CompilerVersion    string                      `json:"compiler_version"`
		TemplateRef        string                      `json:"template_ref"`
		ProfileRef         string                      `json:"profile_ref"`
		ShotRevision       RevisionRef                 `json:"shot_revision"`
		Shot               ShotSpec                    `json:"shot"`
		EffectiveContext   EffectiveContext            `json:"effective_context"`
		Assets             []providercontract.AssetRef `json:"assets"`
		PreviousPromptHash string                      `json:"previous_prompt_hash,omitempty"`
		TailFrameHash      string                      `json:"tail_frame_hash,omitempty"`
		Output             providercontract.OutputSpec `json:"output"`
	}{
		CompilerVersion:    PromptCompilerVersion,
		TemplateRef:        input.TemplateRef,
		ProfileRef:         input.GenerationProfileRef,
		ShotRevision:       input.ShotRevision,
		Shot:               input.Shot,
		EffectiveContext:   effective,
		Assets:             assets,
		PreviousPromptHash: previousHash,
		TailFrameHash:      tailFrameHash,
		Output:             input.Output,
	}
	normalizedHash, err := contentHash(normalizedMaterial)
	if err != nil {
		return PromptSnapshot{}, err
	}
	snapshotMaterial := struct {
		SchemaVersion       string                      `json:"schema_version"`
		CompilerVersion     string                      `json:"compiler_version"`
		NormalizedInputHash string                      `json:"normalized_input_hash"`
		PositivePrompt      string                      `json:"positive_prompt"`
		NegativePrompt      string                      `json:"negative_prompt"`
		ModelPayload        map[string]any              `json:"model_payload"`
		Subtitles           []SubtitleCue               `json:"subtitles"`
		Output              providercontract.OutputSpec `json:"output"`
	}{
		SchemaVersion:       PromptSchemaVersion,
		CompilerVersion:     PromptCompilerVersion,
		NormalizedInputHash: normalizedHash,
		PositivePrompt:      positive,
		NegativePrompt:      negative,
		ModelPayload:        modelPayload,
		Subtitles:           subtitles,
		Output:              input.Output,
	}
	digest, err := contentHash(snapshotMaterial)
	if err != nil {
		return PromptSnapshot{}, err
	}
	snapshot := PromptSnapshot{
		ID:                       derivedID("prompt", digest),
		SchemaVersion:            PromptSchemaVersion,
		CompilerVersion:          PromptCompilerVersion,
		TemplateRef:              input.TemplateRef,
		GenerationProfileRef:     input.GenerationProfileRef,
		ShotRevision:             input.ShotRevision,
		EffectiveContext:         effective,
		Assets:                   assets,
		AssetAliases:             assetAliases,
		PreviousPromptSnapshotID: previousID,
		PreviousPromptHash:       previousHash,
		TailFrameHash:            tailFrameHash,
		PositivePrompt:           positive,
		NegativePrompt:           negative,
		SubtitleTimeline:         subtitles,
		Output:                   input.Output,
		ModelPayload:             modelPayload,
		InputRevisionHashes:      inputHashes,
		NormalizedInputHash:      normalizedHash,
		ContentHash:              digest,
		EvidenceIDs:              sortedUnique(input.EvidenceIDs),
		CreatedAt:                input.CreatedAt.UTC(),
	}
	return c.Registry.Put(snapshot)
}

func BuildGenerationRequest(
	snapshot PromptSnapshot,
	requestID string,
	idempotencyKey string,
	callbackURL string,
	budget providercontract.BudgetEnvelope,
) (providercontract.GenerationRequest, error) {
	if err := snapshot.Ref().Validate(); err != nil {
		return providercontract.GenerationRequest{}, err
	}
	prompt := snapshot.PositivePrompt
	if snapshot.NegativePrompt != "" {
		prompt += "\nNEGATIVE CONSTRAINTS: " + snapshot.NegativePrompt
	}
	request := providercontract.GenerationRequest{
		RequestID:        requestID,
		IdempotencyKey:   idempotencyKey,
		Modality:         providercontract.ModalityVideo,
		Prompt:           prompt,
		PromptSnapshotID: snapshot.ID,
		Context:          snapshot.EffectiveContext.RevisionRefs,
		Assets:           slices.Clone(snapshot.Assets),
		Output:           snapshot.Output,
		CallbackURL:      callbackURL,
		Budget:           budget,
	}
	if err := request.Validate(); err != nil {
		return providercontract.GenerationRequest{}, err
	}
	return request, nil
}

func validateOutput(output providercontract.OutputSpec, shotDuration int) error {
	if output.Width <= 0 || output.Height <= 0 || output.Resolution == "" ||
		output.AspectRatio == "" || output.FPS <= 0 || output.DurationMillis != shotDuration ||
		output.Format == "" {
		return validationf("complete output specification must match the shot duration")
	}
	return nil
}

func validateOutputContext(context EffectiveContext, output providercontract.OutputSpec) error {
	if ratio, ok := context.Values["output.aspect_ratio"]; ok && ratio != output.AspectRatio {
		return conflictf("output aspect ratio %q conflicts with locked series context %q", output.AspectRatio, ratio)
	}
	if rawFPS, ok := context.Values["output.fps"]; ok {
		fps, err := strconv.Atoi(rawFPS)
		if err != nil || fps != output.FPS {
			return conflictf("output FPS %d conflicts with locked series context %q", output.FPS, rawFPS)
		}
	}
	return nil
}

func orderedPromptAssets(values map[string]providercontract.AssetRef) []providercontract.AssetRef {
	aliases := slices.Sorted(maps.Keys(values))
	result := make([]providercontract.AssetRef, 0, len(aliases))
	for _, alias := range aliases {
		result = append(result, values[alias])
	}
	return result
}

func renderPrompt(
	shot ShotSpec,
	context EffectiveContext,
	assets map[string]providercontract.AssetRef,
	previousPromptHash string,
	tailFrameHash string,
	output providercontract.OutputSpec,
) (string, string) {
	var prompt strings.Builder
	prompt.WriteString("Generate one continuity-safe cinematic shot.\n")
	keys := slices.Sorted(maps.Keys(context.Values))
	for _, key := range keys {
		if strings.HasPrefix(key, "negative.") {
			continue
		}
		fmt.Fprintf(&prompt, "CONTEXT %s=%s\n", key, context.Values[key])
	}
	fmt.Fprintf(&prompt, "SHOT narrative=%s\n", normalizePromptText(shot.Narrative))
	fmt.Fprintf(&prompt, "CAMERA size=%s; angle=%s; movement=%s; lens=%s\n",
		shot.Camera.ShotSize, shot.Camera.Angle, shot.Camera.Movement, shot.Camera.Lens)
	if len(shot.CharacterIDs) > 0 {
		fmt.Fprintf(&prompt, "CAST %s\n", strings.Join(shot.CharacterIDs, ","))
	}
	for _, action := range shot.Actions {
		label := "secondary"
		if action.Primary {
			label = "primary"
		}
		fmt.Fprintf(&prompt, "ACTION %s character=%s; %s\n", label, action.CharacterID, normalizePromptText(action.Description))
	}
	expressionIDs := slices.Sorted(maps.Keys(shot.Expressions))
	for _, characterID := range expressionIDs {
		fmt.Fprintf(&prompt, "EXPRESSION character=%s; %s\n", characterID, normalizePromptText(shot.Expressions[characterID]))
	}
	for _, cue := range shot.Dialogue {
		fmt.Fprintf(&prompt, "DIALOGUE %d-%dms character=%s; %s\n", cue.StartMS, cue.EndMS, cue.CharacterID, normalizePromptText(cue.Text))
	}
	if len(shot.PropIDs) > 0 {
		fmt.Fprintf(&prompt, "PROPS %s\n", strings.Join(shot.PropIDs, ","))
	}
	fmt.Fprintf(&prompt, "CONTINUITY entry=%s; exit=%s", normalizePromptText(shot.Continuity.EntryState), normalizePromptText(shot.Continuity.ExitState))
	if previousPromptHash != "" {
		fmt.Fprintf(&prompt, "; previous_prompt_sha256=%s", previousPromptHash)
	}
	if tailFrameHash != "" {
		fmt.Fprintf(&prompt, "; tail_frame_sha256=%s", tailFrameHash)
	}
	prompt.WriteByte('\n')
	aliases := slices.Sorted(maps.Keys(assets))
	for _, alias := range aliases {
		asset := assets[alias]
		fmt.Fprintf(&prompt, "REFERENCE alias=%s; asset=%s; revision=%s; sha256=%s; role=%s\n",
			alias, asset.ID, asset.Revision, asset.SHA256, asset.Role)
	}
	fmt.Fprintf(&prompt, "OUTPUT duration_ms=%d; resolution=%s; aspect_ratio=%s; fps=%d; format=%s",
		output.DurationMillis, output.Resolution, output.AspectRatio, output.FPS, output.Format)

	var negatives []string
	for _, key := range keys {
		if strings.HasPrefix(key, "negative.") {
			negatives = append(negatives, context.Values[key])
		}
	}
	return prompt.String(), strings.Join(negatives, "; ")
}

type PromptRegistry struct {
	mu     sync.RWMutex
	byID   map[string]PromptSnapshot
	byShot map[string][]string
}

func NewPromptRegistry() *PromptRegistry {
	return &PromptRegistry{
		byID:   make(map[string]PromptSnapshot),
		byShot: make(map[string][]string),
	}
}

func (r *PromptRegistry) Put(snapshot PromptSnapshot) (PromptSnapshot, error) {
	if !nonEmpty(snapshot.ID, snapshot.ContentHash, snapshot.NormalizedInputHash, snapshot.ShotRevision.ID) ||
		!validSHA256(snapshot.ContentHash) || !validSHA256(snapshot.NormalizedInputHash) {
		return PromptSnapshot{}, validationf("complete immutable prompt snapshot is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byID[snapshot.ID]; ok {
		if existing.ContentHash != snapshot.ContentHash {
			return PromptSnapshot{}, conflictf("prompt snapshot %q cannot be mutated", snapshot.ID)
		}
		return clonePromptSnapshot(existing), nil
	}
	chain := r.byShot[snapshot.ShotRevision.AggregateID]
	snapshot.RevisionNumber = len(chain) + 1
	r.byID[snapshot.ID] = clonePromptSnapshot(snapshot)
	r.byShot[snapshot.ShotRevision.AggregateID] = append(chain, snapshot.ID)
	return clonePromptSnapshot(snapshot), nil
}

func (r *PromptRegistry) Get(id string) (PromptSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.byID[id]
	return clonePromptSnapshot(snapshot), ok
}

func (r *PromptRegistry) History(shotID string) []PromptSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.byShot[shotID]
	result := make([]PromptSnapshot, 0, len(ids))
	for _, id := range ids {
		result = append(result, clonePromptSnapshot(r.byID[id]))
	}
	return result
}

func clonePromptSnapshot(snapshot PromptSnapshot) PromptSnapshot {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return PromptSnapshot{}
	}
	var cloned PromptSnapshot
	if err := json.Unmarshal(data, &cloned); err != nil {
		return PromptSnapshot{}
	}
	return cloned
}

type PromptDiff struct {
	ChangedShot           bool     `json:"changed_shot"`
	ChangedContextKeys    []string `json:"changed_context_keys"`
	ChangedAssetAliases   []string `json:"changed_asset_aliases"`
	PreviousPromptChanged bool     `json:"previous_prompt_changed"`
	TailFrameChanged      bool     `json:"tail_frame_changed"`
	OutputChanged         bool     `json:"output_changed"`
}

func DiffPrompts(before, after PromptSnapshot) PromptDiff {
	diff := PromptDiff{
		ChangedShot:           before.ShotRevision.ContentHash != after.ShotRevision.ContentHash,
		PreviousPromptChanged: before.PreviousPromptHash != after.PreviousPromptHash,
		TailFrameChanged:      before.TailFrameHash != after.TailFrameHash,
	}
	beforeOutput, _ := contentHash(before.Output)
	afterOutput, _ := contentHash(after.Output)
	diff.OutputChanged = beforeOutput != afterOutput
	contextKeys := make(map[string]struct{})
	for key := range before.EffectiveContext.Values {
		contextKeys[key] = struct{}{}
	}
	for key := range after.EffectiveContext.Values {
		contextKeys[key] = struct{}{}
	}
	for _, key := range slices.Sorted(maps.Keys(contextKeys)) {
		if before.EffectiveContext.Values[key] != after.EffectiveContext.Values[key] {
			diff.ChangedContextKeys = append(diff.ChangedContextKeys, key)
		}
	}
	assetAliases := make(map[string]struct{})
	for alias := range before.AssetAliases {
		assetAliases[alias] = struct{}{}
	}
	for alias := range after.AssetAliases {
		assetAliases[alias] = struct{}{}
	}
	for _, alias := range slices.Sorted(maps.Keys(assetAliases)) {
		if before.AssetAliases[alias] != after.AssetAliases[alias] {
			diff.ChangedAssetAliases = append(diff.ChangedAssetAliases, alias)
		}
	}
	return diff
}
