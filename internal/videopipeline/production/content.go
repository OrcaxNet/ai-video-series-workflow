package production

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

const (
	KindSource       = "source"
	KindWorld        = "world"
	KindCharacter    = "character"
	KindRelationship = "relationship"
	KindLocation     = "location"
	KindProp         = "prop"
	KindEpisode      = "episode"
	KindScene        = "scene"
	KindShotSpec     = "shot_spec"
	KindContext      = "context"
	KindPrompt       = "prompt"
)

type RightsDeclaration struct {
	Authorized        bool      `json:"authorized"`
	AdaptationAllowed bool      `json:"adaptation_allowed"`
	Owner             string    `json:"owner"`
	LicenseReference  string    `json:"license_reference"`
	Territories       []string  `json:"territories,omitempty"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
}

func (r RightsDeclaration) Validate(at time.Time) error {
	if !r.Authorized || !r.AdaptationAllowed {
		return policyf("source authorization and adaptation permission are required")
	}
	if !nonEmpty(r.Owner, r.LicenseReference) {
		return validationf("rights owner and license reference are required")
	}
	if !r.ExpiresAt.IsZero() && !r.ExpiresAt.After(at) {
		return policyf("source authorization is expired")
	}
	return nil
}

type NovelImport struct {
	SeriesID   string            `json:"series_id"`
	SourceID   string            `json:"source_id"`
	Title      string            `json:"title"`
	Language   string            `json:"language"`
	Text       string            `json:"text"`
	Rights     RightsDeclaration `json:"rights"`
	ImportedBy string            `json:"imported_by"`
}

func (n NovelImport) Validate(at time.Time) error {
	if !nonEmpty(n.SeriesID, n.SourceID, n.Title, n.Language, n.Text, n.ImportedBy) {
		return validationf("series, source, title, language, text, and actor are required")
	}
	if err := n.Rights.Validate(at); err != nil {
		return err
	}
	return nil
}

type EvidenceSpan struct {
	ID          string `json:"id"`
	ChapterRef  string `json:"chapter_ref,omitempty"`
	StartOffset int    `json:"start_offset"`
	EndOffset   int    `json:"end_offset"`
	ExcerptHash string `json:"excerpt_hash"`
}

type World struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Premise     string            `json:"premise"`
	Rules       []string          `json:"rules"`
	VisualStyle string            `json:"visual_style"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	EvidenceIDs []string          `json:"evidence_ids"`
}

type Character struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Role        string            `json:"role"`
	Appearance  string            `json:"appearance"`
	Personality string            `json:"personality"`
	Voice       string            `json:"voice,omitempty"`
	States      map[string]string `json:"states,omitempty"`
	EvidenceIDs []string          `json:"evidence_ids"`
}

type Relationship struct {
	ID          string   `json:"id"`
	FromID      string   `json:"from_id"`
	ToID        string   `json:"to_id"`
	Kind        string   `json:"kind"`
	Description string   `json:"description"`
	EvidenceIDs []string `json:"evidence_ids"`
}

type Location struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	EvidenceIDs []string          `json:"evidence_ids"`
}

type Prop struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	OwnerID     string   `json:"owner_id,omitempty"`
	EvidenceIDs []string `json:"evidence_ids"`
}

type StoryStructure struct {
	World         World          `json:"world"`
	Characters    []Character    `json:"characters"`
	Relationships []Relationship `json:"relationships"`
	Locations     []Location     `json:"locations"`
	Props         []Prop         `json:"props"`
	Evidence      []EvidenceSpan `json:"evidence"`
}

type Action struct {
	CharacterID string `json:"character_id"`
	Description string `json:"description"`
	Primary     bool   `json:"primary"`
}

type DialogueCue struct {
	CharacterID string `json:"character_id"`
	Text        string `json:"text"`
	StartMS     int    `json:"start_ms"`
	EndMS       int    `json:"end_ms"`
}

type CameraSpec struct {
	ShotSize string `json:"shot_size"`
	Angle    string `json:"angle"`
	Movement string `json:"movement"`
	Lens     string `json:"lens,omitempty"`
}

type ContinuitySpec struct {
	PredecessorShotRevisionID string `json:"predecessor_shot_revision_id,omitempty"`
	PreviousPromptSnapshotID  string `json:"previous_prompt_snapshot_id,omitempty"`
	TailFrameAssetRevisionID  string `json:"tail_frame_asset_revision_id,omitempty"`
	EntryState                string `json:"entry_state"`
	ExitState                 string `json:"exit_state"`
}

type ShotSpec struct {
	ID                string            `json:"id"`
	Ordinal           int               `json:"ordinal"`
	Narrative         string            `json:"narrative"`
	DurationMillis    int               `json:"duration_millis"`
	CharacterIDs      []string          `json:"character_ids"`
	Actions           []Action          `json:"actions"`
	Expressions       map[string]string `json:"expressions"`
	Dialogue          []DialogueCue     `json:"dialogue,omitempty"`
	PropIDs           []string          `json:"prop_ids,omitempty"`
	Camera            CameraSpec        `json:"camera"`
	Continuity        ContinuitySpec    `json:"continuity"`
	ContextValues     map[string]string `json:"context_values,omitempty"`
	AssetRevisionRefs []string          `json:"asset_revision_refs,omitempty"`
}

func (s ShotSpec) Validate(minDuration, maxDuration int) error {
	if !nonEmpty(s.ID, s.Narrative, s.Camera.ShotSize, s.Camera.Angle, s.Camera.Movement) ||
		s.Ordinal < 1 {
		return validationf("shot ID, ordinal, narrative, and camera specification are required")
	}
	if s.DurationMillis < minDuration || s.DurationMillis > maxDuration {
		return validationf("shot %q duration %d is outside [%d,%d]", s.ID, s.DurationMillis, minDuration, maxDuration)
	}
	if len(s.CharacterIDs) > 2 {
		return validationf("shot %q contains more than two characters", s.ID)
	}
	if len(s.Actions) == 0 {
		return validationf("shot %q requires an action", s.ID)
	}
	primary := 0
	for _, action := range s.Actions {
		if !nonEmpty(action.CharacterID, action.Description) {
			return validationf("shot %q has an incomplete action", s.ID)
		}
		if action.Primary {
			primary++
		}
	}
	if primary != 1 {
		return validationf("shot %q requires exactly one primary action", s.ID)
	}
	characters := make(map[string]struct{}, len(s.CharacterIDs))
	for _, id := range s.CharacterIDs {
		if id == "" {
			return validationf("shot %q has an empty character reference", s.ID)
		}
		characters[id] = struct{}{}
	}
	for characterID := range s.Expressions {
		if _, ok := characters[characterID]; !ok {
			return validationf("shot %q expression references character %q outside its cast", s.ID, characterID)
		}
	}
	for _, cue := range s.Dialogue {
		if _, ok := characters[cue.CharacterID]; !ok {
			return validationf("shot %q dialogue references character %q outside its cast", s.ID, cue.CharacterID)
		}
		if cue.Text == "" || cue.StartMS < 0 || cue.EndMS <= cue.StartMS || cue.EndMS > s.DurationMillis {
			return validationf("shot %q has an invalid dialogue cue", s.ID)
		}
	}
	if !nonEmpty(s.Continuity.EntryState, s.Continuity.ExitState) {
		return validationf("shot %q requires entry and exit continuity states", s.ID)
	}
	return nil
}

type SceneDraft struct {
	ID            string            `json:"id"`
	Ordinal       int               `json:"ordinal"`
	Title         string            `json:"title"`
	LocationID    string            `json:"location_id"`
	Summary       string            `json:"summary"`
	ContextValues map[string]string `json:"context_values,omitempty"`
	Shots         []ShotSpec        `json:"shots"`
}

type EpisodeDraft struct {
	ID                   string            `json:"id"`
	Ordinal              int               `json:"ordinal"`
	Title                string            `json:"title"`
	TargetDurationMillis int               `json:"target_duration_millis"`
	Summary              string            `json:"summary"`
	ContextValues        map[string]string `json:"context_values,omitempty"`
	Scenes               []SceneDraft      `json:"scenes"`
}

type CompilationDraft struct {
	Story    StoryStructure `json:"story"`
	Episodes []EpisodeDraft `json:"episodes"`
}

type ContentGenerationRequest struct {
	SourceRevision RevisionRef `json:"source_revision"`
	SourceHash     string      `json:"source_hash"`
	Title          string      `json:"title"`
	Language       string      `json:"language"`
	Text           string      `json:"text"`
}

// ContentGenerator is the provider-neutral structured-output boundary. A live
// implementation may use any configured text provider; deterministic fixtures
// implement the same interface without a key.
type ContentGenerator interface {
	Generate(context.Context, ContentGenerationRequest) (CompilationDraft, error)
}

type CompileOptions struct {
	ShotMinMillis    int
	ShotMaxMillis    int
	EpisodeMinMillis int
	EpisodeMaxMillis int
	CreatedAt        time.Time
}

func DefaultCompileOptions(at time.Time) CompileOptions {
	return CompileOptions{
		ShotMinMillis:    4_000,
		ShotMaxMillis:    6_000,
		EpisodeMinMillis: 45_000,
		EpisodeMaxMillis: 60_000,
		CreatedAt:        at,
	}
}

type CompilationResult struct {
	Source        RevisionRef            `json:"source"`
	World         RevisionRef            `json:"world"`
	Characters    map[string]RevisionRef `json:"characters"`
	Relationships map[string]RevisionRef `json:"relationships"`
	Locations     map[string]RevisionRef `json:"locations"`
	Props         map[string]RevisionRef `json:"props"`
	Episodes      map[string]RevisionRef `json:"episodes"`
	Scenes        map[string]RevisionRef `json:"scenes"`
	Shots         map[string]RevisionRef `json:"shots"`
}

type ContentCompiler struct {
	Store     *RevisionStore
	Generator ContentGenerator
}

func (c *ContentCompiler) Compile(ctx context.Context, source NovelImport, options CompileOptions) (CompilationResult, error) {
	if c.Store == nil || c.Generator == nil {
		return CompilationResult{}, validationf("revision store and content generator are required")
	}
	if err := source.Validate(options.CreatedAt); err != nil {
		return CompilationResult{}, err
	}
	if options.ShotMinMillis <= 0 || options.ShotMaxMillis < options.ShotMinMillis ||
		options.EpisodeMinMillis <= 0 || options.EpisodeMaxMillis < options.EpisodeMinMillis {
		return CompilationResult{}, validationf("valid shot and episode duration bounds are required")
	}
	sourceRevision, err := c.Store.CreateNext(
		KindSource,
		source.SourceID,
		source,
		nil,
		source.ImportedBy,
		options.CreatedAt,
	)
	if err != nil {
		return CompilationResult{}, err
	}
	draft, err := c.Generator.Generate(ctx, ContentGenerationRequest{
		SourceRevision: sourceRevision.Ref(),
		SourceHash:     sourceRevision.ContentHash,
		Title:          source.Title,
		Language:       source.Language,
		Text:           source.Text,
	})
	if err != nil {
		return CompilationResult{}, fmt.Errorf("generate structured content: %w", err)
	}
	if err := validateCompilation(source.Text, draft, options); err != nil {
		return CompilationResult{}, err
	}

	result := CompilationResult{
		Source:        sourceRevision.Ref(),
		Characters:    make(map[string]RevisionRef),
		Relationships: make(map[string]RevisionRef),
		Locations:     make(map[string]RevisionRef),
		Props:         make(map[string]RevisionRef),
		Episodes:      make(map[string]RevisionRef),
		Scenes:        make(map[string]RevisionRef),
		Shots:         make(map[string]RevisionRef),
	}
	world, err := c.Store.CreateNext(KindWorld, draft.Story.World.ID, draft.Story.World, draft.Story.World.EvidenceIDs, source.ImportedBy, options.CreatedAt)
	if err != nil {
		return CompilationResult{}, err
	}
	result.World = world.Ref()
	for _, character := range draft.Story.Characters {
		revision, createErr := c.Store.CreateNext(KindCharacter, character.ID, character, character.EvidenceIDs, source.ImportedBy, options.CreatedAt)
		if createErr != nil {
			return CompilationResult{}, createErr
		}
		result.Characters[character.ID] = revision.Ref()
	}
	for _, relationship := range draft.Story.Relationships {
		revision, createErr := c.Store.CreateNext(KindRelationship, relationship.ID, relationship, relationship.EvidenceIDs, source.ImportedBy, options.CreatedAt)
		if createErr != nil {
			return CompilationResult{}, createErr
		}
		result.Relationships[relationship.ID] = revision.Ref()
	}
	for _, location := range draft.Story.Locations {
		revision, createErr := c.Store.CreateNext(KindLocation, location.ID, location, location.EvidenceIDs, source.ImportedBy, options.CreatedAt)
		if createErr != nil {
			return CompilationResult{}, createErr
		}
		result.Locations[location.ID] = revision.Ref()
	}
	for _, prop := range draft.Story.Props {
		revision, createErr := c.Store.CreateNext(KindProp, prop.ID, prop, prop.EvidenceIDs, source.ImportedBy, options.CreatedAt)
		if createErr != nil {
			return CompilationResult{}, createErr
		}
		result.Props[prop.ID] = revision.Ref()
	}
	for _, episode := range draft.Episodes {
		episodePayload := episode
		episodePayload.Scenes = nil
		revision, createErr := c.Store.CreateNext(KindEpisode, episode.ID, episodePayload, []string{sourceRevision.ID}, source.ImportedBy, options.CreatedAt)
		if createErr != nil {
			return CompilationResult{}, createErr
		}
		result.Episodes[episode.ID] = revision.Ref()
		for _, scene := range episode.Scenes {
			scenePayload := scene
			scenePayload.Shots = nil
			sceneRevision, sceneErr := c.Store.CreateNext(KindScene, scene.ID, scenePayload, []string{revision.ID}, source.ImportedBy, options.CreatedAt)
			if sceneErr != nil {
				return CompilationResult{}, sceneErr
			}
			result.Scenes[scene.ID] = sceneRevision.Ref()
			for _, shot := range scene.Shots {
				shotRevision, shotErr := c.Store.CreateNext(KindShotSpec, shot.ID, shot, []string{sceneRevision.ID}, source.ImportedBy, options.CreatedAt)
				if shotErr != nil {
					return CompilationResult{}, shotErr
				}
				result.Shots[shot.ID] = shotRevision.Ref()
			}
		}
	}
	return result, nil
}

func validateCompilation(sourceText string, draft CompilationDraft, options CompileOptions) error {
	evidence := make(map[string]struct{}, len(draft.Story.Evidence))
	for _, span := range draft.Story.Evidence {
		if span.ID == "" || span.StartOffset < 0 || span.EndOffset <= span.StartOffset ||
			span.EndOffset > len(sourceText) {
			return validationf("invalid evidence span %q", span.ID)
		}
		if hashString(sourceText[span.StartOffset:span.EndOffset]) != span.ExcerptHash {
			return validationf("evidence span %q does not match the authorized source revision", span.ID)
		}
		if _, duplicate := evidence[span.ID]; duplicate {
			return validationf("duplicate evidence span %q", span.ID)
		}
		evidence[span.ID] = struct{}{}
	}
	checkEvidence := func(owner string, ids []string) error {
		for _, id := range ids {
			if _, ok := evidence[id]; !ok {
				return validationf("%s references unknown evidence %q", owner, id)
			}
		}
		return nil
	}
	if !nonEmpty(draft.Story.World.ID, draft.Story.World.Name, draft.Story.World.Premise, draft.Story.World.VisualStyle) {
		return validationf("complete world structure is required")
	}
	if err := checkEvidence("world", draft.Story.World.EvidenceIDs); err != nil {
		return err
	}

	characters := make(map[string]struct{}, len(draft.Story.Characters))
	for _, character := range draft.Story.Characters {
		if !nonEmpty(character.ID, character.Name, character.Role, character.Appearance, character.Personality) {
			return validationf("incomplete character %q", character.ID)
		}
		if _, duplicate := characters[character.ID]; duplicate {
			return validationf("duplicate character %q", character.ID)
		}
		characters[character.ID] = struct{}{}
		if err := checkEvidence("character "+character.ID, character.EvidenceIDs); err != nil {
			return err
		}
	}
	locations := make(map[string]struct{}, len(draft.Story.Locations))
	for _, location := range draft.Story.Locations {
		if !nonEmpty(location.ID, location.Name, location.Description) {
			return validationf("incomplete location %q", location.ID)
		}
		if _, duplicate := locations[location.ID]; duplicate {
			return validationf("duplicate location %q", location.ID)
		}
		locations[location.ID] = struct{}{}
		if err := checkEvidence("location "+location.ID, location.EvidenceIDs); err != nil {
			return err
		}
	}
	props := make(map[string]struct{}, len(draft.Story.Props))
	for _, prop := range draft.Story.Props {
		if !nonEmpty(prop.ID, prop.Name, prop.Description) {
			return validationf("incomplete prop %q", prop.ID)
		}
		if _, duplicate := props[prop.ID]; duplicate {
			return validationf("duplicate prop %q", prop.ID)
		}
		props[prop.ID] = struct{}{}
		if prop.OwnerID != "" {
			if _, ok := characters[prop.OwnerID]; !ok {
				return validationf("prop %q references unknown owner %q", prop.ID, prop.OwnerID)
			}
		}
		if err := checkEvidence("prop "+prop.ID, prop.EvidenceIDs); err != nil {
			return err
		}
	}
	for _, relationship := range draft.Story.Relationships {
		if !nonEmpty(relationship.ID, relationship.FromID, relationship.ToID, relationship.Kind, relationship.Description) {
			return validationf("incomplete relationship %q", relationship.ID)
		}
		if _, ok := characters[relationship.FromID]; !ok {
			return validationf("relationship %q references unknown character %q", relationship.ID, relationship.FromID)
		}
		if _, ok := characters[relationship.ToID]; !ok {
			return validationf("relationship %q references unknown character %q", relationship.ID, relationship.ToID)
		}
		if err := checkEvidence("relationship "+relationship.ID, relationship.EvidenceIDs); err != nil {
			return err
		}
	}
	if len(draft.Episodes) == 0 {
		return validationf("at least one episode is required")
	}
	episodeIDs := map[string]struct{}{}
	sceneIDs := map[string]struct{}{}
	shotIDs := map[string]struct{}{}
	for _, episode := range draft.Episodes {
		if !nonEmpty(episode.ID, episode.Title, episode.Summary) || episode.Ordinal < 1 ||
			episode.TargetDurationMillis < options.EpisodeMinMillis ||
			episode.TargetDurationMillis > options.EpisodeMaxMillis ||
			len(episode.Scenes) == 0 {
			return validationf("episode %q does not satisfy duration or structure constraints", episode.ID)
		}
		if _, duplicate := episodeIDs[episode.ID]; duplicate {
			return validationf("duplicate episode %q", episode.ID)
		}
		episodeIDs[episode.ID] = struct{}{}
		actualDuration := 0
		for _, scene := range episode.Scenes {
			if !nonEmpty(scene.ID, scene.Title, scene.LocationID, scene.Summary) || scene.Ordinal < 1 || len(scene.Shots) == 0 {
				return validationf("incomplete scene %q", scene.ID)
			}
			if _, ok := locations[scene.LocationID]; !ok {
				return validationf("scene %q references unknown location %q", scene.ID, scene.LocationID)
			}
			if _, duplicate := sceneIDs[scene.ID]; duplicate {
				return validationf("duplicate scene %q", scene.ID)
			}
			sceneIDs[scene.ID] = struct{}{}
			for _, shot := range scene.Shots {
				if _, duplicate := shotIDs[shot.ID]; duplicate {
					return validationf("duplicate shot %q", shot.ID)
				}
				shotIDs[shot.ID] = struct{}{}
				if err := shot.Validate(options.ShotMinMillis, options.ShotMaxMillis); err != nil {
					return err
				}
				for _, characterID := range shot.CharacterIDs {
					if _, ok := characters[characterID]; !ok {
						return validationf("shot %q references unknown character %q", shot.ID, characterID)
					}
				}
				for _, propID := range shot.PropIDs {
					if _, ok := props[propID]; !ok {
						return validationf("shot %q references unknown prop %q", shot.ID, propID)
					}
				}
				actualDuration += shot.DurationMillis
			}
		}
		if actualDuration < options.EpisodeMinMillis || actualDuration > options.EpisodeMaxMillis {
			return validationf("episode %q compiled duration %d is outside [%d,%d]", episode.ID, actualDuration, options.EpisodeMinMillis, options.EpisodeMaxMillis)
		}
		difference := actualDuration - episode.TargetDurationMillis
		if difference < 0 {
			difference = -difference
		}
		if difference > options.ShotMaxMillis {
			return validationf("episode %q compiled duration differs from its target by more than one shot", episode.ID)
		}
	}
	return nil
}

// CloneCompilationDraft protects deterministic fixture implementations from
// accidental caller mutation.
func CloneCompilationDraft(draft CompilationDraft) CompilationDraft {
	data, err := json.Marshal(draft)
	if err != nil {
		return CompilationDraft{}
	}
	var cloned CompilationDraft
	if err := json.Unmarshal(data, &cloned); err != nil {
		return CompilationDraft{}
	}
	return cloned
}

type FixtureContentGenerator struct {
	Draft CompilationDraft
}

func (f FixtureContentGenerator) Generate(ctx context.Context, request ContentGenerationRequest) (CompilationDraft, error) {
	if err := ctx.Err(); err != nil {
		return CompilationDraft{}, err
	}
	if err := request.SourceRevision.Validate(); err != nil {
		return CompilationDraft{}, err
	}
	if !validSHA256(request.SourceHash) || request.SourceHash != request.SourceRevision.ContentHash ||
		!nonEmpty(request.Title, request.Language, request.Text) {
		return CompilationDraft{}, validationf("content generation request is not pinned to an authorized source revision")
	}
	return CloneCompilationDraft(f.Draft), nil
}

func SortedRevisionRefs(values map[string]RevisionRef) []RevisionRef {
	keys := slices.Sorted(maps.Keys(values))
	result := make([]RevisionRef, 0, len(keys))
	for _, key := range keys {
		result = append(result, values[key])
	}
	return result
}

func normalizePromptText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}
