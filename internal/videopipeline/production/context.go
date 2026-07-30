package production

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

type Scope string

const (
	ScopeSeries  Scope = "series"
	ScopeEpisode Scope = "episode"
	ScopeScene   Scope = "scene"
	ScopeShot    Scope = "shot"
)

var orderedScopes = []Scope{ScopeSeries, ScopeEpisode, ScopeScene, ScopeShot}

func (s Scope) Valid() bool {
	return slices.Contains(orderedScopes, s)
}

type AssetStatus string

const (
	AssetActive   AssetStatus = "active"
	AssetDisabled AssetStatus = "disabled"
)

// AssetRevision is the exact reusable reference-art revision consumed by a
// prompt. It may represent a character view/expression, location, prop, or
// predecessor tail frame.
type AssetRevision struct {
	Revision         RevisionRef                `json:"revision"`
	AssetID          string                     `json:"asset_id"`
	Kind             providercontract.Modality  `json:"kind"`
	DefaultRole      providercontract.AssetRole `json:"default_role"`
	URI              string                     `json:"uri"`
	MediaType        string                     `json:"media_type"`
	SizeBytes        int64                      `json:"size_bytes,omitempty"`
	Width            int                        `json:"width,omitempty"`
	Height           int                        `json:"height,omitempty"`
	DurationMillis   int64                      `json:"duration_millis,omitempty"`
	LicenseReference string                     `json:"license_reference"`
	ApprovalID       string                     `json:"approval_id"`
	Authorized       bool                       `json:"authorized"`
	Status           AssetStatus                `json:"status"`
	Attributes       map[string]string          `json:"attributes,omitempty"`
}

func (a AssetRevision) Validate() error {
	if err := a.Revision.Validate(); err != nil {
		return err
	}
	if a.Revision.Kind != "asset" || !nonEmpty(a.AssetID, a.URI, a.MediaType, a.LicenseReference, a.ApprovalID) ||
		a.Kind == "" || a.DefaultRole == "" || !a.Authorized || a.Status != AssetActive {
		return policyf("asset revision %q is incomplete, disabled, unauthorized, or unapproved", a.Revision.ID)
	}
	if !strings.HasPrefix(a.URI, "cas://sha256/") ||
		strings.TrimPrefix(a.URI, "cas://sha256/") != a.Revision.ContentHash {
		return policyf("asset revision %q is not pinned to verified CAS content", a.Revision.ID)
	}
	return nil
}

func (a AssetRevision) ProviderRef(role providercontract.AssetRole) providercontract.AssetRef {
	if role == "" {
		role = a.DefaultRole
	}
	return providercontract.AssetRef{
		ID:               a.AssetID,
		Revision:         a.Revision.ID,
		Kind:             a.Kind,
		Role:             role,
		URI:              a.URI,
		SHA256:           a.Revision.ContentHash,
		LicenseReference: a.LicenseReference,
		MediaType:        a.MediaType,
		SizeBytes:        a.SizeBytes,
		Width:            a.Width,
		Height:           a.Height,
		DurationMillis:   a.DurationMillis,
	}
}

type AssetCatalog struct {
	mu         sync.RWMutex
	byRevision map[string]AssetRevision
}

func NewAssetCatalog() *AssetCatalog {
	return &AssetCatalog{byRevision: make(map[string]AssetRevision)}
}

func (c *AssetCatalog) Add(asset AssetRevision) error {
	if err := asset.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.byRevision[asset.Revision.ID]; ok {
		existingHash, _ := contentHash(existing)
		incomingHash, _ := contentHash(asset)
		if existingHash != incomingHash {
			return conflictf("asset revision %q cannot be mutated", asset.Revision.ID)
		}
		return nil
	}
	asset.Attributes = maps.Clone(asset.Attributes)
	c.byRevision[asset.Revision.ID] = asset
	return nil
}

func (c *AssetCatalog) Resolve(revisionID string) (AssetRevision, error) {
	if revisionID == "" || strings.EqualFold(revisionID, "latest") {
		return AssetRevision{}, fmt.Errorf("%w: exact asset revision ID is required", ErrStaleReference)
	}
	c.mu.RLock()
	asset, ok := c.byRevision[revisionID]
	c.mu.RUnlock()
	if !ok {
		return AssetRevision{}, fmt.Errorf("%w: asset revision %q", ErrNotFound, revisionID)
	}
	if err := asset.Validate(); err != nil {
		return AssetRevision{}, err
	}
	asset.Attributes = maps.Clone(asset.Attributes)
	return asset, nil
}

type ContextLayer struct {
	Revision      RevisionRef       `json:"revision"`
	Scope         Scope             `json:"scope"`
	ScopeID       string            `json:"scope_id"`
	Values        map[string]string `json:"values"`
	AssetBindings map[string]string `json:"asset_bindings,omitempty"`
}

type contextPayload struct {
	Scope         Scope             `json:"scope"`
	ScopeID       string            `json:"scope_id"`
	Values        map[string]string `json:"values"`
	AssetBindings map[string]string `json:"asset_bindings,omitempty"`
}

func CreateContextLayer(
	store *RevisionStore,
	scope Scope,
	scopeID string,
	values map[string]string,
	assetBindings map[string]string,
	actor string,
	at time.Time,
) (ContextLayer, error) {
	if store == nil || !scope.Valid() || !nonEmpty(scopeID, actor) {
		return ContextLayer{}, validationf("store, valid scope, scope ID, and actor are required")
	}
	payload := contextPayload{
		Scope:         scope,
		ScopeID:       scopeID,
		Values:        maps.Clone(values),
		AssetBindings: maps.Clone(assetBindings),
	}
	revision, err := store.CreateNext(
		KindContext,
		string(scope)+":"+scopeID,
		payload,
		nil,
		actor,
		at,
	)
	if err != nil {
		return ContextLayer{}, err
	}
	return ContextLayer{
		Revision:      revision.Ref(),
		Scope:         scope,
		ScopeID:       scopeID,
		Values:        maps.Clone(values),
		AssetBindings: maps.Clone(assetBindings),
	}, nil
}

type ResolverPolicy struct {
	AllowedKeys          map[Scope]map[string]bool
	AllowedAssetOverride map[Scope]map[string]bool
}

func DefaultResolverPolicy() ResolverPolicy {
	return ResolverPolicy{
		AllowedKeys: map[Scope]map[string]bool{
			ScopeSeries: {
				"visual.style":        true,
				"visual.palette":      true,
				"world.rules":         true,
				"audience":            true,
				"output.aspect_ratio": true,
				"output.fps":          true,
				"negative.global":     true,
			},
			ScopeEpisode: {
				"story.mood":       true,
				"story.pacing":     true,
				"camera.language":  true,
				"visual.palette":   true,
				"negative.episode": true,
			},
			ScopeScene: {
				"scene.location":  true,
				"scene.time":      true,
				"scene.weather":   true,
				"lighting":        true,
				"story.mood":      true,
				"camera.language": true,
				"negative.scene":  true,
			},
			ScopeShot: {
				"camera.framing":   true,
				"camera.angle":     true,
				"camera.movement":  true,
				"motion.intensity": true,
				"emotion":          true,
				"negative.shot":    true,
			},
		},
		AllowedAssetOverride: map[Scope]map[string]bool{
			ScopeShot: {
				"character.primary": true,
				"prop.primary":      true,
			},
		},
	}
}

type EffectiveContext struct {
	ID             string                               `json:"id"`
	ContentHash    string                               `json:"content_hash"`
	RevisionRefs   providercontract.ContextRefs         `json:"revision_refs"`
	RevisionHashes map[Scope]string                     `json:"revision_hashes"`
	Values         map[string]string                    `json:"values"`
	Origins        map[string]Scope                     `json:"origins"`
	Assets         map[string]providercontract.AssetRef `json:"assets,omitempty"`
}

func (e EffectiveContext) OrderedAssets() []providercontract.AssetRef {
	aliases := slices.Sorted(maps.Keys(e.Assets))
	result := make([]providercontract.AssetRef, 0, len(aliases))
	for _, alias := range aliases {
		result = append(result, e.Assets[alias])
	}
	return result
}

type ContextResolver struct {
	Policy  ResolverPolicy
	Catalog *AssetCatalog
}

func NewContextResolver(catalog *AssetCatalog) *ContextResolver {
	return &ContextResolver{Policy: DefaultResolverPolicy(), Catalog: catalog}
}

func (r *ContextResolver) Resolve(layers []ContextLayer) (EffectiveContext, error) {
	if r.Catalog == nil {
		return EffectiveContext{}, validationf("asset catalog is required")
	}
	if len(layers) != len(orderedScopes) {
		return EffectiveContext{}, validationf("series, episode, scene, and shot context revisions are required")
	}
	effective := EffectiveContext{
		RevisionHashes: make(map[Scope]string, len(layers)),
		Values:         make(map[string]string),
		Origins:        make(map[string]Scope),
		Assets:         make(map[string]providercontract.AssetRef),
	}
	assetRevisions := make(map[string]string)
	for index, expectedScope := range orderedScopes {
		layer := layers[index]
		if layer.Scope != expectedScope || layer.ScopeID == "" {
			return EffectiveContext{}, validationf("context layer %d must have scope %q", index, expectedScope)
		}
		if err := layer.Revision.Validate(); err != nil {
			return EffectiveContext{}, err
		}
		if layer.Revision.Kind != KindContext {
			return EffectiveContext{}, validationf("context layer %q is not a context revision", layer.Revision.ID)
		}
		effective.RevisionHashes[layer.Scope] = layer.Revision.ContentHash
		allowed := r.Policy.AllowedKeys[layer.Scope]
		for key, value := range layer.Values {
			if !allowed[key] {
				return EffectiveContext{}, policyf("scope %q cannot set or override context key %q", layer.Scope, key)
			}
			if sensitiveContextKey(key) {
				return EffectiveContext{}, policyf("context key %q is not allowed to carry credential material", key)
			}
			value = normalizePromptText(value)
			if value == "" {
				return EffectiveContext{}, validationf("context key %q has an empty value", key)
			}
			effective.Values[key] = value
			effective.Origins[key] = layer.Scope
		}
		aliases := slices.Sorted(maps.Keys(layer.AssetBindings))
		for _, alias := range aliases {
			revisionID := layer.AssetBindings[alias]
			if prior, exists := assetRevisions[alias]; exists && prior != revisionID &&
				!r.Policy.AllowedAssetOverride[layer.Scope][alias] {
				return EffectiveContext{}, conflictf("asset alias %q changes without an allowed override", alias)
			}
			asset, err := r.Catalog.Resolve(revisionID)
			if err != nil {
				return EffectiveContext{}, fmt.Errorf("resolve context asset %q: %w", alias, err)
			}
			assetRevisions[alias] = revisionID
			effective.Assets[alias] = asset.ProviderRef(asset.DefaultRole)
		}
	}
	effective.RevisionRefs = providercontract.ContextRefs{
		SeriesSnapshotID:  layers[0].Revision.ID,
		EpisodeSnapshotID: layers[1].Revision.ID,
		SceneSnapshotID:   layers[2].Revision.ID,
		ShotSnapshotID:    layers[3].Revision.ID,
	}
	hashInput := struct {
		ResolverVersion string                               `json:"resolver_version"`
		RevisionRefs    providercontract.ContextRefs         `json:"revision_refs"`
		RevisionHashes  map[Scope]string                     `json:"revision_hashes"`
		Values          map[string]string                    `json:"values"`
		Assets          map[string]providercontract.AssetRef `json:"assets"`
	}{
		ResolverVersion: "context-resolver-v1",
		RevisionRefs:    effective.RevisionRefs,
		RevisionHashes:  effective.RevisionHashes,
		Values:          effective.Values,
		Assets:          effective.Assets,
	}
	digest, err := contentHash(hashInput)
	if err != nil {
		return EffectiveContext{}, err
	}
	effective.ContentHash = digest
	effective.ID = derivedID("effective-context", digest)
	return effective, nil
}

func sensitiveContextKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{"api_key", "apikey", "access_token", "authorization", "cookie", "secret"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
