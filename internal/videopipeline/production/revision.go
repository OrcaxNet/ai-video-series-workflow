package production

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"
)

const RevisionSchemaVersion = "v1"

// Revision is an immutable, content-addressed domain object. Payload is copied
// on both write and read so callers cannot mutate an accepted revision.
type Revision struct {
	ID            string          `json:"id"`
	Kind          string          `json:"kind"`
	AggregateID   string          `json:"aggregate_id"`
	Number        int             `json:"number"`
	ParentID      string          `json:"parent_id,omitempty"`
	RollbackOf    string          `json:"rollback_of,omitempty"`
	SchemaVersion string          `json:"schema_version"`
	ContentHash   string          `json:"content_hash"`
	Payload       json.RawMessage `json:"payload"`
	EvidenceIDs   []string        `json:"evidence_ids,omitempty"`
	CreatedBy     string          `json:"created_by"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (r Revision) Ref() RevisionRef {
	return RevisionRef{
		ID:          r.ID,
		Kind:        r.Kind,
		AggregateID: r.AggregateID,
		Number:      r.Number,
		ContentHash: r.ContentHash,
	}
}

func (r Revision) Decode(target any) error {
	if target == nil {
		return validationf("revision decode target is required")
	}
	if err := json.Unmarshal(r.Payload, target); err != nil {
		return fmt.Errorf("decode revision %q: %w", r.ID, err)
	}
	return nil
}

type RevisionRef struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	AggregateID string `json:"aggregate_id"`
	Number      int    `json:"number"`
	ContentHash string `json:"content_hash"`
}

func (r RevisionRef) Validate() error {
	if !nonEmpty(r.ID, r.Kind, r.AggregateID, r.ContentHash) || r.Number < 1 ||
		!validSHA256(r.ContentHash) {
		return validationf("complete immutable revision reference is required")
	}
	return nil
}

type CreateRevisionInput struct {
	Kind          string
	AggregateID   string
	ParentID      string
	RollbackOf    string
	SchemaVersion string
	Payload       any
	EvidenceIDs   []string
	CreatedBy     string
	CreatedAt     time.Time
}

// RevisionStore gives the domain layer the same compare-and-append semantics
// used by the PostgreSQL revision tables, while remaining deterministic for
// no-key tests and fixtures.
type RevisionStore struct {
	mu     sync.RWMutex
	byID   map[string]Revision
	chains map[string][]string
}

func NewRevisionStore() *RevisionStore {
	return &RevisionStore{
		byID:   make(map[string]Revision),
		chains: make(map[string][]string),
	}
}

func (s *RevisionStore) Create(input CreateRevisionInput) (Revision, error) {
	if !nonEmpty(input.Kind, input.AggregateID, input.SchemaVersion, input.CreatedBy) {
		return Revision{}, validationf("kind, aggregate ID, schema version, and actor are required")
	}
	if input.Payload == nil {
		return Revision{}, validationf("revision payload is required")
	}
	if input.CreatedAt.IsZero() {
		return Revision{}, validationf("revision creation time is required")
	}
	payload, err := canonicalJSON(input.Payload)
	if err != nil {
		return Revision{}, err
	}
	key := revisionChainKey(input.Kind, input.AggregateID)

	s.mu.Lock()
	defer s.mu.Unlock()

	chain := s.chains[key]
	if len(chain) == 0 && input.ParentID != "" {
		return Revision{}, conflictf("first revision cannot have a parent")
	}
	if len(chain) > 0 && input.ParentID != chain[len(chain)-1] {
		return Revision{}, conflictf("parent %q is not the latest revision", input.ParentID)
	}
	if input.RollbackOf != "" {
		target, ok := s.byID[input.RollbackOf]
		if !ok || target.Kind != input.Kind || target.AggregateID != input.AggregateID {
			return Revision{}, conflictf("rollback target must belong to the same aggregate")
		}
	}

	revisionContentHash, err := contentHash(struct {
		SchemaVersion string          `json:"schema_version"`
		Payload       json.RawMessage `json:"payload"`
	}{
		SchemaVersion: input.SchemaVersion,
		Payload:       payload,
	})
	if err != nil {
		return Revision{}, err
	}
	evidenceIDs := sortedUnique(input.EvidenceIDs)
	if len(chain) > 0 && input.RollbackOf == "" {
		latest := s.byID[chain[len(chain)-1]]
		if latest.ContentHash == revisionContentHash && slices.Equal(latest.EvidenceIDs, evidenceIDs) {
			return cloneRevision(latest), nil
		}
	}
	identityMaterial := struct {
		Kind          string   `json:"kind"`
		AggregateID   string   `json:"aggregate_id"`
		ParentID      string   `json:"parent_id,omitempty"`
		RollbackOf    string   `json:"rollback_of,omitempty"`
		SchemaVersion string   `json:"schema_version"`
		ContentHash   string   `json:"content_hash"`
		EvidenceIDs   []string `json:"evidence_ids,omitempty"`
	}{
		Kind:          input.Kind,
		AggregateID:   input.AggregateID,
		ParentID:      input.ParentID,
		RollbackOf:    input.RollbackOf,
		SchemaVersion: input.SchemaVersion,
		ContentHash:   revisionContentHash,
		EvidenceIDs:   evidenceIDs,
	}
	identityHash, err := contentHash(identityMaterial)
	if err != nil {
		return Revision{}, err
	}
	id := derivedID("rev", identityHash)
	if existing, ok := s.byID[id]; ok {
		return cloneRevision(existing), nil
	}

	revision := Revision{
		ID:            id,
		Kind:          input.Kind,
		AggregateID:   input.AggregateID,
		Number:        len(chain) + 1,
		ParentID:      input.ParentID,
		RollbackOf:    input.RollbackOf,
		SchemaVersion: input.SchemaVersion,
		ContentHash:   revisionContentHash,
		Payload:       append(json.RawMessage(nil), payload...),
		EvidenceIDs:   evidenceIDs,
		CreatedBy:     input.CreatedBy,
		CreatedAt:     input.CreatedAt.UTC(),
	}
	s.byID[id] = revision
	s.chains[key] = append(chain, id)
	return cloneRevision(revision), nil
}

func (s *RevisionStore) CreateNext(kind, aggregateID string, payload any, evidenceIDs []string, actor string, at time.Time) (Revision, error) {
	parent := ""
	if latest, ok := s.Latest(kind, aggregateID); ok {
		parent = latest.ID
	}
	return s.Create(CreateRevisionInput{
		Kind:          kind,
		AggregateID:   aggregateID,
		ParentID:      parent,
		SchemaVersion: RevisionSchemaVersion,
		Payload:       payload,
		EvidenceIDs:   evidenceIDs,
		CreatedBy:     actor,
		CreatedAt:     at,
	})
}

func (s *RevisionStore) Rollback(kind, aggregateID, targetID, actor string, at time.Time) (Revision, error) {
	target, ok := s.Get(targetID)
	if !ok || target.Kind != kind || target.AggregateID != aggregateID {
		return Revision{}, fmt.Errorf("%w: rollback target %q", ErrNotFound, targetID)
	}
	latest, ok := s.Latest(kind, aggregateID)
	if !ok {
		return Revision{}, fmt.Errorf("%w: revision chain", ErrNotFound)
	}
	var payload any
	if err := json.Unmarshal(target.Payload, &payload); err != nil {
		return Revision{}, fmt.Errorf("decode rollback payload: %w", err)
	}
	return s.Create(CreateRevisionInput{
		Kind:          kind,
		AggregateID:   aggregateID,
		ParentID:      latest.ID,
		RollbackOf:    target.ID,
		SchemaVersion: target.SchemaVersion,
		Payload:       payload,
		EvidenceIDs:   target.EvidenceIDs,
		CreatedBy:     actor,
		CreatedAt:     at,
	})
}

func (s *RevisionStore) Get(id string) (Revision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	revision, ok := s.byID[id]
	return cloneRevision(revision), ok
}

func (s *RevisionStore) Latest(kind, aggregateID string) (Revision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.chains[revisionChainKey(kind, aggregateID)]
	if len(chain) == 0 {
		return Revision{}, false
	}
	return cloneRevision(s.byID[chain[len(chain)-1]]), true
}

func (s *RevisionStore) History(kind, aggregateID string) []Revision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.chains[revisionChainKey(kind, aggregateID)]
	result := make([]Revision, 0, len(chain))
	for _, id := range chain {
		result = append(result, cloneRevision(s.byID[id]))
	}
	return result
}

func revisionChainKey(kind, aggregateID string) string {
	return kind + "\x00" + aggregateID
}

func cloneRevision(revision Revision) Revision {
	revision.Payload = append(json.RawMessage(nil), revision.Payload...)
	revision.EvidenceIDs = slices.Clone(revision.EvidenceIDs)
	return revision
}

func sortedUnique(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return slices.Compact(result)
}

type Dependency struct {
	Producer RevisionRef `json:"producer"`
	Consumer RevisionRef `json:"consumer"`
	Role     string      `json:"role"`
}

// DependencyGraph records exact revision dependencies. Replacing a producer
// never silently retargets consumers; Impacted returns the deterministic stale
// closure that must be revalidated or regenerated.
type DependencyGraph struct {
	mu         sync.RWMutex
	downstream map[string][]Dependency
}

func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{downstream: make(map[string][]Dependency)}
}

func (g *DependencyGraph) Add(dependency Dependency) error {
	if err := dependency.Producer.Validate(); err != nil {
		return err
	}
	if err := dependency.Consumer.Validate(); err != nil {
		return err
	}
	if dependency.Role == "" {
		return validationf("dependency role is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, existing := range g.downstream[dependency.Producer.ID] {
		if existing.Consumer.ID == dependency.Consumer.ID && existing.Role == dependency.Role {
			return nil
		}
	}
	g.downstream[dependency.Producer.ID] = append(g.downstream[dependency.Producer.ID], dependency)
	return nil
}

func (g *DependencyGraph) Impacted(producerID string) []RevisionRef {
	g.mu.RLock()
	defer g.mu.RUnlock()
	seen := map[string]struct{}{producerID: {}}
	queue := []string{producerID}
	var result []RevisionRef
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependency := range g.downstream[current] {
			if _, exists := seen[dependency.Consumer.ID]; exists {
				continue
			}
			seen[dependency.Consumer.ID] = struct{}{}
			result = append(result, dependency.Consumer)
			queue = append(queue, dependency.Consumer.ID)
		}
	}
	slices.SortFunc(result, func(left, right RevisionRef) int {
		if left.Kind != right.Kind {
			if left.Kind < right.Kind {
				return -1
			}
			return 1
		}
		if left.AggregateID < right.AggregateID {
			return -1
		}
		if left.AggregateID > right.AggregateID {
			return 1
		}
		return left.Number - right.Number
	})
	return result
}
