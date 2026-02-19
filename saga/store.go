package saga

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/health"
)

// MemoryStore is an in-memory saga store for testing
type MemoryStore struct {
	mu    sync.RWMutex
	sagas map[string]*State
}

// NewMemoryStore creates a new in-memory saga store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sagas: make(map[string]*State),
	}
}

// Create creates a new saga instance
func (s *MemoryStore) Create(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.sagas[state.ID]; exists {
		return fmt.Errorf("saga already exists: %s", state.ID)
	}

	stored := *state
	if state.CompletedSteps != nil {
		stored.CompletedSteps = make([]string, len(state.CompletedSteps))
		copy(stored.CompletedSteps, state.CompletedSteps)
	}
	s.sagas[state.ID] = &stored

	return nil
}

// Get retrieves saga state by ID
func (s *MemoryStore) Get(ctx context.Context, id string) (*State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.sagas[id]
	if !ok {
		return nil, fmt.Errorf("saga not found: %s", id)
	}

	// Return a copy
	result := *state
	return &result, nil
}

// Update updates saga state with optimistic locking.
//
// The update uses the Version field for optimistic locking. If the version
// doesn't match the expected version, ErrVersionConflict is returned.
// On successful update, the state's Version is incremented.
func (s *MemoryStore) Update(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.sagas[state.ID]
	if !exists {
		return fmt.Errorf("saga not found: %s", state.ID)
	}

	// Check version for optimistic locking
	if existing.Version != state.Version {
		return ErrVersionConflict
	}

	// Increment version
	state.Version++

	stored := *state
	if state.CompletedSteps != nil {
		stored.CompletedSteps = make([]string, len(state.CompletedSteps))
		copy(stored.CompletedSteps, state.CompletedSteps)
	}
	s.sagas[state.ID] = &stored

	return nil
}

// List lists sagas matching the filter
func (s *MemoryStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*State

	for _, state := range s.sagas {
		if filter.Name != "" && state.Name != filter.Name {
			continue
		}

		if len(filter.Status) > 0 {
			matched := false
			for _, status := range filter.Status {
				if state.Status == status {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		result := *state
		results = append(results, &result)

		if filter.Limit > 0 && len(results) >= filter.Limit {
			break
		}
	}

	return results, nil
}

// Delete removes a saga by ID.
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sagas[id]; !ok {
		return fmt.Errorf("saga not found: %s", id)
	}

	delete(s.sagas, id)
	return nil
}

// Cleanup removes completed sagas older than the specified age
func (s *MemoryStore) Cleanup(age time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-age)
	deleted := 0

	for id, state := range s.sagas {
		if state.CompletedAt != nil && state.CompletedAt.Before(cutoff) {
			delete(s.sagas, id)
			deleted++
		}
	}

	return deleted
}

// Health performs a health check on the memory store.
// Always returns healthy since in-memory stores don't have connectivity issues.
func (s *MemoryStore) Health(ctx context.Context) *health.Result {
	s.mu.RLock()
	count := len(s.sagas)
	s.mu.RUnlock()

	return &health.Result{
		Status:    health.StatusHealthy,
		CheckedAt: time.Now(),
		Details: map[string]any{
			"sagas_count": count,
		},
	}
}

// Compile-time checks
var (
	_ Store          = (*MemoryStore)(nil)
	_ health.Checker = (*MemoryStore)(nil)
)
