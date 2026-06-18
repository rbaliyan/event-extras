package lifecycle

import (
	"context"
	"sync"
	"time"
)

// MemoryStore is an in-process Store for single-instance deployments and
// tests. It is safe for concurrent use but provides no coordination across
// processes — use RedisStore, PostgresStore, or MongoStore for multi-pod
// deployments. MemoryStore does not implement health.Checker since there
// is nothing remote to check.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*memoryEntry
	now     func() time.Time
}

type memoryEntry struct {
	state       State
	holder      string
	leaseUntil  time.Time
	completedAt time.Time
	failedAt    time.Time
	errMsg      string
}

// NewMemoryStore creates an in-process Store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*memoryEntry),
		now:     time.Now,
	}
}

// Acquire implements Store.
//
// Behavior summary (all under a single mutex):
//   - completed/failed entry present -> return it unchanged
//   - running by same instance       -> refresh lease, return running
//   - running by other, lease valid  -> return running by other
//   - running by other, lease expired -> take over: set running + new lease
//   - no entry present                -> create: set running + new lease
//
// A lease is valid while now < leaseUntil and expired once now >= leaseUntil.
func (s *MemoryStore) Acquire(_ context.Context, key, instanceID string, lease time.Duration) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e, ok := s.entries[key]
	if !ok {
		e = &memoryEntry{}
		s.entries[key] = e
	}
	switch e.state {
	case StateCompleted, StateFailed:
		return e.toStatus(), nil
	case StateRunning:
		// Same holder re-entering is idempotent: refresh the lease.
		if e.holder == instanceID {
			e.leaseUntil = now.Add(lease)
			return e.toStatus(), nil
		}
		// Different holder and lease still valid: contention.
		if now.Before(e.leaseUntil) {
			return e.toStatus(), nil
		}
		// Lease expired — fall through and take over.
	}
	e.state = StateRunning
	e.holder = instanceID
	e.leaseUntil = now.Add(lease)
	e.completedAt = time.Time{}
	e.failedAt = time.Time{}
	e.errMsg = ""
	return e.toStatus(), nil
}

// Refresh implements Store.
func (s *MemoryStore) Refresh(_ context.Context, key, instanceID string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || e.state != StateRunning || e.holder != instanceID {
		return ErrLeaseLost
	}
	now := s.now()
	// Expiry is exclusive: at now == leaseUntil the lease is already gone and
	// Acquire may have handed it to another instance, so reject the refresh.
	if !now.Before(e.leaseUntil) {
		return ErrLeaseLost
	}
	e.leaseUntil = now.Add(lease)
	return nil
}

// Complete implements Store.
func (s *MemoryStore) Complete(_ context.Context, key, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || e.state != StateRunning || e.holder != instanceID {
		return ErrLeaseLost
	}
	e.state = StateCompleted
	e.completedAt = s.now()
	e.leaseUntil = time.Time{}
	return nil
}

// Fail implements Store.
func (s *MemoryStore) Fail(_ context.Context, key, instanceID, errMsg string, retryable bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || e.state != StateRunning || e.holder != instanceID {
		return ErrLeaseLost
	}
	if retryable {
		delete(s.entries, key)
		return nil
	}
	e.state = StateFailed
	e.failedAt = s.now()
	e.errMsg = errMsg
	e.leaseUntil = time.Time{}
	return nil
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key string) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return Status{State: StatePending}, nil
	}
	return e.toStatus(), nil
}

// Reset implements Store.
func (s *MemoryStore) Reset(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

func (e *memoryEntry) toStatus() Status {
	return Status{
		State:       e.state,
		Holder:      e.holder,
		LeaseUntil:  e.leaseUntil,
		CompletedAt: e.completedAt,
		FailedAt:    e.failedAt,
		Error:       e.errMsg,
	}
}

var _ Store = (*MemoryStore)(nil)
