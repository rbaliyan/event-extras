package saga

import (
	"context"
	"testing"
	"time"

	eventerrors "github.com/rbaliyan/event/v3/errors"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStoreContract verifies the MemoryStore satisfies the full Store contract.
func TestMemoryStoreContract(t *testing.T) {
	runStoreContractTests(t, NewMemoryStore())
}

// TestRedisStoreContract verifies the RedisStore satisfies the full Store contract
// using an in-process Redis mock.
func TestRedisStoreContract(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	require.NoError(t, err)
	runStoreContractTests(t, store)
}

// runStoreContractTests verifies a Store implementation satisfies the full
// filter and query contract. The store must be empty on entry.
//
// Seed layout (6 sagas):
//
//	ID        Name           Status
//	s-1       order-saga     pending
//	s-2       order-saga     running
//	s-3       order-saga     completed
//	s-4       payment-saga   pending
//	s-5       payment-saga   failed
//	s-6       payment-saga   compensated
func runStoreContractTests(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	seed := []struct {
		id     string
		name   string
		status Status
	}{
		{"s-1", "order-saga", StatusPending},
		{"s-2", "order-saga", StatusRunning},
		{"s-3", "order-saga", StatusCompleted},
		{"s-4", "payment-saga", StatusPending},
		{"s-5", "payment-saga", StatusFailed},
		{"s-6", "payment-saga", StatusCompensated},
	}
	for _, s := range seed {
		state := &State{
			ID:            s.id,
			Name:          s.name,
			Status:        s.status,
			StartedAt:     now,
			LastUpdatedAt: now,
		}
		require.NoError(t, store.Create(ctx, state), "seed Create %s", s.id)
	}

	t.Run("Get/Found", func(t *testing.T) {
		got, err := store.Get(ctx, "s-1")
		require.NoError(t, err)
		assert.Equal(t, "s-1", got.ID)
		assert.Equal(t, "order-saga", got.Name)
		assert.Equal(t, StatusPending, got.Status)
	})

	t.Run("Get/NotFound", func(t *testing.T) {
		_, err := store.Get(ctx, "no-such-id")
		require.Error(t, err)
		assert.True(t, eventerrors.IsNotFound(err))
	})

	t.Run("Create/Duplicate", func(t *testing.T) {
		dup := &State{ID: "s-1", Name: "order-saga", Status: StatusPending, StartedAt: now, LastUpdatedAt: now}
		err := store.Create(ctx, dup)
		require.Error(t, err, "creating duplicate saga must return an error")
	})

	t.Run("Update/Success", func(t *testing.T) {
		// Use a fresh saga so this test is independent of seed data.
		state := &State{
			ID: "s-update-tmp", Name: "order-saga",
			Status: StatusPending, StartedAt: now, LastUpdatedAt: now,
		}
		require.NoError(t, store.Create(ctx, state))
		t.Cleanup(func() {
			// Delete via a type-asserted Delete if available; otherwise ignored.
			if d, ok := store.(interface {
				Delete(context.Context, string) error
			}); ok {
				_ = d.Delete(ctx, "s-update-tmp")
			}
		})

		state.Status = StatusRunning
		state.CurrentStep = 1
		state.CompletedSteps = []string{"step-0"}
		state.LastUpdatedAt = now.Add(time.Second)
		require.NoError(t, store.Update(ctx, state))
		assert.Equal(t, int64(1), state.Version)

		got, err := store.Get(ctx, "s-update-tmp")
		require.NoError(t, err)
		assert.Equal(t, StatusRunning, got.Status)
		assert.Equal(t, 1, got.CurrentStep)
		assert.Equal(t, []string{"step-0"}, got.CompletedSteps)
		assert.Equal(t, int64(1), got.Version)
	})

	t.Run("Update/VersionConflict", func(t *testing.T) {
		state := &State{
			ID: "s-conflict-tmp", Name: "order-saga",
			Status: StatusPending, StartedAt: now, LastUpdatedAt: now,
		}
		require.NoError(t, store.Create(ctx, state))
		t.Cleanup(func() {
			if d, ok := store.(interface {
				Delete(context.Context, string) error
			}); ok {
				_ = d.Delete(ctx, "s-conflict-tmp")
			}
		})

		// Advance version once
		state.Status = StatusRunning
		require.NoError(t, store.Update(ctx, state))

		// Attempt with stale version
		stale := &State{
			ID: "s-conflict-tmp", Name: "order-saga",
			Status: StatusCompleted, StartedAt: now, LastUpdatedAt: now, Version: 0,
		}
		err := store.Update(ctx, stale)
		require.Error(t, err)
		assert.True(t, eventerrors.IsVersionConflict(err))
	})

	t.Run("Update/NotFound", func(t *testing.T) {
		state := &State{
			ID: "no-such-saga", Name: "order-saga",
			Status: StatusPending, StartedAt: now, LastUpdatedAt: now,
		}
		err := store.Update(ctx, state)
		require.Error(t, err)
		assert.True(t, eventerrors.IsNotFound(err))
	})

	t.Run("List/All", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 6)
	})

	t.Run("List/Name", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Name: "order-saga"})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.Equal(t, "order-saga", r.Name)
		}
	})

	t.Run("List/Status", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Status: []Status{StatusPending}})
		require.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Equal(t, StatusPending, r.Status)
		}
	})

	t.Run("List/MultipleStatuses", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Status: []Status{StatusPending, StatusRunning}})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.Contains(t, []Status{StatusPending, StatusRunning}, r.Status)
		}
	})

	t.Run("List/NameAndStatus", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{
			Name:   "payment-saga",
			Status: []Status{StatusFailed, StatusCompensated},
		})
		require.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Equal(t, "payment-saga", r.Name)
		}
	})

	t.Run("List/Limit", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Limit: 2})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("List/LimitBeyondTotal", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Limit: 1000})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 6)
	})

	t.Run("List/NoMatch", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Name: "nonexistent-saga"})
		require.NoError(t, err)
		assert.Empty(t, results)
	})
}
