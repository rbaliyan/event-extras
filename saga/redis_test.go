package saga

import (
	"context"
	"testing"
	"time"

	eventerrors "github.com/rbaliyan/event/v3/errors"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { _ = client.Close() })

	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return mr, store
}

func makeTestState(id, name string, status Status) *State {
	now := time.Now().Truncate(time.Second)
	return &State{
		ID:            id,
		Name:          name,
		Status:        status,
		CurrentStep:   0,
		Data:          map[string]string{"key": "value"},
		StartedAt:     now,
		LastUpdatedAt: now,
		Version:       0,
	}
}

func TestRedisStore_NewRedisStore(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		_, err := NewRedisStore(nil)
		if err == nil {
			t.Fatal("expected error for nil client")
		}
	})

	t.Run("custom prefix", func(t *testing.T) {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })

		store, err := NewRedisStore(client, WithKeyPrefix("custom:"))
		if err != nil {
			t.Fatalf("NewRedisStore: %v", err)
		}
		if store.prefix != "custom:" {
			t.Errorf("expected prefix 'custom:', got %q", store.prefix)
		}
	})
}

func TestRedisStore_Create(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		state := makeTestState("create-1", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		state := makeTestState("create-dup", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}
		err := store.Create(ctx, state)
		if err == nil {
			t.Fatal("expected error for duplicate create")
		}
	})

	t.Run("nil state", func(t *testing.T) {
		if err := store.Create(ctx, nil); err == nil {
			t.Fatal("expected error for nil state")
		}
	})

	t.Run("empty ID", func(t *testing.T) {
		state := makeTestState("", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err == nil {
			t.Fatal("expected error for empty ID")
		}
	})
}

func TestRedisStore_Get(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		state := makeTestState("get-1", "test-saga", StatusRunning)
		state.CompletedSteps = []string{"step-1"}
		state.Error = "some error"
		now := time.Now().Truncate(time.Second)
		state.CompletedAt = &now

		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := store.Get(ctx, "get-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		if got.ID != "get-1" {
			t.Errorf("ID: got %q, want %q", got.ID, "get-1")
		}
		if got.Name != "test-saga" {
			t.Errorf("Name: got %q, want %q", got.Name, "test-saga")
		}
		if got.Status != StatusRunning {
			t.Errorf("Status: got %q, want %q", got.Status, StatusRunning)
		}
		if len(got.CompletedSteps) != 1 || got.CompletedSteps[0] != "step-1" {
			t.Errorf("CompletedSteps: got %v, want [step-1]", got.CompletedSteps)
		}
		if got.Error != "some error" {
			t.Errorf("Error: got %q, want %q", got.Error, "some error")
		}
		if got.CompletedAt == nil {
			t.Error("CompletedAt: got nil, want non-nil")
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := store.Get(ctx, "nonexistent")
		if err == nil {
			t.Fatal("expected error for not found")
		}
		if !eventerrors.IsNotFound(err) {
			t.Errorf("expected IsNotFound to be true, got false; error: %v", err)
		}
	})
}

func TestRedisStore_Update(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		state := makeTestState("update-1", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}

		state.Status = StatusRunning
		state.CurrentStep = 1
		state.LastUpdatedAt = time.Now().Truncate(time.Second)
		if err := store.Update(ctx, state); err != nil {
			t.Fatalf("Update: %v", err)
		}

		if state.Version != 1 {
			t.Errorf("Version: got %d, want 1", state.Version)
		}

		got, err := store.Get(ctx, "update-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status != StatusRunning {
			t.Errorf("Status: got %q, want %q", got.Status, StatusRunning)
		}
		if got.CurrentStep != 1 {
			t.Errorf("CurrentStep: got %d, want 1", got.CurrentStep)
		}
		if got.Version != 1 {
			t.Errorf("Version: got %d, want 1", got.Version)
		}
	})

	t.Run("version conflict", func(t *testing.T) {
		state := makeTestState("update-conflict", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}

		// First update succeeds
		state.Status = StatusRunning
		if err := store.Update(ctx, state); err != nil {
			t.Fatalf("Update: %v", err)
		}

		// Simulate stale version by resetting
		stale := makeTestState("update-conflict", "test-saga", StatusCompleted)
		stale.Version = 0 // stale version
		err := store.Update(ctx, stale)
		if err == nil {
			t.Fatal("expected ErrVersionConflict")
		}
		if !eventerrors.IsVersionConflict(err) {
			t.Errorf("expected IsVersionConflict to be true; error: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		state := makeTestState("update-nonexistent", "test-saga", StatusPending)
		err := store.Update(ctx, state)
		if err == nil {
			t.Fatal("expected not found error")
		}
		if !eventerrors.IsNotFound(err) {
			t.Errorf("expected IsNotFound to be true; error: %v", err)
		}
	})

	t.Run("nil state", func(t *testing.T) {
		if err := store.Update(ctx, nil); err == nil {
			t.Fatal("expected error for nil state")
		}
	})

	t.Run("empty ID", func(t *testing.T) {
		state := makeTestState("", "test-saga", StatusPending)
		if err := store.Update(ctx, state); err == nil {
			t.Fatal("expected error for empty ID")
		}
	})

	t.Run("status index updated atomically", func(t *testing.T) {
		state := makeTestState("update-status-idx", "test-saga", StatusPending)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}

		// Update status from pending to running
		state.Status = StatusRunning
		if err := store.Update(ctx, state); err != nil {
			t.Fatalf("Update: %v", err)
		}

		// Verify the status index was updated
		pendingCount, err := store.CountByStatus(ctx, StatusPending)
		if err != nil {
			t.Fatalf("CountByStatus pending: %v", err)
		}
		runningCount, err := store.CountByStatus(ctx, StatusRunning)
		if err != nil {
			t.Fatalf("CountByStatus running: %v", err)
		}

		// The saga should only be in the running set now
		if pendingCount != 0 {
			t.Errorf("expected 0 pending sagas with this ID, got %d", pendingCount)
		}
		if runningCount == 0 {
			t.Error("expected at least 1 running saga")
		}
	})
}

func TestRedisStore_Delete(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		state := makeTestState("delete-1", "test-saga", StatusCompleted)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}

		if err := store.Delete(ctx, "delete-1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		// Verify it's gone
		_, err := store.Get(ctx, "delete-1")
		if !eventerrors.IsNotFound(err) {
			t.Errorf("expected not found after delete, got: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		err := store.Delete(ctx, "nonexistent")
		if err == nil {
			t.Fatal("expected error for not found")
		}
		if !eventerrors.IsNotFound(err) {
			t.Errorf("expected IsNotFound to be true; error: %v", err)
		}
	})
}

func TestRedisStore_List(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	// Create test sagas
	sagas := []struct {
		id     string
		name   string
		status Status
	}{
		{"list-1", "order-saga", StatusPending},
		{"list-2", "order-saga", StatusRunning},
		{"list-3", "order-saga", StatusCompleted},
		{"list-4", "payment-saga", StatusPending},
		{"list-5", "payment-saga", StatusFailed},
	}
	for _, s := range sagas {
		state := makeTestState(s.id, s.name, s.status)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create %s: %v", s.id, err)
		}
	}

	t.Run("all", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 5 {
			t.Errorf("expected 5 results, got %d", len(results))
		}
	})

	t.Run("by name", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Name: "order-saga"})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results for order-saga, got %d", len(results))
		}
	})

	t.Run("by status", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Status: []Status{StatusPending}})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 pending results, got %d", len(results))
		}
	})

	t.Run("by name and status", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{
			Name:   "order-saga",
			Status: []Status{StatusPending, StatusRunning},
		})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results for order-saga pending/running, got %d", len(results))
		}
	})

	t.Run("with limit", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Limit: 2})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results with limit, got %d", len(results))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Name: "nonexistent"})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})
}

func TestRedisStore_Statistics(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	// Create test sagas
	for i, status := range []Status{StatusPending, StatusRunning, StatusCompleted, StatusFailed} {
		state := makeTestState(
			"stats-"+string(rune('a'+i)),
			"test-saga",
			status,
		)
		if err := store.Create(ctx, state); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	t.Run("Count", func(t *testing.T) {
		count, err := store.Count(ctx)
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if count != 4 {
			t.Errorf("expected 4, got %d", count)
		}
	})

	t.Run("CountByStatus", func(t *testing.T) {
		pending, err := store.CountByStatus(ctx, StatusPending)
		if err != nil {
			t.Fatalf("CountByStatus: %v", err)
		}
		if pending != 1 {
			t.Errorf("expected 1 pending, got %d", pending)
		}
	})

	t.Run("CountByName", func(t *testing.T) {
		count, err := store.CountByName(ctx, "test-saga")
		if err != nil {
			t.Fatalf("CountByName: %v", err)
		}
		if count != 4 {
			t.Errorf("expected 4 for test-saga, got %d", count)
		}
	})

	t.Run("GetFailed", func(t *testing.T) {
		failed, err := store.GetFailed(ctx, "", 10)
		if err != nil {
			t.Fatalf("GetFailed: %v", err)
		}
		if len(failed) != 1 {
			t.Errorf("expected 1 failed, got %d", len(failed))
		}
	})

	t.Run("GetPending", func(t *testing.T) {
		pending, err := store.GetPending(ctx, 10)
		if err != nil {
			t.Fatalf("GetPending: %v", err)
		}
		if len(pending) != 2 {
			t.Errorf("expected 2 pending/running, got %d", len(pending))
		}
	})
}

func TestRedisStore_TTL(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := NewRedisStore(client, WithTTL(10*time.Second))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}

	ctx := context.Background()
	state := makeTestState("ttl-1", "test-saga", StatusPending)
	if err := store.Create(ctx, state); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Update to completed status - should set TTL
	state.Status = StatusCompleted
	now := time.Now().Truncate(time.Second)
	state.CompletedAt = &now
	if err := store.Update(ctx, state); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Check that TTL was set
	ttl := mr.TTL(store.prefix + "ttl-1")
	if ttl == 0 {
		t.Error("expected TTL to be set for completed saga")
	}
}

func TestRedisStore_DeleteCompleted(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	oldAt := now.Add(-48 * time.Hour)

	// Old completed saga — should be deleted
	oldCompleted := makeTestState("dc-old-completed", "test-saga", StatusCompleted)
	oldCompleted.CompletedAt = &oldAt
	if err := store.Create(ctx, oldCompleted); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Old compensated saga — should be deleted
	oldCompensated := makeTestState("dc-old-compensated", "test-saga", StatusCompensated)
	oldCompensated.CompletedAt = &oldAt
	if err := store.Create(ctx, oldCompensated); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Recent completed — should NOT be deleted
	recentCompleted := makeTestState("dc-recent-completed", "test-saga", StatusCompleted)
	recentCompleted.CompletedAt = &now
	if err := store.Create(ctx, recentCompleted); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Running saga (no CompletedAt) — should NOT be deleted
	running := makeTestState("dc-running", "test-saga", StatusRunning)
	if err := store.Create(ctx, running); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deleted, err := store.DeleteCompleted(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteCompleted: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	if _, err := store.Get(ctx, "dc-old-completed"); !eventerrors.IsNotFound(err) {
		t.Error("expected old completed saga to be deleted")
	}
	if _, err := store.Get(ctx, "dc-old-compensated"); !eventerrors.IsNotFound(err) {
		t.Error("expected old compensated saga to be deleted")
	}
	if _, err := store.Get(ctx, "dc-recent-completed"); err != nil {
		t.Errorf("expected recent completed saga to still exist: %v", err)
	}
	if _, err := store.Get(ctx, "dc-running"); err != nil {
		t.Errorf("expected running saga to still exist: %v", err)
	}
}

func TestRedisStore_Health(t *testing.T) {
	_, store := newTestRedis(t)
	ctx := context.Background()

	result := store.Health(ctx)
	if result.Status != "healthy" {
		t.Errorf("expected healthy, got %q", result.Status)
	}
}

func TestRedisStore_DeleteOlderThan(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}

	ctx := context.Background()

	// Create an old saga
	old := makeTestState("old-1", "test-saga", StatusCompleted)
	old.StartedAt = time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Create a recent saga
	recent := makeTestState("recent-1", "test-saga", StatusCompleted)
	if err := store.Create(ctx, recent); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Fast-forward miniredis to ensure time-based sorting works
	mr.FastForward(time.Second)

	deleted, err := store.DeleteOlderThan(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	// Recent should still exist
	_, err = store.Get(ctx, "recent-1")
	if err != nil {
		t.Errorf("expected recent saga to still exist: %v", err)
	}
}
