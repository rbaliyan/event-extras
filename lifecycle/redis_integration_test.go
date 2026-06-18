package lifecycle

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// getRedisClient returns a live client, or skips the test when REDIS_ADDR is
// unset. Runs in the normal build: skips instantly with no env, runs against a
// container when REDIS_ADDR is set. (The miniredis-backed tests already cover
// the Redis store in the default build; these exercise a real server.)
func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newRedisIntegrationStore(t *testing.T) *RedisStore {
	t.Helper()
	client := getRedisClient(t)
	prefix := fmt.Sprintf("lifecycle:test:%d:", time.Now().UnixNano())
	store, err := NewRedisStore(client, WithRedisPrefix(prefix))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
	})
	return store
}

func TestRedisStore_ContractIntegration(t *testing.T) {
	store := newRedisIntegrationStore(t)
	runStoreContractTests(t, store)
}

func TestRedisStore_HealthIntegration(t *testing.T) {
	store := newRedisIntegrationStore(t)
	res := store.Health(context.Background())
	if string(res.Status) != "healthy" {
		t.Fatalf("expected healthy, got %q (msg=%q)", res.Status, res.Message)
	}
	if _, ok := res.Details["prefix"]; !ok {
		t.Fatalf("expected prefix detail, got %v", res.Details)
	}
}

func TestRedisStore_LeaseExpiry(t *testing.T) {
	store := newRedisIntegrationStore(t)
	ctx := context.Background()
	key := "lease-expiry"

	// Drive expiry deterministically through the injectable clock (the Lua
	// scripts compare against the now_ms we pass) instead of a wall-clock sleep.
	base := time.Now()
	store.now = func() time.Time { return base }
	if _, err := store.Acquire(ctx, key, "pod-a", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return base.Add(time.Second) } // past the lease

	s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over expired lease, got %+v", s)
	}
}

// TestRedisStore_ErrorsWhenServerDown injects a backend failure: after the
// client is closed, store operations must surface a wrapped error rather than
// silently succeeding.
func TestRedisStore_ErrorsWhenServerDown(t *testing.T) {
	client := getRedisClient(t)
	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Acquire to error against a closed client")
	}
	if err := store.Reset(ctx, "k"); err == nil {
		t.Fatal("expected Reset to error against a closed client")
	}
	if _, err := store.Get(ctx, "k"); err == nil {
		t.Fatal("expected Get to error against a closed client")
	}
	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Refresh to error against a closed client")
	}
	if err := store.Complete(ctx, "k", "pod-a"); err == nil {
		t.Fatal("expected Complete to error against a closed client")
	}
	if err := store.Fail(ctx, "k", "pod-a", "x", false); err == nil {
		t.Fatal("expected Fail to error against a closed client")
	}
}
