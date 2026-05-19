//go:build integration

package lifecycle

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
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

func TestRedisStore_LeaseExpiry(t *testing.T) {
	store := newRedisIntegrationStore(t)
	ctx := context.Background()
	key := "lease-expiry"

	if _, err := store.Acquire(ctx, key, "pod-a", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over expired lease, got %+v", s)
	}
}
