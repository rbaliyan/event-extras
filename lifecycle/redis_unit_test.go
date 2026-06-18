package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisClientForAddr(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestNewRedisStore_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisStore(nil); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestWithRedisPrefix(t *testing.T) {
	t.Parallel()
	_, def := newRedisTestStore(t)
	if def.prefix != "lifecycle:" {
		t.Fatalf("expected default prefix, got %q", def.prefix)
	}
	mr, _ := newRedisTestStore(t)
	custom, err := NewRedisStore(redisClientForAddr(t, mr.Addr()), WithRedisPrefix("app:hooks:"))
	if err != nil {
		t.Fatal(err)
	}
	if custom.prefix != "app:hooks:" {
		t.Fatalf("expected custom prefix, got %q", custom.prefix)
	}
	// Empty prefix is ignored, falling back to the default.
	def2, err := NewRedisStore(redisClientForAddr(t, mr.Addr()), WithRedisPrefix(""))
	if err != nil {
		t.Fatal(err)
	}
	if def2.prefix != "lifecycle:" {
		t.Fatalf("empty prefix should keep default, got %q", def2.prefix)
	}
}

// TestRedisStore_RefreshAtExactBoundary pins the lease-expiry boundary for the
// Redis Lua script: at the exact instant now == lease_until the refresh must
// be rejected (the >= comparison in refreshScript) while a takeover Acquire
// succeeds — never both. Uses the injectable clock so the boundary is exact.
func TestRedisStore_RefreshAtExactBoundary(t *testing.T) {
	t.Parallel()
	_, store := newRedisTestStore(t)
	base := time.Now()
	store.now = func() time.Time { return base }
	ctx := context.Background()

	if _, err := store.Acquire(ctx, "k", "pod-a", 100*time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Step to exactly lease_until.
	store.now = func() time.Time { return base.Add(100 * time.Millisecond) }

	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("refresh at exact boundary should fail with ErrLeaseLost, got %v", err)
	}
	s, err := store.Acquire(ctx, "k", "pod-b", time.Minute)
	if err != nil {
		t.Fatalf("Acquire takeover: %v", err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over at exact boundary, got %+v", s)
	}
}

func TestRedisStore_GetAndReset(t *testing.T) {
	t.Parallel()
	_, store := newRedisTestStore(t)
	ctx := context.Background()

	if s, err := store.Get(ctx, "absent"); err != nil || s.State != StatePending {
		t.Fatalf("absent Get should be pending, got %+v err=%v", s, err)
	}
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	s, err := store.Get(ctx, "k")
	if err != nil || s.State != StateRunning || s.Holder != "pod-a" {
		t.Fatalf("expected running/pod-a, got %+v err=%v", s, err)
	}
	if err := store.Reset(ctx, "k"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if s, err := store.Get(ctx, "k"); err != nil || s.State != StatePending {
		t.Fatalf("after reset expected pending, got %+v err=%v", s, err)
	}
}

// TestRedisStore_ErrorsWhenClosed covers the Redis-error return branch of every
// store method in the default build. It points the client at an unreachable
// address with retries disabled and short timeouts so every call fails fast.
func TestRedisStore_ErrorsWhenClosed(t *testing.T) {
	t.Parallel()
	client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1", // nothing listens here -> connection refused
		MaxRetries:   -1,            // no retry/backoff
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("Acquire should error against a closed server")
	}
	if _, err := store.Get(ctx, "k"); err == nil {
		t.Fatal("Get should error against a closed server")
	}
	if err := store.Reset(ctx, "k"); err == nil {
		t.Fatal("Reset should error against a closed server")
	}
	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("Refresh should error against a closed server")
	}
	if err := store.Complete(ctx, "k", "pod-a"); err == nil {
		t.Fatal("Complete should error against a closed server")
	}
	if err := store.Fail(ctx, "k", "pod-a", "x", false); err == nil {
		t.Fatal("Fail should error against a closed server")
	}
}

func TestParseHashResult(t *testing.T) {
	t.Parallel()
	t.Run("non_array_errors", func(t *testing.T) {
		if _, err := parseHashResult("not-an-array"); err == nil {
			t.Fatal("expected error for non-array result")
		}
	})
	t.Run("empty_is_pending", func(t *testing.T) {
		s, err := parseHashResult([]any{})
		if err != nil || s.State != StatePending {
			t.Fatalf("empty array should be pending, got %+v err=%v", s, err)
		}
	})
	t.Run("populated_running", func(t *testing.T) {
		res := []any{"state", "running", "holder", "pod-a", "lease_until_ms", "1700000000000"}
		s, err := parseHashResult(res)
		if err != nil || s.State != StateRunning || s.Holder != "pod-a" {
			t.Fatalf("expected running/pod-a, got %+v err=%v", s, err)
		}
		if s.LeaseUntil.IsZero() {
			t.Fatal("expected non-zero lease")
		}
	})
	t.Run("odd_length_is_tolerated", func(t *testing.T) {
		// A trailing key with no value must not panic; it is simply ignored.
		if _, err := parseHashResult([]any{"state", "running", "holder"}); err != nil {
			t.Fatalf("odd-length array should not error, got %v", err)
		}
	})
}

func TestHashMapToStatus_UnknownStateIsPending(t *testing.T) {
	t.Parallel()
	s := hashMapToStatus(map[string]string{"state": "gibberish", "holder": "pod-a"})
	if s.State != StatePending {
		t.Fatalf("unknown state should map to pending, got %+v", s)
	}
	// Pending status must carry no holder per the documented invariant.
	if s.Holder != "" {
		t.Fatalf("pending status should have empty holder, got %q", s.Holder)
	}
}

func TestMsToTime(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		zero bool
	}{
		{"empty", "", true},
		{"zero", "0", true},
		{"unparseable", "not-a-number", true},
		{"valid", "1700000000000", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := msToTime(tc.in)
			if got.IsZero() != tc.zero {
				t.Fatalf("msToTime(%q).IsZero() = %v, want %v", tc.in, got.IsZero(), tc.zero)
			}
		})
	}
}
