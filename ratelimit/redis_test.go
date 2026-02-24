package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rbaliyan/event/v3/health"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { client.Close() })
	return mr, client
}

func TestRedisLimiter_Allow(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-allow", 5, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Should allow up to limit
	for i := 0; i < 5; i++ {
		if !limiter.Allow(ctx) {
			t.Errorf("expected Allow to return true at iteration %d", i)
		}
	}

	// Should deny after limit
	if limiter.Allow(ctx) {
		t.Error("expected Allow to return false after limit exhausted")
	}
}

func TestRedisLimiter_Allow_NilClient(t *testing.T) {
	_, err := NewRedisLimiter(nil, "test", 10, time.Second)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestRedisLimiter_Allow_DefaultsForInvalidParams(t *testing.T) {
	_, client := newTestRedisClient(t)

	limiter, err := NewRedisLimiter(client, "test-defaults", 0, 0)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// limit defaults to 1, window defaults to 1s
	ctx := context.Background()
	if !limiter.Allow(ctx) {
		t.Error("expected first Allow to succeed")
	}
	if limiter.Allow(ctx) {
		t.Error("expected second Allow to fail with limit=1")
	}
}

func TestRedisLimiter_Wait(t *testing.T) {
	mr, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-wait", 2, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Exhaust limit
	limiter.Allow(ctx)
	limiter.Allow(ctx)

	// Wait should block; use a short timeout to verify it does not return immediately
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err = limiter.Wait(waitCtx)
	if err == nil {
		t.Error("expected Wait to return error when limit exhausted and context times out")
	}

	// After fast-forwarding the window, Allow should work again
	mr.FastForward(2 * time.Second)

	if !limiter.Allow(ctx) {
		t.Error("expected Allow to succeed after window expired")
	}
}

func TestRedisLimiter_Wait_ImmediateAllow(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-wait-immediate", 10, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Should return immediately when tokens are available
	err = limiter.Wait(ctx)
	if err != nil {
		t.Errorf("Wait failed: %v", err)
	}
}

func TestRedisLimiter_Reserve(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-reserve", 3, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Reserve when tokens available
	res := limiter.Reserve(ctx)
	if !res.OK() {
		t.Error("expected reservation to be OK")
	}
	if res.Delay() != 0 {
		t.Errorf("expected zero delay, got %v", res.Delay())
	}

	// Exhaust remaining tokens
	limiter.Allow(ctx)
	limiter.Allow(ctx)

	// Reserve when exhausted
	res = limiter.Reserve(ctx)
	if res.OK() {
		t.Error("expected reservation to not be OK when exhausted")
	}
	if res.Delay() == 0 {
		t.Error("expected non-zero delay when exhausted")
	}

	// Cancel should not panic
	res.Cancel()
}

func TestRedisLimiter_Remaining(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-remaining", 10, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Should start at limit
	remaining, err := limiter.Remaining(ctx)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	if remaining != 10 {
		t.Errorf("expected 10 remaining, got %d", remaining)
	}

	// Consume 3 tokens
	limiter.Allow(ctx)
	limiter.Allow(ctx)
	limiter.Allow(ctx)

	remaining, err = limiter.Remaining(ctx)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	if remaining != 7 {
		t.Errorf("expected 7 remaining, got %d", remaining)
	}

	// Exhaust all tokens
	for i := 0; i < 7; i++ {
		limiter.Allow(ctx)
	}

	remaining, err = limiter.Remaining(ctx)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected 0 remaining, got %d", remaining)
	}
}

func TestRedisLimiter_Reset(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	limiter, err := NewRedisLimiter(client, "test-reset", 5, time.Second)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Consume all tokens
	for i := 0; i < 5; i++ {
		limiter.Allow(ctx)
	}

	// Should be denied
	if limiter.Allow(ctx) {
		t.Error("expected Allow to fail after exhausting limit")
	}

	// Reset
	if err := limiter.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Should be allowed again
	remaining, err := limiter.Remaining(ctx)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}
	if remaining != 5 {
		t.Errorf("expected 5 remaining after reset, got %d", remaining)
	}

	if !limiter.Allow(ctx) {
		t.Error("expected Allow to succeed after reset")
	}
}

func TestRedisLimiter_Health(t *testing.T) {
	_, client := newTestRedisClient(t)
	ctx := context.Background()

	t.Run("healthy when capacity available", func(t *testing.T) {
		limiter, err := NewRedisLimiter(client, "test-health-healthy", 100, time.Second)
		if err != nil {
			t.Fatalf("NewRedisLimiter: %v", err)
		}

		result := limiter.Health(ctx)
		if result.Status != health.StatusHealthy {
			t.Errorf("expected healthy, got %v", result.Status)
		}
		if result.Details["remaining"] != 100 {
			t.Errorf("expected remaining=100, got %v", result.Details["remaining"])
		}
		if result.Details["limit"] != 100 {
			t.Errorf("expected limit=100, got %v", result.Details["limit"])
		}
	})

	t.Run("degraded when capacity low", func(t *testing.T) {
		limiter, err := NewRedisLimiter(client, "test-health-degraded", 10, time.Second)
		if err != nil {
			t.Fatalf("NewRedisLimiter: %v", err)
		}

		// Consume all tokens (threshold = 10/10 = 1, so 0 remaining triggers degraded)
		for i := 0; i < 10; i++ {
			limiter.Allow(ctx)
		}

		result := limiter.Health(ctx)
		if result.Status != health.StatusDegraded {
			t.Errorf("expected degraded, got %v", result.Status)
		}
	})

	t.Run("unhealthy when redis down", func(t *testing.T) {
		mr := miniredis.RunT(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })

		limiter, err := NewRedisLimiter(c, "test-health-unhealthy", 10, time.Second)
		if err != nil {
			t.Fatalf("NewRedisLimiter: %v", err)
		}

		// Close miniredis to simulate failure
		mr.Close()

		result := limiter.Health(ctx)
		if result.Status != health.StatusUnhealthy {
			t.Errorf("expected unhealthy, got %v", result.Status)
		}
	})
}
