package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRedisTestStore(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return mr, store
}

// runStoreContractTests verifies that any Store implementation honors the
// documented state machine. Each subtest uses a unique key so a single
// shared store can be reused safely.
func runStoreContractTests(t *testing.T, store Store) {
	t.Run("acquire_winner_runs_others_skip", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-1"

		s1, err := store.Acquire(ctx, key, "pod-a", time.Minute)
		if err != nil {
			t.Fatalf("acquire pod-a: %v", err)
		}
		if s1.State != StateRunning || s1.Holder != "pod-a" {
			t.Fatalf("pod-a should hold lease, got %+v", s1)
		}

		s2, err := store.Acquire(ctx, key, "pod-b", time.Minute)
		if err != nil {
			t.Fatalf("acquire pod-b: %v", err)
		}
		if s2.Holder != "pod-a" {
			t.Fatalf("pod-b should observe pod-a as holder, got %+v", s2)
		}
	})

	t.Run("complete_is_terminal", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-complete"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := store.Complete(ctx, key, "pod-a"); err != nil {
			t.Fatal(err)
		}
		s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if s.State != StateCompleted {
			t.Fatalf("expected StateCompleted after complete, got %+v", s)
		}
	})

	t.Run("fail_non_retryable_is_terminal", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-fail"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := store.Fail(ctx, key, "pod-a", "boom", false); err != nil {
			t.Fatal(err)
		}
		s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if s.State != StateFailed || s.Error != "boom" {
			t.Fatalf("expected StateFailed, got %+v", s)
		}
	})

	t.Run("fail_retryable_releases_claim", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-retry"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := store.Fail(ctx, key, "pod-a", "transient", true); err != nil {
			t.Fatal(err)
		}
		s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if s.State != StateRunning || s.Holder != "pod-b" {
			t.Fatalf("pod-b should claim after retryable fail, got %+v", s)
		}
	})

	t.Run("refresh_by_non_holder_fails", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-refresh"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		err := store.Refresh(ctx, key, "pod-b", time.Minute)
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost, got %v", err)
		}
	})

	t.Run("refresh_by_holder_after_own_expiry_fails", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-refresh-expired"

		// Acquire with a very short lease, wait for it to expire, then
		// the original holder must be told its lease is gone — driver of
		// the "body may run twice" warning in refreshLoop.
		if _, err := store.Acquire(ctx, key, "pod-a", 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(150 * time.Millisecond)
		err := store.Refresh(ctx, key, "pod-a", time.Minute)
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost for same holder after own lease expiry, got %v", err)
		}
	})

	t.Run("complete_by_non_holder_fails", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-complete-wrong"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		err := store.Complete(ctx, key, "pod-b")
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost, got %v", err)
		}
	})

	t.Run("reset_clears_terminal_state", func(t *testing.T) {
		ctx := context.Background()
		key := "hook-reset"

		if _, err := store.Acquire(ctx, key, "pod-a", time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := store.Complete(ctx, key, "pod-a"); err != nil {
			t.Fatal(err)
		}
		if err := store.Reset(ctx, key); err != nil {
			t.Fatal(err)
		}
		s, err := store.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if s.State != StatePending {
			t.Fatalf("expected StatePending after reset, got %+v", s)
		}
	})

	t.Run("get_absent_returns_pending", func(t *testing.T) {
		s, err := store.Get(context.Background(), "never-existed")
		if err != nil {
			t.Fatal(err)
		}
		if s.State != StatePending {
			t.Fatalf("expected StatePending, got %+v", s)
		}
	})
}

func TestMemoryStore_Contract(t *testing.T) {
	runStoreContractTests(t, NewMemoryStore())
}

func TestRedisStore_Contract(t *testing.T) {
	_, store := newRedisTestStore(t)
	runStoreContractTests(t, store)
}

func TestMemoryStore_ExpiredLeaseTransfers(t *testing.T) {
	store := NewMemoryStore()
	base := time.Now()
	store.now = func() time.Time { return base }

	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return base.Add(time.Second) } // past lease

	s, err := store.Acquire(ctx, "k", "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over expired lease, got %+v", s)
	}
}

func TestMemoryStore_RefreshAtExactExpiryFails(t *testing.T) {
	store := NewMemoryStore()
	base := time.Now()
	store.now = func() time.Time { return base }
	ctx := context.Background()

	if _, err := store.Acquire(ctx, "k", "pod-a", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Advance to the exact expiry instant. The lease boundary is exclusive, so
	// the holder's own Refresh must fail and a takeover must be allowed — the
	// two verdicts must never both succeed at now == leaseUntil.
	store.now = func() time.Time { return base.Add(100 * time.Millisecond) }

	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("refresh at exact expiry should fail with ErrLeaseLost, got %v", err)
	}
	s, err := store.Acquire(ctx, "k", "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over at exact expiry, got %+v", s)
	}
}

// TestOnce_LeaseLostDuringExecution_Takeover drives a real lease expiry and
// takeover end-to-end: while pod-a's body is still running, its lease expires
// and pod-b claims the hook. This exercises refreshLoop's ErrLeaseLost branch
// and the OutcomeCompleteFailed path where the original holder's Complete is
// rejected because it no longer holds the claim.
func TestOnce_LeaseLostDuringExecution_Takeover(t *testing.T) {
	store := NewMemoryStore()
	var mu sync.Mutex
	nowVal := time.Now()
	store.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }
	advance := func(d time.Duration) { mu.Lock(); nowVal = nowVal.Add(d); mu.Unlock() }

	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}

	bodyStarted := make(chan struct{})
	releaseBody := make(chan struct{})

	var (
		outcome Outcome
		onceErr error
	)
	done := make(chan struct{})
	go func() {
		outcome, onceErr = Once(ctx, store, hook, func(context.Context) error {
			close(bodyStarted)
			<-releaseBody
			return nil
		}, WithInstanceID("pod-a"), WithLease(50*time.Millisecond), WithRefreshInterval(5*time.Millisecond))
		close(done)
	}()

	<-bodyStarted
	// Expire pod-a's lease and let pod-b take it over while the body is blocked.
	advance(time.Second)
	s, err := store.Acquire(ctx, hook.Key(), "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("pod-b should have taken over the expired lease, got %+v", s)
	}
	// Give the refresh loop a few ticks to observe the lost lease.
	time.Sleep(30 * time.Millisecond)
	close(releaseBody)
	<-done

	if outcome != OutcomeCompleteFailed {
		t.Fatalf("expected OutcomeCompleteFailed after takeover, got %v", outcome)
	}
	if !errors.Is(onceErr, ErrLeaseLost) {
		t.Fatalf("expected Complete to surface ErrLeaseLost, got %v", onceErr)
	}
}

// TestRefreshLoop_GivesUpAfterLeaseWindowOnTransientErrors covers the
// escalation branch: when Refresh keeps returning a non-sentinel (transient)
// error past a full lease window, the loop presumes the lease lost and exits
// on its own — without the parent context being cancelled.
func TestRefreshLoop_GivesUpAfterLeaseWindowOnTransientErrors(t *testing.T) {
	store := &refreshErrStore{Store: NewMemoryStore(), err: errors.New("transient blip")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})

	// Short lease so the deadline lapses quickly; shorter interval so ticks
	// fire several times within the window. Parent context is never cancelled.
	go refreshLoop(context.Background(), store, "k", "pod-a",
		40*time.Millisecond, 10*time.Millisecond, log, done)

	select {
	case <-done:
		// Loop gave up by itself — escalation branch exercised.
	case <-time.After(2 * time.Second):
		t.Fatal("refreshLoop did not give up after the lease window elapsed under persistent transient errors")
	}
}

// refreshErrStore wraps a Store and makes Refresh always return a chosen
// non-sentinel error; used to drive the transient give-up branch.
type refreshErrStore struct {
	Store
	err error
}

func (s *refreshErrStore) Refresh(context.Context, string, string, time.Duration) error {
	return s.err
}

func TestOnce_SingleInstance(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	var ran int32
	outcome, err := Once(ctx, store, Hook{Name: "migrate", Version: "v1"}, func(context.Context) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}, WithLease(5*time.Second), WithInstanceID("pod-a"))
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if outcome != OutcomeRan {
		t.Fatalf("expected OutcomeRan, got %v", outcome)
	}
	if ran != 1 {
		t.Fatalf("expected fn to run exactly once, got %d", ran)
	}

	// Second call: completed, body not invoked.
	outcome2, err := Once(ctx, store, Hook{Name: "migrate", Version: "v1"}, func(context.Context) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}, WithInstanceID("pod-a"))
	if err != nil {
		t.Fatalf("Once second: %v", err)
	}
	if outcome2 != OutcomeSkippedCompleted {
		t.Fatalf("expected OutcomeSkippedCompleted, got %v", outcome2)
	}
	if ran != 1 {
		t.Fatalf("body should not have re-run, ran=%d", ran)
	}
}

func TestOnce_Fleet(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	const fleet = 20
	var ran int32
	var skippedRunning, skippedCompleted int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range fleet {
		wg.Add(1)
		instanceID := fmt.Sprintf("pod-%d", i)
		go func() {
			defer wg.Done()
			<-start
			outcome, err := Once(ctx, store, Hook{Name: "migrate", Version: "v1"},
				func(context.Context) error {
					atomic.AddInt32(&ran, 1)
					time.Sleep(50 * time.Millisecond)
					return nil
				},
				WithInstanceID(instanceID),
				WithLease(2*time.Second),
			)
			if err != nil {
				t.Errorf("instance %s: %v", instanceID, err)
				return
			}
			switch outcome {
			case OutcomeRan:
				// winner
			case OutcomeSkippedRunning:
				atomic.AddInt32(&skippedRunning, 1)
			case OutcomeSkippedCompleted:
				atomic.AddInt32(&skippedCompleted, 1)
			default:
				t.Errorf("instance %s unexpected outcome %v", instanceID, outcome)
			}
		}()
	}
	close(start)
	wg.Wait()

	if ran != 1 {
		t.Fatalf("body should run exactly once across the fleet, got %d", ran)
	}
	if total := skippedRunning + skippedCompleted; int(total) != fleet-1 {
		t.Fatalf("expected %d skips, got %d (running=%d completed=%d)",
			fleet-1, total, skippedRunning, skippedCompleted)
	}
}

func TestOnce_BodyFailure_NonRetryable(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}

	want := errors.New("migration boom")
	outcome, err := Once(ctx, store, hook, func(context.Context) error {
		return want
	}, WithInstanceID("pod-a"))
	if outcome != OutcomeFailed || !errors.Is(err, want) {
		t.Fatalf("expected OutcomeFailed with err=%v, got outcome=%v err=%v", want, outcome, err)
	}

	// Second call sees terminal failed state.
	outcome2, err := Once(ctx, store, hook, func(context.Context) error {
		t.Fatal("should not run after terminal failure")
		return nil
	}, WithInstanceID("pod-b"))
	if err != nil {
		t.Fatalf("Once after failure: %v", err)
	}
	if outcome2 != OutcomeSkippedFailed {
		t.Fatalf("expected OutcomeSkippedFailed, got %v", outcome2)
	}
}

func TestOnce_BodyFailure_Retryable(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}

	var attempts int32
	want := errors.New("transient")
	for i := range 2 {
		instance := fmt.Sprintf("pod-%d", i)
		_, _ = Once(ctx, store, hook, func(context.Context) error {
			atomic.AddInt32(&attempts, 1)
			return want
		}, WithInstanceID(instance), WithRetryable(true))
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts under retryable, got %d", attempts)
	}
}

func TestOnce_BodyPanic_RecordedAsFailure(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}

	outcome, err := Once(ctx, store, hook, func(context.Context) error {
		panic("boom")
	}, WithInstanceID("pod-a"))
	if outcome != OutcomeFailed {
		t.Fatalf("expected OutcomeFailed after panic, got %v", outcome)
	}
	if err == nil {
		t.Fatal("expected error after panic")
	}
	// Subsequent Once call must see terminal failed state — proves Fail
	// bookkeeping ran instead of leaking the claim until lease expiry.
	outcome2, _ := Once(ctx, store, hook, func(context.Context) error {
		t.Fatal("should not run after panicked body")
		return nil
	}, WithInstanceID("pod-b"))
	if outcome2 != OutcomeSkippedFailed {
		t.Fatalf("expected OutcomeSkippedFailed, got %v", outcome2)
	}
}

func TestOnce_CompleteFailureSurfacedDistinctly(t *testing.T) {
	// Wrap a MemoryStore so Complete returns a transient error while every
	// other method delegates normally. Verifies Once returns
	// OutcomeCompleteFailed (not OutcomeFailed) when fn succeeded but the
	// bookkeeping call did not.
	base := NewMemoryStore()
	store := &completeFailingStore{Store: base, completeErr: errors.New("redis blip")}
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}

	var ran int32
	outcome, err := Once(ctx, store, hook, func(context.Context) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}, WithInstanceID("pod-a"))
	if ran != 1 {
		t.Fatalf("body should have run, ran=%d", ran)
	}
	if outcome != OutcomeCompleteFailed {
		t.Fatalf("expected OutcomeCompleteFailed, got %v", outcome)
	}
	if !errors.Is(err, store.completeErr) {
		t.Fatalf("expected wrapped completeErr, got %v", err)
	}
}

func TestOnce_RejectsInvalidHookName(t *testing.T) {
	store := NewMemoryStore()
	_, err := Once(context.Background(), store, Hook{Name: "bad@name", Version: "v1"},
		func(context.Context) error { return nil })
	if !errors.Is(err, ErrInvalidHookName) {
		t.Fatalf("expected ErrInvalidHookName, got %v", err)
	}
}

// completeFailingStore wraps a Store and makes Complete return a chosen
// error; used by TestOnce_CompleteFailureSurfacedDistinctly.
type completeFailingStore struct {
	Store
	completeErr error
}

func (c *completeFailingStore) Complete(context.Context, string, string) error {
	return c.completeErr
}

func TestHook_Validate(t *testing.T) {
	cases := []struct {
		name    string
		hook    Hook
		wantErr error
	}{
		{name: "valid_no_version", hook: Hook{Name: "migrate"}, wantErr: nil},
		{name: "valid_with_version", hook: Hook{Name: "migrate", Version: "v1"}, wantErr: nil},
		{name: "empty_name_rejected", hook: Hook{Name: "", Version: "v1"}, wantErr: errors.New("required")},
		{name: "at_in_name_rejected", hook: Hook{Name: "bad@name"}, wantErr: ErrInvalidHookName},
		{name: "at_in_name_with_version", hook: Hook{Name: "bad@name", Version: "v1"}, wantErr: ErrInvalidHookName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.hook.Validate()
			switch {
			case tc.wantErr == nil:
				if err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
			case errors.Is(tc.wantErr, ErrInvalidHookName):
				if !errors.Is(err, ErrInvalidHookName) {
					t.Fatalf("expected ErrInvalidHookName, got %v", err)
				}
			default:
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
			}
		})
	}
}

func TestHook_Key(t *testing.T) {
	cases := []struct {
		name string
		hook Hook
		want string
	}{
		{name: "bare_when_version_empty", hook: Hook{Name: "migrate"}, want: "migrate"},
		{name: "joined_when_version_set", hook: Hook{Name: "migrate", Version: "v1.2.3"}, want: "migrate@v1.2.3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.hook.Key(); got != tc.want {
				t.Fatalf("Key() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedisStore_Health(t *testing.T) {
	mr, store := newRedisTestStore(t)
	ctx := context.Background()

	res := store.Health(ctx)
	if string(res.Status) != "healthy" {
		t.Fatalf("expected healthy status, got %q (msg=%q)", res.Status, res.Message)
	}
	if res.Details["prefix"] != "lifecycle:" {
		t.Fatalf("expected prefix in details, got %v", res.Details)
	}

	// Simulate Redis being unreachable.
	mr.Close()
	res = store.Health(ctx)
	if string(res.Status) == "healthy" {
		t.Fatalf("expected non-healthy status after closing redis, got %q", res.Status)
	}
	if res.Message == "" {
		t.Fatalf("expected error message in unhealthy result")
	}
}
