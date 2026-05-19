# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
go test ./...          # Run all tests
go test -run TestName  # Run a specific test
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
```

## Project Overview

Event Extras (`github.com/rbaliyan/event-extras`) provides extended functionality packages for the [event](https://github.com/rbaliyan/event) library. These packages add capabilities like rate limiting and saga orchestration that complement the core event library.

## Architecture

### Packages

#### ratelimit

Rate limiting implementations for controlling event processing rates:

**Implementations:**
- `TokenBucket`: Local token bucket using golang.org/x/time/rate
- `RedisLimiter`: Distributed fixed-window rate limiting with Redis
- `SlidingWindowLimiter`: Distributed sliding window rate limiting with Redis

**Key Interfaces:**
```go
type Limiter interface {
    Allow(ctx context.Context) bool           // Check and consume one token
    Wait(ctx context.Context) error           // Block until token available
    Reserve(ctx context.Context) Reservation  // Reserve a token for future use
}

type Reservation interface {
    OK() bool           // Whether reservation succeeded
    Delay() time.Duration // How long to wait
    Cancel()            // Cancel the reservation
}
```

#### lifecycle

"Run exactly once per fleet" coordination for deployment-time hooks (schema migrations, cache warmup). Keyed by `{Hook.Name}@{Hook.Version}`; bumping the version makes the hook eligible to run again on the next rollout.

**Core Components:**

- `Hook`: `{Name, Version}` — `Name` must not contain `@` (validated by `Hook.Validate` and `Once`)
- `Once(ctx, store, hook, fn, opts...) (Outcome, error)`: high-level entry point; winner runs `fn` with an auto-refreshing lease, others observe and skip
- `Store`: persistence contract — `Acquire / Refresh / Complete / Fail / Get / Reset`
- `Outcome` enum: `OutcomeRan`, `OutcomeSkippedRunning`, `OutcomeSkippedCompleted`, `OutcomeSkippedFailed`, `OutcomeFailed` (body errored or panicked), `OutcomeCompleteFailed` (body succeeded but bookkeeping write failed)
- `State` enum: `StatePending`, `StateRunning`, `StateCompleted`, `StateFailed`
- Sentinel errors: `ErrLeaseLost` (Refresh/Complete/Fail by non-holder or after own lease expiry), `ErrInvalidHookName`

**Store Implementations:**
- `MemoryStore`: in-process; tests/single-instance. Does NOT implement `health.Checker`.
- `RedisStore`: atomic transitions via four Lua scripts (acquire, refresh, complete, fail); single HASH per hook. Implements `health.Checker` (PING).
- `PostgresStore`: atomic via `SELECT ... FOR UPDATE` inside a ReadCommitted txn. `EnsureSchema` creates the table. Implements `health.Checker` (per-state counts).
- `MongoStore`: atomic via `findOneAndUpdate` with `upsert=true`; duplicate-key collision triggers a follow-up `Get`. Implements `health.Checker`.

**Clock semantics:**
- `PostgresStore` compares `lease_until` against the database server's `NOW()` → robust to cross-pod clock skew.
- Redis, Mongo, Memory compare against caller's `time.Now()`.

**Options on Once:**
- `WithInstanceID(id)`: defaults to `<hostname>:<pid>`. In K8s use `POD_NAME`.
- `WithLease(d)`: default 60s.
- `WithRefreshInterval(d)`: default `lease/3` with a 1-second floor.
- `WithRetryable(bool)`: default `false`. `false` puts body errors into terminal `StateFailed` requiring manual `Reset` — safer for non-idempotent work. `true` releases the claim on body error for retry.
- `WithLogger(*slog.Logger)`.

**Behavioral guarantees:**
- A panicking body is caught, recorded via `Fail`, and surfaces as `OutcomeFailed`. The claim is NOT leaked until lease expiry.
- `Reset` is unconditional — calling it on a `running` entry silently yanks the lease from the live holder. Documented in `Store.Reset` godoc.
- Lease loss during execution does not cancel the body; another instance may take over after lease expiry, and the first writer's `Complete` returns `ErrLeaseLost`.

**Key Types:**
```go
type Hook struct {
    Name    string
    Version string
}

type Store interface {
    Acquire(ctx, key, instanceID string, lease time.Duration) (Status, error)
    Refresh(ctx, key, instanceID string, lease time.Duration) error
    Complete(ctx, key, instanceID string) error
    Fail(ctx, key, instanceID, errMsg string, retryable bool) error
    Get(ctx, key string) (Status, error)
    Reset(ctx, key string) error
}
```

#### saga

Saga pattern for distributed transactions with automatic compensation:

**Core Components:**

- `Saga`: Orchestrator that executes steps in sequence with automatic compensation on failure
- `Step`: Interface for saga steps (Execute + Compensate)
- `Store`: Interface for saga state persistence
- `BackoffStrategy`: Pluggable retry delay calculation (type alias for `backoff.Strategy`)
- `MetricsRecorder`: OpenTelemetry metrics for saga execution

**Store Implementations:**
- `MemoryStore`: In-memory store for testing
- `RedisStore`: Distributed storage with set-based indexes and optional TTL
- `MongoStore`: MongoDB document storage with aggregation-based statistics
- `PostgresStore`: PostgreSQL storage with JSONB data and array types

**Key Types:**
```go
type Step interface {
    Name() string
    Execute(ctx context.Context, data any) error
    Compensate(ctx context.Context, data any) error
}

type State struct {
    ID             string
    Name           string
    Status         Status
    CurrentStep    int
    CompletedSteps []string
    Data           any
    Error          string
    StartedAt      time.Time
    CompletedAt    *time.Time
}

type Status string  // pending, running, completed, compensating, compensated, failed

type Store interface {
    Create(ctx context.Context, state *State) error
    Get(ctx context.Context, id string) (*State, error)
    Update(ctx context.Context, state *State) error
    List(ctx context.Context, filter StoreFilter) ([]*State, error)
}
```

### Data Flow

```
Saga.Execute(ctx, sagaID, data)
    |
    v
For each step:
    +-> Execute step
    |       |
    |       +-> Success: Update store, continue to next step
    |       |
    |       +-> Failure: Check retry logic
    |               |
    |               +-> Retry count < MaxRetries: Apply backoff, retry
    |               |
    |               +-> Retry exhausted: Start compensation
    |
    v
Compensation (on failure):
    +-> For each completed step (reverse order):
            +-> Call step.Compensate()
            +-> Update store
    +-> Return original error
```

### Saga Status Transitions

```
pending -> running -> completed
                   \
                    compensating -> compensated
                                \
                                 failed
```

### Retry and Backoff

The saga uses the same backoff strategies as the main event library:
```go
type BackoffStrategy = backoff.Strategy

s, err := saga.New("order-creation", steps,
    saga.WithBackoff(&backoff.Exponential{
        Initial:    100 * time.Millisecond,
        Multiplier: 2.0,
        Max:        5 * time.Second,
    }),
    saga.WithMaxRetries(3),
)
```

### Key Design Patterns

- **Interface-Based Design**: All stores implement the `Store` interface; all limiters implement the `Limiter` interface
- **Functional Options**: Saga and stores configured via `New(name, steps, ...Option)` pattern
- **Idempotent Compensations**: Compensate methods must be safe to call multiple times
- **Compile-Time Checks**: `var _ Store = (*MemoryStore)(nil)` ensures interface compliance

### Default Configuration

**Saga:**
- Max Retries: 0 (no automatic retry)
- Backoff: nil (immediate retry if retries enabled)
- Logger: slog.Default()

**Rate Limiters:**
- TokenBucket: Configured with RPS and burst size
- RedisLimiter: Configured with limit count and window duration

## Dependencies

- `github.com/rbaliyan/event/v3` - Core event library and backoff strategies
- `github.com/redis/go-redis/v9` - Redis client for distributed implementations
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel` - OpenTelemetry metrics
- `golang.org/x/time/rate` - Token bucket rate limiter

## Related Libraries

- `github.com/rbaliyan/event/v3` - Core event bus with transports
- `github.com/rbaliyan/event-scheduler` - Delayed/scheduled message delivery
- `github.com/rbaliyan/event-dlq` - Dead-letter queue management

## Testing

```bash
go test ./...
```

Redis-dependent tests require a running Redis instance. MongoDB and PostgreSQL tests require their respective databases.
