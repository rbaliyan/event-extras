# Event Extras

[![CI](https://github.com/rbaliyan/event-extras/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event-extras/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event-extras.svg)](https://pkg.go.dev/github.com/rbaliyan/event-extras)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event-extras)](https://goreportcard.com/report/github.com/rbaliyan/event-extras)
[![Release](https://img.shields.io/github/v/release/rbaliyan/event-extras)](https://github.com/rbaliyan/event-extras/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/event-extras/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/event-extras)

Extended functionality packages for the [event](https://github.com/rbaliyan/event) library.

## Installation

```bash
go get github.com/rbaliyan/event-extras
```

## Packages

### ratelimit

Rate limiting for event processing with local and distributed implementations.

```go
import "github.com/rbaliyan/event-extras/ratelimit"
```

#### Implementations

| Limiter | Use Case |
|---------|----------|
| `TokenBucket` | Single instance, per-instance limits, low latency |
| `RedisLimiter` | Multi-instance, global limits, simple fixed-window |
| `SlidingWindowLimiter` | Multi-instance, accurate limiting, no boundary burst |

#### Basic Usage

```go
// Local rate limiter: 100 requests/second with burst of 10
limiter := ratelimit.NewTokenBucket(100, 10)

// Distributed rate limiter using Redis
limiter, err := ratelimit.NewRedisLimiter(redisClient, "my-service", 100, time.Second)

// Sliding window for more accurate limiting
limiter, err := ratelimit.NewSlidingWindowLimiter(redisClient, "my-service", 100, time.Second)

// Blocking wait
if err := limiter.Wait(ctx); err != nil {
    return err // Context cancelled
}

// Non-blocking check
if limiter.Allow(ctx) {
    // Process immediately
} else {
    // Rate limited
}
```

#### Metrics

OpenTelemetry metrics for rate limiters:

```go
metrics, _ := ratelimit.NewMetrics(
    ratelimit.WithMeterProvider(provider),
    ratelimit.WithMetricsNamespace("myapp"),
)

// Wrap any limiter with metrics
limiter := ratelimit.NewMetricsLimiter(baseLimiter, "api-limiter", metrics)
```

Available metrics:
- `ratelimit_allowed_total` - Counter of allowed requests
- `ratelimit_rejected_total` - Counter of rejected requests
- `ratelimit_wait_duration_seconds` - Histogram of wait times

#### Health Checks

Redis-based limiters implement `health.Checker`:

```go
result := limiter.Health(ctx)
// result.Status: healthy, degraded (< 10% capacity), unhealthy
// result.Details: remaining, limit, window, key
```

### saga

Saga pattern for distributed transactions with automatic compensation.

```go
import "github.com/rbaliyan/event-extras/saga"
```

#### Core Concepts

A saga orchestrates a sequence of steps, automatically compensating completed steps if any step fails:

```go
// Define steps
type CreateOrderStep struct{}

func (s *CreateOrderStep) Name() string { return "create-order" }

func (s *CreateOrderStep) Execute(ctx context.Context, data any) error {
    order := data.(*Order)
    return orderService.Create(ctx, order)
}

func (s *CreateOrderStep) Compensate(ctx context.Context, data any) error {
    order := data.(*Order)
    return orderService.Cancel(ctx, order.ID)
}
```

#### Creating a Saga

```go
// Create saga with steps and options
recorder := saga.NewMetricsRecorder("myapp")
orderSaga, err := saga.New("order-creation",
    []saga.Step{
        &CreateOrderStep{},
        &ReserveInventoryStep{},
        &ChargePaymentStep{},
    },
    saga.WithStore(store),
    saga.WithBackoff(&backoff.Exponential{
        Initial:    100 * time.Millisecond,
        Multiplier: 2.0,
        Max:        5 * time.Second,
    }),
    saga.WithMaxRetries(3),
    saga.WithMetrics(recorder),
)
if err != nil {
    log.Fatal(err)
}

// Execute saga
err = orderSaga.Execute(ctx, sagaID, &Order{...})
```

#### Saga Status Flow

```
pending -> running -> completed
                   \
                    compensating -> compensated
                                 \
                                  failed
```

#### Store Implementations

| Store | Use Case |
|-------|----------|
| `MemoryStore` | Testing and development |
| `RedisStore` | Production with Redis |
| `MongoStore` | Production with MongoDB |
| `PostgresStore` | Production with PostgreSQL |

All stores implement the `Store` interface:

```go
type Store interface {
    Create(ctx context.Context, state *State) error
    Get(ctx context.Context, id string) (*State, error)
    Update(ctx context.Context, state *State) error
    List(ctx context.Context, filter StoreFilter) ([]*State, error)
}
```

#### Store Setup

```go
// Redis
store, err := saga.NewRedisStore(redisClient,
    saga.WithKeyPrefix("sagas:"),
    saga.WithTTL(7*24*time.Hour),
)

// MongoDB
store, err := saga.NewMongoStore(db,
    saga.WithCollection("sagas"),
)
store.EnsureIndexes(ctx)

// PostgreSQL
store, err := saga.NewPostgresStore(db,
    saga.WithTable("sagas"),
)
store.EnsureTable(ctx)
```

#### Health Checks

All stores implement `health.Checker`:

```go
result := store.Health(ctx)
// result.Status: healthy or unhealthy
// result.Latency: check duration
```

#### Metrics

OpenTelemetry metrics for saga execution:

```go
recorder := saga.NewMetricsRecorder("myapp")

orderSaga, err := saga.New("order-creation", steps,
    saga.WithMetrics(recorder),
)
```

Available metrics:
- `saga_executions_total` - Counter by status (completed, compensated, failed)
- `saga_execution_duration_seconds` - Histogram of execution time
- `saga_step_duration_seconds` - Histogram of step execution time
- `saga_compensation_steps_total` - Counter of compensation step executions

#### Saga State

```go
type State struct {
    ID             string
    Name           string
    Status         Status
    CurrentStep    int
    CompletedSteps []string
    Data           any
    Error          string
    Version        int64  // Optimistic locking
    StartedAt      time.Time
    CompletedAt    *time.Time
}
```

### lifecycle

Run-exactly-once-per-fleet coordination for deployment-time hooks (schema
migrations, cache warmup, version-pinned bootstrap work). On rollout every
pod calls `lifecycle.Once`; one wins the claim and runs the body, the rest
observe and skip.

```go
import "github.com/rbaliyan/event-extras/lifecycle"
```

#### Basic Usage

```go
// Build/deployment version comes from your CI or the go-version library.
hook := lifecycle.Hook{Name: "users-table-migration", Version: "v1.2.3"}

outcome, err := lifecycle.Once(ctx, store, hook, func(ctx context.Context) error {
    return runMigration(ctx, db)
})
if err != nil {
    return err
}

switch outcome {
case lifecycle.OutcomeRan:
    log.Info("migration executed on this pod")
case lifecycle.OutcomeSkippedCompleted:
    log.Info("migration already completed for this version")
case lifecycle.OutcomeSkippedRunning:
    log.Info("another pod is running the migration")
case lifecycle.OutcomeSkippedFailed:
    log.Error("migration is in terminal failed state — manual intervention required")
case lifecycle.OutcomeCompleteFailed:
    log.Warn("body ran but bookkeeping write failed", "err", err)
}
```

#### State Machine

```
(absent) -- Acquire ---------> running
running  -- Complete --------> completed                 (terminal)
running  -- Fail(retryable=false) --> failed             (terminal)
running  -- Fail(retryable=true)  --> (key deleted; re-acquirable)
running  -- lease expiry ----------> (takeover available via Acquire)
```

Completed and failed are terminal: subsequent `Acquire` calls observe and
return without modifying state. Bumping the `Hook.Version` (or calling
`Store.Reset`) is the only way to re-run a completed hook.

#### Store Implementations

| Store | Use Case | `health.Checker` |
|-------|----------|------------------|
| `MemoryStore` | Tests and single-instance deployments | — |
| `RedisStore` | Production; atomic via Lua scripts | ✓ |
| `PostgresStore` | Production; atomic via `SELECT ... FOR UPDATE` | ✓ |
| `MongoStore` | Production; atomic via `findOneAndUpdate` + upsert | ✓ |

#### Lease Semantics

`Once` claims the hook with a 60-second lease by default and auto-refreshes
at `lease/3` (1-second floor) while the body runs. If the holder crashes,
the lease expires and another pod can take over via `Acquire` — at-most-
once is preserved only as long as the holder refreshes; under crash recovery
the body can run twice.

Combine `WithRetryable(false)` (default) with idempotent body code for
non-idempotent work like destructive migrations: the body's error pins the
hook into terminal `StateFailed`, requiring manual `Store.Reset`. Use
`WithRetryable(true)` only for bodies that are safe to run multiple times.

`PostgresStore` compares `lease_until` against the database server's
`NOW()` rather than the caller's wall clock, so it is robust to cross-pod
clock skew greater than the lease duration. The other backends compare
against the caller's clock.

#### Store Setup

```go
// Redis — no bootstrap required
store, _ := lifecycle.NewRedisStore(redisClient,
    lifecycle.WithRedisPrefix("lifecycle:"),
)

// PostgreSQL — call EnsureSchema once at bootstrap
store, _ := lifecycle.NewPostgresStore(db,
    lifecycle.WithPostgresTable("lifecycle_hooks"),
)
store.EnsureSchema(ctx)

// MongoDB — no bootstrap required (uses default _id index)
store, _ := lifecycle.NewMongoStore(db,
    lifecycle.WithMongoCollection("lifecycle_hooks"),
)
```

## Related Packages

- [event](https://github.com/rbaliyan/event) - Core event pub-sub library
- [event-dlq](https://github.com/rbaliyan/event-dlq) - Dead letter queue
- [event-scheduler](https://github.com/rbaliyan/event-scheduler) - Delayed message delivery
- [event-mongodb](https://github.com/rbaliyan/event-mongodb) - MongoDB change stream transport

## License

MIT
