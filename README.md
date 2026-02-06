# Event Extras

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
limiter := ratelimit.NewRedisLimiter(redisClient, "my-service", 100, time.Second)

// Sliding window for more accurate limiting
limiter := ratelimit.NewSlidingWindowLimiter(redisClient, "my-service", 100, time.Second)

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
// Create saga with steps
orderSaga := saga.New("order-creation",
    &CreateOrderStep{},
    &ReserveInventoryStep{},
    &ChargePaymentStep{},
).
    WithStore(store).
    WithBackoff(&backoff.Exponential{
        Initial:    100 * time.Millisecond,
        Multiplier: 2.0,
        Max:        5 * time.Second,
    }).
    WithMaxRetries(3).
    WithMetrics(metrics)

// Execute saga
err := orderSaga.Execute(ctx, sagaID, &Order{...})
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
store := saga.NewRedisStore(redisClient).
    WithKeyPrefix("sagas:").
    WithTTL(7 * 24 * time.Hour)

// MongoDB
store := saga.NewMongoStore(db).
    WithCollection("sagas")
store.EnsureIndexes(ctx)

// PostgreSQL
store := saga.NewPostgresStore(db).
    WithTable("sagas")
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
metrics, _ := saga.NewMetrics(
    saga.WithMeterProvider(provider),
    saga.WithMetricsNamespace("orders"),
)

orderSaga := saga.New("order-creation", steps...).
    WithMetrics(metrics)
```

Available metrics:
- `saga_executions_total` - Counter by status (completed, compensated, failed)
- `saga_execution_duration_seconds` - Histogram of execution time
- `saga_step_duration_seconds` - Histogram of step execution time
- `saga_retries_total` - Counter of retry attempts

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

## Related Packages

- [event](https://github.com/rbaliyan/event) - Core event pub-sub library
- [event-dlq](https://github.com/rbaliyan/event-dlq) - Dead letter queue
- [event-scheduler](https://github.com/rbaliyan/event-scheduler) - Delayed message delivery
- [event-mongodb](https://github.com/rbaliyan/event-mongodb) - MongoDB change stream transport

## License

MIT
