# CLAUDE.md

## Project Overview

Event Extras provides extended functionality packages for the [event](https://github.com/rbaliyan/event) library.

## Build Commands

```bash
go test ./...          # Run all tests
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
```

## Packages

### ratelimit

Rate limiting implementations:
- `TokenBucket`: Local token bucket using golang.org/x/time/rate
- `RedisLimiter`: Distributed fixed-window rate limiting
- `SlidingWindowLimiter`: Distributed sliding window rate limiting

Key interfaces:
- `Limiter`: Common interface for all rate limiters (Allow, Wait, Reserve)
- `Reservation`: Represents a reserved slot for future use

### saga

Saga pattern for distributed transactions:
- `Saga`: Orchestrator that executes steps in sequence with automatic compensation on failure
- `Step`: Interface for saga steps (Execute + Compensate)
- `Store`: Interface for saga state persistence

Store implementations:
- `MemoryStore`: In-memory store for testing
- `RedisStore`: Distributed storage with set-based indexes and optional TTL
- `MongoStore`: MongoDB document storage with aggregation-based statistics
- `PostgresStore`: PostgreSQL storage with JSONB data and array types

Key interfaces:
- `Step`: Execute/Compensate contract for saga steps
- `Store`: Create/Get/Update/List for saga state persistence
- `BackoffStrategy`: Pluggable retry delay calculation
- `MetricsRecorder`: OpenTelemetry metrics for saga execution

## Architecture Patterns

Follow the same patterns as the main event library:

### Constructor Pattern
```go
func NewTokenBucket(rps float64, burst int) *TokenBucket
func NewRedisLimiter(client redis.Cmdable, key string, limit int64, window time.Duration) *RedisLimiter
```

### Interface-Based Design
All limiters implement the `Limiter` interface for interchangeability.

## Testing

Run tests with:
```bash
go test ./...
```

Redis-dependent tests require a running Redis instance.
