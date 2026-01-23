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
