# Event Extras

Extended functionality packages for the [event](https://github.com/rbaliyan/event) library.

## Packages

### ratelimit

Rate limiting for event processing with local and distributed implementations.

```go
import "github.com/rbaliyan/event-extras/ratelimit"
```

**Features:**
- **TokenBucket**: Local in-memory token bucket (golang.org/x/time/rate)
- **RedisLimiter**: Distributed fixed-window using Redis counters
- **SlidingWindowLimiter**: Distributed sliding window using Redis sorted sets

**Usage:**

```go
// Local rate limiter: 100 requests/second with burst of 10
limiter := ratelimit.NewTokenBucket(100, 10)

// Distributed rate limiter using Redis
limiter := ratelimit.NewRedisLimiter(redisClient, "my-service", 100, time.Second)

// Sliding window for more accurate limiting
limiter := ratelimit.NewSlidingWindowLimiter(redisClient, "my-service", 100, time.Second)

// Use in handler
if err := limiter.Wait(ctx); err != nil {
    return err // Context cancelled
}
// Process message

// Or non-blocking
if limiter.Allow(ctx) {
    // Process immediately
} else {
    // Rate limited
}
```

**When to use each limiter:**

| Limiter | Use Case |
|---------|----------|
| TokenBucket | Single instance, per-instance limits, low latency |
| RedisLimiter | Multi-instance, global limits, simple fixed-window |
| SlidingWindowLimiter | Multi-instance, accurate limiting, no boundary burst |

## Installation

```bash
go get github.com/rbaliyan/event-extras
```

## License

MIT
