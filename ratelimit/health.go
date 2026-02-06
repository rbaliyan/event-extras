package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/health"
)

// Health performs a health check on the RedisLimiter.
//
// The health check:
//   - Verifies Redis connectivity by checking remaining capacity
//   - Returns current usage and remaining capacity
//
// Returns health.StatusHealthy if Redis is responsive.
// Returns health.StatusDegraded if capacity is low (< 10% remaining).
// Returns health.StatusUnhealthy if Redis is not responsive.
func (r *RedisLimiter) Health(ctx context.Context) *health.Result {
	start := time.Now()

	remaining, err := r.Remaining(ctx)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("redis connectivity failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	status := health.StatusHealthy
	message := ""

	// Degraded if capacity is low (< 10% remaining)
	threshold := r.limit / 10
	if threshold < 1 {
		threshold = 1
	}
	if remaining < threshold {
		status = health.StatusDegraded
		message = fmt.Sprintf("low capacity: %d/%d remaining", remaining, r.limit)
	}

	return &health.Result{
		Status:    status,
		Message:   message,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"remaining": remaining,
			"limit":     r.limit,
			"window":    r.window.String(),
			"key":       r.key,
		},
	}
}

// Health performs a health check on the SlidingWindowLimiter.
//
// The health check:
//   - Verifies Redis connectivity by checking remaining capacity
//   - Returns current usage and remaining capacity
//
// Returns health.StatusHealthy if Redis is responsive.
// Returns health.StatusDegraded if capacity is low (< 10% remaining).
// Returns health.StatusUnhealthy if Redis is not responsive.
func (s *SlidingWindowLimiter) Health(ctx context.Context) *health.Result {
	start := time.Now()

	remaining, err := s.Remaining(ctx)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("redis connectivity failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	status := health.StatusHealthy
	message := ""

	// Degraded if capacity is low (< 10% remaining)
	threshold := s.limit / 10
	if threshold < 1 {
		threshold = 1
	}
	if remaining < threshold {
		status = health.StatusDegraded
		message = fmt.Sprintf("low capacity: %d/%d remaining", remaining, s.limit)
	}

	return &health.Result{
		Status:    status,
		Message:   message,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"remaining": remaining,
			"limit":     s.limit,
			"window":    s.window.String(),
			"key":       s.key,
		},
	}
}

// Compile-time checks
var (
	_ health.Checker = (*RedisLimiter)(nil)
	_ health.Checker = (*SlidingWindowLimiter)(nil)
)
