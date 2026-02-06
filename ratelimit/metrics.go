package ratelimit

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics provides OpenTelemetry metrics for rate limiters.
//
// All methods are nil-safe; calling methods on a nil *Metrics is a no-op.
// This allows optional metrics without nil checks in application code.
//
// Available metrics:
//   - ratelimit_allowed_total: Counter of allowed requests
//   - ratelimit_rejected_total: Counter of rejected requests
//   - ratelimit_wait_duration_seconds: Histogram of wait times
//
// Example:
//
//	metrics, _ := ratelimit.NewMetrics()
//	limiter := ratelimit.NewMetricsLimiter(baseLimiter, "api", metrics)
type Metrics struct {
	allowedCounter  metric.Int64Counter
	rejectedCounter metric.Int64Counter
	waitHistogram   metric.Float64Histogram
}

// metricsOptions holds configuration for Metrics
type metricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// MetricsOption is a functional option for configuring Metrics
type MetricsOption func(*metricsOptions)

// WithMeterProvider sets a custom OpenTelemetry meter provider.
//
// If not set, the global meter provider is used.
func WithMeterProvider(provider metric.MeterProvider) MetricsOption {
	return func(o *metricsOptions) {
		if provider != nil {
			o.meterProvider = provider
		}
	}
}

// WithMetricsNamespace sets a prefix for metric names.
//
// Example: WithMetricsNamespace("myapp") produces "myapp_ratelimit_allowed_total"
func WithMetricsNamespace(namespace string) MetricsOption {
	return func(o *metricsOptions) {
		o.namespace = namespace
	}
}

// NewMetrics creates a new Metrics instance with OpenTelemetry instruments.
//
// Example:
//
//	// Default configuration
//	metrics, err := ratelimit.NewMetrics()
//
//	// Custom meter provider
//	metrics, err := ratelimit.NewMetrics(
//	    ratelimit.WithMeterProvider(provider),
//	    ratelimit.WithMetricsNamespace("myapp"),
//	)
func NewMetrics(opts ...MetricsOption) (*Metrics, error) {
	o := &metricsOptions{
		meterProvider: otel.GetMeterProvider(),
		namespace:     "",
	}
	for _, opt := range opts {
		opt(o)
	}

	prefix := ""
	if o.namespace != "" {
		prefix = o.namespace + "_"
	}

	meter := o.meterProvider.Meter("github.com/rbaliyan/event-extras/ratelimit")

	allowedCounter, err := meter.Int64Counter(
		prefix+"ratelimit_allowed_total",
		metric.WithDescription("Total number of allowed requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, err
	}

	rejectedCounter, err := meter.Int64Counter(
		prefix+"ratelimit_rejected_total",
		metric.WithDescription("Total number of rejected (rate limited) requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, err
	}

	waitHistogram, err := meter.Float64Histogram(
		prefix+"ratelimit_wait_duration_seconds",
		metric.WithDescription("Time spent waiting for rate limit"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		allowedCounter:  allowedCounter,
		rejectedCounter: rejectedCounter,
		waitHistogram:   waitHistogram,
	}, nil
}

// RecordAllowed records an allowed request.
func (m *Metrics) RecordAllowed(ctx context.Context, limiterName string) {
	if m == nil {
		return
	}
	m.allowedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("limiter", limiterName),
	))
}

// RecordRejected records a rejected (rate limited) request.
func (m *Metrics) RecordRejected(ctx context.Context, limiterName string) {
	if m == nil {
		return
	}
	m.rejectedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("limiter", limiterName),
	))
}

// RecordWaitDuration records the time spent waiting for rate limit.
func (m *Metrics) RecordWaitDuration(ctx context.Context, limiterName string, seconds float64) {
	if m == nil {
		return
	}
	m.waitHistogram.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("limiter", limiterName),
	))
}

// MetricsLimiter wraps a Limiter with metrics recording.
//
// Example:
//
//	metrics, _ := ratelimit.NewMetrics()
//	baseLimiter := ratelimit.NewRedisLimiter(client, "api", 100, time.Second)
//	limiter := ratelimit.NewMetricsLimiter(baseLimiter, "api-limiter", metrics)
type MetricsLimiter struct {
	limiter Limiter
	name    string
	metrics *Metrics
}

// NewMetricsLimiter creates a new MetricsLimiter wrapping the given limiter.
//
// Parameters:
//   - limiter: The underlying rate limiter
//   - name: Name for metrics labels
//   - metrics: Metrics instance (can be nil for no-op)
func NewMetricsLimiter(limiter Limiter, name string, metrics *Metrics) *MetricsLimiter {
	return &MetricsLimiter{
		limiter: limiter,
		name:    name,
		metrics: metrics,
	}
}

// Allow returns true if an event can happen right now.
// Records allowed/rejected metrics.
func (m *MetricsLimiter) Allow(ctx context.Context) bool {
	allowed := m.limiter.Allow(ctx)
	if allowed {
		m.metrics.RecordAllowed(ctx, m.name)
	} else {
		m.metrics.RecordRejected(ctx, m.name)
	}
	return allowed
}

// Wait blocks until an event is allowed or context is cancelled.
// Records wait duration and allowed metrics on success.
func (m *MetricsLimiter) Wait(ctx context.Context) error {
	start := time.Now()

	err := m.limiter.Wait(ctx)
	if err != nil {
		// Record rejection on timeout/cancel
		m.metrics.RecordRejected(ctx, m.name)
		return err
	}

	// Record metrics on success
	duration := time.Since(start).Seconds()
	m.metrics.RecordWaitDuration(ctx, m.name, duration)
	m.metrics.RecordAllowed(ctx, m.name)
	return nil
}

// Reserve returns a reservation for a future event.
func (m *MetricsLimiter) Reserve(ctx context.Context) Reservation {
	return m.limiter.Reserve(ctx)
}

// Unwrap returns the underlying limiter.
func (m *MetricsLimiter) Unwrap() Limiter {
	return m.limiter
}

// Compile-time check
var _ Limiter = (*MetricsLimiter)(nil)
