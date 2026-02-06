package ratelimit

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics(t *testing.T) {
	ctx := context.Background()

	// Create a manual reader for testing
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	metrics, err := NewMetrics(WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}

	t.Run("RecordAllowed", func(t *testing.T) {
		metrics.RecordAllowed(ctx, "test-limiter")

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ratelimit_allowed_total" {
					found = true
				}
			}
		}
		if !found {
			t.Error("expected ratelimit_allowed_total metric")
		}
	})

	t.Run("RecordRejected", func(t *testing.T) {
		metrics.RecordRejected(ctx, "test-limiter")

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ratelimit_rejected_total" {
					found = true
				}
			}
		}
		if !found {
			t.Error("expected ratelimit_rejected_total metric")
		}
	})

	t.Run("RecordWaitDuration", func(t *testing.T) {
		metrics.RecordWaitDuration(ctx, "test-limiter", 0.5)

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ratelimit_wait_duration_seconds" {
					found = true
				}
			}
		}
		if !found {
			t.Error("expected ratelimit_wait_duration_seconds metric")
		}
	})

	t.Run("NilMetricsSafe", func(t *testing.T) {
		var nilMetrics *Metrics

		// These should not panic
		nilMetrics.RecordAllowed(ctx, "test")
		nilMetrics.RecordRejected(ctx, "test")
		nilMetrics.RecordWaitDuration(ctx, "test", 0.1)
	})
}

func TestMetricsLimiter(t *testing.T) {
	ctx := context.Background()

	// Create a manual reader for testing
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	metrics, err := NewMetrics(WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}

	// Use TokenBucket as base limiter (doesn't require Redis)
	baseLimiter := NewTokenBucket(100, 10)
	limiter := NewMetricsLimiter(baseLimiter, "test-api", metrics)

	t.Run("Allow records metrics", func(t *testing.T) {
		allowed := limiter.Allow(ctx)
		if !allowed {
			t.Error("expected Allow to return true")
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ratelimit_allowed_total" {
					found = true
				}
			}
		}
		if !found {
			t.Error("expected ratelimit_allowed_total metric after Allow")
		}
	})

	t.Run("Wait records metrics", func(t *testing.T) {
		err := limiter.Wait(ctx)
		if err != nil {
			t.Errorf("Wait failed: %v", err)
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		foundAllowed := false
		foundDuration := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ratelimit_allowed_total" {
					foundAllowed = true
				}
				if m.Name == "ratelimit_wait_duration_seconds" {
					foundDuration = true
				}
			}
		}
		if !foundAllowed {
			t.Error("expected ratelimit_allowed_total metric after Wait")
		}
		if !foundDuration {
			t.Error("expected ratelimit_wait_duration_seconds metric after Wait")
		}
	})

	t.Run("Unwrap returns base limiter", func(t *testing.T) {
		unwrapped := limiter.Unwrap()
		if unwrapped != baseLimiter {
			t.Error("Unwrap should return the base limiter")
		}
	})

	t.Run("Reserve works", func(t *testing.T) {
		reservation := limiter.Reserve(ctx)
		if !reservation.OK() {
			t.Error("expected reservation to be OK")
		}
	})
}

func TestMetricsWithNamespace(t *testing.T) {
	ctx := context.Background()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	metrics, err := NewMetrics(
		WithMeterProvider(provider),
		WithMetricsNamespace("myapp"),
	)
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}

	metrics.RecordAllowed(ctx, "test")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "myapp_ratelimit_allowed_total" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected myapp_ratelimit_allowed_total metric with namespace")
	}
}
