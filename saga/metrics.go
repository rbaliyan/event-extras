package saga

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsRecorder records saga execution metrics using OpenTelemetry.
//
// Metrics recorded:
//   - saga_executions_total: Counter of saga executions by status
//   - saga_step_executions_total: Counter of step executions by step name and result
//   - saga_execution_duration_seconds: Histogram of saga execution duration
//   - saga_step_duration_seconds: Histogram of step execution duration
//   - saga_active_count: Gauge of currently running sagas
//
// Example:
//
//	recorder := saga.NewMetricsRecorder("myapp")
//	orderSaga := saga.New("order-creation", steps...).
//	    WithMetrics(recorder)
type MetricsRecorder struct {
	meterName string
	meter     metric.Meter

	// Counters
	sagaExecutions     metric.Int64Counter
	stepExecutions     metric.Int64Counter
	compensationSteps  metric.Int64Counter

	// Histograms
	sagaDuration metric.Float64Histogram
	stepDuration metric.Float64Histogram

	// Gauge tracking
	activeCount int64
	activeGauge metric.Int64ObservableGauge

	initOnce sync.Once
	initErr  error
}

// NewMetricsRecorder creates a new metrics recorder for saga execution.
//
// The meterName is used to create the OpenTelemetry meter and should be
// unique to your application (e.g., "myapp" or "github.com/myorg/myapp").
//
// Example:
//
//	recorder := saga.NewMetricsRecorder("myapp")
func NewMetricsRecorder(meterName string) *MetricsRecorder {
	return &MetricsRecorder{
		meterName: meterName,
	}
}

// init lazily initializes the metrics instruments.
// This allows the recorder to be created before the OTel SDK is configured.
func (m *MetricsRecorder) init() error {
	m.initOnce.Do(func() {
		m.meter = otel.Meter(m.meterName)

		// Create counters
		m.sagaExecutions, m.initErr = m.meter.Int64Counter(
			"saga_executions_total",
			metric.WithDescription("Total number of saga executions"),
			metric.WithUnit("{execution}"),
		)
		if m.initErr != nil {
			return
		}

		m.stepExecutions, m.initErr = m.meter.Int64Counter(
			"saga_step_executions_total",
			metric.WithDescription("Total number of step executions"),
			metric.WithUnit("{execution}"),
		)
		if m.initErr != nil {
			return
		}

		m.compensationSteps, m.initErr = m.meter.Int64Counter(
			"saga_compensation_steps_total",
			metric.WithDescription("Total number of compensation step executions"),
			metric.WithUnit("{execution}"),
		)
		if m.initErr != nil {
			return
		}

		// Create histograms
		m.sagaDuration, m.initErr = m.meter.Float64Histogram(
			"saga_execution_duration_seconds",
			metric.WithDescription("Duration of saga execution in seconds"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
		)
		if m.initErr != nil {
			return
		}

		m.stepDuration, m.initErr = m.meter.Float64Histogram(
			"saga_step_duration_seconds",
			metric.WithDescription("Duration of individual step execution in seconds"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
		)
		if m.initErr != nil {
			return
		}

		// Create observable gauge for active sagas
		m.activeGauge, m.initErr = m.meter.Int64ObservableGauge(
			"saga_active_count",
			metric.WithDescription("Number of currently running sagas"),
			metric.WithUnit("{saga}"),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(atomic.LoadInt64(&m.activeCount))
				return nil
			}),
		)
	})

	return m.initErr
}

// RecordSagaStart records the start of a saga execution.
// Call this when a saga begins executing.
func (m *MetricsRecorder) RecordSagaStart(ctx context.Context, sagaName string) {
	if m == nil {
		return
	}
	if err := m.init(); err != nil {
		return
	}
	atomic.AddInt64(&m.activeCount, 1)
}

// RecordSagaEnd records the completion of a saga execution.
// Call this when a saga finishes (regardless of success or failure).
func (m *MetricsRecorder) RecordSagaEnd(ctx context.Context, sagaName string, status Status, duration time.Duration) {
	if m == nil {
		return
	}
	if err := m.init(); err != nil {
		return
	}

	atomic.AddInt64(&m.activeCount, -1)

	attrs := metric.WithAttributes(
		attribute.String("saga_name", sagaName),
		attribute.String("status", string(status)),
	)

	m.sagaExecutions.Add(ctx, 1, attrs)
	m.sagaDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordStepExecution records a step execution.
// The result should be "success" or "failure".
func (m *MetricsRecorder) RecordStepExecution(ctx context.Context, sagaName, stepName, result string, duration time.Duration) {
	if m == nil {
		return
	}
	if err := m.init(); err != nil {
		return
	}

	attrs := metric.WithAttributes(
		attribute.String("saga_name", sagaName),
		attribute.String("step_name", stepName),
		attribute.String("result", result),
	)

	m.stepExecutions.Add(ctx, 1, attrs)
	m.stepDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordCompensation records a compensation step execution.
// The result should be "success" or "failure".
func (m *MetricsRecorder) RecordCompensation(ctx context.Context, sagaName, stepName, result string, duration time.Duration) {
	if m == nil {
		return
	}
	if err := m.init(); err != nil {
		return
	}

	attrs := metric.WithAttributes(
		attribute.String("saga_name", sagaName),
		attribute.String("step_name", stepName),
		attribute.String("result", result),
	)

	m.compensationSteps.Add(ctx, 1, attrs)
	m.stepDuration.Record(ctx, duration.Seconds(), attrs)
}

