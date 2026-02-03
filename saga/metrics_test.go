package saga

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsRecorder_Init(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	if recorder == nil {
		t.Fatal("expected non-nil recorder")
	}
	if recorder.meterName != "test" {
		t.Errorf("expected meterName 'test', got %q", recorder.meterName)
	}
}

func TestMetricsRecorder_RecordSagaStartEnd(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	ctx := context.Background()

	// Record start - should increment active count
	recorder.RecordSagaStart(ctx, "test-saga")
	if atomic.LoadInt64(&recorder.activeCount) != 1 {
		t.Errorf("expected activeCount 1, got %d", recorder.activeCount)
	}

	// Record another start
	recorder.RecordSagaStart(ctx, "test-saga")
	if atomic.LoadInt64(&recorder.activeCount) != 2 {
		t.Errorf("expected activeCount 2, got %d", recorder.activeCount)
	}

	// Record end - should decrement active count
	recorder.RecordSagaEnd(ctx, "test-saga", StatusCompleted, time.Second)
	if atomic.LoadInt64(&recorder.activeCount) != 1 {
		t.Errorf("expected activeCount 1, got %d", recorder.activeCount)
	}

	// Record another end
	recorder.RecordSagaEnd(ctx, "test-saga", StatusFailed, time.Second)
	if atomic.LoadInt64(&recorder.activeCount) != 0 {
		t.Errorf("expected activeCount 0, got %d", recorder.activeCount)
	}
}

func TestMetricsRecorder_RecordStepExecution(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	ctx := context.Background()

	// Should not panic
	recorder.RecordStepExecution(ctx, "test-saga", "step1", "success", time.Millisecond*100)
	recorder.RecordStepExecution(ctx, "test-saga", "step2", "failure", time.Millisecond*50)
}

func TestMetricsRecorder_RecordCompensation(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	ctx := context.Background()

	// Should not panic
	recorder.RecordCompensation(ctx, "test-saga", "step1", "success", time.Millisecond*100)
	recorder.RecordCompensation(ctx, "test-saga", "step2", "failure", time.Millisecond*50)
}

func TestSagaWithMetrics_Success(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	step1 := newMockStep("step1")
	step2 := newMockStep("step2")

	s := New("test-saga", step1, step2).WithMetrics(recorder)

	ctx := context.Background()
	err := s.Execute(ctx, "saga-1", "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Active count should be 0 after completion
	if atomic.LoadInt64(&recorder.activeCount) != 0 {
		t.Errorf("expected activeCount 0, got %d", recorder.activeCount)
	}
}

func TestSagaWithMetrics_Failure(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	step1 := newMockStep("step1")
	step2 := newMockStep("step2")
	step2.executeErr = errors.New("step2 failed")

	s := New("test-saga", step1, step2).WithMetrics(recorder)

	ctx := context.Background()
	err := s.Execute(ctx, "saga-1", "data")
	if err == nil {
		t.Fatal("expected error")
	}

	// Active count should be 0 after completion (even on failure)
	if atomic.LoadInt64(&recorder.activeCount) != 0 {
		t.Errorf("expected activeCount 0, got %d", recorder.activeCount)
	}

	// step1 should have been compensated
	if !step1.wasCompensated() {
		t.Error("expected step1 to be compensated")
	}
}

func TestSagaWithMetrics_CompensationFailure(t *testing.T) {
	recorder := NewMetricsRecorder("test")
	step1 := newMockStep("step1")
	step1.compensateErr = errors.New("compensate failed")
	step2 := newMockStep("step2")
	step2.executeErr = errors.New("step2 failed")

	s := New("test-saga", step1, step2).WithMetrics(recorder)

	ctx := context.Background()
	err := s.Execute(ctx, "saga-1", "data")
	if err == nil {
		t.Fatal("expected error")
	}

	// Active count should be 0 after completion
	if atomic.LoadInt64(&recorder.activeCount) != 0 {
		t.Errorf("expected activeCount 0, got %d", recorder.activeCount)
	}
}

func TestSagaWithMetrics_NilRecorder(t *testing.T) {
	// Ensure saga works without metrics (nil recorder)
	step1 := newMockStep("step1")

	s := New("test-saga", step1)
	// Don't call WithMetrics - metrics should be nil

	ctx := context.Background()
	err := s.Execute(ctx, "saga-1", "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// delayStep wraps a mockStep with an execution delay for testing concurrent behavior
type delayStep struct {
	*mockStep
	executeDelay time.Duration
}

func newDelayStep(name string, delay time.Duration) *delayStep {
	return &delayStep{
		mockStep:     newMockStep(name),
		executeDelay: delay,
	}
}

func (s *delayStep) Execute(ctx context.Context, data any) error {
	if s.executeDelay > 0 {
		time.Sleep(s.executeDelay)
	}
	return s.mockStep.Execute(ctx, data)
}

func TestSagaWithMetrics_ConcurrentExecutions(t *testing.T) {
	recorder := NewMetricsRecorder("test")

	// Create saga with delay step
	createSaga := func() *Saga {
		step := newDelayStep("step1", time.Millisecond*10)
		return New("test-saga", step).WithMetrics(recorder)
	}

	ctx := context.Background()

	// Start multiple sagas concurrently
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			s := createSaga()
			s.Execute(ctx, "saga-"+string(rune('0'+id)), "data")
			done <- true
		}(i)
	}

	// Wait a bit then check active count is > 0
	time.Sleep(time.Millisecond * 5)
	active := atomic.LoadInt64(&recorder.activeCount)
	if active == 0 {
		t.Error("expected some active sagas during concurrent execution")
	}

	// Wait for all to complete
	for i := 0; i < 5; i++ {
		<-done
	}

	// Active count should be 0 after all complete
	if atomic.LoadInt64(&recorder.activeCount) != 0 {
		t.Errorf("expected activeCount 0, got %d", recorder.activeCount)
	}
}
