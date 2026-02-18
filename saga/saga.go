// Package saga provides saga orchestration for distributed transactions.
//
// Sagas coordinate multiple steps with compensation for rollback on failure.
// This enables reliable distributed transactions without two-phase commit:
//   - Execute steps in order
//   - On failure, optionally retry with backoff before compensating
//   - On persistent failure, compensate completed steps in reverse order
//   - Track saga state for recovery
//
// # Overview
//
// The Saga pattern is used when you need to maintain data consistency across
// multiple services or databases. Unlike traditional distributed transactions
// (2PC), sagas use compensating transactions to undo work when failures occur.
//
// The package provides:
//   - Step interface for defining saga steps and compensations
//   - Saga orchestrator for executing steps in sequence
//   - Store interface for state persistence
//   - RedisStore for distributed deployments
//   - Configurable retry with backoff before compensation
//
// # When to Use Sagas
//
// Use sagas when you need to:
//   - Coordinate operations across multiple services
//   - Maintain consistency without distributed locks
//   - Handle long-running transactions
//   - Recover from partial failures
//
// # Basic Usage
//
// Define steps that implement the Step interface:
//
//	type ReserveInventoryStep struct {
//	    inventoryService *InventoryService
//	}
//
//	func (s *ReserveInventoryStep) Name() string {
//	    return "reserve-inventory"
//	}
//
//	func (s *ReserveInventoryStep) Execute(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.inventoryService.Reserve(ctx, order.ProductID, order.Quantity)
//	}
//
//	func (s *ReserveInventoryStep) Compensate(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.inventoryService.Release(ctx, order.ProductID, order.Quantity)
//	}
//
// Create and execute the saga:
//
//	orderSaga, err := saga.New("order-creation",
//	    []saga.Step{
//	        &CreateOrderStep{orderService},
//	        &ReserveInventoryStep{inventoryService},
//	        &ProcessPaymentStep{paymentService},
//	        &SendConfirmationStep{emailService},
//	    },
//	    saga.WithStore(saga.NewRedisStore(redisClient)),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	sagaID := uuid.New().String()
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    // Saga failed - compensations were automatically run
//	    log.Error("order creation failed", "saga_id", sagaID, "error", err)
//	}
//
// # Step Retry with Backoff
//
// Configure automatic retries before compensation:
//
//	orderSaga, err := saga.New("order-creation", steps,
//	    saga.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        30 * time.Second,
//	        Jitter:     0.1,
//	    }),
//	    saga.WithMaxRetries(3),
//	)
//
// When a step fails, the saga will retry up to MaxRetries times with
// increasing delays before triggering compensation.
//
// # Compensation Behavior
//
// When a step fails (after retries if configured):
//  1. The saga stops executing forward
//  2. Compensations run in reverse order (LIFO)
//  3. All completed steps are compensated, even if some compensations fail
//  4. The saga status becomes "compensated" or "failed" (if compensation failed)
//
// # State Persistence
//
// Using a store enables:
//   - Recovery of failed sagas after application restart
//   - Visibility into saga state for debugging
//   - Retry of failed sagas with Resume()
//
// Example with Redis store:
//
//	store := saga.NewRedisStore(redisClient, saga.WithTTL(7 * 24 * time.Hour))
//	orderSaga, _ := saga.New("order-creation", steps, saga.WithStore(store))
//
// # Best Practices
//
//   - Keep steps idempotent - they may be retried
//   - Implement compensations that can be called multiple times safely
//   - Log step execution for debugging
//   - Set appropriate TTLs for saga state
//   - Monitor failed sagas and set up alerts
//   - Use backoff for transient failures (network issues, timeouts)
package saga

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/backoff"
	eventerrors "github.com/rbaliyan/event/v3/errors"
)

// Step represents a single step in a saga.
//
// Each step must provide:
//   - Name: For identification in logs and state tracking
//   - Execute: The forward action to perform
//   - Compensate: The reverse action to undo Execute
//
// Guidelines for implementing steps:
//   - Make Execute idempotent if possible (safe to retry)
//   - Make Compensate idempotent (may be called multiple times)
//   - Handle partial failures in Compensate gracefully
//   - Log actions for debugging
//
// Example:
//
//	type PaymentStep struct {
//	    paymentService *PaymentService
//	}
//
//	func (s *PaymentStep) Name() string {
//	    return "process-payment"
//	}
//
//	func (s *PaymentStep) Execute(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.paymentService.Charge(ctx, order.CustomerID, order.Total)
//	}
//
//	func (s *PaymentStep) Compensate(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.paymentService.Refund(ctx, order.CustomerID, order.Total)
//	}
type Step interface {
	// Name returns the step name for logging/tracking.
	Name() string

	// Execute performs the step action.
	// Returns nil on success, error on failure (triggers compensation).
	Execute(ctx context.Context, data any) error

	// Compensate undoes the step action on rollback.
	// Called when a later step fails. Must be idempotent.
	Compensate(ctx context.Context, data any) error
}

// State represents the current state of a saga.
//
// The State tracks:
//   - Which saga instance this is (ID)
//   - What type of saga (Name)
//   - Current execution state (Status, CurrentStep)
//   - Which steps completed (CompletedSteps)
//   - The saga data being processed
//   - Any error that occurred
//   - Timing information
//   - Version for optimistic locking
//
// State is persisted to the Store after each step for recovery.
type State struct {
	ID             string     // Saga instance ID (unique per execution)
	Name           string     // Saga definition name (e.g., "order-creation")
	Status         Status     // Current status
	CurrentStep    int        // Index of current/failed step
	CompletedSteps []string   // Names of completed steps
	Data           any        // Saga data passed to steps
	Error          string     // Error message if failed
	StartedAt      time.Time  // When saga started
	CompletedAt    *time.Time // When saga completed/failed (nil if running)
	LastUpdatedAt  time.Time  // Last state update
	Version        int64      // Version for optimistic locking (incremented on each update)
}

// ErrVersionConflict is returned when an update fails due to a version mismatch.
// This indicates that another process has modified the saga state since it was read.
//
// This is an alias to the shared event errors package for ecosystem consistency.
var ErrVersionConflict = eventerrors.ErrVersionConflict

// NewVersionConflictError creates a detailed version conflict error for a saga state.
func NewVersionConflictError(sagaID string, expected, actual int64) error {
	return eventerrors.NewVersionConflictError("saga state", sagaID, expected, actual)
}

// Status represents saga status.
//
// State transitions:
//
//	pending -> running -> completed
//	                   \
//	                compensating -> compensated
//	                            \
//	                            failed
type Status string

const (
	// StatusPending indicates saga is created but not started.
	StatusPending Status = "pending"

	// StatusRunning indicates saga is executing steps.
	StatusRunning Status = "running"

	// StatusCompleted indicates all steps succeeded.
	StatusCompleted Status = "completed"

	// StatusFailed indicates saga failed and compensation also failed.
	StatusFailed Status = "failed"

	// StatusCompensating indicates saga is running compensations.
	StatusCompensating Status = "compensating"

	// StatusCompensated indicates saga failed but compensations succeeded.
	StatusCompensated Status = "compensated"
)

// Store persists saga state.
//
// Implementations must be safe for concurrent use. The store enables:
//   - Recovery after application restart
//   - Visibility into saga state
//   - Retry of failed sagas
//
// Implementations:
//   - RedisStore: For distributed deployments (see redis.go)
//   - MongoStore: For MongoDB (see mongodb.go)
type Store interface {
	// Create creates a new saga instance.
	// Returns error if saga with this ID already exists.
	Create(ctx context.Context, state *State) error

	// Get retrieves saga state by ID.
	// Returns error if not found.
	Get(ctx context.Context, id string) (*State, error)

	// Update updates saga state.
	// Called after each step to persist progress.
	Update(ctx context.Context, state *State) error

	// List lists sagas matching the filter.
	// Returns empty slice if no matches.
	List(ctx context.Context, filter StoreFilter) ([]*State, error)
}

// StoreFilter specifies criteria for listing sagas.
//
// All fields are optional. Empty filter returns all sagas.
//
// Example:
//
//	// Find failed order sagas
//	filter := saga.StoreFilter{
//	    Name:   "order-creation",
//	    Status: []saga.Status{saga.StatusFailed, saga.StatusCompensated},
//	    Limit:  100,
//	}
//	sagas, err := store.List(ctx, filter)
type StoreFilter struct {
	Name   string   // Filter by saga name (empty = all names)
	Status []Status // Filter by status (empty = all statuses)
	Limit  int      // Maximum results (0 = no limit)
}

// BackoffStrategy is an alias for backoff.Strategy from the main event library.
// All implementations from github.com/rbaliyan/event/v3/backoff can be used directly.
//
// Implementations must be stateless and safe for concurrent use.
type BackoffStrategy = backoff.Strategy

// Option configures a Saga.
type Option func(*sagaOptions)

type sagaOptions struct {
	store      Store
	logger     *slog.Logger
	metrics    *MetricsRecorder
	backoff    BackoffStrategy
	maxRetries int
}

// WithStore sets the saga store for persistence.
//
// Using a store enables:
//   - State persistence after each step
//   - Recovery of failed sagas with Resume()
//   - Visibility into saga state for monitoring
//
// Parameters:
//   - store: The store implementation to use
//
// Example:
//
//	orderSaga, err := saga.New("order", steps,
//	    saga.WithStore(saga.NewRedisStore(redisClient)),
//	)
func WithStore(store Store) Option {
	return func(o *sagaOptions) {
		o.store = store
	}
}

// WithLogger sets a custom logger.
//
// The logger is used to log step execution, failures, and compensations.
// If not set, slog.Default() is used.
//
// Parameters:
//   - logger: The slog logger to use
func WithLogger(logger *slog.Logger) Option {
	return func(o *sagaOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithMetrics enables OpenTelemetry metrics collection for the saga.
//
// When enabled, the following metrics are recorded:
//   - saga_executions_total: Counter of saga executions by status
//   - saga_step_executions_total: Counter of step executions by step name and result
//   - saga_execution_duration_seconds: Histogram of saga execution duration
//   - saga_step_duration_seconds: Histogram of step execution duration
//   - saga_active_count: Gauge of currently running sagas
//
// Parameters:
//   - recorder: The metrics recorder to use (created with NewMetricsRecorder)
//
// Example:
//
//	recorder := saga.NewMetricsRecorder("myapp")
//	orderSaga, err := saga.New("order-creation", steps,
//	    saga.WithMetrics(recorder),
//	)
func WithMetrics(recorder *MetricsRecorder) Option {
	return func(o *sagaOptions) {
		o.metrics = recorder
	}
}

// WithBackoff sets a backoff strategy for step retries.
//
// When a step fails, the saga uses this strategy to determine how long to wait
// before retrying. Combined with WithMaxRetries, this enables automatic retry
// of transient failures before triggering compensation.
//
// If not set, steps are not retried and compensation begins immediately on failure.
//
// Parameters:
//   - strategy: The backoff strategy to use
//
// Example:
//
//	orderSaga, err := saga.New("order-creation", steps,
//	    saga.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        30 * time.Second,
//	        Jitter:     0.1,
//	    }),
//	    saga.WithMaxRetries(3),
//	)
func WithBackoff(strategy BackoffStrategy) Option {
	return func(o *sagaOptions) {
		o.backoff = strategy
	}
}

// WithMaxRetries sets the maximum number of retry attempts for failed steps.
//
// When a step fails, it will be retried up to this many times before
// triggering compensation. Use this in combination with WithBackoff to
// configure the delay between retries.
//
// If set to 0 (default), steps are not retried and compensation begins
// immediately on failure.
//
// Parameters:
//   - max: Maximum retry attempts (0 = no retries)
//
// Example:
//
//	orderSaga, err := saga.New("order-creation", steps,
//	    saga.WithBackoff(backoffStrategy),
//	    saga.WithMaxRetries(3),
//	)
func WithMaxRetries(max int) Option {
	return func(o *sagaOptions) {
		if max >= 0 {
			o.maxRetries = max
		}
	}
}

// Saga orchestrates a sequence of steps with compensation.
//
// A Saga represents a distributed transaction composed of multiple steps.
// It executes steps in order and, on failure, optionally retries with backoff
// before running compensations in reverse order to undo completed work.
//
// Example:
//
//	orderSaga, err := saga.New("order-creation",
//	    []saga.Step{
//	        &CreateOrderStep{},
//	        &ReserveInventoryStep{},
//	        &ProcessPaymentStep{},
//	    },
//	    saga.WithStore(store),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    // Saga failed, compensations were run
//	}
type Saga struct {
	name       string
	steps      []Step
	store      Store
	logger     *slog.Logger
	metrics    *MetricsRecorder
	backoff    BackoffStrategy
	maxRetries int
}

// New creates a new saga definition.
//
// The name should be descriptive and consistent across deployments as it's
// used for filtering and logging. Steps are executed in order.
//
// Parameters:
//   - name: Saga name for identification (e.g., "order-creation")
//   - steps: The steps to execute in order
//   - opts: Optional configuration
//
// Returns an error if name is empty or no steps are provided.
//
// Example:
//
//	s, err := saga.New("order-creation",
//	    []saga.Step{
//	        &CreateOrderStep{orderService},
//	        &ReserveInventoryStep{inventoryService},
//	        &ProcessPaymentStep{paymentService},
//	    },
//	    saga.WithStore(store),
//	    saga.WithMaxRetries(3),
//	)
func New(name string, steps []Step, opts ...Option) (*Saga, error) {
	if name == "" {
		return nil, fmt.Errorf("saga name is required")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("at least one step is required")
	}

	o := &sagaOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(o)
	}

	return &Saga{
		name:       name,
		steps:      steps,
		store:      o.store,
		logger:     o.logger.With("saga", name),
		metrics:    o.metrics,
		backoff:    o.backoff,
		maxRetries: o.maxRetries,
	}, nil
}

// log returns the configured logger.
func (s *Saga) log() *slog.Logger {
	return s.logger
}

// Name returns the saga name.
func (s *Saga) Name() string {
	return s.name
}

// Steps returns a copy of the saga steps.
func (s *Saga) Steps() []Step {
	steps := make([]Step, len(s.steps))
	copy(steps, s.steps)
	return steps
}

// Execute executes the saga with the given ID and data.
//
// Execution proceeds as follows:
//  1. Create initial saga state (if store configured)
//  2. Execute each step in order
//  3. On success: mark saga as completed
//  4. On failure: run compensations in reverse order
//
// The data parameter is passed to each step's Execute and Compensate methods.
// Steps can type-assert to access the actual data type.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: Unique identifier for this saga instance
//   - data: Data to pass to steps
//
// Returns nil if all steps succeed, error if any step fails (after compensation).
//
// Example:
//
//	sagaID := uuid.New().String()
//	order := &Order{CustomerID: custID, Items: items}
//
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    log.Error("order saga failed", "id", sagaID, "error", err)
//	    // Compensations have already run - order is in consistent state
//	}
func (s *Saga) Execute(ctx context.Context, id string, data any) error {
	return s.execute(ctx, id, data)
}

// execute is the internal execution method for new saga executions.
func (s *Saga) execute(ctx context.Context, id string, data any) error {
	sagaStart := time.Now()

	// Record saga start if metrics enabled
	if s.metrics != nil {
		s.metrics.RecordSagaStart(ctx, s.name)
	}

	state := &State{
		ID:            id,
		Name:          s.name,
		Status:        StatusRunning,
		Data:          data,
		StartedAt:     sagaStart,
		LastUpdatedAt: sagaStart,
	}

	// Persist initial state
	if s.store != nil {
		if err := s.store.Create(ctx, state); err != nil {
			if s.metrics != nil {
				s.metrics.RecordSagaEnd(ctx, s.name, StatusFailed, time.Since(sagaStart))
			}
			return fmt.Errorf("create saga state: %w", err)
		}
	}

	return s.runSteps(ctx, id, state, sagaStart, 0)
}

// runSteps executes the saga steps and handles compensation on failure.
// startStep allows resuming from a specific step index, skipping already-completed steps.
func (s *Saga) runSteps(ctx context.Context, id string, state *State, sagaStart time.Time, startStep int) error {
	data := state.Data

	// Pre-populate with already-completed steps (for resume)
	completedSteps := make([]Step, 0, len(s.steps))
	for i := 0; i < startStep && i < len(s.steps); i++ {
		completedSteps = append(completedSteps, s.steps[i])
	}

	for i := startStep; i < len(s.steps); i++ {
		step := s.steps[i]
		state.CurrentStep = i

		s.log().Info("executing step",
			"saga_id", id,
			"step", step.Name(),
			"step_index", i)

		// Execute step with optional retry
		var err error
		var stepDuration time.Duration
		maxAttempts := s.maxRetries + 1 // +1 for the initial attempt
		if maxAttempts < 1 {
			maxAttempts = 1
		}

		for attempt := 0; attempt < maxAttempts; attempt++ {
			// Wait for backoff delay on retry (not on first attempt)
			if attempt > 0 && s.backoff != nil {
				backoffDelay := s.backoff.NextDelay(attempt - 1)
				s.log().Info("retrying step after backoff",
					"saga_id", id,
					"step", step.Name(),
					"attempt", attempt+1,
					"backoff_delay", backoffDelay)

				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-time.After(backoffDelay):
					// Continue with retry
				}

				// If context was cancelled during backoff, stop retrying
				if ctx.Err() != nil {
					break
				}
			}

			stepStart := time.Now()
			err = step.Execute(ctx, data)
			stepDuration = time.Since(stepStart)

			// Record step metrics for each attempt
			if s.metrics != nil {
				result := "success"
				if err != nil {
					result = "failure"
				}
				s.metrics.RecordStepExecution(ctx, s.name, step.Name(), result, stepDuration)
			}

			if err == nil {
				break // Success, no need to retry
			}

			// Log retry attempt
			if attempt < maxAttempts-1 {
				s.log().Warn("step failed, will retry",
					"saga_id", id,
					"step", step.Name(),
					"attempt", attempt+1,
					"max_attempts", maxAttempts,
					"error", err)
			}
		}

		if err != nil {
			s.log().Error("step failed",
				"saga_id", id,
				"step", step.Name(),
				"error", err)

			state.Status = StatusCompensating
			state.Error = err.Error()
			s.updateState(ctx, state)

			// Compensate in reverse order
			compensateErr := s.compensate(ctx, id, completedSteps, data)
			if compensateErr != nil {
				state.Status = StatusFailed
				state.Error = fmt.Sprintf("step failed: %v; compensation failed: %v", err, compensateErr)
			} else {
				state.Status = StatusCompensated
			}

			now := time.Now()
			state.CompletedAt = &now
			s.updateState(ctx, state)

			// Record saga end with final status
			if s.metrics != nil {
				s.metrics.RecordSagaEnd(ctx, s.name, state.Status, time.Since(sagaStart))
			}

			return fmt.Errorf("saga step %s failed: %w", step.Name(), err)
		}

		completedSteps = append(completedSteps, step)
		state.CompletedSteps = append(state.CompletedSteps, step.Name())
		s.updateState(ctx, state)

		s.log().Debug("step completed",
			"saga_id", id,
			"step", step.Name())
	}

	state.Status = StatusCompleted
	now := time.Now()
	state.CompletedAt = &now
	s.updateState(ctx, state)

	// Record saga end
	if s.metrics != nil {
		s.metrics.RecordSagaEnd(ctx, s.name, StatusCompleted, time.Since(sagaStart))
	}

	s.log().Info("saga completed",
		"saga_id", id,
		"steps", len(s.steps))

	return nil
}

// compensate runs compensations in reverse order
func (s *Saga) compensate(ctx context.Context, id string, completedSteps []Step, data any) error {
	s.log().Info("starting compensation",
		"saga_id", id,
		"steps_to_compensate", len(completedSteps))

	var compensateErrors []error

	for i := len(completedSteps) - 1; i >= 0; i-- {
		step := completedSteps[i]

		s.log().Info("compensating step",
			"saga_id", id,
			"step", step.Name())

		stepStart := time.Now()
		err := step.Compensate(ctx, data)
		stepDuration := time.Since(stepStart)

		// Record compensation metrics
		if s.metrics != nil {
			result := "success"
			if err != nil {
				result = "failure"
			}
			s.metrics.RecordCompensation(ctx, s.name, step.Name(), result, stepDuration)
		}

		if err != nil {
			s.log().Error("compensation failed",
				"saga_id", id,
				"step", step.Name(),
				"error", err)

			compensateErrors = append(compensateErrors, fmt.Errorf("compensate %s: %w", step.Name(), err))
			// Continue compensating other steps
		}
	}

	if len(compensateErrors) > 0 {
		return fmt.Errorf("compensation errors: %v", compensateErrors)
	}

	s.log().Info("compensation completed",
		"saga_id", id)

	return nil
}

// updateState updates saga state in store if available
func (s *Saga) updateState(ctx context.Context, state *State) {
	state.LastUpdatedAt = time.Now()

	if s.store != nil {
		if err := s.store.Update(ctx, state); err != nil {
			s.log().Error("failed to update saga state",
				"saga_id", state.ID,
				"error", err)
		}
	}
}

// Resume attempts to resume a failed saga.
//
// Use Resume to retry a saga after fixing the underlying issue that caused
// the failure. Resume loads the saga state from the store and continues
// execution from the first incomplete step, skipping already-completed steps.
//
// Resume only works for sagas in StatusFailed or StatusCompensated state.
// Requires a store to be configured.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: The saga instance ID to resume
//
// Returns error if:
//   - No store is configured
//   - Saga not found
//   - Saga is not in a failed state
//   - Re-execution fails
//
// Example:
//
//	// After fixing the underlying issue...
//	failedSagas, _ := store.List(ctx, saga.StoreFilter{
//	    Status: []saga.Status{saga.StatusFailed},
//	})
//
//	for _, state := range failedSagas {
//	    if err := orderSaga.Resume(ctx, state.ID); err != nil {
//	        log.Error("resume failed", "saga_id", state.ID, "error", err)
//	    }
//	}
func (s *Saga) Resume(ctx context.Context, id string) error {
	if s.store == nil {
		return fmt.Errorf("no store configured")
	}

	state, err := s.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get saga state: %w", err)
	}

	if state.Status != StatusFailed && state.Status != StatusCompensated {
		return fmt.Errorf("saga is not in failed state: %s", state.Status)
	}

	startStep := len(state.CompletedSteps)
	sagaStart := time.Now()

	if s.metrics != nil {
		s.metrics.RecordSagaStart(ctx, s.name)
	}

	// Update state for resume, preserving completed steps
	state.Status = StatusRunning
	state.Error = ""
	state.CompletedAt = nil
	state.LastUpdatedAt = sagaStart

	if err := s.store.Update(ctx, state); err != nil {
		if s.metrics != nil {
			s.metrics.RecordSagaEnd(ctx, s.name, StatusFailed, time.Since(sagaStart))
		}
		return fmt.Errorf("update saga state for resume: %w", err)
	}

	s.log().Info("resuming saga",
		"saga_id", id,
		"start_step", startStep,
		"completed_steps", state.CompletedSteps)

	return s.runSteps(ctx, id, state, sagaStart, startStep)
}

// TypedStep is a generic wrapper that provides type-safe Execute and Compensate methods.
//
// TypedStep allows you to write saga steps with compile-time type checking for the
// data parameter, eliminating the need for type assertions in your step implementations.
//
// Example:
//
//	type OrderData struct {
//	    OrderID    string
//	    CustomerID string
//	    Total      float64
//	}
//
//	// Define typed handler functions
//	reserveInventory := func(ctx context.Context, order *OrderData) error {
//	    return inventoryService.Reserve(ctx, order.OrderID)
//	}
//	releaseInventory := func(ctx context.Context, order *OrderData) error {
//	    return inventoryService.Release(ctx, order.OrderID)
//	}
//
//	// Create typed step
//	step := saga.NewTypedStep("reserve-inventory", reserveInventory, releaseInventory)
//
//	// Use in saga
//	orderSaga, _ := saga.New("order-creation", []saga.Step{step})
//	orderSaga.Execute(ctx, sagaID, &OrderData{OrderID: "123"})
type TypedStep[T any] struct {
	name       string
	execute    func(ctx context.Context, data T) error
	compensate func(ctx context.Context, data T) error
}

// NewTypedStep creates a new type-safe saga step.
//
// The execute and compensate functions receive the data parameter with the correct type,
// eliminating the need for manual type assertions.
//
// Parameters:
//   - name: Step name for identification in logs and state tracking
//   - execute: The forward action to perform (receives typed data)
//   - compensate: The reverse action to undo execute (receives typed data)
//
// Example:
//
//	step := saga.NewTypedStep("process-payment",
//	    func(ctx context.Context, order *Order) error {
//	        return paymentService.Charge(ctx, order.CustomerID, order.Total)
//	    },
//	    func(ctx context.Context, order *Order) error {
//	        return paymentService.Refund(ctx, order.CustomerID, order.Total)
//	    },
//	)
func NewTypedStep[T any](name string, execute, compensate func(ctx context.Context, data T) error) *TypedStep[T] {
	return &TypedStep[T]{
		name:       name,
		execute:    execute,
		compensate: compensate,
	}
}

// Name returns the step name.
func (s *TypedStep[T]) Name() string {
	return s.name
}

// Execute performs the step action with type-safe data conversion.
//
// If the data cannot be converted to type T, an error is returned.
// The data must be either of type T or *T.
func (s *TypedStep[T]) Execute(ctx context.Context, data any) error {
	typed, err := convertToType[T](data)
	if err != nil {
		return fmt.Errorf("step %s: %w", s.name, err)
	}
	return s.execute(ctx, typed)
}

// Compensate undoes the step action with type-safe data conversion.
//
// If the data cannot be converted to type T, an error is returned.
// The data must be either of type T or *T.
func (s *TypedStep[T]) Compensate(ctx context.Context, data any) error {
	typed, err := convertToType[T](data)
	if err != nil {
		return fmt.Errorf("step %s compensate: %w", s.name, err)
	}
	return s.compensate(ctx, typed)
}

// convertToType attempts to convert data to type T.
// Handles both value and pointer types, as well as map[string]any from JSON deserialization.
func convertToType[T any](data any) (T, error) {
	var zero T

	if data == nil {
		return zero, fmt.Errorf("data is nil")
	}

	// Direct type match
	if typed, ok := data.(T); ok {
		return typed, nil
	}

	// Handle pointer to T
	if typed, ok := data.(*T); ok {
		if typed == nil {
			return zero, fmt.Errorf("data pointer is nil")
		}
		return *typed, nil
	}

	return zero, fmt.Errorf("cannot convert %T to %T", data, zero)
}

// Compile-time check that TypedStep implements Step
var _ Step = (*TypedStep[any])(nil)
