// Package lifecycle provides "run exactly once per fleet" coordination
// for deployment-time hooks such as schema migrations, cache warmup, or any
// other action that must execute on exactly one instance across a multi-pod
// rollout.
//
// # Use case
//
// On startup, every pod calls lifecycle.Once with a hook keyed by name and
// deployment version. The first pod to claim the hook runs the body with an
// auto-refreshing lease; the others see the hook is already running or
// already completed and skip. When the deployment version changes, the key
// changes and the hook runs again on the next rollout.
//
//	hook := lifecycle.Hook{Name: "migrate-users-table", Version: "v1.2.3"}
//	_, err := lifecycle.Once(ctx, store, hook, func(ctx context.Context) error {
//	    return runMigration(ctx, db)
//	})
//
// # State machine
//
// A hook key transitions through these states:
//
//	(absent) -- Acquire ---------> running
//	running  -- Complete --------> completed   (terminal)
//	running  -- Fail(retryable=false) --> failed  (terminal)
//	running  -- Fail(retryable=true)  --> (key deleted; re-acquirable)
//	running  -- lease expiry ----------> (takeover available via Acquire)
//
// Completed and failed are terminal: subsequent Acquire calls observe the
// terminal state and return without modifying it. Lease expiry lets a
// second pod take over a stuck claim — good for crash recovery, but the
// body can then run twice (combine with WithRetryable(false) and an
// idempotent fn for destructive work).
//
// # Permanence vs replay
//
// "Once per version" is enforced by including the version in the hook key.
// To re-run a hook after a successful completion, either bump the version
// or call Store.Reset on the key (admin operation). There is no automatic
// TTL on completed entries.
//
// # Default instance identity
//
// Each call to Once derives an instance identifier of "<hostname>:<pid>"
// unless overridden with WithInstanceID. In Kubernetes the canonical
// override is the pod name: WithInstanceID(os.Getenv("POD_NAME")).
//
// # Backend support
//
// Four Store implementations ship in this package:
//
//   - MemoryStore — single-process, for tests and single-instance deployments.
//   - RedisStore — production-ready, atomic transitions via Lua scripts.
//   - PostgresStore — production-ready, atomic transitions via SELECT ... FOR UPDATE.
//   - MongoStore — production-ready, atomic transitions via findOneAndUpdate + upsert.
//
// All three production backends implement health.Checker. MemoryStore does
// not (there is nothing remote to check). Postgres compares lease_until
// against the database server's NOW(); the other backends compare against
// the caller's wall clock — under cross-pod clock skew Postgres therefore
// gives stronger lease semantics.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// State enumerates the possible states of a hook key.
type State string

const (
	// StatePending is the implicit initial state: the key does not exist.
	// A Status with State == StatePending has all other fields zero-valued.
	StatePending State = "pending"
	// StateRunning means a holder has claimed the hook and is executing it.
	// The claim is bounded by a lease that the holder must refresh.
	StateRunning State = "running"
	// StateCompleted is a terminal success state. The hook will not run
	// again until the key is reset or the version changes.
	StateCompleted State = "completed"
	// StateFailed is a terminal failure state set when Fail is called with
	// retryable=false. Manual intervention (Reset) is required to retry.
	StateFailed State = "failed"
)

// Outcome reports what happened when Once was called on a hook.
//
// Callers that switch on Outcome should also check the returned error
// first: argument validation failures (nil store, empty Hook.Name, "@" in
// Hook.Name, nil fn) return ("", err) without producing any outcome value.
type Outcome string

const (
	// OutcomeRan means this caller executed the body, it returned nil,
	// and the Complete bookkeeping call succeeded.
	OutcomeRan Outcome = "ran"
	// OutcomeSkippedCompleted means another instance already completed
	// the hook successfully; the body was not executed.
	OutcomeSkippedCompleted Outcome = "skipped_completed"
	// OutcomeSkippedRunning means another instance currently holds the
	// claim; the body was not executed. Caller may choose to wait or move on.
	OutcomeSkippedRunning Outcome = "skipped_running"
	// OutcomeSkippedFailed means the hook is in terminal failed state;
	// the body was not executed. Manual intervention is required.
	OutcomeSkippedFailed Outcome = "skipped_failed"
	// OutcomeFailed means this caller executed the body and the body
	// returned an error (or panicked). The body error is returned alongside.
	OutcomeFailed Outcome = "failed"
	// OutcomeCompleteFailed means the body ran cleanly but the
	// follow-up Complete call failed (e.g. transient backend error).
	// The work happened; only the bookkeeping is wedged. The claim will
	// remain in StateRunning until the lease expires, after which another
	// pod may take over and re-run the body — relevant only if the body
	// is non-idempotent.
	OutcomeCompleteFailed Outcome = "complete_failed"
)

// Sentinel errors. Callers should compare via errors.Is.
var (
	// ErrLeaseLost is returned by Refresh/Complete/Fail when the caller
	// no longer holds the lease (lease expired, or another holder claimed).
	ErrLeaseLost = errors.New("lifecycle: lease lost")
	// ErrInvalidHookName is returned by Once and Hook.Validate when a
	// Hook.Name contains the "@" separator that Key uses to join name and
	// version.
	ErrInvalidHookName = errors.New("lifecycle: hook name must not contain '@'")
)

// Hook identifies a single coordinated action by logical name plus a
// version qualifier. The version is what gives "run once per deployment"
// semantics — bump the version to make the hook eligible to run again.
type Hook struct {
	// Name is a stable identifier for the action (e.g. "migrate-users-table").
	// Must not contain "@", which is reserved as the separator between
	// Name and Version in Key.
	Name string
	// Version is the version qualifier; typically the deployment version
	// or schema version. Empty version is allowed (hook runs exactly once
	// forever unless Reset is called).
	Version string
}

// Validate returns ErrInvalidHookName if Name contains "@". An empty Name
// is also rejected (Once enforces this separately). Returns nil otherwise.
func (h Hook) Validate() error {
	if h.Name == "" {
		return errors.New("lifecycle: hook.Name is required")
	}
	if strings.Contains(h.Name, "@") {
		return fmt.Errorf("%w: %q", ErrInvalidHookName, h.Name)
	}
	return nil
}

// Key returns the storage key for the hook. Format: "{name}@{version}",
// or just "{name}" if Version is empty. Validate the hook before relying
// on the result: Key does not enforce the "@"-in-name constraint.
func (h Hook) Key() string {
	if h.Version == "" {
		return h.Name
	}
	return h.Name + "@" + h.Version
}

// Status is a point-in-time snapshot of a hook's state in storage. When
// State is StatePending the rest of the fields are zero-valued.
type Status struct {
	State       State
	Holder      string    // instance ID of current/last holder
	LeaseUntil  time.Time // when the current running lease expires
	CompletedAt time.Time // set when State == StateCompleted
	FailedAt    time.Time // set when State == StateFailed
	Error       string    // set when State == StateFailed
}

// Store is the persistence contract for lifecycle hooks. Implementations
// must provide atomic state transitions; in particular Acquire must be
// race-free across concurrent callers.
type Store interface {
	// Acquire attempts to claim the hook for instanceID with the given lease.
	// Returns the post-call status. If the returned status is StateRunning
	// and Holder == instanceID, the caller won the claim and must run the body.
	// In every other case the caller must skip.
	//
	// Acquire is also responsible for taking over expired leases: if the
	// current holder's lease is in the past, Acquire transfers ownership
	// to the new caller and returns StateRunning with the new holder.
	Acquire(ctx context.Context, key, instanceID string, lease time.Duration) (Status, error)

	// Refresh extends the current lease. Returns ErrLeaseLost if the
	// caller no longer holds the claim (lease expired — including by the
	// same holder failing to refresh in time — or another instance has
	// taken over).
	Refresh(ctx context.Context, key, instanceID string, lease time.Duration) error

	// Complete transitions a running claim to terminal completed state.
	// Returns ErrLeaseLost if instanceID is not the current holder.
	Complete(ctx context.Context, key, instanceID string) error

	// Fail transitions a running claim either to terminal failed state
	// (retryable=false) or back to absent by deleting the row
	// (retryable=true). Returns ErrLeaseLost if instanceID is not the
	// current holder.
	Fail(ctx context.Context, key, instanceID, errMsg string, retryable bool) error

	// Get returns the current status. State == StatePending means the
	// key does not exist; all other Status fields are zero in that case.
	Get(ctx context.Context, key string) (Status, error)

	// Reset removes the entry unconditionally. This is an admin operation
	// intended for forcing replay of a completed hook or recovering from
	// a failed terminal state.
	//
	// WARNING: Reset does not check the current state. Calling Reset
	// while another instance is actively running the body will silently
	// release the claim — the running pod's next Refresh will return
	// ErrLeaseLost (and log "body may run twice"), and another pod can
	// acquire the same hook. Use Get first to confirm the hook is in
	// a terminal state before calling Reset, or only invoke Reset out of
	// band when no pods are running the hook.
	Reset(ctx context.Context, key string) error
}

// Option configures a Once call.
type Option func(*onceOptions)

type onceOptions struct {
	instanceID      string
	lease           time.Duration
	refreshInterval time.Duration
	retryable       bool
	logger          *slog.Logger
}

func defaultOptions() *onceOptions {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	return &onceOptions{
		instanceID:      host + ":" + strconv.Itoa(os.Getpid()),
		lease:           60 * time.Second,
		refreshInterval: 0, // derived from lease if unset
		retryable:       false,
		logger:          slog.Default(),
	}
}

// WithInstanceID overrides the default instance identifier (hostname:pid).
// In Kubernetes, pass POD_NAME for traceability.
func WithInstanceID(id string) Option {
	return func(o *onceOptions) {
		if id != "" {
			o.instanceID = id
		}
	}
}

// WithLease sets the lease duration. The body must complete or refresh
// within this window or another instance may take over. Default 60s.
func WithLease(d time.Duration) Option {
	return func(o *onceOptions) {
		if d > 0 {
			o.lease = d
		}
	}
}

// WithRefreshInterval sets how often the auto-refresh goroutine extends
// the lease while the body runs. Default is lease/3 with a 1-second floor
// for very short leases. Set to lease (or longer) to disable mid-run
// refresh — the body must then finish within a single lease window.
func WithRefreshInterval(d time.Duration) Option {
	return func(o *onceOptions) {
		if d > 0 {
			o.refreshInterval = d
		}
	}
}

// WithRetryable controls how body errors are recorded.
//
// Default is false: a body error puts the hook into terminal StateFailed
// and the hook will not run again until manually Reset. This is the safe
// default for non-idempotent work — destructive schema migrations,
// external API calls without an idempotency key, sending notifications.
//
// WithRetryable(true) releases the claim on a body error so another
// instance can retry. Use only when fn is fully idempotent: repeated
// successful executions across pods must produce no extra side effects.
func WithRetryable(retryable bool) Option {
	return func(o *onceOptions) {
		o.retryable = retryable
	}
}

// WithLogger overrides the slog logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *onceOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// Once runs fn at most once across the fleet for the given hook.
//
// Behavior:
//   - If no instance has claimed the hook, this caller wins and runs fn
//     with an auto-refreshing lease. Returns (OutcomeRan, nil) on success.
//   - If another instance is currently running the hook, returns
//     (OutcomeSkippedRunning, nil) and does not run fn.
//   - If the hook is already in a terminal state (completed/failed),
//     returns the matching skipped outcome and does not run fn.
//   - If fn returns an error or panics, the claim is either released
//     (WithRetryable(true)) or moved to terminal failed
//     (WithRetryable(false), default). Returns (OutcomeFailed, err).
//   - If fn returns nil but the Complete call fails (e.g. transient
//     backend error), returns (OutcomeCompleteFailed, err). The work
//     happened; only the bookkeeping is wedged.
//   - Argument validation failures (nil store, nil fn, invalid hook)
//     return ("", err) before any state work; check the error before
//     switching on the Outcome.
//
// Lease loss during execution does not cancel the body. Once logs an
// error and lets fn complete; another instance may take over the claim
// after the lease expires, and the first writer's Complete will then
// return ErrLeaseLost. For non-idempotent work, size WithLease to comfortably
// exceed expected body duration.
//
// Once is the high-level entry point; callers needing fine-grained
// control (e.g. running the body across multiple processes) should use
// the Store methods directly.
func Once(ctx context.Context, store Store, hook Hook, fn func(context.Context) error, opts ...Option) (Outcome, error) {
	if store == nil {
		return "", errors.New("lifecycle: store is required")
	}
	if err := hook.Validate(); err != nil {
		return "", err
	}
	if fn == nil {
		return "", errors.New("lifecycle: fn is required")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	if o.refreshInterval == 0 {
		o.refreshInterval = max(o.lease/3, time.Second)
	}

	key := hook.Key()
	log := o.logger.With("hook", key, "instance", o.instanceID)

	status, err := store.Acquire(ctx, key, o.instanceID, o.lease)
	if err != nil {
		return "", fmt.Errorf("lifecycle: acquire %s: %w", key, err)
	}

	switch status.State {
	case StateCompleted:
		log.Debug("hook already completed, skipping")
		return OutcomeSkippedCompleted, nil
	case StateFailed:
		log.Warn("hook in terminal failed state, skipping", "error", status.Error)
		return OutcomeSkippedFailed, nil
	case StateRunning:
		if status.Holder != o.instanceID {
			log.Debug("hook held by another instance, skipping", "holder", status.Holder)
			return OutcomeSkippedRunning, nil
		}
		// We are the holder — fall through to execute.
	default:
		return "", fmt.Errorf("lifecycle: unexpected post-acquire state %q", status.State)
	}

	log.Info("hook claim acquired, executing body")

	refreshCtx, stopRefresh := context.WithCancel(ctx)
	refreshDone := make(chan struct{})
	go refreshLoop(refreshCtx, store, key, o.instanceID, o.lease, o.refreshInterval, log, refreshDone)

	bodyErr := runWithRecover(ctx, fn)

	stopRefresh()
	<-refreshDone

	if bodyErr != nil {
		log.Error("hook body failed", "error", bodyErr, "retryable", o.retryable)
		if failErr := store.Fail(context.WithoutCancel(ctx), key, o.instanceID, bodyErr.Error(), o.retryable); failErr != nil {
			log.Error("failed to record body failure", "error", failErr)
		}
		return OutcomeFailed, bodyErr
	}

	if err := store.Complete(context.WithoutCancel(ctx), key, o.instanceID); err != nil {
		log.Error("body succeeded but Complete call failed; hook remains in running state until lease expiry", "error", err)
		return OutcomeCompleteFailed, fmt.Errorf("lifecycle: complete %s: %w", key, err)
	}
	log.Info("hook completed")
	return OutcomeRan, nil
}

// runWithRecover invokes fn and converts a panic into an error so the
// caller's Once handling (Fail bookkeeping + outcome reporting) runs.
// Without recovery a panic would skip Fail and leak the claim until the
// lease expires.
func runWithRecover(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("lifecycle: hook body panicked: %v", r)
		}
	}()
	return fn(ctx)
}

func refreshLoop(ctx context.Context, store Store, key, instanceID string, lease, interval time.Duration, log *slog.Logger, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Use a detached context so a parent cancellation racing with
			// this tick doesn't truncate an in-flight refresh write mid-call.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
			err := store.Refresh(rctx, key, instanceID, lease)
			cancel()
			if err != nil {
				if errors.Is(err, ErrLeaseLost) {
					log.Error("lease lost during execution; body may run twice", "key", key)
					return
				}
				log.Warn("lease refresh failed", "error", err)
			}
		}
	}
}
