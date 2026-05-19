package lifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/rbaliyan/event/v3/health"
)

// validIdentifier matches safe SQL identifiers (alphanumeric and underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// PostgresStore is a PostgreSQL-backed Store. State transitions are made
// atomic by a per-key SELECT ... FOR UPDATE inside a transaction, so the
// implementation is safe against concurrent callers on multiple pods.
//
// Use EnsureSchema during bootstrap to create the underlying table.
//
// Clock semantics: PostgresStore compares lease_until against the database
// server's NOW() rather than the caller's wall clock, so it is robust to
// cross-pod clock skew. The Redis, Mongo, and Memory backends compare
// against the caller's clock and can mis-arbitrate under skew greater
// than the lease duration.
type PostgresStore struct {
	db    *sql.DB
	table string
}

// PostgresOption configures a PostgresStore.
type PostgresOption func(*postgresOptions)

type postgresOptions struct {
	table string
}

// WithPostgresTable sets a custom table name. Default: "lifecycle_hooks".
func WithPostgresTable(table string) PostgresOption {
	return func(o *postgresOptions) {
		if table != "" {
			o.table = table
		}
	}
}

// NewPostgresStore creates a Postgres-backed Store. db must be non-nil.
func NewPostgresStore(db *sql.DB, opts ...PostgresOption) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("lifecycle: db is required")
	}
	o := &postgresOptions{table: "lifecycle_hooks"}
	for _, opt := range opts {
		opt(o)
	}
	if !validIdentifier.MatchString(o.table) {
		return nil, fmt.Errorf("lifecycle: invalid table name %q", o.table)
	}
	return &PostgresStore{db: db, table: o.table}, nil
}

// EnsureSchema creates the lifecycle_hooks table if it does not exist.
//
// Production deployments typically run this once at bootstrap; alternatively,
// run it as part of your application's migration suite.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			hook_key     VARCHAR(512) PRIMARY KEY,
			state        VARCHAR(32)  NOT NULL,
			holder       VARCHAR(255),
			lease_until  TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			failed_at    TIMESTAMPTZ,
			error        TEXT
		)
	`, s.table)
	if _, err := s.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

// Acquire implements Store.
//
// Performed inside a transaction with SELECT ... FOR UPDATE to serialize
// concurrent acquirers on the same hook_key. After locking the row (or
// observing its absence), the method applies the same state-machine rules
// as the memory and Redis stores.
func (s *PostgresStore) Acquire(ctx context.Context, key, instanceID string, lease time.Duration) (Status, error) {
	var status Status
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		row, ok, err := s.selectForUpdate(ctx, tx, key)
		if err != nil {
			return err
		}
		now := time.Now()
		newLease := now.Add(lease)

		if !ok {
			// No row: insert running.
			if err := s.insertRunning(ctx, tx, key, instanceID, newLease); err != nil {
				return err
			}
			status = Status{State: StateRunning, Holder: instanceID, LeaseUntil: newLease}
			return nil
		}

		switch row.State {
		case StateCompleted, StateFailed:
			status = row
			return nil
		case StateRunning:
			if row.Holder == instanceID {
				if err := s.updateLease(ctx, tx, key, newLease); err != nil {
					return err
				}
				row.LeaseUntil = newLease
				status = row
				return nil
			}
			if now.Before(row.LeaseUntil) {
				// Different holder, lease still valid.
				status = row
				return nil
			}
			// Lease expired — take over.
			if err := s.takeOver(ctx, tx, key, instanceID, newLease); err != nil {
				return err
			}
			status = Status{State: StateRunning, Holder: instanceID, LeaseUntil: newLease}
			return nil
		default:
			return fmt.Errorf("acquire: unexpected stored state %q", row.State)
		}
	})
	if err != nil {
		return Status{}, fmt.Errorf("acquire: %w", err)
	}
	return status, nil
}

// Refresh implements Store.
func (s *PostgresStore) Refresh(ctx context.Context, key, instanceID string, lease time.Duration) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		UPDATE %s
		SET lease_until = $1
		WHERE hook_key = $2
		  AND state = 'running'
		  AND holder = $3
		  AND lease_until > NOW()
	`, s.table)
	res, err := s.db.ExecContext(ctx, q, time.Now().Add(lease), key, instanceID)
	if err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete implements Store.
func (s *PostgresStore) Complete(ctx context.Context, key, instanceID string) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		UPDATE %s
		SET state = 'completed', completed_at = NOW(), lease_until = NULL
		WHERE hook_key = $1 AND state = 'running' AND holder = $2
	`, s.table)
	res, err := s.db.ExecContext(ctx, q, key, instanceID)
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail implements Store.
func (s *PostgresStore) Fail(ctx context.Context, key, instanceID, errMsg string, retryable bool) error {
	if retryable {
		// #nosec G201 -- table name is set at construction, not user input
		q := fmt.Sprintf(`
			DELETE FROM %s
			WHERE hook_key = $1 AND state = 'running' AND holder = $2
		`, s.table)
		res, err := s.db.ExecContext(ctx, q, key, instanceID)
		if err != nil {
			return fmt.Errorf("fail (retryable): %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrLeaseLost
		}
		return nil
	}

	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		UPDATE %s
		SET state = 'failed', failed_at = NOW(), error = $1, lease_until = NULL
		WHERE hook_key = $2 AND state = 'running' AND holder = $3
	`, s.table)
	res, err := s.db.ExecContext(ctx, q, errMsg, key, instanceID)
	if err != nil {
		return fmt.Errorf("fail: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Get implements Store.
func (s *PostgresStore) Get(ctx context.Context, key string) (Status, error) {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		SELECT state, holder, lease_until, completed_at, failed_at, error
		FROM %s
		WHERE hook_key = $1
	`, s.table)
	row := s.db.QueryRowContext(ctx, q, key)
	st, ok, err := scanStatus(row)
	if err != nil {
		return Status{}, fmt.Errorf("get: %w", err)
	}
	if !ok {
		return Status{State: StatePending}, nil
	}
	return st, nil
}

// Reset implements Store.
func (s *PostgresStore) Reset(ctx context.Context, key string) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf("DELETE FROM %s WHERE hook_key = $1", s.table)
	if _, err := s.db.ExecContext(ctx, q, key); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// Health implements health.Checker.
func (s *PostgresStore) Health(ctx context.Context) *health.Result {
	start := time.Now()
	if err := s.db.PingContext(ctx); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("postgres ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE state = 'running'),
			COUNT(*) FILTER (WHERE state = 'completed'),
			COUNT(*) FILTER (WHERE state = 'failed')
		FROM %s
	`, s.table)
	var running, completed, failed int64
	if err := s.db.QueryRowContext(ctx, q).Scan(&running, &completed, &failed); err != nil {
		return &health.Result{
			Status:    health.StatusDegraded,
			Message:   fmt.Sprintf("count by state failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}
	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"running":   running,
			"completed": completed,
			"failed":    failed,
			"table":     s.table,
		},
	}
}

// withTx runs fn inside a ReadCommitted transaction. Per-key serialization
// comes from the row-level lock taken via SELECT ... FOR UPDATE inside
// selectForUpdate; callers must take that lock before mutating.
func (s *PostgresStore) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) selectForUpdate(ctx context.Context, tx *sql.Tx, key string) (Status, bool, error) {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		SELECT state, holder, lease_until, completed_at, failed_at, error
		FROM %s
		WHERE hook_key = $1
		FOR UPDATE
	`, s.table)
	row := tx.QueryRowContext(ctx, q, key)
	return scanStatus(row)
}

func (s *PostgresStore) insertRunning(ctx context.Context, tx *sql.Tx, key, instanceID string, leaseUntil time.Time) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		INSERT INTO %s (hook_key, state, holder, lease_until)
		VALUES ($1, 'running', $2, $3)
	`, s.table)
	_, err := tx.ExecContext(ctx, q, key, instanceID, leaseUntil)
	return err
}

func (s *PostgresStore) updateLease(ctx context.Context, tx *sql.Tx, key string, leaseUntil time.Time) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`UPDATE %s SET lease_until = $1 WHERE hook_key = $2`, s.table)
	_, err := tx.ExecContext(ctx, q, leaseUntil, key)
	return err
}

func (s *PostgresStore) takeOver(ctx context.Context, tx *sql.Tx, key, instanceID string, leaseUntil time.Time) error {
	// #nosec G201 -- table name is set at construction, not user input
	q := fmt.Sprintf(`
		UPDATE %s
		SET holder = $1, lease_until = $2, completed_at = NULL, failed_at = NULL, error = NULL, state = 'running'
		WHERE hook_key = $3
	`, s.table)
	_, err := tx.ExecContext(ctx, q, instanceID, leaseUntil, key)
	return err
}

// scanStatus reads a single row into a Status. Returns (Status, false, nil)
// when the row does not exist.
func scanStatus(row *sql.Row) (Status, bool, error) {
	var (
		state       string
		holder      sql.NullString
		leaseUntil  sql.NullTime
		completedAt sql.NullTime
		failedAt    sql.NullTime
		errMsg      sql.NullString
	)
	if err := row.Scan(&state, &holder, &leaseUntil, &completedAt, &failedAt, &errMsg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Status{}, false, nil
		}
		return Status{}, false, err
	}
	st := Status{State: State(state)}
	if holder.Valid {
		st.Holder = holder.String
	}
	if leaseUntil.Valid {
		st.LeaseUntil = leaseUntil.Time
	}
	if completedAt.Valid {
		st.CompletedAt = completedAt.Time
	}
	if failedAt.Valid {
		st.FailedAt = failedAt.Time
	}
	if errMsg.Valid {
		st.Error = errMsg.String
	}
	return st, true, nil
}

var (
	_ Store          = (*PostgresStore)(nil)
	_ health.Checker = (*PostgresStore)(nil)
)
