package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3/health"
)

/*
PostgreSQL Schema:

CREATE TABLE sagas (
    id              VARCHAR(36) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    status          VARCHAR(50) NOT NULL,
    current_step    INT NOT NULL DEFAULT 0,
    completed_steps TEXT[],
    data            JSONB,
    error           TEXT,
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL,
    version         BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_sagas_name ON sagas(name);
CREATE INDEX idx_sagas_status ON sagas(status);
CREATE INDEX idx_sagas_started_at ON sagas(started_at);
*/

// PostgresStore is a PostgreSQL-based saga store
type PostgresStore struct {
	db    *sql.DB
	table string
}

// PostgresStoreOption configures a PostgresStore.
type PostgresStoreOption func(*postgresStoreOptions)

type postgresStoreOptions struct {
	table string
}

// WithTable sets a custom table name for the PostgreSQL saga store.
func WithTable(table string) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if table != "" {
			o.table = table
		}
	}
}

// NewPostgresStore creates a new PostgreSQL saga store.
//
// The default table name is "sagas".
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) *PostgresStore {
	o := &postgresStoreOptions{
		table: "sagas",
	}
	for _, opt := range opts {
		opt(o)
	}

	return &PostgresStore{
		db:    db,
		table: o.table,
	}
}

// Create creates a new saga instance
func (s *PostgresStore) Create(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		state.ID,
		state.Name,
		state.Status,
		state.CurrentStep,
		state.CompletedSteps,
		data,
		state.Error,
		state.StartedAt,
		state.CompletedAt,
		state.LastUpdatedAt,
		state.Version,
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves saga state by ID
func (s *PostgresStore) Get(ctx context.Context, id string) (*State, error) {
	query := fmt.Sprintf(`
		SELECT id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at, version
		FROM %s
		WHERE id = $1
	`, s.table)

	var state State
	var data []byte
	var completedSteps []string
	var completedAt sql.NullTime
	var errorStr sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&state.ID,
		&state.Name,
		&state.Status,
		&state.CurrentStep,
		&completedSteps,
		&data,
		&errorStr,
		&state.StartedAt,
		&completedAt,
		&state.LastUpdatedAt,
		&state.Version,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("saga not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	state.CompletedSteps = completedSteps

	if len(data) > 0 {
		if err := json.Unmarshal(data, &state.Data); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
	}

	if completedAt.Valid {
		state.CompletedAt = &completedAt.Time
	}

	if errorStr.Valid {
		state.Error = errorStr.String
	}

	return &state, nil
}

// Update updates saga state with optimistic locking.
//
// The update uses the Version field for optimistic locking. If the version
// in PostgreSQL doesn't match the expected version, ErrVersionConflict is returned.
// On successful update, the state's Version is incremented.
func (s *PostgresStore) Update(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	newVersion := state.Version + 1

	// Use optimistic locking: only update if version matches
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $1, current_step = $2, completed_steps = $3, data = $4, error = $5, completed_at = $6, last_updated_at = $7, version = $8
		WHERE id = $9 AND version = $10
	`, s.table)

	result, err := s.db.ExecContext(ctx, query,
		state.Status,
		state.CurrentStep,
		state.CompletedSteps,
		data,
		state.Error,
		state.CompletedAt,
		state.LastUpdatedAt,
		newVersion,
		state.ID,
		state.Version,
	)

	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		// Check if saga exists to distinguish between not found and version conflict
		var exists bool
		checkQuery := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", s.table)
		_ = s.db.QueryRowContext(ctx, checkQuery, state.ID).Scan(&exists)
		if exists {
			return ErrVersionConflict
		}
		return fmt.Errorf("saga not found: %s", state.ID)
	}

	// Update local version on success
	state.Version = newVersion
	return nil
}

// List lists sagas matching the filter
func (s *PostgresStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	query := fmt.Sprintf(`
		SELECT id, name, status, current_step, completed_steps, data, error, started_at, completed_at, last_updated_at, version
		FROM %s
		WHERE 1=1
	`, s.table)

	var args []any
	argIndex := 1

	if filter.Name != "" {
		query += fmt.Sprintf(" AND name = $%d", argIndex)
		args = append(args, filter.Name)
		argIndex++
	}

	if len(filter.Status) > 0 {
		placeholders := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, status)
			argIndex++
		}
		query += fmt.Sprintf(" AND status IN (%s)", strings.Join(placeholders, ", "))
	}

	query += " ORDER BY started_at DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []*State
	for rows.Next() {
		var state State
		var data []byte
		var completedSteps []string
		var completedAt sql.NullTime
		var errorStr sql.NullString

		err := rows.Scan(
			&state.ID,
			&state.Name,
			&state.Status,
			&state.CurrentStep,
			&completedSteps,
			&data,
			&errorStr,
			&state.StartedAt,
			&completedAt,
			&state.LastUpdatedAt,
			&state.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		state.CompletedSteps = completedSteps

		if len(data) > 0 {
			if err := json.Unmarshal(data, &state.Data); err != nil {
				return nil, fmt.Errorf("unmarshal data: %w", err)
			}
		}

		if completedAt.Valid {
			state.CompletedAt = &completedAt.Time
		}

		if errorStr.Valid {
			state.Error = errorStr.String
		}

		results = append(results, &state)
	}

	return results, nil
}

// DeleteOlderThan removes sagas older than the specified age
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE started_at < $1", s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now().Add(-age))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// Health performs a health check on the PostgreSQL saga store.
func (s *PostgresStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping PostgreSQL
	if err := s.db.PingContext(ctx); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("postgres ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	// Count total sagas
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)
	if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return &health.Result{
			Status:    health.StatusDegraded,
			Message:   fmt.Sprintf("failed to count sagas: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	// Count by status
	var pending, running, compensating int64
	pendingQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = $1", s.table)
	_ = s.db.QueryRowContext(ctx, pendingQuery, StatusPending).Scan(&pending)
	_ = s.db.QueryRowContext(ctx, pendingQuery, StatusRunning).Scan(&running)
	_ = s.db.QueryRowContext(ctx, pendingQuery, StatusCompensating).Scan(&compensating)

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"total_sagas":        count,
			"pending_sagas":      pending,
			"running_sagas":      running,
			"compensating_sagas": compensating,
			"table":              s.table,
		},
	}
}

// Compile-time checks
var (
	_ Store          = (*PostgresStore)(nil)
	_ health.Checker = (*PostgresStore)(nil)
)
