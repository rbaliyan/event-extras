package lifecycle

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// newPGMock returns a PostgresStore backed by an in-process sqlmock DB. No
// real database is required, so these tests run in the default build and lift
// coverage of postgres.go (which is otherwise only reachable via the
// container-backed integration tests).
func newPGMock(t *testing.T) (*PostgresStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewPostgresStore(db)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	return store, mock
}

func assertExpectationsMet(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

func TestNewPostgresStore_Guards(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresStore(nil); err == nil {
		t.Fatal("expected error for nil db")
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := NewPostgresStore(db, WithPostgresTable("bad-name!")); err == nil {
		t.Fatal("expected error for invalid table name")
	}
	if _, err := NewPostgresStore(db, WithPostgresTable("custom_table")); err != nil {
		t.Fatalf("valid custom table should be accepted: %v", err)
	}
	// Empty table name is ignored, falling back to the default.
	if _, err := NewPostgresStore(db, WithPostgresTable("")); err != nil {
		t.Fatalf("empty table should fall back to default: %v", err)
	}
}

func TestPostgresStore_EnsureSchema(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS lifecycle_hooks").
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Acquire_InsertsWhenAbsent(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").
		WithArgs("k").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO lifecycle_hooks").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	s, err := store.Acquire(context.Background(), "k", "pod-a", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if s.State != StateRunning || s.Holder != "pod-a" {
		t.Fatalf("expected running/pod-a, got %+v", s)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Acquire_SameHolderRefreshes(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
		AddRow("running", "pod-a", time.Now().Add(time.Minute), nil, nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs("k").WillReturnRows(rows)
	mock.ExpectExec("UPDATE lifecycle_hooks SET lease_until").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s, err := store.Acquire(context.Background(), "k", "pod-a", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if s.Holder != "pod-a" {
		t.Fatalf("expected pod-a holder, got %+v", s)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Acquire_OtherHolderValidLease(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
		AddRow("running", "pod-a", time.Now().Add(time.Minute), nil, nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs("k").WillReturnRows(rows)
	mock.ExpectCommit()

	s, err := store.Acquire(context.Background(), "k", "pod-b", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if s.Holder != "pod-a" {
		t.Fatalf("pod-b should observe pod-a as holder, got %+v", s)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Acquire_TakesOverExpiredLease(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
		AddRow("running", "pod-a", time.Now().Add(-time.Minute), nil, nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs("k").WillReturnRows(rows)
	mock.ExpectExec("UPDATE lifecycle_hooks").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s, err := store.Acquire(context.Background(), "k", "pod-b", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if s.State != StateRunning || s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over, got %+v", s)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Acquire_TerminalStatesPassThrough(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state string
	}{
		{"completed", "completed"},
		{"failed", "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, mock := newPGMock(t)
			rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
				AddRow(tc.state, "pod-a", nil, nil, nil, "boom")
			mock.ExpectBegin()
			mock.ExpectQuery("FOR UPDATE").WithArgs("k").WillReturnRows(rows)
			mock.ExpectCommit()

			s, err := store.Acquire(context.Background(), "k", "pod-b", time.Minute)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			if string(s.State) != tc.state {
				t.Fatalf("expected %s pass-through, got %+v", tc.state, s)
			}
			assertExpectationsMet(t, mock)
		})
	}
}

func TestPostgresStore_Acquire_UnexpectedStateErrors(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
		AddRow("bogus", "pod-a", nil, nil, nil, nil)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs("k").WillReturnRows(rows)
	mock.ExpectRollback()

	if _, err := store.Acquire(context.Background(), "k", "pod-b", time.Minute); err == nil {
		t.Fatal("expected error for unexpected stored state")
	}
}

func TestPostgresStore_Refresh(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("UPDATE lifecycle_hooks SET lease_until").
			WillReturnResult(sqlmock.NewResult(0, 1))
		if err := store.Refresh(context.Background(), "k", "pod-a", time.Minute); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("lease_lost", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("UPDATE lifecycle_hooks SET lease_until").
			WillReturnResult(sqlmock.NewResult(0, 0))
		if err := store.Refresh(context.Background(), "k", "pod-a", time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost, got %v", err)
		}
		assertExpectationsMet(t, mock)
	})
}

func TestPostgresStore_Complete(t *testing.T) {
	t.Parallel()
	t.Run("lease_lost", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("SET state = 'completed'").WillReturnResult(sqlmock.NewResult(0, 0))
		if err := store.Complete(context.Background(), "k", "pod-a"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost when no row updated, got %v", err)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("db_error_wrapped", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("SET state = 'completed'").WillReturnError(errors.New("conn reset"))
		err := store.Complete(context.Background(), "k", "pod-a")
		if err == nil || errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected a wrapped DB error, got %v", err)
		}
		assertExpectationsMet(t, mock)
	})
}

func TestPostgresStore_Fail(t *testing.T) {
	t.Parallel()
	t.Run("terminal", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("SET state = 'failed'").WillReturnResult(sqlmock.NewResult(0, 1))
		if err := store.Fail(context.Background(), "k", "pod-a", "boom", false); err != nil {
			t.Fatalf("Fail: %v", err)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("retryable_deletes", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("DELETE FROM lifecycle_hooks").WillReturnResult(sqlmock.NewResult(0, 1))
		if err := store.Fail(context.Background(), "k", "pod-a", "transient", true); err != nil {
			t.Fatalf("Fail retryable: %v", err)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("retryable_lease_lost", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("DELETE FROM lifecycle_hooks").WillReturnResult(sqlmock.NewResult(0, 0))
		if err := store.Fail(context.Background(), "k", "pod-a", "transient", true); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost, got %v", err)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("db_error_wrapped", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectExec("SET state = 'failed'").WillReturnError(errors.New("conn reset"))
		err := store.Fail(context.Background(), "k", "pod-a", "boom", false)
		if err == nil || errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expected a wrapped DB error, got %v", err)
		}
		assertExpectationsMet(t, mock)
	})
}

func TestPostgresStore_Get_ScansNullsAndPending(t *testing.T) {
	t.Parallel()
	t.Run("pending_when_absent", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectQuery("SELECT state, holder").WithArgs("missing").WillReturnError(sql.ErrNoRows)
		s, err := store.Get(context.Background(), "missing")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if s.State != StatePending {
			t.Fatalf("expected StatePending, got %+v", s)
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("running_with_null_terminal_columns", func(t *testing.T) {
		store, mock := newPGMock(t)
		rows := sqlmock.NewRows([]string{"state", "holder", "lease_until", "completed_at", "failed_at", "error"}).
			AddRow("running", "pod-a", time.Now().Add(time.Minute), nil, nil, nil)
		mock.ExpectQuery("SELECT state, holder").WithArgs("k").WillReturnRows(rows)
		s, err := store.Get(context.Background(), "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if s.State != StateRunning || s.Holder != "pod-a" || !s.CompletedAt.IsZero() || s.Error != "" {
			t.Fatalf("null columns should yield zero values, got %+v", s)
		}
		assertExpectationsMet(t, mock)
	})
}

func TestPostgresStore_Reset(t *testing.T) {
	t.Parallel()
	store, mock := newPGMock(t)
	mock.ExpectExec("DELETE FROM lifecycle_hooks").WithArgs("k").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Reset(context.Background(), "k"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	assertExpectationsMet(t, mock)
}

func TestPostgresStore_Health_Unit(t *testing.T) {
	t.Parallel()
	t.Run("healthy", func(t *testing.T) {
		// Default sqlmock does not require a ping expectation; PingContext
		// succeeds silently, so we only set up the count query.
		store, mock := newPGMock(t)
		mock.ExpectQuery("COUNT").WillReturnRows(
			sqlmock.NewRows([]string{"running", "completed", "failed"}).AddRow(1, 2, 3))
		res := store.Health(context.Background())
		if string(res.Status) != "healthy" {
			t.Fatalf("expected healthy, got %q (%s)", res.Status, res.Message)
		}
		for _, k := range []string{"running", "completed", "failed", "table"} {
			if _, ok := res.Details[k]; !ok {
				t.Fatalf("missing detail %q in %v", k, res.Details)
			}
		}
		assertExpectationsMet(t, mock)
	})
	t.Run("unhealthy_on_ping_failure", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		store, err := NewPostgresStore(db)
		if err != nil {
			t.Fatalf("NewPostgresStore: %v", err)
		}
		mock.ExpectPing().WillReturnError(errors.New("connection refused"))
		res := store.Health(context.Background())
		if string(res.Status) != "unhealthy" {
			t.Fatalf("expected unhealthy, got %q", res.Status)
		}
		if res.Message == "" {
			t.Fatal("expected a message on unhealthy result")
		}
	})
	t.Run("degraded_on_count_failure", func(t *testing.T) {
		store, mock := newPGMock(t)
		mock.ExpectQuery("COUNT").WillReturnError(errors.New("relation does not exist"))
		res := store.Health(context.Background())
		if string(res.Status) != "degraded" {
			t.Fatalf("expected degraded, got %q (%s)", res.Status, res.Message)
		}
		if res.Message == "" {
			t.Fatal("expected a message on degraded result")
		}
	})
}
