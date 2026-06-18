package lifecycle

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// getPostgresDB returns a live *sql.DB, or skips the test when POSTGRES_URI is
// unset. These tests run in the normal build: with no env they skip instantly
// (fast default CI job); with POSTGRES_URI set (the integration CI job or a
// local container) they run and count toward coverage.
func getPostgresDB(t *testing.T) *sql.DB {
	t.Helper()
	uri := os.Getenv("POSTGRES_URI")
	if uri == "" {
		t.Skip("POSTGRES_URI not set; skipping PostgreSQL integration test")
	}
	db, err := sql.Open("postgres", uri)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newPostgresTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	db := getPostgresDB(t)
	table := "lifecycle_test_" + time.Now().Format("20060102150405000000")
	store, err := NewPostgresStore(db, WithPostgresTable(table))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	})
	return store
}

func TestPostgresStore_Contract(t *testing.T) {
	store := newPostgresTestStore(t)
	runStoreContractTests(t, store)
}

func TestPostgresStore_Health(t *testing.T) {
	store := newPostgresTestStore(t)
	ctx := context.Background()

	// Seed one entry per state so the per-state counts are non-zero.
	if _, err := store.Acquire(ctx, "h-run", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "h-done", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, "h-done", "pod-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "h-failed", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(ctx, "h-failed", "pod-a", "boom", false); err != nil {
		t.Fatal(err)
	}

	res := store.Health(ctx)
	if string(res.Status) != "healthy" {
		t.Fatalf("expected healthy, got %q (msg=%q)", res.Status, res.Message)
	}
	for _, k := range []string{"running", "completed", "failed", "table"} {
		if _, ok := res.Details[k]; !ok {
			t.Fatalf("expected detail %q, got %v", k, res.Details)
		}
	}
}

// TestPostgresStore_ErrorsWhenDBClosed injects a backend failure: once the
// connection pool is closed, operations must surface a wrapped error.
func TestPostgresStore_ErrorsWhenDBClosed(t *testing.T) {
	db := getPostgresDB(t)
	store, err := NewPostgresStore(db, WithPostgresTable("lifecycle_err_test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Acquire to error against a closed DB")
	}
	if err := store.Reset(ctx, "k"); err == nil {
		t.Fatal("expected Reset to error against a closed DB")
	}
	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Refresh to error against a closed DB")
	}
}

func TestPostgresStore_LeaseExpiry(t *testing.T) {
	store := newPostgresTestStore(t)
	ctx := context.Background()
	key := "lease-expiry"

	if _, err := store.Acquire(ctx, key, "pod-a", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over expired lease, got %+v", s)
	}
}

