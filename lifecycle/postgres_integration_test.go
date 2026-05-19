//go:build integration

package lifecycle

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func getPostgresDB(t *testing.T) *sql.DB {
	t.Helper()
	uri := os.Getenv("POSTGRES_URI")
	if uri == "" {
		uri = "postgres://localhost:5432/test?sslmode=disable"
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

