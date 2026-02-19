//go:build integration

package saga

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// getMongoClient creates a MongoDB client for integration tests.
// Set MONGO_URI environment variable to override the default connection string.
func getMongoClient(t *testing.T) *mongo.Client {
	t.Helper()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}

	t.Cleanup(func() {
		client.Disconnect(context.Background())
	})

	return client
}

// getPostgresDB creates a PostgreSQL connection for integration tests.
// Set POSTGRES_URI environment variable to override the default connection string.
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

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

// getRedisClient creates a Redis client for integration tests.
// Set REDIS_ADDR environment variable to override the default address.
func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis not available: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

func TestMongoStoreIntegration(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	// Use a unique database for this test
	dbName := "saga_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	t.Cleanup(func() {
		db.Drop(context.Background())
	})

	store, err := NewMongoStore(db, WithCollection("saga_states"))
	if err != nil {
		t.Fatalf("NewMongoStore failed: %v", err)
	}
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes failed: %v", err)
	}

	runStoreTests(t, store)
	runMongoSpecificTests(t, store)
}

func TestPostgresStoreIntegration(t *testing.T) {
	db := getPostgresDB(t)
	ctx := context.Background()

	// Use a unique table for this test
	tableName := "saga_test_" + time.Now().Format("20060102150405")

	store, err := NewPostgresStore(db, WithTable(tableName))
	if err != nil {
		t.Fatalf("NewPostgresStore failed: %v", err)
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
		)
	`, tableName)

	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS " + tableName)
	})

	runStoreTests(t, store)
}

func TestRedisStoreIntegration(t *testing.T) {
	client := getRedisClient(t)
	ctx := context.Background()

	// Use a unique prefix for this test
	prefix := "saga:test:" + time.Now().Format("20060102150405") + ":"

	store, err := NewRedisStore(client, WithKeyPrefix(prefix))
	if err != nil {
		t.Fatalf("NewRedisStore failed: %v", err)
	}

	t.Cleanup(func() {
		// Clean up test keys
		iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
	})

	runStoreTests(t, store)
}

// runStoreTests runs common store tests against any Store implementation
func runStoreTests(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("Create and Get", func(t *testing.T) {
		now := time.Now()
		state := &State{
			ID:             "saga-int-1",
			Name:           "order-creation",
			Status:         StatusPending,
			CurrentStep:    0,
			CompletedSteps: []string{},
			Data:           map[string]any{"order_id": "123"},
			StartedAt:      now,
			LastUpdatedAt:  now,
			Version:        0,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		retrieved, err := store.Get(ctx, "saga-int-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.ID != "saga-int-1" {
			t.Errorf("expected ID saga-int-1, got %s", retrieved.ID)
		}
		if retrieved.Name != "order-creation" {
			t.Errorf("expected name order-creation, got %s", retrieved.Name)
		}
		if retrieved.Status != StatusPending {
			t.Errorf("expected status pending, got %s", retrieved.Status)
		}
	})

	t.Run("Create duplicate returns error", func(t *testing.T) {
		now := time.Now()
		state := &State{
			ID:            "saga-int-duplicate",
			Name:          "test-saga",
			Status:        StatusPending,
			StartedAt:     now,
			LastUpdatedAt: now,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("First Create failed: %v", err)
		}

		err = store.Create(ctx, state)
		if err == nil {
			t.Error("expected error when creating duplicate saga")
		}
	})

	t.Run("Get non-existent returns error", func(t *testing.T) {
		_, err := store.Get(ctx, "non-existent-saga-id")
		if err == nil {
			t.Error("expected error for non-existent saga")
		}
	})

	t.Run("Update", func(t *testing.T) {
		now := time.Now()
		state := &State{
			ID:             "saga-int-update",
			Name:           "update-test",
			Status:         StatusPending,
			CurrentStep:    0,
			CompletedSteps: []string{},
			StartedAt:      now,
			LastUpdatedAt:  now,
			Version:        0,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Update the state
		state.Status = StatusRunning
		state.CurrentStep = 1
		state.CompletedSteps = []string{"step-1"}
		state.LastUpdatedAt = time.Now()

		err = store.Update(ctx, state)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Verify update
		retrieved, _ := store.Get(ctx, "saga-int-update")
		if retrieved.Status != StatusRunning {
			t.Errorf("expected status running, got %s", retrieved.Status)
		}
		if retrieved.CurrentStep != 1 {
			t.Errorf("expected current step 1, got %d", retrieved.CurrentStep)
		}
		if len(retrieved.CompletedSteps) != 1 || retrieved.CompletedSteps[0] != "step-1" {
			t.Errorf("unexpected completed steps: %v", retrieved.CompletedSteps)
		}
	})

	t.Run("Update with version conflict", func(t *testing.T) {
		now := time.Now()
		state := &State{
			ID:            "saga-int-conflict",
			Name:          "conflict-test",
			Status:        StatusPending,
			StartedAt:     now,
			LastUpdatedAt: now,
			Version:       0,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Update once to increment version
		state.Status = StatusRunning
		state.LastUpdatedAt = time.Now()
		err = store.Update(ctx, state)
		if err != nil {
			t.Fatalf("First Update failed: %v", err)
		}

		// Now state.Version should be 1, try to update with stale version
		staleState := &State{
			ID:            "saga-int-conflict",
			Name:          "conflict-test",
			Status:        StatusCompleted,
			LastUpdatedAt: time.Now(),
			Version:       0, // Stale version
		}

		err = store.Update(ctx, staleState)
		if err == nil {
			t.Error("expected version conflict error")
		}
	})

	t.Run("List with filters", func(t *testing.T) {
		now := time.Now()
		// Create multiple sagas
		store.Create(ctx, &State{ID: "saga-list-1", Name: "order-saga", Status: StatusCompleted, StartedAt: now, LastUpdatedAt: now})
		store.Create(ctx, &State{ID: "saga-list-2", Name: "order-saga", Status: StatusFailed, StartedAt: now, LastUpdatedAt: now})
		store.Create(ctx, &State{ID: "saga-list-3", Name: "payment-saga", Status: StatusCompleted, StartedAt: now, LastUpdatedAt: now})

		// Filter by name
		results, err := store.List(ctx, StoreFilter{Name: "order-saga"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		count := 0
		for _, s := range results {
			if s.Name == "order-saga" {
				count++
			}
		}
		if count < 2 {
			t.Errorf("expected at least 2 order-saga results, got %d", count)
		}

		// Filter by status
		results, err = store.List(ctx, StoreFilter{Status: []Status{StatusFailed}})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		for _, s := range results {
			if s.Status != StatusFailed {
				t.Errorf("expected status failed, got %s", s.Status)
			}
		}
	})

	t.Run("List with limit", func(t *testing.T) {
		results, err := store.List(ctx, StoreFilter{Limit: 2})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) > 2 {
			t.Errorf("expected at most 2 results, got %d", len(results))
		}
	})
}

// runMongoSpecificTests runs MongoDB-specific tests
func runMongoSpecificTests(t *testing.T, store *MongoStore) {
	ctx := context.Background()

	t.Run("GetStats", func(t *testing.T) {
		stats, err := store.GetStats(ctx)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}

		if stats.Total < 1 {
			t.Errorf("expected at least 1 total saga, got %d", stats.Total)
		}
		if stats.ByStatus == nil {
			t.Error("expected ByStatus to be initialized")
		}
		if stats.ByName == nil {
			t.Error("expected ByName to be initialized")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		now := time.Now()
		state := &State{
			ID:            "saga-mongo-delete",
			Name:          "delete-test",
			Status:        StatusPending,
			StartedAt:     now,
			LastUpdatedAt: now,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = store.Delete(ctx, "saga-mongo-delete")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = store.Get(ctx, "saga-mongo-delete")
		if err == nil {
			t.Error("expected error after delete")
		}
	})

	t.Run("Count", func(t *testing.T) {
		count, err := store.Count(ctx, StatusCompleted)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		// We created at least one completed saga in the List tests
		if count < 1 {
			t.Logf("count of completed sagas: %d", count)
		}
	})

	t.Run("Health", func(t *testing.T) {
		result := store.Health(ctx)
		if result.Status != "healthy" {
			t.Errorf("expected healthy status, got %s: %s", result.Status, result.Message)
		}
	})
}
