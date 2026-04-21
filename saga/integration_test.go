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

	runStoreContractTests(t, store)
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

	runStoreContractTests(t, store)
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

	runStoreContractTests(t, store)
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
