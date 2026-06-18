package lifecycle

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// getMongoClient returns a live client, or skips the test when MONGO_URI is
// unset. Runs in the normal build: skips instantly with no env (fast default
// CI job), runs against a container when MONGO_URI is set and counts toward
// coverage. A short server-selection timeout keeps the skip fast if the URI
// points at an unreachable server.
func getMongoClient(t *testing.T) *mongo.Client {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set; skipping MongoDB integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

func newMongoIntegrationStore(t *testing.T) *MongoStore {
	t.Helper()
	client := getMongoClient(t)
	dbName := fmt.Sprintf("lifecycle_test_%d", time.Now().UnixNano())
	db := client.Database(dbName)
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	store, err := NewMongoStore(db)
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	return store
}

func TestMongoStore_Contract(t *testing.T) {
	store := newMongoIntegrationStore(t)
	runStoreContractTests(t, store)
}

func TestMongoStore_Health(t *testing.T) {
	store := newMongoIntegrationStore(t)
	ctx := context.Background()

	if _, err := store.Acquire(ctx, "h-run", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "h-done", "pod-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, "h-done", "pod-a"); err != nil {
		t.Fatal(err)
	}

	res := store.Health(ctx)
	if string(res.Status) != "healthy" {
		t.Fatalf("expected healthy, got %q (msg=%q)", res.Status, res.Message)
	}
	for _, k := range []string{"running", "completed", "failed", "collection"} {
		if _, ok := res.Details[k]; !ok {
			t.Fatalf("expected detail %q, got %v", k, res.Details)
		}
	}
}

// TestMongoStore_ErrorsWhenDisconnected injects a backend failure: after the
// client disconnects, operations must surface a wrapped error.
func TestMongoStore_ErrorsWhenDisconnected(t *testing.T) {
	client := getMongoClient(t)
	db := client.Database(fmt.Sprintf("lifecycle_err_%d", time.Now().UnixNano()))
	store, err := NewMongoStore(db)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Disconnect(context.Background())
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Acquire to error after disconnect")
	}
	if err := store.Reset(ctx, "k"); err == nil {
		t.Fatal("expected Reset to error after disconnect")
	}
	if err := store.Refresh(ctx, "k", "pod-a", time.Minute); err == nil {
		t.Fatal("expected Refresh to error after disconnect")
	}
	if err := store.Complete(ctx, "k", "pod-a"); err == nil {
		t.Fatal("expected Complete to error after disconnect")
	}
	if err := store.Fail(ctx, "k", "pod-a", "x", false); err == nil {
		t.Fatal("expected Fail to error after disconnect")
	}
}

func TestMongoStore_LeaseExpiry(t *testing.T) {
	store := newMongoIntegrationStore(t)
	ctx := context.Background()
	key := "lease-expiry"

	if _, err := store.Acquire(ctx, key, "pod-a", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	s, err := store.Acquire(ctx, key, "pod-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Holder != "pod-b" {
		t.Fatalf("expected pod-b to take over expired lease, got %+v", s)
	}
}
