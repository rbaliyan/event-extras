//go:build integration

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
