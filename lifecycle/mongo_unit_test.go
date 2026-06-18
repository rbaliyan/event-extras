package lifecycle

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// offlineMongoStore builds a MongoStore from a lazily-connected client. The
// driver does not dial until an operation runs, so the store and its pure
// helpers (toStatus, Collection, constructor) are testable with no server.
func offlineMongoStore(t *testing.T, opts ...MongoOption) *MongoStore {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	store, err := NewMongoStore(client.Database("lifecycle_unit"), opts...)
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	return store
}

func TestNewMongoStore_Guards(t *testing.T) {
	t.Parallel()
	if _, err := NewMongoStore(nil); err == nil {
		t.Fatal("expected error for nil db")
	}
	store := offlineMongoStore(t, WithMongoCollection("custom_hooks"))
	if got := store.Collection().Name(); got != "custom_hooks" {
		t.Fatalf("WithMongoCollection not applied, got %q", got)
	}
	// Empty collection name falls back to the default.
	def := offlineMongoStore(t, WithMongoCollection(""))
	if got := def.Collection().Name(); got != "lifecycle_hooks" {
		t.Fatalf("expected default collection, got %q", got)
	}
}

func TestMongoHook_ToStatus(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name string
		hook mongoHook
		want Status
	}{
		{
			name: "running_with_lease",
			hook: mongoHook{ID: "k", State: StateRunning, Holder: "pod-a", LeaseUntil: &now},
			want: Status{State: StateRunning, Holder: "pod-a", LeaseUntil: now},
		},
		{
			name: "completed_nil_pointers",
			hook: mongoHook{ID: "k", State: StateCompleted, Holder: "pod-a"},
			want: Status{State: StateCompleted, Holder: "pod-a"},
		},
		{
			name: "failed_with_error_and_time",
			hook: mongoHook{ID: "k", State: StateFailed, Holder: "pod-a", FailedAt: &now, Error: "boom"},
			want: Status{State: StateFailed, Holder: "pod-a", FailedAt: now, Error: "boom"},
		},
		{
			name: "completed_with_time",
			hook: mongoHook{ID: "k", State: StateCompleted, CompletedAt: &now},
			want: Status{State: StateCompleted, CompletedAt: now},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.hook.toStatus()
			if got != tc.want {
				t.Fatalf("toStatus() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestMongoStore_Health_Unhealthy(t *testing.T) {
	t.Parallel()
	// Point at an unreachable port with a short server-selection timeout so the
	// ping fails fast — no server required to exercise the unhealthy branch.
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(500 * time.Millisecond))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	store, err := NewMongoStore(client.Database("lifecycle_unit"))
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := store.Health(ctx)
	if string(res.Status) != "unhealthy" {
		t.Fatalf("expected unhealthy against unreachable mongo, got %q", res.Status)
	}
	if res.Message == "" {
		t.Fatal("expected a message on unhealthy result")
	}
}

// TestMongoStore_Health_Degraded exercises the degraded branch (ping ok but a
// count fails) via the injectable countByState seam. The ping still hits the
// real client, so a reachable MongoDB is required; skipped otherwise.
func TestMongoStore_Health_Degraded(t *testing.T) {
	t.Parallel()
	store := mongoStoreOrSkip(t)
	store.countByState = func(context.Context, State) (int64, error) {
		return 0, errors.New("count blew up")
	}
	res := store.Health(context.Background())
	if string(res.Status) != "degraded" {
		t.Fatalf("expected degraded, got %q (%s)", res.Status, res.Message)
	}
	if res.Message == "" {
		t.Fatal("expected a message on degraded result")
	}
}

// mongoStoreOrSkip returns a MongoStore backed by a reachable MongoDB, or skips
// the test. Uses MONGO_URI when set (the container-backed environment), else a
// localhost default.
func mongoStoreOrSkip(t *testing.T) *MongoStore {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		t.Skipf("MongoDB not available: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	store, err := NewMongoStore(client.Database("lifecycle_unit_health"))
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	return store
}
