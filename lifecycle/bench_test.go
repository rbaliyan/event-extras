package lifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Canonical invocation (variance-aware, for benchstat comparison):
//
//	go test -run '^$' -bench=. -benchmem -count=6 ./lifecycle/ | tee new.txt
//	benchstat old.txt new.txt
//
// The Postgres/Mongo subtests of BenchmarkStore_Acquire run only when
// POSTGRES_URI / MONGO_URI are set (see `just backends-up`); otherwise they skip.

// benchDiscardLogger silences hook lifecycle logs so they don't pollute
// benchmark output or skew timings.
var benchDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newRedisBenchStore(b *testing.B) *RedisStore {
	b.Helper()
	mr := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

// BenchmarkMemoryStore_Acquire measures each distinct branch of the Acquire
// state machine in isolation. Acquire is on the hot path: every pod in a fleet
// calls it on startup, so its contended and take-over branches matter most.
func BenchmarkMemoryStore_Acquire(b *testing.B) {
	ctx := context.Background()

	b.Run("fresh_insert", func(b *testing.B) {
		store := NewMemoryStore()
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			_ = store.Reset(ctx, "k")
			b.StartTimer()
			_, _ = store.Acquire(ctx, "k", "pod-a", time.Minute)
		}
	})

	b.Run("same_holder_refresh", func(b *testing.B) {
		store := NewMemoryStore()
		if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			_, _ = store.Acquire(ctx, "k", "pod-a", time.Minute)
		}
	})

	b.Run("contended_observe", func(b *testing.B) {
		store := NewMemoryStore()
		if _, err := store.Acquire(ctx, "k", "pod-a", time.Hour); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			// pod-b repeatedly observes pod-a's still-valid claim.
			_, _ = store.Acquire(ctx, "k", "pod-b", time.Minute)
		}
	})

	b.Run("expired_takeover", func(b *testing.B) {
		store := NewMemoryStore()
		base := time.Now()
		store.now = func() time.Time { return base }
		if _, err := store.Acquire(ctx, "k", "pod-a", time.Nanosecond); err != nil {
			b.Fatal(err)
		}
		// Freeze the clock past every lease so each Acquire takes over.
		store.now = func() time.Time { return base.Add(time.Hour) }
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			holder := "pod-a"
			if i%2 == 0 {
				holder = "pod-b"
			}
			i++
			_, _ = store.Acquire(ctx, "k", holder, time.Nanosecond)
		}
	})
}

// BenchmarkMemoryStore_Refresh measures the lease-extension path that the
// auto-refresh goroutine drives for the entire duration of a hook body.
func BenchmarkMemoryStore_Refresh(b *testing.B) {
	store := NewMemoryStore()
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Hour); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := store.Refresh(ctx, "k", "pod-a", time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryStore_AcquireParallel models the real rollout shape: many pods
// hammering the same hook key concurrently, where one wins and the rest observe
// contention. Measures lock contention on the shared store.
func BenchmarkMemoryStore_AcquireParallel(b *testing.B) {
	store := NewMemoryStore()
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "winner", time.Hour); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = store.Acquire(ctx, "k", "challenger", time.Minute)
		}
	})
}

// BenchmarkOnce_Winner measures the full uncontended Once path
// (Acquire -> run body -> Complete) including the refresh goroutine setup and
// teardown. The store is reset with the timer paused so only Once is measured.
func BenchmarkOnce_Winner(b *testing.B) {
	store := NewMemoryStore()
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}
	noop := func(context.Context) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		_ = store.Reset(ctx, hook.Key())
		b.StartTimer()
		if _, err := Once(ctx, store, hook, noop, WithInstanceID("pod-a"), WithLogger(benchDiscardLogger)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOnce_SkippedRunning measures the common fleet case: a pod that loses
// the race and observes another instance holding the claim. At rollout there is
// one winner and N-1 of these, so this is the hotter of the two Once paths.
func BenchmarkOnce_SkippedRunning(b *testing.B) {
	store := NewMemoryStore()
	ctx := context.Background()
	hook := Hook{Name: "migrate", Version: "v1"}
	if _, err := store.Acquire(ctx, hook.Key(), "other-pod", time.Hour); err != nil {
		b.Fatal(err)
	}
	noop := func(context.Context) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		out, _ := Once(ctx, store, hook, noop, WithInstanceID("pod-a"), WithLogger(benchDiscardLogger))
		if out != OutcomeSkippedRunning {
			b.Fatalf("expected OutcomeSkippedRunning, got %v", out)
		}
	}
}

// BenchmarkStore_Acquire compares the same-holder Acquire cost across every
// backend so benchstat can render one comparative table. Memory and Redis
// (miniredis) always run; Postgres and Mongo run only when their URI env vars
// are set (the integration environment).
func BenchmarkStore_Acquire(b *testing.B) {
	ctx := context.Background()
	run := func(b *testing.B, store Store) {
		if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			_, _ = store.Acquire(ctx, "k", "pod-a", time.Minute)
		}
	}

	// For an apples-to-apples networked comparison, the redis row uses a LIVE
	// server (REDIS_ADDR) like the postgres/mongo rows; the in-process miniredis
	// number lives in BenchmarkRedisStore_Acquire.
	b.Run("memory", func(b *testing.B) { run(b, NewMemoryStore()) })
	b.Run("redis", func(b *testing.B) { run(b, newLiveRedisBenchStore(b)) })
	b.Run("postgres", func(b *testing.B) { run(b, newPostgresBenchStore(b)) })
	b.Run("mongo", func(b *testing.B) { run(b, newMongoBenchStore(b)) })
}

func newLiveRedisBenchStore(b *testing.B) *RedisStore {
	b.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR not set; skipping live Redis benchmark")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Skipf("redis not reachable: %v", err)
	}
	prefix := fmt.Sprintf("lifecycle:bench:%d:", b.N)
	b.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
		_ = client.Close()
	})
	store, err := NewRedisStore(client, WithRedisPrefix(prefix))
	if err != nil {
		b.Fatal(err)
	}
	return store
}

func newPostgresBenchStore(b *testing.B) *PostgresStore {
	b.Helper()
	uri := os.Getenv("POSTGRES_URI")
	if uri == "" {
		b.Skip("POSTGRES_URI not set; skipping Postgres benchmark")
	}
	db, err := sql.Open("postgres", uri)
	if err != nil {
		b.Skipf("postgres open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	table := "lifecycle_bench"
	store, err := NewPostgresStore(db, WithPostgresTable(table))
	if err != nil {
		b.Fatal(err)
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		b.Skipf("ensure schema: %v", err)
	}
	b.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	return store
}

func newMongoBenchStore(b *testing.B) *MongoStore {
	b.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		b.Skip("MONGO_URI not set; skipping Mongo benchmark")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		b.Skipf("mongo connect: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		b.Skipf("mongo ping: %v", err)
	}
	db := client.Database("lifecycle_bench")
	b.Cleanup(func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	store, err := NewMongoStore(db)
	if err != nil {
		b.Fatal(err)
	}
	return store
}

// BenchmarkRedisStore_Acquire measures the Redis Lua round-trip via miniredis
// (no Docker). This is the cost a real deployment pays per pod for the
// distributed backend, modulo network latency.
func BenchmarkRedisStore_Acquire(b *testing.B) {
	store := newRedisBenchStore(b)
	ctx := context.Background()
	if _, err := store.Acquire(ctx, "k", "pod-a", time.Minute); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = store.Acquire(ctx, "k", "pod-a", time.Minute)
	}
}
