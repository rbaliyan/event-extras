package saga

import "context"

// Deleter is an optional capability implemented by stores that support
// deleting a saga by ID.
//
// Not every store supports deletion — for example, an append-only audit store
// may refuse to drop history. Callers should type-assert on this interface
// and degrade gracefully when it is not satisfied.
//
// Delete must return an error satisfying IsNotFound when the saga does not
// exist, so callers can distinguish missing-vs-failed deletion.
//
// MemoryStore, RedisStore, MongoStore, and PostgresStore all satisfy Deleter.
type Deleter interface {
	Delete(ctx context.Context, id string) error
}

// Aggregator is an optional capability implemented by stores that can report
// counts by status without materializing every saga.
//
// Real backends (Mongo, Redis, Postgres) can answer this with a single
// aggregation/group-by query; implementing Aggregator avoids an unbounded
// List when computing summary statistics.
//
// Callers should type-assert on this interface and fall back to a capped
// List when it is not satisfied.
//
// The returned map is keyed by Status; absent keys are implicitly zero.
type Aggregator interface {
	CountByStatus(ctx context.Context) (map[Status]int, error)
}
