package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoStore is a MongoDB-backed Store. State transitions are made atomic
// via conditional findOneAndUpdate / updateOne operations against a single
// document per hook keyed by _id, so the implementation is safe against
// concurrent callers across multiple pods.
//
// Document shape:
//
//	{
//	  _id:          "<name>@<version>",
//	  state:        "running" | "completed" | "failed",
//	  holder:       "<instance-id>",
//	  lease_until:  ISODate,
//	  completed_at: ISODate,
//	  failed_at:    ISODate,
//	  error:        "<message>"
//	}
type MongoStore struct {
	collection *mongo.Collection
	// countByState counts documents in the given state. It defaults to a
	// CountDocuments call and is only overridden in tests to exercise the
	// Health degraded branch (mirrors MemoryStore's injectable clock).
	countByState func(ctx context.Context, state State) (int64, error)
}

// MongoOption configures a MongoStore.
type MongoOption func(*mongoOptions)

type mongoOptions struct {
	collection string
}

// WithMongoCollection sets a custom collection name.
// Default: "lifecycle_hooks".
func WithMongoCollection(name string) MongoOption {
	return func(o *mongoOptions) {
		if name != "" {
			o.collection = name
		}
	}
}

// NewMongoStore creates a MongoDB-backed Store. db must be non-nil.
func NewMongoStore(db *mongo.Database, opts ...MongoOption) (*MongoStore, error) {
	if db == nil {
		return nil, errors.New("lifecycle: db is required")
	}
	o := &mongoOptions{collection: "lifecycle_hooks"}
	for _, opt := range opts {
		opt(o)
	}
	s := &MongoStore{collection: db.Collection(o.collection)}
	s.countByState = func(ctx context.Context, state State) (int64, error) {
		return s.collection.CountDocuments(ctx, bson.M{"state": state})
	}
	return s, nil
}

// Collection returns the underlying MongoDB collection. Exposed so that
// integrators can drop their own indexes, attach change streams, or run
// ad-hoc administrative queries. No setup is required for correctness:
// the unique _id index is created by MongoDB automatically and is the
// only index Acquire/Refresh/Complete/Fail need.
func (s *MongoStore) Collection() *mongo.Collection { return s.collection }

// mongoHook is the on-the-wire representation.
type mongoHook struct {
	ID          string     `bson:"_id"`
	State       State      `bson:"state"`
	Holder      string     `bson:"holder,omitempty"`
	LeaseUntil  *time.Time `bson:"lease_until,omitempty"`
	CompletedAt *time.Time `bson:"completed_at,omitempty"`
	FailedAt    *time.Time `bson:"failed_at,omitempty"`
	Error       string     `bson:"error,omitempty"`
}

func (h *mongoHook) toStatus() Status {
	st := Status{
		State:  h.State,
		Holder: h.Holder,
		Error:  h.Error,
	}
	if h.LeaseUntil != nil {
		st.LeaseUntil = *h.LeaseUntil
	}
	if h.CompletedAt != nil {
		st.CompletedAt = *h.CompletedAt
	}
	if h.FailedAt != nil {
		st.FailedAt = *h.FailedAt
	}
	return st
}

// Acquire implements Store.
//
// Implementation: a single findOneAndUpdate with upsert=true. The filter
// matches the two running-claimable cases — same holder, or running with
// expired lease — and upsert=true handles the "no document" case by
// inserting a fresh running row from the $set fields. When a document
// exists but the filter doesn't match (terminal state, or another holder
// still has a valid lease), the upsert attempt collides on _id and
// returns a duplicate-key error; we recover by reading the current state.
func (s *MongoStore) Acquire(ctx context.Context, key, instanceID string, lease time.Duration) (Status, error) {
	now := time.Now()
	leaseUntil := now.Add(lease)

	filter := bson.M{
		"_id": key,
		"$or": []bson.M{
			{"state": StateRunning, "holder": instanceID},
			{"state": StateRunning, "lease_until": bson.M{"$lte": now}},
		},
	}
	update := bson.M{
		"$set": bson.M{
			"state":       StateRunning,
			"holder":      instanceID,
			"lease_until": leaseUntil,
		},
		"$unset": bson.M{
			"completed_at": "",
			"failed_at":    "",
			"error":        "",
		},
	}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var doc mongoHook
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err == nil {
		return doc.toStatus(), nil
	}
	if mongo.IsDuplicateKeyError(err) {
		// Doc exists with a non-matching state (terminal, or held by another
		// instance with a valid lease) — read and return current state.
		return s.Get(ctx, key)
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		// Should not happen with upsert=true, but fall back to a fresh read.
		return s.Get(ctx, key)
	}
	return Status{}, fmt.Errorf("acquire: %w", err)
}

// Refresh implements Store.
func (s *MongoStore) Refresh(ctx context.Context, key, instanceID string, lease time.Duration) error {
	now := time.Now()
	filter := bson.M{
		"_id":         key,
		"state":       StateRunning,
		"holder":      instanceID,
		"lease_until": bson.M{"$gt": now},
	}
	update := bson.M{"$set": bson.M{"lease_until": now.Add(lease)}}
	res, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete implements Store.
func (s *MongoStore) Complete(ctx context.Context, key, instanceID string) error {
	filter := bson.M{"_id": key, "state": StateRunning, "holder": instanceID}
	update := bson.M{
		"$set":   bson.M{"state": StateCompleted, "completed_at": time.Now()},
		"$unset": bson.M{"lease_until": ""},
	}
	res, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail implements Store.
func (s *MongoStore) Fail(ctx context.Context, key, instanceID, errMsg string, retryable bool) error {
	filter := bson.M{"_id": key, "state": StateRunning, "holder": instanceID}

	if retryable {
		res, err := s.collection.DeleteOne(ctx, filter)
		if err != nil {
			return fmt.Errorf("fail (retryable): %w", err)
		}
		if res.DeletedCount == 0 {
			return ErrLeaseLost
		}
		return nil
	}

	update := bson.M{
		"$set": bson.M{
			"state":     StateFailed,
			"failed_at": time.Now(),
			"error":     errMsg,
		},
		"$unset": bson.M{"lease_until": ""},
	}
	res, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("fail: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Get implements Store.
func (s *MongoStore) Get(ctx context.Context, key string) (Status, error) {
	var doc mongoHook
	err := s.collection.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Status{State: StatePending}, nil
	}
	if err != nil {
		return Status{}, fmt.Errorf("get: %w", err)
	}
	return doc.toStatus(), nil
}

// Reset implements Store.
func (s *MongoStore) Reset(ctx context.Context, key string) error {
	if _, err := s.collection.DeleteOne(ctx, bson.M{"_id": key}); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// Health implements health.Checker.
//
// A failed ping reports StatusUnhealthy. If the ping succeeds but the
// per-state document counts cannot be read, it reports StatusDegraded (the
// backend is reachable but the state breakdown is unavailable). Otherwise it
// reports StatusHealthy with running/completed/failed counts in Details.
func (s *MongoStore) Health(ctx context.Context) *health.Result {
	start := time.Now()
	if err := s.collection.Database().Client().Ping(ctx, nil); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("mongodb ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}
	counts := map[string]any{"collection": s.collection.Name()}
	for field, state := range map[string]State{
		"running":   StateRunning,
		"completed": StateCompleted,
		"failed":    StateFailed,
	} {
		n, err := s.countByState(ctx, state)
		if err != nil {
			return &health.Result{
				Status:    health.StatusDegraded,
				Message:   fmt.Sprintf("count by state failed: %v", err),
				Latency:   time.Since(start),
				CheckedAt: start,
			}
		}
		counts[field] = n
	}
	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details:   counts,
	}
}

var (
	_ Store          = (*MongoStore)(nil)
	_ health.Checker = (*MongoStore)(nil)
)
