package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	eventerrors "github.com/rbaliyan/event/v3/errors"
	"github.com/rbaliyan/event/v3/health"
	"github.com/redis/go-redis/v9"
)

/*
Redis Schema:

Uses Redis Hashes for saga state:
- Hash: saga:{id} - saga state
- Set: saga:by_name:{name} - saga IDs by name
- Set: saga:by_status:{status} - saga IDs by status
- Sorted Set: saga:by_time - saga IDs sorted by start time
*/

// RedisStore is a Redis-based saga store.
//
// RedisStore provides distributed saga state storage using Redis. It supports:
//   - Hash storage for saga state
//   - Set-based indexes for efficient filtering
//   - Optional TTL for automatic cleanup of completed sagas
//   - Multiple saga instances across application nodes
//
// Redis Schema:
//   - saga:{id} - Hash containing saga state fields
//   - saga:by_name:{name} - Set of saga IDs for a given saga name
//   - saga:by_status:{status} - Set of saga IDs in a given status
//   - saga:by_time - Sorted set of saga IDs by start time
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := saga.NewRedisStore(rdb,
//	    saga.WithKeyPrefix("myapp:saga:"),
//	    saga.WithTTL(7 * 24 * time.Hour),
//	)
//
//	orderSaga, _ := saga.New("order-creation", steps, saga.WithStore(store))
type RedisStore struct {
	client       redis.Cmdable
	prefix       string
	namePrefix   string
	statusPrefix string
	timeKey      string
	ttl          time.Duration // TTL for completed sagas (0 = no expiry)
}

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*redisStoreOptions)

type redisStoreOptions struct {
	keyPrefix string
	ttl       time.Duration
}

// WithKeyPrefix sets a custom key prefix for the Redis saga store.
//
// Use this for multi-tenant deployments or to organize keys by application.
//
// Parameters:
//   - prefix: The key prefix (e.g., "myapp:saga:")
func WithKeyPrefix(prefix string) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if prefix != "" {
			o.keyPrefix = prefix
		}
	}
}

// WithTTL sets the TTL for completed sagas.
//
// When set, completed and compensated sagas are automatically deleted
// after the TTL expires. This prevents unbounded growth of saga data.
//
// Parameters:
//   - ttl: Time-to-live for completed sagas (0 = no expiry)
//
// Example:
//
//	store := saga.NewRedisStore(rdb,
//	    saga.WithTTL(7 * 24 * time.Hour),
//	)
func WithTTL(ttl time.Duration) RedisStoreOption {
	return func(o *redisStoreOptions) {
		o.ttl = ttl
	}
}

// NewRedisStore creates a new Redis saga store.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - opts: Optional configuration
//
// Default configuration:
//   - Key prefix: "saga:"
//   - TTL: 0 (no expiry)
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := saga.NewRedisStore(rdb)
func NewRedisStore(client redis.Cmdable, opts ...RedisStoreOption) (*RedisStore, error) {
	if client == nil {
		return nil, fmt.Errorf("client must not be nil")
	}

	o := &redisStoreOptions{
		keyPrefix: "saga:",
	}
	for _, opt := range opts {
		opt(o)
	}

	return &RedisStore{
		client:       client,
		prefix:       o.keyPrefix,
		namePrefix:   o.keyPrefix + "by_name:",
		statusPrefix: o.keyPrefix + "by_status:",
		timeKey:      o.keyPrefix + "by_time",
		ttl:          o.ttl,
	}, nil
}

// Create creates a new saga instance
func (s *RedisStore) Create(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	key := s.prefix + state.ID

	// Atomic existence check using HSetNX on the id field
	ok, err := s.client.HSetNX(ctx, key, "id", state.ID).Result()
	if err != nil {
		return fmt.Errorf("hsetnx: %w", err)
	}
	if !ok {
		return fmt.Errorf("saga already exists: %s", state.ID)
	}

	// Save remaining state fields
	if err := s.saveState(ctx, key, state); err != nil {
		return err
	}

	// Add to indexes
	if err := s.client.SAdd(ctx, s.namePrefix+state.Name, state.ID).Err(); err != nil {
		return fmt.Errorf("index by name: %w", err)
	}
	if err := s.client.SAdd(ctx, s.statusPrefix+string(state.Status), state.ID).Err(); err != nil {
		return fmt.Errorf("index by status: %w", err)
	}
	if err := s.client.ZAdd(ctx, s.timeKey, redis.Z{
		Score:  float64(state.StartedAt.Unix()),
		Member: state.ID,
	}).Err(); err != nil {
		return fmt.Errorf("index by time: %w", err)
	}

	return nil
}

// saveState saves saga state to Redis hash
func (s *RedisStore) saveState(ctx context.Context, key string, state *State) error {
	completedSteps, err := json.Marshal(state.CompletedSteps)
	if err != nil {
		return fmt.Errorf("marshal completed_steps: %w", err)
	}
	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	fields := map[string]any{
		"id":              state.ID,
		"name":            state.Name,
		"status":          string(state.Status),
		"current_step":    state.CurrentStep,
		"completed_steps": completedSteps,
		"data":            data,
		"error":           state.Error,
		"started_at":      state.StartedAt.Unix(),
		"last_updated_at": state.LastUpdatedAt.Unix(),
		"version":         state.Version,
	}

	if state.CompletedAt != nil {
		fields["completed_at"] = state.CompletedAt.Unix()
	}

	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	return nil
}

// Get retrieves saga state by ID
func (s *RedisStore) Get(ctx context.Context, id string) (*State, error) {
	key := s.prefix + id

	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}

	if len(fields) == 0 {
		return nil, eventerrors.NewNotFoundError("saga", id)
	}

	return s.parseState(fields)
}

// parseState converts hash fields to State
func (s *RedisStore) parseState(fields map[string]string) (*State, error) {
	state := &State{
		ID:     fields["id"],
		Name:   fields["name"],
		Status: Status(fields["status"]),
		Error:  fields["error"],
	}

	if cs := fields["current_step"]; cs != "" {
		var err error
		state.CurrentStep, err = strconv.Atoi(cs)
		if err != nil {
			return nil, fmt.Errorf("parse current_step: %w", err)
		}
	}

	if steps := fields["completed_steps"]; steps != "" {
		if err := json.Unmarshal([]byte(steps), &state.CompletedSteps); err != nil {
			return nil, fmt.Errorf("unmarshal completed_steps: %w", err)
		}
	}

	if data := fields["data"]; data != "" {
		if err := json.Unmarshal([]byte(data), &state.Data); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
	}

	if ts := fields["started_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse started_at: %w", err)
		}
		state.StartedAt = time.Unix(unix, 0)
	}

	if ts := fields["completed_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse completed_at: %w", err)
		}
		t := time.Unix(unix, 0)
		state.CompletedAt = &t
	}

	if ts := fields["last_updated_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse last_updated_at: %w", err)
		}
		state.LastUpdatedAt = time.Unix(unix, 0)
	}

	if v := fields["version"]; v != "" {
		version, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse version: %w", err)
		}
		state.Version = version
	}

	return state, nil
}

// updateScript is a Lua script for atomic saga state update.
//
// It atomically: reads old status, checks version, updates all hash fields,
// updates status indexes (SRem old + SAdd new), and optionally sets TTL.
// This eliminates the TOCTOU race between reading the old status and updating indexes.
//
// KEYS[1] = saga hash key
// KEYS[2] = old status index key (prefix only, old status appended by script)
// KEYS[3] = new status index key
//
// ARGV[1]  = expected version
// ARGV[2]  = new version
// ARGV[3]  = new status string
// ARGV[4]  = saga ID (for index membership)
// ARGV[5]  = status prefix (for building old status index key)
// ARGV[6]  = TTL in seconds (0 = no expiry)
// ARGV[7..N] = field, value pairs for HSET
//
// Returns: 0 = success, 1 = version conflict, 2 = not found
var updateScript = redis.NewScript(`
local key = KEYS[1]
local expected_version = tonumber(ARGV[1])
local new_version = tonumber(ARGV[2])
local new_status = ARGV[3]
local saga_id = ARGV[4]
local status_prefix = ARGV[5]
local ttl = tonumber(ARGV[6])

-- Check if key exists and read current version + status atomically
local current = redis.call('HMGET', key, 'version', 'status')
if current[1] == false then
    return 2  -- not found
end

-- Check version
if tonumber(current[1]) ~= expected_version then
    return 1  -- version conflict
end

local old_status = current[2]

-- Update all hash fields (passed as field/value pairs starting from ARGV[7])
local field_count = (#ARGV - 6) / 2
if field_count > 0 then
    local hset_args = {}
    for i = 7, #ARGV, 2 do
        hset_args[#hset_args + 1] = ARGV[i]
        hset_args[#hset_args + 1] = ARGV[i + 1]
    end
    redis.call('HSET', key, unpack(hset_args))
end

-- Update status index if changed
if old_status ~= new_status then
    if old_status and old_status ~= '' then
        redis.call('SREM', status_prefix .. old_status, saga_id)
    end
    redis.call('SADD', status_prefix .. new_status, saga_id)
end

-- Set TTL for completed sagas
if ttl > 0 then
    redis.call('EXPIRE', key, ttl)
end

return 0  -- success
`)

// Update updates saga state with optimistic locking.
//
// The update uses the Version field for optimistic locking. If the version
// in Redis doesn't match the expected version, ErrVersionConflict is returned.
// On successful update, the state's Version is incremented.
//
// The entire operation (version check, state update, and status index update)
// is performed atomically using a Lua script to prevent TOCTOU races.
func (s *RedisStore) Update(ctx context.Context, state *State) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.ID == "" {
		return fmt.Errorf("state ID is required")
	}

	key := s.prefix + state.ID
	newVersion := state.Version + 1

	// Marshal fields for the Lua script
	completedSteps, err := json.Marshal(state.CompletedSteps)
	if err != nil {
		return fmt.Errorf("marshal completed_steps: %w", err)
	}
	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	// Build field/value pairs for HSET
	fieldPairs := []any{
		"id", state.ID,
		"name", state.Name,
		"status", string(state.Status),
		"current_step", state.CurrentStep,
		"completed_steps", completedSteps,
		"data", data,
		"error", state.Error,
		"started_at", state.StartedAt.Unix(),
		"last_updated_at", state.LastUpdatedAt.Unix(),
		"version", newVersion,
	}

	if state.CompletedAt != nil {
		fieldPairs = append(fieldPairs, "completed_at", state.CompletedAt.Unix())
	}

	// Determine TTL
	var ttlSeconds int64
	if s.ttl > 0 && (state.Status == StatusCompleted || state.Status == StatusCompensated) {
		ttlSeconds = int64(s.ttl.Seconds())
	}

	// Build script arguments: expected_version, new_version, new_status, saga_id, status_prefix, ttl, ...field_pairs
	args := make([]any, 0, 6+len(fieldPairs))
	args = append(args, state.Version, newVersion, string(state.Status), state.ID, s.statusPrefix, ttlSeconds)
	args = append(args, fieldPairs...)

	// Run atomic Lua script (only KEYS[1] is needed; status keys are built inside the script)
	result, err := updateScript.Run(ctx, s.client, []string{key}, args...).Int()
	if err != nil {
		return fmt.Errorf("update script: %w", err)
	}

	switch result {
	case 1:
		return ErrVersionConflict
	case 2:
		return eventerrors.NewNotFoundError("saga", state.ID)
	}

	// Update local version on success
	state.Version = newVersion

	return nil
}

// List lists sagas matching the filter
func (s *RedisStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	var ids []string

	if filter.Name != "" && len(filter.Status) > 0 {
		// Intersect name and status sets
		nameKey := s.namePrefix + filter.Name
		statusKeys := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			statusKeys[i] = s.statusPrefix + string(status)
		}

		// Get IDs matching name
		nameIDs, _ := s.client.SMembers(ctx, nameKey).Result()

		// Filter by status
		for _, id := range nameIDs {
			for _, statusKey := range statusKeys {
				isMember, _ := s.client.SIsMember(ctx, statusKey, id).Result()
				if isMember {
					ids = append(ids, id)
					break
				}
			}
		}
	} else if filter.Name != "" {
		ids, _ = s.client.SMembers(ctx, s.namePrefix+filter.Name).Result()
	} else if len(filter.Status) > 0 {
		// Union of status sets
		for _, status := range filter.Status {
			statusIDs, _ := s.client.SMembers(ctx, s.statusPrefix+string(status)).Result()
			ids = append(ids, statusIDs...)
		}
	} else {
		// Get all IDs from sorted set
		ids, _ = s.client.ZRevRange(ctx, s.timeKey, 0, -1).Result()
	}

	// Apply limit
	if filter.Limit > 0 && len(ids) > filter.Limit {
		ids = ids[:filter.Limit]
	}

	// Fetch states
	var states []*State
	for _, id := range ids {
		state, err := s.Get(ctx, id)
		if err != nil {
			continue
		}
		states = append(states, state)
	}

	return states, nil
}

// Delete removes a saga by ID
func (s *RedisStore) Delete(ctx context.Context, id string) error {
	state, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	key := s.prefix + id

	// Remove from all indexes
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("del: %w", err)
	}
	if err := s.client.SRem(ctx, s.namePrefix+state.Name, id).Err(); err != nil {
		return fmt.Errorf("srem name index: %w", err)
	}
	if err := s.client.SRem(ctx, s.statusPrefix+string(state.Status), id).Err(); err != nil {
		return fmt.Errorf("srem status index: %w", err)
	}
	if err := s.client.ZRem(ctx, s.timeKey, id).Err(); err != nil {
		return fmt.Errorf("zrem time index: %w", err)
	}

	return nil
}

// DeleteOlderThan removes sagas older than the specified age
func (s *RedisStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := float64(time.Now().Add(-age).Unix())

	// Get old saga IDs
	ids, err := s.client.ZRangeByScore(ctx, s.timeKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", cutoff),
	}).Result()

	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}

	var deleted int64
	for _, id := range ids {
		if err := s.Delete(ctx, id); err == nil {
			deleted++
		}
	}

	return deleted, nil
}

// GetFailed returns all failed sagas
func (s *RedisStore) GetFailed(ctx context.Context, name string, limit int) ([]*State, error) {
	filter := StoreFilter{
		Status: []Status{StatusFailed},
		Limit:  limit,
	}
	if name != "" {
		filter.Name = name
	}
	return s.List(ctx, filter)
}

// GetPending returns all pending/running sagas
func (s *RedisStore) GetPending(ctx context.Context, limit int) ([]*State, error) {
	return s.List(ctx, StoreFilter{
		Status: []Status{StatusPending, StatusRunning, StatusCompensating},
		Limit:  limit,
	})
}

// Count returns the total number of sagas
func (s *RedisStore) Count(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.timeKey).Result()
}

// CountByStatus returns the count of sagas by status
func (s *RedisStore) CountByStatus(ctx context.Context, status Status) (int64, error) {
	return s.client.SCard(ctx, s.statusPrefix+string(status)).Result()
}

// CountByName returns the count of sagas by name
func (s *RedisStore) CountByName(ctx context.Context, name string) (int64, error) {
	return s.client.SCard(ctx, s.namePrefix+name).Result()
}

// Health performs a health check on the Redis saga store.
func (s *RedisStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping Redis
	if err := s.client.Ping(ctx).Err(); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("redis ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	// Count total sagas
	count, err := s.Count(ctx)
	if err != nil {
		return &health.Result{
			Status:    health.StatusDegraded,
			Message:   fmt.Sprintf("failed to count sagas: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	// Count pending/running sagas
	pending, _ := s.CountByStatus(ctx, StatusPending)
	running, _ := s.CountByStatus(ctx, StatusRunning)
	compensating, _ := s.CountByStatus(ctx, StatusCompensating)

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"total_sagas":        count,
			"pending_sagas":      pending,
			"running_sagas":      running,
			"compensating_sagas": compensating,
		},
	}
}

// Compile-time checks
var (
	_ Store          = (*RedisStore)(nil)
	_ health.Checker = (*RedisStore)(nil)
)
