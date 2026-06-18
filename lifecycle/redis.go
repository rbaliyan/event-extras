package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/redis/go-redis/v9"
)

// RedisStore is a Redis-backed Store providing fleet-wide coordination.
//
// Each hook is stored as a single Redis HASH whose fields hold the state,
// holder, lease, and terminal-state metadata. All state transitions are
// performed via Lua scripts so they are atomic against concurrent callers.
//
// RedisStore is the recommended production backend when the application
// already uses Redis. PostgresStore and MongoStore offer the same
// state-machine contract with their respective storage layers.
//
// No bootstrap setup is required: the Lua scripts create and update the
// HASH lazily. RedisStore implements health.Checker.
type RedisStore struct {
	client redis.Cmdable
	prefix string
}

// RedisOption configures a RedisStore.
type RedisOption func(*redisOptions)

type redisOptions struct {
	prefix string
}

// WithRedisPrefix sets the Redis key prefix. Default: "lifecycle:".
func WithRedisPrefix(prefix string) RedisOption {
	return func(o *redisOptions) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// NewRedisStore creates a Redis-backed Store. client must be non-nil.
func NewRedisStore(client redis.Cmdable, opts ...RedisOption) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("lifecycle: redis client is required")
	}
	o := &redisOptions{prefix: "lifecycle:"}
	for _, opt := range opts {
		opt(o)
	}
	return &RedisStore{client: client, prefix: o.prefix}, nil
}

func (s *RedisStore) key(k string) string { return s.prefix + k }

// Acquire implements Store.
//
// Behavior summary (all atomic under a single Lua script):
//   - completed/failed hash present -> return it unchanged
//   - running by same instance      -> refresh lease, return running
//   - running by other, lease valid -> return running by other
//   - running by other, lease past  -> take over: set running + new lease
//   - no hash present               -> create: set running + new lease
func (s *RedisStore) Acquire(ctx context.Context, key, instanceID string, lease time.Duration) (Status, error) {
	nowMs := time.Now().UnixMilli()
	leaseMs := lease.Milliseconds()
	res, err := acquireScript.Run(ctx, s.client, []string{s.key(key)},
		instanceID, leaseMs, nowMs).Result()
	if err != nil {
		return Status{}, fmt.Errorf("acquire: %w", err)
	}
	return parseHashResult(res)
}

// Refresh implements Store.
func (s *RedisStore) Refresh(ctx context.Context, key, instanceID string, lease time.Duration) error {
	nowMs := time.Now().UnixMilli()
	leaseMs := lease.Milliseconds()
	res, err := refreshScript.Run(ctx, s.client, []string{s.key(key)},
		instanceID, leaseMs, nowMs).Int()
	if err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	if res == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete implements Store.
func (s *RedisStore) Complete(ctx context.Context, key, instanceID string) error {
	nowMs := time.Now().UnixMilli()
	res, err := completeScript.Run(ctx, s.client, []string{s.key(key)},
		instanceID, nowMs).Int()
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	if res == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail implements Store.
func (s *RedisStore) Fail(ctx context.Context, key, instanceID, errMsg string, retryable bool) error {
	nowMs := time.Now().UnixMilli()
	retryFlag := "0"
	if retryable {
		retryFlag = "1"
	}
	res, err := failScript.Run(ctx, s.client, []string{s.key(key)},
		instanceID, errMsg, retryFlag, nowMs).Int()
	if err != nil {
		return fmt.Errorf("fail: %w", err)
	}
	if res == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Get implements Store.
func (s *RedisStore) Get(ctx context.Context, key string) (Status, error) {
	res, err := s.client.HGetAll(ctx, s.key(key)).Result()
	if err != nil {
		return Status{}, fmt.Errorf("get: %w", err)
	}
	if len(res) == 0 {
		return Status{State: StatePending}, nil
	}
	return hashMapToStatus(res), nil
}

// Reset implements Store.
func (s *RedisStore) Reset(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, s.key(key)).Err(); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// Health implements health.Checker.
//
// Sends a PING and reports the prefix in use. The check does not scan or
// count keys — that would be O(n) on the entire keyspace under SCAN and
// is not part of Redis's lifecycle for this Store.
func (s *RedisStore) Health(ctx context.Context) *health.Result {
	start := time.Now()
	if err := s.client.Ping(ctx).Err(); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("redis ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}
	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"prefix": s.prefix,
		},
	}
}

// parseHashResult parses the []any result of HGETALL returned by a Lua
// script (alternating field/value strings).
func parseHashResult(res any) (Status, error) {
	arr, ok := res.([]any)
	if !ok {
		return Status{}, fmt.Errorf("unexpected script result type %T", res)
	}
	if len(arr) == 0 {
		return Status{State: StatePending}, nil
	}
	m := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		v, _ := arr[i+1].(string)
		m[k] = v
	}
	return hashMapToStatus(m), nil
}

func hashMapToStatus(m map[string]string) Status {
	s := Status{}
	switch m["state"] {
	case string(StateRunning):
		s.State = StateRunning
	case string(StateCompleted):
		s.State = StateCompleted
	case string(StateFailed):
		s.State = StateFailed
	default:
		s.State = StatePending
		return s
	}
	s.Holder = m["holder"]
	s.LeaseUntil = msToTime(m["lease_until_ms"])
	s.CompletedAt = msToTime(m["completed_at_ms"])
	s.FailedAt = msToTime(m["failed_at_ms"])
	s.Error = m["error"]
	return s
}

func msToTime(s string) time.Time {
	if s == "" || s == "0" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	return time.UnixMilli(n)
}

// acquireScript performs the full Acquire state-machine atomically.
//
// KEYS[1] = hook key
// ARGV[1] = instanceID
// ARGV[2] = lease ms
// ARGV[3] = now ms
//
// Returns: HGETALL of the post-call hash. Every branch ends with an
// HGETALL after either a take-over write, a same-holder refresh, or a
// pass-through observation, so the array is always populated.
var acquireScript = redis.NewScript(`
local key = KEYS[1]
local instance = ARGV[1]
local lease_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state')
if state == 'completed' or state == 'failed' then
    return redis.call('HGETALL', key)
end

if state == 'running' then
    local holder = redis.call('HGET', key, 'holder')
    local lease_until = tonumber(redis.call('HGET', key, 'lease_until_ms')) or 0
    if holder == instance then
        redis.call('HSET', key, 'lease_until_ms', now_ms + lease_ms)
        return redis.call('HGETALL', key)
    end
    if now_ms < lease_until then
        return redis.call('HGETALL', key)
    end
    -- lease expired, fall through and take over
end

redis.call('DEL', key)
redis.call('HSET', key,
    'state', 'running',
    'holder', instance,
    'lease_until_ms', now_ms + lease_ms)
return redis.call('HGETALL', key)
`)

// Returns 1 on success, 0 on lease lost.
var refreshScript = redis.NewScript(`
local key = KEYS[1]
local instance = ARGV[1]
local lease_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state')
local holder = redis.call('HGET', key, 'holder')
local lease_until = tonumber(redis.call('HGET', key, 'lease_until_ms')) or 0

-- Expiry is exclusive: at now_ms == lease_until the lease is already gone
-- (Acquire would take it over), so reject the refresh to stay consistent.
if state ~= 'running' or holder ~= instance or now_ms >= lease_until then
    return 0
end

redis.call('HSET', key, 'lease_until_ms', now_ms + lease_ms)
return 1
`)

// Returns 1 on success, 0 on lease lost.
var completeScript = redis.NewScript(`
local key = KEYS[1]
local instance = ARGV[1]
local now_ms = ARGV[2]

local state = redis.call('HGET', key, 'state')
local holder = redis.call('HGET', key, 'holder')

if state ~= 'running' or holder ~= instance then
    return 0
end

redis.call('HSET', key,
    'state', 'completed',
    'completed_at_ms', now_ms,
    'lease_until_ms', 0)
return 1
`)

// Returns 1 on success, 0 on lease lost.
// ARGV[3] = '1' to release the claim (retryable), '0' to record terminal failure.
var failScript = redis.NewScript(`
local key = KEYS[1]
local instance = ARGV[1]
local err_msg = ARGV[2]
local retryable = ARGV[3]
local now_ms = ARGV[4]

local state = redis.call('HGET', key, 'state')
local holder = redis.call('HGET', key, 'holder')

if state ~= 'running' or holder ~= instance then
    return 0
end

if retryable == '1' then
    redis.call('DEL', key)
    return 1
end

redis.call('HSET', key,
    'state', 'failed',
    'failed_at_ms', now_ms,
    'error', err_msg,
    'lease_until_ms', 0)
return 1
`)

var (
	_ Store          = (*RedisStore)(nil)
	_ health.Checker = (*RedisStore)(nil)
)
