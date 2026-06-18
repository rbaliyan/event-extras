package lifecycle

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// FuzzHookValidateAndKey asserts the two pure Hook methods never panic on
// arbitrary input, and that the name@version key round-trips: because a valid
// Name may not contain '@', splitting Key on the first '@' must recover the
// original Name and Version exactly.
func FuzzHookValidateAndKey(f *testing.F) {
	f.Add("migrate", "v1.2.3")
	f.Add("migrate", "")
	f.Add("bad@name", "v1")
	f.Add("", "")
	f.Add("name", "v@with@ats")
	f.Add("trailing@", "")
	f.Add("\x00", "\n")
	f.Add("name", "\x00\xff")

	f.Fuzz(func(t *testing.T, name, version string) {
		h := Hook{Name: name, Version: version}
		err := h.Validate()
		key := h.Key() // must never panic regardless of validity

		if err != nil {
			return // invalid hooks make no round-trip guarantee
		}
		// Valid: Name is non-empty and contains no '@'.
		if name == "" || strings.Contains(name, "@") {
			t.Fatalf("Validate accepted an invalid name %q", name)
		}
		if version == "" {
			if key != name {
				t.Fatalf("empty-version key = %q, want %q", key, name)
			}
			return
		}
		gotName, gotVersion, found := strings.Cut(key, "@")
		if !found {
			t.Fatalf("key %q missing '@' separator for version %q", key, version)
		}
		if gotName != name || gotVersion != version {
			t.Fatalf("round-trip mismatch: key=%q -> (%q,%q), want (%q,%q)",
				key, gotName, gotVersion, name, version)
		}
	})
}

// FuzzMsToTime asserts the Redis millisecond decoder never panics and is an
// exact inverse of strconv+UnixMilli: any value that parses to a non-zero
// int64 must round-trip through UnixMilli, and everything else must be zero.
func FuzzMsToTime(f *testing.F) {
	f.Add("0")
	f.Add("")
	f.Add("1700000000000")
	f.Add("-1")
	f.Add("-9999999999999")
	f.Add("not-a-number")
	f.Add("99999999999999999999999999")

	f.Fuzz(func(t *testing.T, s string) {
		got := msToTime(s)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n == 0 {
			if !got.IsZero() {
				t.Fatalf("unparseable/zero input %q must yield zero time, got %v", s, got)
			}
			return
		}
		if got.UnixMilli() != n {
			t.Fatalf("round-trip failed: msToTime(%q).UnixMilli() = %d, want %d", s, got.UnixMilli(), n)
		}
	})
}

// FuzzHashMapToStatus asserts the HGETALL decoder never panics and always
// yields one of the four documented States; a pending result must carry no
// other data at all (the zero-value invariant documented on Status).
func FuzzHashMapToStatus(f *testing.F) {
	f.Add("running", "pod-a", "1700000000000", "", "", "")
	f.Add("completed", "pod-a", "0", "1700000000000", "", "")
	f.Add("failed", "pod-b", "0", "", "1700000000000", "boom")
	f.Add("gibberish", "pod-c", "x", "y", "z", "err")
	f.Add("", "", "", "", "", "")

	f.Fuzz(func(t *testing.T, state, holder, lease, completed, failed, errMsg string) {
		s := hashMapToStatus(map[string]string{
			"state":           state,
			"holder":          holder,
			"lease_until_ms":  lease,
			"completed_at_ms": completed,
			"failed_at_ms":    failed,
			"error":           errMsg,
		})
		switch s.State {
		case StatePending:
			if s.Holder != "" || !s.LeaseUntil.IsZero() ||
				!s.CompletedAt.IsZero() || !s.FailedAt.IsZero() || s.Error != "" {
				t.Fatalf("pending status must be fully zero-valued, got %+v", s)
			}
		case StateRunning, StateCompleted, StateFailed:
			// Value-equivalence oracle: on a recognized state, a parseable
			// lease must round-trip exactly through to LeaseUntil.
			if n, err := strconv.ParseInt(lease, 10, 64); err == nil && n != 0 {
				if s.LeaseUntil.UnixMilli() != n {
					t.Fatalf("lease_until_ms %q did not round-trip: got %d, want %d",
						lease, s.LeaseUntil.UnixMilli(), n)
				}
			}
		default:
			t.Fatalf("decoded an undefined State %q", s.State)
		}
	})
}

// FuzzRedisGetDecode is a differential/integration fuzz target: it writes
// arbitrary hash fields into a real (in-process miniredis) server, then decodes
// them through the full RedisStore.Get -> HGETALL -> hashMapToStatus path.
// This exercises a Store method surface (not just the pure helpers) and asserts
// the decoder never panics and always yields a defined State. Go runs each
// fuzz worker as its own process, so the shared miniredis here is used
// sequentially within a process — no concurrency hazard.
func FuzzRedisGetDecode(f *testing.F) {
	mr, err := miniredis.Run()
	if err != nil {
		f.Fatalf("miniredis.Run: %v", err)
	}
	f.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	if err != nil {
		f.Fatalf("NewRedisStore: %v", err)
	}
	ctx := context.Background()

	f.Add("running", "pod-a", "1700000000000")
	f.Add("completed", "", "0")
	f.Add("garbage", "x", "not-a-number")
	f.Add("", "", "")

	f.Fuzz(func(t *testing.T, state, holder, lease string) {
		key := store.key("k")
		mr.HSet(key, "state", state, "holder", holder, "lease_until_ms", lease)
		defer mr.Del(key)

		s, err := store.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get must not error on a present key: %v", err)
		}
		switch s.State {
		case StatePending, StateRunning, StateCompleted, StateFailed:
		default:
			t.Fatalf("decoded an undefined State %q", s.State)
		}
	})
}

// FuzzParseHashResult drives both the input type and array shape from fuzzed
// bytes so it reaches every branch of parseHashResult: the non-[]any
// type-assert failure, the empty-array pending case, the odd-length guard, and
// the non-string element comma-ok asserts. It must never panic, and any []any
// input must decode to a defined State.
func FuzzParseHashResult(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("state"))
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{5, 10, 15}) // odd length

	f.Fuzz(func(t *testing.T, data []byte) {
		// One in five inputs is passed as a non-slice value to exercise the
		// type-assertion failure branch.
		if len(data) > 0 && data[0]%5 == 0 {
			if _, err := parseHashResult(string(data)); err == nil {
				t.Fatalf("non-array input %q should return an error", data)
			}
			return
		}
		// Build an alternating array of mixed string / non-string elements.
		// Occasionally inject the real field names and valid state literals so
		// the populated decode path (not just the pending default) is driven.
		states := []string{"running", "completed", "failed"}
		fields := []string{"state", "holder", "lease_until_ms", "completed_at_ms", "failed_at_ms", "error"}
		res := make([]any, len(data))
		for i, b := range data {
			switch {
			case b%7 == 0 && i+1 < len(data):
				res[i] = fields[int(b)%len(fields)] // real field-name vocabulary
			case b%7 == 1:
				res[i] = states[int(b)%len(states)]
			case b%7 == 2:
				res[i] = "1700000000000" // a valid timestamp value
			case b%3 == 0:
				res[i] = int(b) // non-string exercises the comma-ok asserts
			default:
				res[i] = string([]byte{b})
			}
		}
		s, err := parseHashResult(res)
		if err != nil {
			// A []any can never error by construction (parseHashResult only
			// errors on a non-slice); guard so a future change fails loudly.
			t.Fatalf("[]any input must not error, got %v", err)
		}
		switch s.State {
		case StatePending, StateRunning, StateCompleted, StateFailed:
		default:
			t.Fatalf("decoded an undefined State %q", s.State)
		}
	})
}
