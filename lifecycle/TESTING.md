# Testing the `lifecycle` package

## Layout

| Kind | Files | Runs without backends? |
|------|-------|------------------------|
| Contract / orchestration | `lifecycle_test.go` | Yes (Memory + miniredis) |
| Postgres unit | `postgres_unit_test.go` (sqlmock) | Yes |
| Mongo unit | `mongo_unit_test.go` (offline client + `countByState` seam) | Yes |
| Redis unit | `redis_unit_test.go` (miniredis + injectable clock) | Yes |
| Integration | `*_integration_test.go` | No — env-gated, see below |
| Fuzz | `fuzz_test.go` | Yes |
| Benchmarks | `bench_test.go` | Memory/miniredis yes; PG/Mongo env-gated |

The shared `runStoreContractTests` harness drives the identical state-machine
contract against every `Store` implementation (Memory, Redis, Postgres, Mongo).

## Running

```bash
just test                       # default build (in-process only; PG/Mongo/live-Redis skip)
just backends-up                # start redis/postgres/mongo containers (podman)
just test-integration           # full suite against live backends
just test-cover-gate 85 ./lifecycle/...
just backends-down
```

Integration tests are gated on `REDIS_ADDR` / `POSTGRES_URI` / `MONGO_URI`;
they skip instantly when unset and run (counting toward coverage) when set.

## Fuzzing

Four targets in `fuzz_test.go`, all pure/deterministic:

| Target | Surface |
|--------|---------|
| `FuzzHookValidateAndKey` | `Hook.Validate` + `Hook.Key` name@version round-trip |
| `FuzzMsToTime` | Redis millisecond decoder (exact strconv inverse) |
| `FuzzHashMapToStatus` | HGETALL → `Status` decode (State domain + pending zero-value) |
| `FuzzParseHashResult` | Lua-result parser (all four branches) |

```bash
just fuzz FuzzParseHashResult 1m   # local campaign
```

CI: `.github/workflows/fuzz.yml` replays the seed corpus on every PR (20s/target)
and runs a 5-minute nightly campaign per target.

### Crash triage loop

1. A failing CI run uploads a `fuzz-corpus-<target>` artifact (the failing input
   under `testdata/fuzz/<target>/`).
2. Download it into `lifecycle/testdata/fuzz/<target>/` and reproduce:
   `go test -run '<target>/<hash>' ./lifecycle/`.
3. Fix the bug, then commit the corpus entry as a permanent regression seed.

Hand-authored regression seeds already live under `testdata/fuzz/`.

## Benchmarks

```bash
just bench                # -count=6, ready for benchstat
just bench-profile        # cpu/mem/mutex profiles for the contended path
```

`BenchmarkStore_Acquire` renders a cross-backend comparison; its Postgres,
Mongo, and live-Redis rows run only when the matching env var is set.
`.github/workflows/bench.yml` gates PRs on >15% ns/op or >20% B/op regression
versus the merge base.
