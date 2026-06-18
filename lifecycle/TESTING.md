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

Five targets in `fuzz_test.go`. Each row lists the surface and the invariant it
guards (so a change that breaks the property fails self-documentingly):

| Target | Surface | Invariant guarded |
|--------|---------|-------------------|
| `FuzzHookValidateAndKey` | `Hook.Validate` + `Hook.Key` | valid hook ⇒ `name@version` round-trips via first `@` |
| `FuzzMsToTime` | Redis millisecond decoder | exact `strconv`+`UnixMilli` inverse |
| `FuzzHashMapToStatus` | HGETALL → `Status` | State ∈ domain; pending ⇒ zero-valued; lease value round-trips |
| `FuzzParseHashResult` | Lua-result parser | never panics; State ∈ domain (all four branches) |
| `FuzzRedisGetDecode` | `RedisStore.Get` → decode (miniredis) | never panics; State ∈ domain through the real client path |

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

## CI gating

Every test type runs in CI and gates PRs to `main`:

| Workflow | Jobs | Aggregate check |
|----------|------|-----------------|
| `ci.yml` | Build+Test (`-race`, no env → integration skips), Integration (redis/postgres/mongo service containers + reachability precheck + assert-not-skipped + 85% lifecycle coverage gate), Lint, CodeQL | **CI Gate** |
| `fuzz.yml` | Per-target seed replay on PRs (20s) + 5-min nightly campaign | **Fuzz Gate** |
| `bench.yml` | `-count=6` HEAD-vs-base regression gate (>15% ns/op or >20% B/op; CV-noisy benches skipped) | Benchmark regression gate |

Branch protection on `main` requires the aggregate **CI Gate** and **Fuzz Gate**
checks (each is green only when all of its underlying jobs pass), so adding or
renaming a job tightens the gate without editing repo settings. The Integration
job fails loudly rather than silently skipping when a backend is unreachable.

## Benchmarks

```bash
just bench                # -count=6, ready for benchstat
just bench-profile        # cpu/mem/mutex profiles for the contended path
```

`BenchmarkStore_Acquire` renders a cross-backend comparison; its Postgres,
Mongo, and live-Redis rows run only when the matching env var is set.
`.github/workflows/bench.yml` gates PRs on >15% ns/op or >20% B/op regression
versus the merge base.
