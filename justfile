# Default recipe
default:
    @just --list

# Build all packages
build:
    go build ./...

# Run tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Container backends for integration tests (Redis, PostgreSQL, MongoDB)
LE_REDIS_ADDR := "localhost:6390"
LE_POSTGRES_URI := "postgres://test:test@localhost:5440/lifecycle_test?sslmode=disable"
LE_MONGO_URI := "mongodb://localhost:27027"

# Container runtime for local integration backends
container := env_var_or_default("CONTAINER_ENGINE", "podman")

# Start backend containers for integration tests
backends-up:
    {{container}} run -d --rm --name le-redis -p 6390:6379 docker.io/library/redis:7-alpine
    {{container}} run -d --rm --name le-pg -p 5440:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=lifecycle_test docker.io/library/postgres:16-alpine
    {{container}} run -d --rm --name le-mongo -p 27027:27017 docker.io/library/mongo:7

# Stop backend containers
backends-down:
    -{{container}} rm -f le-redis le-pg le-mongo

# Run the full suite against live backends (start with `just backends-up` first)
test-integration:
    REDIS_ADDR={{LE_REDIS_ADDR}} POSTGRES_URI="{{LE_POSTGRES_URI}}" MONGO_URI={{LE_MONGO_URI}} \
        go test -race ./...

# Enforce a minimum coverage threshold for a package (run against live backends for full coverage)
test-cover-gate threshold="85" pkg="./lifecycle/...":
    #!/usr/bin/env bash
    set -euo pipefail
    go test -coverprofile=coverage.out {{pkg}} >/dev/null
    total=$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$NF); print $NF}')
    echo "{{pkg}} coverage: ${total}% (threshold: {{threshold}}%)"
    awk -v t="$total" -v min="{{threshold}}" 'BEGIN { exit !(t+0 >= min+0) }' \
        || { echo "coverage ${total}% below threshold {{threshold}}%"; exit 1; }

# Run benchmarks (variance-aware, ready for benchstat)
bench count="6":
    go test -run '^$' -bench=. -benchmem -count={{count}} ./lifecycle/

# Profile the lock-contention benchmark (cpu/mem/mutex profiles into bench/)
bench-profile:
    mkdir -p bench
    go test -run '^$' -bench=BenchmarkMemoryStore_AcquireParallel -benchmem \
        -cpuprofile=bench/cpu.out -memprofile=bench/mem.out -mutexprofile=bench/mutex.out ./lifecycle/

# Fuzz a single target (default 30s): just fuzz FuzzParseHashResult 1m
fuzz target="FuzzHookValidateAndKey" fuzztime="30s":
    go test -run '^$' -fuzz="^{{target}}$" -fuzztime={{fuzztime}} ./lifecycle/

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Tidy dependencies
tidy:
    go mod tidy

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
