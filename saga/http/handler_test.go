package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event-extras/saga"
	sagahttp "github.com/rbaliyan/event-extras/saga/http"
)

func newStore(t *testing.T) *saga.MemoryStore {
	t.Helper()
	return saga.NewMemoryStore()
}

func newHandler(t *testing.T) (*sagahttp.Handler, *saga.MemoryStore) {
	t.Helper()
	store := newStore(t)
	return sagahttp.New(store), store
}

func do(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v (body: %s)", err, w.Body.String())
	}
}

func seedSaga(t *testing.T, store *saga.MemoryStore, id, name string, status saga.Status) {
	t.Helper()
	now := time.Now()
	state := &saga.State{
		ID:            id,
		Name:          name,
		Status:        status,
		StartedAt:     now,
		LastUpdatedAt: now,
	}
	if err := store.Create(context.Background(), state); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestList_Empty verifies an empty store returns an empty sagas array.
func TestList_Empty(t *testing.T) {
	h, _ := newHandler(t)
	w := do(t, h, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Sagas []any `json:"sagas"`
		Count int   `json:"count"`
	}
	decodeJSON(t, w, &result)
	if len(result.Sagas) != 0 {
		t.Errorf("want empty, got %d", len(result.Sagas))
	}
}

// TestList_WithSagas verifies the list endpoint returns all seeded sagas.
func TestList_WithSagas(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "order.create", saga.StatusRunning)
	seedSaga(t, store, "s2", "order.create", saga.StatusCompleted)
	seedSaga(t, store, "s3", "payment.capture", saga.StatusFailed)

	w := do(t, h, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Count int `json:"count"`
	}
	decodeJSON(t, w, &result)
	if result.Count != 3 {
		t.Errorf("want count=3, got %d", result.Count)
	}
}

// TestList_FilterByName verifies name filtering.
func TestList_FilterByName(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "order.create", saga.StatusRunning)
	seedSaga(t, store, "s2", "order.create", saga.StatusCompleted)
	seedSaga(t, store, "s3", "payment.capture", saga.StatusFailed)

	w := do(t, h, http.MethodGet, "/v1/sagas?name=order.create")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Count int `json:"count"`
	}
	decodeJSON(t, w, &result)
	if result.Count != 2 {
		t.Errorf("want count=2, got %d", result.Count)
	}
}

// TestList_FilterByStatus verifies status filtering.
func TestList_FilterByStatus(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "order.create", saga.StatusRunning)
	seedSaga(t, store, "s2", "order.create", saga.StatusCompleted)
	seedSaga(t, store, "s3", "payment.capture", saga.StatusFailed)

	w := do(t, h, http.MethodGet, "/v1/sagas?status=failed")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Sagas []struct {
			Status string `json:"status"`
		} `json:"sagas"`
		Count int `json:"count"`
	}
	decodeJSON(t, w, &result)
	if result.Count != 1 {
		t.Errorf("want count=1, got %d", result.Count)
	}
	if result.Sagas[0].Status != "failed" {
		t.Errorf("want status=failed, got %s", result.Sagas[0].Status)
	}
}

// TestGet returns the correct saga by ID.
func TestGet(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "saga-abc", "order.create", saga.StatusCompleted)

	w := do(t, h, http.MethodGet, "/v1/sagas/saga-abc")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var result struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	decodeJSON(t, w, &result)
	if result.ID != "saga-abc" {
		t.Errorf("want id=saga-abc, got %s", result.ID)
	}
	if result.Name != "order.create" {
		t.Errorf("want name=order.create, got %s", result.Name)
	}
	if result.Status != "completed" {
		t.Errorf("want status=completed, got %s", result.Status)
	}
}

// TestGet_NotFound returns 404 for a missing saga.
func TestGet_NotFound(t *testing.T) {
	h, _ := newHandler(t)
	w := do(t, h, http.MethodGet, "/v1/sagas/nonexistent")
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

// TestStats returns correct counts by status.
func TestStats(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "order.create", saga.StatusRunning)
	seedSaga(t, store, "s2", "order.create", saga.StatusRunning)
	seedSaga(t, store, "s3", "order.create", saga.StatusCompleted)
	seedSaga(t, store, "s4", "payment.capture", saga.StatusFailed)

	w := do(t, h, http.MethodGet, "/v1/sagas/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var result struct {
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
	}
	decodeJSON(t, w, &result)

	if result.Total != 4 {
		t.Errorf("want total=4, got %d", result.Total)
	}
	if result.ByStatus["running"] != 2 {
		t.Errorf("want running=2, got %d", result.ByStatus["running"])
	}
	if result.ByStatus["completed"] != 1 {
		t.Errorf("want completed=1, got %d", result.ByStatus["completed"])
	}
	if result.ByStatus["failed"] != 1 {
		t.Errorf("want failed=1, got %d", result.ByStatus["failed"])
	}
	if result.ByStatus["pending"] != 0 {
		t.Errorf("want pending=0, got %d", result.ByStatus["pending"])
	}
}

// TestStats_Empty returns zeros for all statuses on an empty store.
func TestStats_Empty(t *testing.T) {
	h, _ := newHandler(t)
	w := do(t, h, http.MethodGet, "/v1/sagas/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
	}
	decodeJSON(t, w, &result)
	if result.Total != 0 {
		t.Errorf("want total=0, got %d", result.Total)
	}
	for status, count := range result.ByStatus {
		if count != 0 {
			t.Errorf("want %s=0, got %d", status, count)
		}
	}
}

// TestDelete removes a saga and confirms subsequent GET returns 404.
func TestDelete(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "to-delete", "order.create", saga.StatusCompleted)

	w := do(t, h, http.MethodDelete, "/v1/sagas/to-delete")
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE: want 204, got %d (body: %s)", w.Code, w.Body.String())
	}

	w = do(t, h, http.MethodGet, "/v1/sagas/to-delete")
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE: want 404, got %d", w.Code)
	}
}

// TestDelete_NotFound returns 404 for a missing saga.
func TestDelete_NotFound(t *testing.T) {
	h, _ := newHandler(t)
	w := do(t, h, http.MethodDelete, "/v1/sagas/missing")
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

// TestDelete_StoreNoDeleter returns 405 when the store does not implement Deleter.
func TestDelete_StoreNoDeleter(t *testing.T) {
	h := sagahttp.New(&readOnlyStore{})
	w := do(t, h, http.MethodDelete, "/v1/sagas/any-id")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

// TestList_ResponseFields verifies expected fields are present in saga list items.
func TestList_ResponseFields(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "field-test", "order.create", saga.StatusRunning)

	w := do(t, h, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Sagas []struct {
			ID             string   `json:"id"`
			Name           string   `json:"name"`
			Status         string   `json:"status"`
			CurrentStep    int      `json:"current_step"`
			CompletedSteps []string `json:"completed_steps"`
			Version        int64    `json:"version"`
		} `json:"sagas"`
	}
	decodeJSON(t, w, &result)
	if len(result.Sagas) != 1 {
		t.Fatalf("want 1 saga, got %d", len(result.Sagas))
	}
	s := result.Sagas[0]
	if s.ID != "field-test" {
		t.Errorf("id: want field-test, got %s", s.ID)
	}
	if s.CompletedSteps == nil {
		t.Error("completed_steps should be [] not null")
	}
}

// readOnlyStore is a minimal Store that does not implement Deleter.
type readOnlyStore struct{}

func (*readOnlyStore) Create(_ context.Context, _ *saga.State) error        { return nil }
func (*readOnlyStore) Get(_ context.Context, _ string) (*saga.State, error) { return nil, nil }
func (*readOnlyStore) Update(_ context.Context, _ *saga.State) error        { return nil }
func (*readOnlyStore) List(_ context.Context, _ saga.StoreFilter) ([]*saga.State, error) {
	return nil, nil
}

// TestList_LimitEdgeCases verifies the limit query param is robust to bad input.
// Negative, zero, non-numeric, and empty values all fall back to the default.
func TestList_LimitEdgeCases(t *testing.T) {
	h, store := newHandler(t)
	for i := 0; i < 10; i++ {
		seedSaga(t, store, fmt.Sprintf("s%d", i), "n", saga.StatusRunning)
	}

	cases := []struct {
		name  string
		query string
		want  int // expected count in response
	}{
		{"negative", "?limit=-5", 10},
		{"zero", "?limit=0", 10},
		{"nonnumeric", "?limit=abc", 10},
		{"empty", "?limit=", 10},
		{"explicit_small", "?limit=3", 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := do(t, h, http.MethodGet, "/v1/sagas"+tc.query)
			if w.Code != http.StatusOK {
				t.Fatalf("want 200, got %d", w.Code)
			}
			var result struct {
				Count int `json:"count"`
			}
			decodeJSON(t, w, &result)
			if result.Count != tc.want {
				t.Errorf("count: want %d, got %d", tc.want, result.Count)
			}
		})
	}
}

// TestList_RepeatableStatus verifies multiple ?status= values OR together.
func TestList_RepeatableStatus(t *testing.T) {
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "n", saga.StatusRunning)
	seedSaga(t, store, "s2", "n", saga.StatusFailed)
	seedSaga(t, store, "s3", "n", saga.StatusCompleted)

	w := do(t, h, http.MethodGet, "/v1/sagas?status=running&status=failed")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Sagas []struct {
			Status string `json:"status"`
		} `json:"sagas"`
		Count int `json:"count"`
	}
	decodeJSON(t, w, &result)
	if result.Count != 2 {
		t.Errorf("want count=2, got %d", result.Count)
	}
	seen := map[string]bool{}
	for _, s := range result.Sagas {
		seen[s.Status] = true
	}
	if !seen["running"] || !seen["failed"] {
		t.Errorf("want both running and failed; got %v", seen)
	}
}

// aggStore wraps MemoryStore and records whether CountByStatus was called,
// so the test can verify Aggregator delegation without hitting List.
type aggStore struct {
	*saga.MemoryStore
	called  atomic.Int32
	counts  map[saga.Status]int
	failErr error
}

func (a *aggStore) CountByStatus(_ context.Context) (map[saga.Status]int, error) {
	a.called.Add(1)
	if a.failErr != nil {
		return nil, a.failErr
	}
	return a.counts, nil
}

// TestStats_UsesAggregator verifies stats delegates to CountByStatus when
// the store implements saga.Aggregator, avoiding the List scan.
func TestStats_UsesAggregator(t *testing.T) {
	agg := &aggStore{
		MemoryStore: saga.NewMemoryStore(),
		counts: map[saga.Status]int{
			saga.StatusRunning:   7,
			saga.StatusCompleted: 3,
		},
	}
	h := sagahttp.New(agg)

	w := do(t, h, http.MethodGet, "/v1/sagas/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	if agg.called.Load() != 1 {
		t.Fatalf("CountByStatus should have been called exactly once, got %d", agg.called.Load())
	}

	var result struct {
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
	}
	decodeJSON(t, w, &result)
	if result.Total != 10 {
		t.Errorf("total: want 10, got %d", result.Total)
	}
	if result.ByStatus["running"] != 7 {
		t.Errorf("running: want 7, got %d", result.ByStatus["running"])
	}
	if result.ByStatus["completed"] != 3 {
		t.Errorf("completed: want 3, got %d", result.ByStatus["completed"])
	}
	// Statuses not returned by the aggregator should still be zero-filled.
	if _, ok := result.ByStatus["pending"]; !ok {
		t.Errorf("pending should be present with zero count, got %v", result.ByStatus)
	}
}

// TestStats_AggregatorError propagates the backend error as a 500.
func TestStats_AggregatorError(t *testing.T) {
	agg := &aggStore{
		MemoryStore: saga.NewMemoryStore(),
		failErr:     errors.New("backend down"),
	}
	h := sagahttp.New(agg)

	w := do(t, h, http.MethodGet, "/v1/sagas/stats")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", w.Code)
	}
}

// TestStats_FallbackWhenNoAggregator verifies stats falls back to List for
// stores that do not implement saga.Aggregator (MemoryStore does not).
func TestStats_FallbackWhenNoAggregator(t *testing.T) {
	// MemoryStore satisfies Deleter but not Aggregator.
	h, store := newHandler(t)
	seedSaga(t, store, "s1", "n", saga.StatusRunning)
	seedSaga(t, store, "s2", "n", saga.StatusRunning)

	w := do(t, h, http.MethodGet, "/v1/sagas/stats")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result struct {
		Total     int            `json:"total"`
		ByStatus  map[string]int `json:"by_status"`
		Truncated bool           `json:"truncated"`
	}
	decodeJSON(t, w, &result)
	if result.Total != 2 {
		t.Errorf("total: want 2, got %d", result.Total)
	}
	if result.ByStatus["running"] != 2 {
		t.Errorf("running: want 2, got %d", result.ByStatus["running"])
	}
	if result.Truncated {
		t.Errorf("should not be truncated at only 2 rows")
	}
}

// TestRegisterRoutes lets callers register directly on their parent mux and
// avoid the 307 trailing-slash redirect that would otherwise strand /v1/sagas.
func TestRegisterRoutes(t *testing.T) {
	store := saga.NewMemoryStore()
	seedSaga(t, store, "s1", "order.create", saga.StatusCompleted)

	parent := http.NewServeMux()
	h := sagahttp.New(store)
	h.RegisterRoutes(parent)

	// Exact prefix (no trailing slash) must serve list — not redirect.
	w := do(t, parent, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("list: want 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	// Single-saga GET works too.
	w = do(t, parent, http.MethodGet, "/v1/sagas/s1")
	if w.Code != http.StatusOK {
		t.Fatalf("get: want 200, got %d", w.Code)
	}
}

// TestWithLogger verifies the option is accepted and applied. Log-output
// assertions for the marshal-error path live in handler_internal_test.go
// (in-package) where writeJSON is reachable directly.
func TestWithLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	store := saga.NewMemoryStore()
	h := sagahttp.New(store, sagahttp.WithLogger(logger))

	w := do(t, h, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

// TestWithLogger_NilIgnored confirms a nil logger option does not clear the
// default — the handler must still produce a non-nil logger internally.
func TestWithLogger_NilIgnored(t *testing.T) {
	store := saga.NewMemoryStore()
	h := sagahttp.New(store, sagahttp.WithLogger(nil))

	w := do(t, h, http.MethodGet, "/v1/sagas")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

