package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func (*readOnlyStore) Create(_ context.Context, _ *saga.State) error              { return nil }
func (*readOnlyStore) Get(_ context.Context, _ string) (*saga.State, error)       { return nil, nil }
func (*readOnlyStore) Update(_ context.Context, _ *saga.State) error              { return nil }
func (*readOnlyStore) List(_ context.Context, _ saga.StoreFilter) ([]*saga.State, error) {
	return nil, nil
}
