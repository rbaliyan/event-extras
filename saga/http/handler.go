// Package http provides a read-oriented REST handler for inspecting saga state.
//
// Mount the handler at any prefix; it registers routes under /v1/sagas:
//
//	h := sagahttp.New(store)
//	mux.Handle("/v1/sagas/", h)  // or any router that strips the prefix
//
// Endpoints:
//
//	GET  /v1/sagas              — list sagas (?name=, &status=, &limit=)
//	GET  /v1/sagas/stats        — counts by status
//	GET  /v1/sagas/{id}         — single saga detail
//	DELETE /v1/sagas/{id}       — delete saga (only if store implements Deleter)
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rbaliyan/event-extras/saga"
)

const defaultListLimit = 50

// Deleter is an optional interface for saga stores that support deletion.
// If the store implements Deleter, DELETE /v1/sagas/{id} is enabled;
// otherwise it returns 405 Method Not Allowed.
type Deleter interface {
	Delete(ctx context.Context, id string) error
}

// Handler is an HTTP handler that exposes saga state as a REST API.
type Handler struct {
	store saga.Store
	mux   *http.ServeMux
}

// New creates a Handler backed by the given store.
func New(store saga.Store) *Handler {
	h := &Handler{store: store}
	mux := http.NewServeMux()
	// /stats must be registered before /{id} — more specific exact match wins.
	mux.HandleFunc("GET /v1/sagas/stats", h.stats)
	mux.HandleFunc("GET /v1/sagas/{id}", h.get)
	mux.HandleFunc("DELETE /v1/sagas/{id}", h.delete)
	mux.HandleFunc("GET /v1/sagas", h.list)
	h.mux = mux
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// list handles GET /v1/sagas.
// Query params: name (string), status (repeatable), limit (int, default 50).
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := saga.StoreFilter{Limit: defaultListLimit}

	if name := q.Get("name"); name != "" {
		filter.Name = name
	}

	for _, s := range q["status"] {
		filter.Status = append(filter.Status, saga.Status(s))
	}

	if ls := q.Get("limit"); ls != "" {
		if n, err := strconv.Atoi(ls); err == nil && n > 0 {
			filter.Limit = n
		}
	}

	states, err := h.store.List(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	views := make([]sagaView, len(states))
	for i, s := range states {
		views[i] = fromState(s)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"sagas": views,
		"count": len(views),
	})
}

// stats handles GET /v1/sagas/stats.
// Returns total count and a breakdown by status.
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	// A single unbounded List is fewer round-trips than one per status.
	states, err := h.store.List(r.Context(), saga.StoreFilter{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	counts := map[string]int{
		string(saga.StatusPending):      0,
		string(saga.StatusRunning):      0,
		string(saga.StatusCompleted):    0,
		string(saga.StatusFailed):       0,
		string(saga.StatusCompensating): 0,
		string(saga.StatusCompensated):  0,
	}
	for _, s := range states {
		counts[string(s.Status)]++
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total":     len(states),
		"by_status": counts,
	})
}

// get handles GET /v1/sagas/{id}.
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, err := h.store.Get(r.Context(), id)
	if err != nil {
		if saga.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "saga not found: "+id)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, fromState(state))
}

// delete handles DELETE /v1/sagas/{id}.
// Returns 405 if the store does not implement Deleter.
func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	d, ok := h.store.(Deleter)
	if !ok {
		writeError(w, http.StatusMethodNotAllowed, "store does not support deletion")
		return
	}
	id := r.PathValue("id")
	if err := d.Delete(r.Context(), id); err != nil {
		if saga.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "saga not found: "+id)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// sagaView is the JSON representation of a saga state.
// The Data field is intentionally omitted — it is business-domain-specific
// and can be arbitrarily large.
type sagaView struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	CurrentStep    int        `json:"current_step"`
	CompletedSteps []string   `json:"completed_steps"`
	Error          string     `json:"error,omitempty"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	LastUpdatedAt  time.Time  `json:"last_updated_at"`
	Version        int64      `json:"version"`
}

func fromState(s *saga.State) sagaView {
	steps := s.CompletedSteps
	if steps == nil {
		steps = []string{}
	}
	return sagaView{
		ID:             s.ID,
		Name:           s.Name,
		Status:         string(s.Status),
		CurrentStep:    s.CurrentStep,
		CompletedSteps: steps,
		Error:          s.Error,
		StartedAt:      s.StartedAt,
		CompletedAt:    s.CompletedAt,
		LastUpdatedAt:  s.LastUpdatedAt,
		Version:        s.Version,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
