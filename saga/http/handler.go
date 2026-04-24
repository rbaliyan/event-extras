// Package http provides a read-oriented REST handler for inspecting saga state.
//
// There are two ways to mount the handler.
//
// Direct handler (own sub-tree):
//
//	h := sagahttp.New(store)
//	// Register on both the trailing-slash and bare-prefix form so Go's ServeMux
//	// does not 307-redirect "/v1/sagas" and strand the child routes.
//	mux.Handle("/v1/sagas", h)
//	mux.Handle("/v1/sagas/", h)
//
// Route registration (share the parent mux):
//
//	h := sagahttp.New(store)
//	h.RegisterRoutes(mux)  // registers all /v1/sagas[/...] patterns directly
//
// Endpoints:
//
//	GET    /v1/sagas             — list sagas (?name=, &status=, &limit=)
//	GET    /v1/sagas/stats       — counts by status
//	GET    /v1/sagas/{id}        — single saga detail
//	DELETE /v1/sagas/{id}        — delete saga (only if store implements saga.Deleter)
package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/rbaliyan/event-extras/saga"
)

// defaultListLimit is the default page size for list responses when no limit
// is supplied via ?limit=.
const defaultListLimit = 50

// statsFallbackCap bounds the List call used when the store does not
// implement saga.Aggregator. Above this cap the handler refuses to scan
// further and reports truncation in the response body so operators do not
// silently get a misleading total.
const statsFallbackCap = 10_000

// Deleter is an alias preserved for backward compatibility with callers that
// may have depended on the http-layer name.
//
// Deprecated: use saga.Deleter — the canonical location is the saga package
// so other transports (e.g. gRPC) can share the same capability interface.
type Deleter = saga.Deleter

// Handler is an HTTP handler that exposes saga state as a REST API.
//
// Handler is safe for concurrent use. All state beyond immutable configuration
// lives in the underlying store.
type Handler struct {
	store  saga.Store
	mux    *http.ServeMux
	logger *slog.Logger
}

// Option configures a Handler.
type Option func(*handlerOptions)

type handlerOptions struct {
	logger *slog.Logger
}

// WithLogger sets a custom logger. A nil logger is ignored; the default is
// slog.Default() scoped with component=saga.http.
func WithLogger(logger *slog.Logger) Option {
	return func(o *handlerOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// New creates a Handler backed by the given store.
//
// Deletion support is advertised as 405 Method Not Allowed when the store
// does not implement saga.Deleter, and /stats falls back to a capped List
// when the store does not implement saga.Aggregator.
func New(store saga.Store, opts ...Option) *Handler {
	o := &handlerOptions{
		logger: slog.Default().With("component", "saga.http"),
	}
	for _, opt := range opts {
		opt(o)
	}

	h := &Handler{
		store:  store,
		logger: o.logger,
	}
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	h.mux = mux
	return h
}

// RegisterRoutes registers the handler's routes on the supplied mux.
//
// Use this when you want the /v1/sagas routes on a parent mux directly,
// avoiding the 307 trailing-slash redirect that occurs when the handler is
// mounted as a sub-tree via mux.Handle("/v1/sagas/", h).
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Go 1.22+ ServeMux picks the most specific pattern regardless of
	// registration order — see https://pkg.go.dev/net/http#ServeMux.
	mux.HandleFunc("GET /v1/sagas", h.list)
	mux.HandleFunc("GET /v1/sagas/stats", h.stats)
	mux.HandleFunc("GET /v1/sagas/{id}", h.get)
	mux.HandleFunc("DELETE /v1/sagas/{id}", h.deleteSaga)
}

// ServeHTTP dispatches requests via the internal mux built in New.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// list handles GET /v1/sagas.
// Query params: name (string), status (repeatable), limit (int, default 50).
// Non-numeric, zero, and negative limits fall back to the default.
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
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	views := make([]sagaView, len(states))
	for i, s := range states {
		views[i] = fromState(s)
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"sagas": views,
		"count": len(views),
	})
}

// stats handles GET /v1/sagas/stats.
//
// When the store implements saga.Aggregator, stats delegates to a single
// backend-native aggregation query. Otherwise it falls back to a capped
// List; if the cap is reached the response includes "truncated": true so
// operators know the total is a lower bound.
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	counts := emptyStatusCounts()

	if agg, ok := h.store.(saga.Aggregator); ok {
		byStatus, err := agg.CountByStatus(r.Context())
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		total := 0
		for status, n := range byStatus {
			counts[string(status)] = n
			total += n
		}
		h.writeJSON(w, http.StatusOK, map[string]any{
			"total":     total,
			"by_status": counts,
		})
		return
	}

	// Fallback: capped scan. Cap+1 lets us detect truncation without a
	// follow-up query.
	states, err := h.store.List(r.Context(), saga.StoreFilter{Limit: statsFallbackCap + 1})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	truncated := len(states) > statsFallbackCap
	if truncated {
		states = states[:statsFallbackCap]
	}
	for _, s := range states {
		counts[string(s.Status)]++
	}

	resp := map[string]any{
		"total":     len(states),
		"by_status": counts,
	}
	if truncated {
		resp["truncated"] = true
		resp["note"] = "store does not implement saga.Aggregator; counts truncated at fallback cap"
	}
	h.writeJSON(w, http.StatusOK, resp)
}

// emptyStatusCounts returns a status->count map seeded with every known
// status at zero so clients see a stable shape.
func emptyStatusCounts() map[string]int {
	return map[string]int{
		string(saga.StatusPending):      0,
		string(saga.StatusRunning):      0,
		string(saga.StatusCompleted):    0,
		string(saga.StatusFailed):       0,
		string(saga.StatusCompensating): 0,
		string(saga.StatusCompensated):  0,
	}
}

// get handles GET /v1/sagas/{id}.
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, err := h.store.Get(r.Context(), id)
	if err != nil {
		if saga.IsNotFound(err) {
			h.writeError(w, http.StatusNotFound, "saga not found: "+id)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, fromState(state))
}

// deleteSaga handles DELETE /v1/sagas/{id}.
// Returns 405 if the store does not implement saga.Deleter.
//
// The method is named deleteSaga (not delete) to avoid shadowing the builtin.
func (h *Handler) deleteSaga(w http.ResponseWriter, r *http.Request) {
	d, ok := h.store.(saga.Deleter)
	if !ok {
		h.writeError(w, http.StatusMethodNotAllowed, "store does not support deletion")
		return
	}
	id := r.PathValue("id")
	if err := d.Delete(r.Context(), id); err != nil {
		if saga.IsNotFound(err) {
			h.writeError(w, http.StatusNotFound, "saga not found: "+id)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err.Error())
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

// writeJSON marshals v and writes it with the given status code.
// On marshal failure, it logs the error and writes a plain 500 so the operator
// has visibility instead of a silent empty body.
func (h *Handler) writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		h.logger.Error("saga.http: marshal response",
			"error", err,
			"status", code)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(b); err != nil {
		h.logger.Warn("saga.http: write response", "error", err)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, code int, msg string) {
	h.writeJSON(w, code, map[string]string{"error": msg})
}

// Compile-time check that Handler satisfies http.Handler.
var _ http.Handler = (*Handler)(nil)
