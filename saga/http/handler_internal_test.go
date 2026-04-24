package http

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

// unencodable is a value whose MarshalJSON always fails, letting us force
// writeJSON down its error branch without relying on store implementation
// quirks.
type unencodable struct{}

func (unencodable) MarshalJSON() ([]byte, error) {
	return nil, errBoom
}

var errBoom = &marshalErr{msg: "boom"}

type marshalErr struct{ msg string }

func (e *marshalErr) Error() string { return e.msg }

// TestWriteJSON_MarshalErrorLogsAndRespondsWith500 verifies that when the
// response payload cannot be marshaled, writeJSON:
//   - logs an error (not silently swallows it),
//   - responds with 500, and
//   - writes a non-empty JSON error body (not the empty-body pre-fix behavior).
func TestWriteJSON_MarshalErrorLogsAndRespondsWith500(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	h := &Handler{logger: logger}
	w := httptest.NewRecorder()

	h.writeJSON(w, 200, unencodable{})

	if w.Code != 500 {
		t.Fatalf("status: want 500, got %d", w.Code)
	}
	if w.Body.Len() == 0 {
		t.Fatal("body must not be empty on marshal failure")
	}
	if !strings.Contains(w.Body.String(), "internal server error") {
		t.Errorf("body: want to contain 'internal server error', got %q", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: want application/json, got %q", ct)
	}
	if !strings.Contains(buf.String(), "marshal response") {
		t.Errorf("logger should have recorded 'marshal response'; got %q", buf.String())
	}
}
