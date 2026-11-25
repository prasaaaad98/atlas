// Package api is Atlas's HTTP interface (ADR-001: HTTP + JSON).
//
// Why it exists: clients (the Python SDK, the owner with curl) and later the
// workers talk to the control plane only through these endpoints; no client
// ever writes to Postgres directly.
// Who calls it: cmd/atlas wires Handler into the http.Server.
//
// Invariant: a 2xx is written only after the store call returned, i.e. after
// COMMIT. A crash before the response means the client sees an error and
// retries with the same idempotency key, which is safe.
//
// A1 endpoints:
//
//	POST /pipelines   register a pipeline version   201 new, 200 identical
//	POST /runs        submit a run                  201 new, 200 replay, 409 conflict
//	GET  /runs/{id}   run with its tasks            200, 404
//	GET  /healthz     liveness (checks the database)
//
// A2 endpoints (workers):
//
//	POST /claim                    lease the next READY task    200 claim, 204 nothing ready
//	POST /attempts/{id}/start      LEASED -> RUNNING            200, 409 stale/illegal, 404
//	POST /attempts/{id}/complete   RUNNING -> SUCCEEDED (or FAILED_TERMINAL on a failed blocking check)
//	POST /attempts/{id}/fail       RUNNING/LEASED -> READY (retry) or FAILED_TERMINAL
//
// A repeated report with the same fencing token (lost response) returns 200
// with "replay": true; a report from an attempt that no longer holds the task
// returns 409 stale_attempt.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
	"github.com/prasaaaad98/atlas/internal/store"
)

const maxBody = 1 << 20 // 1 MiB: definitions and run requests are small

type Store interface {
	RegisterPipeline(ctx context.Context, def store.PipelineDef) (store.Pipeline, bool, error)
	SubmitRun(ctx context.Context, req store.SubmitRequest) (store.Run, bool, error)
	GetRun(ctx context.Context, id string) (store.Run, error)
	Ping(ctx context.Context) error
	Claim(ctx context.Context, req store.ClaimRequest) (store.Claim, bool, error)
	Start(ctx context.Context, attemptID string, req store.StartRequest) (store.AttemptResult, error)
	Complete(ctx context.Context, attemptID string, req store.CompleteRequest) (store.AttemptResult, error)
	Fail(ctx context.Context, attemptID string, req store.FailRequest) (store.AttemptResult, error)
}

func Handler(s Store, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pipelines", func(w http.ResponseWriter, r *http.Request) {
		var def store.PipelineDef
		if !decode(w, r, &def) {
			return
		}
		p, created, err := s.RegisterPipeline(r.Context(), def)
		if err != nil {
			writeErr(w, log, err)
			return
		}
		writeJSON(w, statusFor(created), p)
	})
	mux.HandleFunc("POST /runs", func(w http.ResponseWriter, r *http.Request) {
		var req store.SubmitRequest
		if !decode(w, r, &req) {
			return
		}
		run, created, err := s.SubmitRun(r.Context(), req)
		if err != nil {
			writeErr(w, log, err)
			return
		}
		writeJSON(w, statusFor(created), run)
	})
	mux.HandleFunc("GET /runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		run, err := s.GetRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writeErr(w, log, err)
			return
		}
		writeJSON(w, http.StatusOK, run)
	})
	mux.HandleFunc("POST /claim", func(w http.ResponseWriter, r *http.Request) {
		var req store.ClaimRequest
		if !decode(w, r, &req) {
			return
		}
		c, ok, err := s.Claim(r.Context(), req)
		if err != nil {
			writeErr(w, log, err)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, c)
	})
	mux.HandleFunc("POST /attempts/{id}/start", func(w http.ResponseWriter, r *http.Request) {
		var req store.StartRequest
		if decode(w, r, &req) {
			res, err := s.Start(r.Context(), r.PathValue("id"), req)
			writeResult(w, log, res, err)
		}
	})
	mux.HandleFunc("POST /attempts/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		var req store.CompleteRequest
		if decode(w, r, &req) {
			res, err := s.Complete(r.Context(), r.PathValue("id"), req)
			writeResult(w, log, res, err)
		}
	})
	mux.HandleFunc("POST /attempts/{id}/fail", func(w http.ResponseWriter, r *http.Request) {
		var req store.FailRequest
		if decode(w, r, &req) {
			res, err := s.Fail(r.Context(), r.PathValue("id"), req)
			writeResult(w, log, res, err)
		}
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database unreachable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func statusFor(created bool) int {
	if created {
		return http.StatusCreated
	}
	return http.StatusOK
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields() // a misspelt field must fail, not be silently ignored
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{Error: "invalid_json", Detail: err.Error()})
		return false
	}
	return true
}

type errBody struct {
	Error       string `json:"error"`
	Detail      string `json:"detail,omitempty"`
	ActiveRunID string `json:"active_run_id,omitempty"`
}

func writeErr(w http.ResponseWriter, log *slog.Logger, err error) {
	var active *store.ActiveRunError
	switch {
	case errors.As(err, &active):
		writeJSON(w, http.StatusConflict, errBody{Error: "active_run_exists", Detail: err.Error(), ActiveRunID: active.RunID})
	case errors.Is(err, store.ErrActiveRunExists):
		writeJSON(w, http.StatusConflict, errBody{Error: "active_run_exists", Detail: err.Error()})
	case errors.Is(err, store.ErrIdempotencyMismatch):
		writeJSON(w, http.StatusConflict, errBody{Error: "idempotency_key_reused", Detail: err.Error()})
	case errors.Is(err, store.ErrStaleAttempt):
		// Fencing: this attempt no longer holds the task; its result is discarded.
		writeJSON(w, http.StatusConflict, errBody{Error: "stale_attempt", Detail: err.Error()})
	case errors.Is(err, domain.ErrIllegalTransition):
		writeJSON(w, http.StatusConflict, errBody{Error: "illegal_transition", Detail: err.Error()})
	case errors.Is(err, store.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, errBody{Error: "invalid_request", Detail: err.Error()})
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errBody{Error: "not_found", Detail: err.Error()})
	default:
		// Outcome unknown to the client (e.g. commit status unclear): 500, and the
		// client retries with the same idempotency key.
		log.Error("request failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, errBody{Error: "internal"})
	}
}

func writeResult(w http.ResponseWriter, log *slog.Logger, res store.AttemptResult, err error) {
	if err != nil {
		writeErr(w, log, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
