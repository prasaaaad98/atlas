// Entry point for the Atlas control plane binary.
//
// Why it exists: Atlas is a modular monolith, so there is exactly one binary;
// every internal package (api, store, later scheduler/reaper/evaluators) is
// wired together here.
// Who calls it: `make run`, `go run ./cmd/atlas`, tests/integration (as a real
// process it can kill).
//
// Configuration (environment):
//
//	DATABASE_URL  Postgres URL (default: the docker-compose database)
//	ATLAS_ADDR    listen address (default 127.0.0.1:8080; port 0 picks a free port)
//	ATLAS_REAPER_INTERVAL  how often expired leases are reaped (default 5s, ADR-004)
//	ATLAS_SCHEDULER_INTERVAL  how often schedules tick and backfills advance (default 30s, ADR-011)
//	ATLAS_DEADLINE_INTERVAL   how often deadlines are evaluated (default 30s, ADR-012)
//	ATLAS_FRESHNESS_INTERVAL  how often asset freshness is evaluated (default 60s, ADR-013)
//	ATLAS_UPTIME_INTERVAL     how often this process records that it (and its workers) are up (default 60s)
//
// On start it prints "atlas listening on <addr>" once the socket is bound, so
// scripts and tests know when it accepts requests.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prasaaaad98/atlas/internal/api"
	"github.com/prasaaaad98/atlas/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("atlas stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dbURL := env("DATABASE_URL", "postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable")
	addr := env("ATLAS_ADDR", "127.0.0.1:8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	st, err := store.Open(openCtx, dbURL)
	cancel()
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer st.Close()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           api.Handler(st, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	fmt.Printf("atlas listening on %s\n", ln.Addr())

	reapEvery, err := time.ParseDuration(env("ATLAS_REAPER_INTERVAL", "5s"))
	if err != nil || reapEvery <= 0 {
		return fmt.Errorf("ATLAS_REAPER_INTERVAL: %q is not a positive duration", os.Getenv("ATLAS_REAPER_INTERVAL"))
	}
	go reaper(ctx, st, reapEvery, log)
	tickEvery, err := time.ParseDuration(env("ATLAS_SCHEDULER_INTERVAL", "30s"))
	if err != nil || tickEvery <= 0 {
		return fmt.Errorf("ATLAS_SCHEDULER_INTERVAL: %q is not a positive duration", os.Getenv("ATLAS_SCHEDULER_INTERVAL"))
	}
	go scheduler(ctx, st, tickEvery, log)
	dlEvery, err := time.ParseDuration(env("ATLAS_DEADLINE_INTERVAL", "30s"))
	if err != nil || dlEvery <= 0 {
		return fmt.Errorf("ATLAS_DEADLINE_INTERVAL: %q is not a positive duration", os.Getenv("ATLAS_DEADLINE_INTERVAL"))
	}
	go deadlines(ctx, st, dlEvery, log)
	frEvery, err := time.ParseDuration(env("ATLAS_FRESHNESS_INTERVAL", "60s"))
	if err != nil || frEvery <= 0 {
		return fmt.Errorf("ATLAS_FRESHNESS_INTERVAL: %q is not a positive duration", os.Getenv("ATLAS_FRESHNESS_INTERVAL"))
	}
	go freshness(ctx, st, frEvery, log)
	// LISTEN/NOTIFY wake-ups for long-poll claims (POST /claim?wait=N); claims
	// still work, re-checking every second, while this reconnects.
	go st.Listen(ctx, func(err error) { log.Warn("listen for ready tasks", "err", err) })
	upEvery, err := time.ParseDuration(env("ATLAS_UPTIME_INTERVAL", "60s"))
	if err != nil || upEvery <= 0 || upEvery > time.Minute {
		return fmt.Errorf("ATLAS_UPTIME_INTERVAL: %q must be a positive duration up to 1m", os.Getenv("ATLAS_UPTIME_INTERVAL"))
	}
	go uptime(ctx, st, upEvery, log)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		// Graceful stop: finish in-flight requests (each either commits or not).
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	}
	return nil
}

// reaper re-queues tasks whose lease lapsed. A failed pass is logged and
// retried next tick: missing one pass only delays a re-run, never loses one,
// because expiry is a fact in the database, not an in-memory timer.
func reaper(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := st.ReapExpired(ctx, 500)
			if err != nil && ctx.Err() == nil {
				log.Error("reaper pass failed", "err", err)
			} else if n > 0 {
				log.Info("reaped expired leases", "tasks", n)
			}
		}
	}
}

// scheduler submits due scheduled partitions and tops up backfills. Missing a
// pass only delays runs: the cursor and the backfill position live in the
// database, and every submission is idempotent.
func scheduler(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := st.TickSchedules(ctx); err != nil && ctx.Err() == nil {
				log.Error("schedule tick failed", "err", err)
			} else if n > 0 {
				log.Info("scheduled runs submitted", "runs", n)
			}
			if n, err := st.AdvanceBackfills(ctx); err != nil && ctx.Err() == nil {
				log.Error("backfill pass failed", "err", err)
			} else if n > 0 {
				log.Info("backfill runs submitted", "runs", n)
			}
		}
	}
}

// deadlines marks at-risk and missed runs and applies late policies. A missed
// pass only delays detection: a finishing late run is marked by its own
// transition, and the evaluator catches active ones on the next pass.
func deadlines(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rep, err := st.EvaluateDeadlines(ctx)
			if err != nil && ctx.Err() == nil {
				log.Error("deadline pass failed", "err", err)
			} else if rep != (store.DeadlineReport{}) {
				log.Info("deadlines evaluated", "at_risk", rep.AtRisk, "missed", rep.Missed,
					"canceled", rep.Canceled, "fallbacks", rep.Fallbacks)
			}
		}
	}
}

// freshness marks assets late when their expected partition isn't fresh by
// the policy deadline, judged on the database clock.
func freshness(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now, err := st.DBNow(ctx)
			if err == nil {
				var n int
				n, err = st.EvaluateFreshness(ctx, now)
				if n > 0 {
					log.Info("assets late", "partitions", n)
				}
			}
			if err != nil && ctx.Err() == nil {
				log.Error("freshness pass failed", "err", err)
			}
		}
	}
}

// uptime writes one row per minute while this control plane runs, with the
// number of workers that asked for work, so the schedule report can tell
// "the source was late" from "the machine was off".
func uptime(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	host, _ := os.Hostname()
	instance := fmt.Sprintf("%s-%d", host, os.Getpid())
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := st.RecordUptime(ctx, instance, st.FlushWorkersSeen()); err != nil && ctx.Err() == nil {
				log.Error("uptime record failed", "err", err)
			}
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
