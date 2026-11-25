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

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
