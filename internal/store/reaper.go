// Reaper: re-queues tasks whose lease lapsed on the database clock (Slice A3).
//
// Why it exists: a worker can crash (kill -9), hang, lose its network or be
// paused (GC, laptop sleep, SIGSTOP). It can't report, so Atlas must notice by
// time: lease_expires_at < now(). Execution is therefore at-least-once; the
// fencing check in attempts.go makes the old attempt's late report harmless.
// Who calls it: the reaper loop in cmd/atlas (every ATLAS_REAPER_INTERVAL), tests.
//
// Invariants protected:
//   - Every expiry is recorded: the attempt ends LEASE_EXPIRED with outcome
//     `unknown` (we can't know what the worker did), and a task.lease_expired event.
//   - The reaper never blocks a worker that is completing: it uses
//     SKIP LOCKED, so a task row locked by a concurrent complete/fail/heartbeat is
//     left for the next pass (and that report then decides, under fencing).
//   - One task per transaction, locking task then run, the same order as
//     every other transition (ADR-015).
package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/prasaaaad98/atlas/internal/domain"
)

// ReapExpired handles up to limit expired leases and returns how many it re-queued or failed.
func (s *Store) ReapExpired(ctx context.Context, limit int) (int, error) {
	n := 0
	for n < limit {
		done, err := s.reapOne(ctx)
		if err != nil {
			return n, err
		}
		if !done {
			return n, nil
		}
		n++
	}
	return n, nil
}

func (s *Store) reapOne(ctx context.Context) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var taskID, runID, attemptID string
	var status string
	var attempts, maxAttempts int
	// Database clock only (ADR-004). SKIP LOCKED: a task whose row is locked
	// right now is being reported on; never wait for it.
	err = tx.QueryRow(ctx, `
		SELECT id, run_id, current_attempt_id, status, attempt_count, max_attempts
		FROM tasks
		WHERE status IN ('LEASED', 'RUNNING') AND lease_expires_at < now()
		ORDER BY lease_expires_at
		LIMIT 1
		FOR NO KEY UPDATE SKIP LOCKED`).Scan(&taskID, &runID, &attemptID, &status, &attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A lapsed lease is always retryable when attempts remain: its outcome is
	// unknown, not failed (domain.LeaseExpiredEvent).
	ev := domain.LeaseExpiredEvent(attempts, maxAttempts)
	next, err := domain.Next(domain.State(status), ev)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts SET status = 'LEASE_EXPIRED', outcome = 'unknown', error_class = 'lease_expired',
		       finished_at = now()
		WHERE id = $1`, attemptID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status = $2, current_attempt_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1`, taskID, string(next)); err != nil {
		return false, err
	}
	if err := taskEvent(ctx, tx, runID, taskID, "task.lease_expired", map[string]any{
		"attempt_id": attemptID, "attempt_number": attempts, "next_state": next}); err != nil {
		return false, err
	}
	if _, err := syncRunStatus(ctx, tx, runID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
