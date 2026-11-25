// DAG readiness, fail-fast dependents and run cancellation (Slice A5, ADR-002).
//
// Why it exists: a task's dependents must become claimable exactly when all
// of their parents have succeeded, and never after a parent failed. Both
// updates happen INSIDE the transaction that ends the parent (ADR-003
// decision 6), so a crash can't leave a child BLOCKED behind a finished parent.
// Who calls it: attempts.go (Complete, Fail), reaper.go, CancelRun.
//
// Lock order: the parent task row is already locked by the caller; children
// are locked next (in id order), the run row last (syncRunStatus). Two parents
// of one child completing at once both lock the child, so the second sees the
// first's committed SUCCEEDED and readies the child exactly once.
// CancelRun locks all of a run's tasks in id order, which can still form a
// cycle with a completing parent; Postgres detects it, aborts one side, and
// retryDeadlock re-runs that transaction.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func children(ctx context.Context, tx pgx.Tx, taskID string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT child_task_id FROM task_edges WHERE parent_task_id = $1 ORDER BY child_task_id`, taskID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// readyChildren: after taskID SUCCEEDED, each BLOCKED child whose parents
// have all succeeded becomes READY (domain EvDepsSatisfied).
func readyChildren(ctx context.Context, tx pgx.Tx, runID, taskID string) error {
	kids, err := children(ctx, tx, taskID)
	if err != nil {
		return err
	}
	for _, kid := range kids {
		var st string
		// Lock the child before counting its parents (see the file header).
		if err := tx.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1 FOR NO KEY UPDATE`, kid).Scan(&st); err != nil {
			return err
		}
		if domain.State(st) != domain.Blocked {
			continue // already READY, or CANCELED by a failed sibling parent
		}
		var pending int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM task_edges e JOIN tasks p ON p.id = e.parent_task_id
			WHERE e.child_task_id = $1 AND p.status <> 'SUCCEEDED'`, kid).Scan(&pending); err != nil {
			return err
		}
		if pending > 0 {
			continue
		}
		next, err := domain.Next(domain.Blocked, domain.EvDepsSatisfied)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, available_at = now(), updated_at = now() WHERE id = $1`,
			kid, string(next)); err != nil {
			return err
		}
		if err := taskEvent(ctx, tx, runID, kid, "task.ready", map[string]any{"after": taskID}); err != nil {
			return err
		}
		if err := notifyReady(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// cancelDependents: after taskID ended FAILED_TERMINAL or CANCELED, every
// BLOCKED descendant becomes CANCELED (domain EvDepFailed), breadth first.
// "Failed" is reserved for tasks that had a failed attempt (ADR-003 decision 4).
func cancelDependents(ctx context.Context, tx pgx.Tx, runID, taskID string) error {
	queue := []string{taskID}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		kids, err := children(ctx, tx, parent)
		if err != nil {
			return err
		}
		for _, kid := range kids {
			var st string
			if err := tx.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1 FOR NO KEY UPDATE`, kid).Scan(&st); err != nil {
				return err
			}
			if domain.State(st) != domain.Blocked {
				continue
			}
			next, err := domain.Next(domain.Blocked, domain.EvDepFailed)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, updated_at = now() WHERE id = $1`, kid, string(next)); err != nil {
				return err
			}
			if err := taskEvent(ctx, tx, runID, kid, "task.dep_failed", map[string]any{"parent": parent}); err != nil {
				return err
			}
			queue = append(queue, kid)
		}
	}
	return nil
}

// afterTaskEnded applies the DAG consequences of a task reaching `state`.
func afterTaskEnded(ctx context.Context, tx pgx.Tx, runID, taskID string, state domain.State) error {
	switch state {
	case domain.Succeeded:
		return readyChildren(ctx, tx, runID, taskID)
	case domain.FailedTerminal, domain.Canceled:
		return cancelDependents(ctx, tx, runID, taskID)
	}
	return nil
}

// CancelRun cancels every non-terminal task of a run in one transaction
// (ADR-003 decision 3 and 6). A leased or running attempt ends CANCELED with
// outcome `unknown`; its worker learns it from the next heartbeat
// (cancel_requested) and its completion is refused (it no longer holds the task).
// Cancelling a finished run changes nothing and returns it.
func (s *Store) CancelRun(ctx context.Context, runID string) (Run, error) {
	if !uuidRE.MatchString(runID) {
		return Run{}, fmt.Errorf("run %q: %w", runID, ErrNotFound)
	}
	err := retryDeadlock(ctx, func() error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
		// Lock order: tasks (id order) before the run row (in syncRunStatus).
		rows, err := tx.Query(ctx, `
			SELECT id, status, current_attempt_id FROM tasks WHERE run_id = $1 ORDER BY id FOR NO KEY UPDATE`, runID)
		if err != nil {
			return err
		}
		type tk struct {
			id, status string
			attempt    *string
		}
		var tasks []tk
		for rows.Next() {
			var t tk
			if err := rows.Scan(&t.id, &t.status, &t.attempt); err != nil {
				rows.Close()
				return err
			}
			tasks = append(tasks, t)
		}
		rows.Close()
		if len(tasks) == 0 {
			return fmt.Errorf("run %s: %w", runID, ErrNotFound)
		}
		changed := false
		for _, t := range tasks {
			if domain.State(t.status).Terminal() {
				continue
			}
			next, err := domain.Next(domain.State(t.status), domain.EvCancel)
			if err != nil {
				return err
			}
			if t.attempt != nil {
				if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status = 'CANCELED', outcome = 'unknown',
					error_class = 'canceled', finished_at = now() WHERE id = $1`, *t.attempt); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, current_attempt_id = NULL, lease_expires_at = NULL,
				updated_at = now() WHERE id = $1`, t.id, string(next)); err != nil {
				return err
			}
			if err := taskEvent(ctx, tx, runID, t.id, "task.canceled", map[string]any{"from": t.status}); err != nil {
				return err
			}
			changed = true
		}
		if changed {
			if _, err := tx.Exec(ctx, `INSERT INTO events (run_id, type) VALUES ($1, 'run.cancel_requested')`, runID); err != nil {
				return err
			}
			if _, err := syncRunStatus(ctx, tx, runID); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	})
	if err != nil {
		return Run{}, err
	}
	return s.GetRun(ctx, runID)
}

// retryDeadlock re-runs a whole transaction that Postgres aborted as a
// deadlock victim (40P01) or serialization failure (40001). Safe because the
// aborted transaction changed nothing.
func retryDeadlock(ctx context.Context, fn func() error) error {
	var err error
	for range 5 {
		err = fn()
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || (pg.Code != "40P01" && pg.Code != "40001") {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}
