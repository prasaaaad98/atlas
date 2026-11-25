// Deadline evaluator: at-risk and missed runs, late policies (Slice A7, ADR-012).
//
// Why it exists: deadlines are best effort. Atlas orders claims by deadline
// (EDF, in Claim), and this evaluator makes every miss visible: it is
// detected, recorded once, and acted on per the pipeline's late policy.
// Who calls it: the deadline loop in cmd/atlas (ATLAS_DEADLINE_INTERVAL),
// syncRunStatus (a run that finishes late), tests.
//
// Invariants protected:
//   - Every run that is still active after its deadline, or finishes after
//     it, gets exactly one run.deadline_missed event: deadline_missed_at is
//     set by a guarded UPDATE (`... IS NULL`) under the run's row lock, by
//     whichever comes first, the evaluator or the finishing transition.
//   - At-risk fires at most once per run, before the miss when durations are known.
//   - Database clock only.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/prasaaaad98/atlas/internal/domain"
)

const minHistorySamples = 3 // below this, a task's p95 isn't trusted; the declared estimate is used

// missedAtFinish runs inside syncRunStatus right after a run turned terminal
// (the run row is locked there): a run that finished after its deadline is a miss.
func missedAtFinish(ctx context.Context, tx pgx.Tx, runID string) error {
	var lateBy float64
	err := tx.QueryRow(ctx, `
		UPDATE workflow_runs SET deadline_missed_at = finished_at
		WHERE id = $1 AND finished_at IS NOT NULL AND deadline_at IS NOT NULL
		  AND finished_at > deadline_at AND deadline_missed_at IS NULL
		RETURNING extract(epoch FROM finished_at - deadline_at)`, runID).Scan(&lateBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	b, _ := json.Marshal(map[string]any{"detected": "at_finish", "late_by_seconds": lateBy})
	_, err = tx.Exec(ctx, `INSERT INTO events (run_id, type, payload) VALUES ($1, 'run.deadline_missed', $2)`, runID, b)
	return err
}

type DeadlineReport struct {
	AtRisk, Missed, Canceled, Fallbacks int
}

// EvaluateDeadlines marks missed and at-risk runs and applies late policies.
func (s *Store) EvaluateDeadlines(ctx context.Context) (DeadlineReport, error) {
	var rep DeadlineReport
	missed, err := s.markMissed(ctx)
	if err != nil {
		return rep, err
	}
	rep.Missed = len(missed)
	// Late policies run after the miss is committed: CancelRun and SubmitRun are
	// their own transactions (CancelRun locks tasks before the run row, so it
	// must not run while we hold a run row lock).
	for _, m := range missed {
		if m.rule == nil {
			continue
		}
		if m.rule.LatePolicy == "cancel" {
			if _, err := s.CancelRun(ctx, m.id); err != nil {
				return rep, err
			}
			rep.Canceled++
		}
		if m.rule.FallbackPipeline != "" {
			fb, _, err := s.SubmitRun(ctx, SubmitRequest{IdempotencyKey: "fallback:" + m.id,
				PipelineName: m.rule.FallbackPipeline, PartitionKey: m.partition, Trigger: "manual", PriorityClass: "live"})
			if err != nil && !errors.Is(err, ErrActiveRunExists) {
				// Recorded, not fatal: the miss itself is already recorded.
				_ = s.runEvent(ctx, m.id, "run.fallback_failed", map[string]any{"pipeline": m.rule.FallbackPipeline, "error": err.Error()})
				continue
			}
			if err == nil {
				if _, err := s.pool.Exec(ctx, `UPDATE workflow_runs SET fallback_run_id = $2 WHERE id = $1`, m.id, fb.ID); err != nil {
					return rep, err
				}
				_ = s.runEvent(ctx, m.id, "run.fallback_submitted", map[string]any{"fallback_run_id": fb.ID, "pipeline": m.rule.FallbackPipeline})
				rep.Fallbacks++
			}
		}
	}
	n, err := s.markAtRisk(ctx)
	rep.AtRisk = n
	return rep, err
}

type missedRun struct {
	id, partition string
	rule          *domain.DeadlineRule
}

func (s *Store) markMissed(ctx context.Context) ([]missedRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	// Guarded UPDATE under the row lock: set once, whoever comes first (see header).
	// SKIP LOCKED: a run whose row is locked is mid-transition; next pass.
	rows, err := tx.Query(ctx, `
		WITH due AS (
		    SELECT id FROM workflow_runs
		    WHERE status IN ('PENDING', 'RUNNING') AND deadline_at < now() AND deadline_missed_at IS NULL
		    ORDER BY deadline_at LIMIT 500
		    FOR NO KEY UPDATE SKIP LOCKED)
		UPDATE workflow_runs r SET deadline_missed_at = now()
		FROM due WHERE r.id = due.id
		RETURNING r.id, r.partition_key, r.pipeline_name, r.pipeline_version, extract(epoch FROM now() - r.deadline_at)`)
	if err != nil {
		return nil, err
	}
	type row struct {
		id, partition, pipeline string
		version                 int
		lateBy                  float64
	}
	var list []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.partition, &r.pipeline, &r.version, &r.lateBy); err != nil {
			rows.Close()
			return nil, err
		}
		list = append(list, r)
	}
	rows.Close()
	var out []missedRun
	for _, r := range list {
		b, _ := json.Marshal(map[string]any{"detected": "evaluator", "late_by_seconds": r.lateBy})
		if _, err := tx.Exec(ctx, `INSERT INTO events (run_id, type, payload) VALUES ($1, 'run.deadline_missed', $2)`, r.id, b); err != nil {
			return nil, err
		}
		var def PipelineDef
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT definition FROM pipelines WHERE name = $1 AND version = $2`, r.pipeline, r.version).Scan(&raw); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(raw, &def)
		out = append(out, missedRun{id: r.id, partition: r.partition, rule: def.Deadline})
	}
	return out, tx.Commit(ctx)
}

// markAtRisk flags active runs whose expected remaining work no longer fits
// before the deadline: now + critical_path(remaining p95 durations) > deadline.
func (s *Store) markAtRisk(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, pipeline_name, deadline_at, now() FROM workflow_runs
		WHERE status IN ('PENDING', 'RUNNING') AND deadline_at IS NOT NULL AND deadline_at >= now()
		  AND deadline_at_risk_at IS NULL ORDER BY deadline_at LIMIT 500`)
	if err != nil {
		return 0, err
	}
	type cand struct {
		id, pipeline  string
		deadline, now time.Time
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.id, &c.pipeline, &c.deadline, &c.now); err != nil {
			rows.Close()
			return 0, err
		}
		cands = append(cands, c)
	}
	rows.Close()
	history := map[string]map[string]float64{}
	n := 0
	for _, c := range cands {
		if _, ok := history[c.pipeline]; !ok {
			h, err := s.p95Durations(ctx, c.pipeline)
			if err != nil {
				return n, err
			}
			history[c.pipeline] = h
		}
		remaining, estimated, err := s.expectedRemaining(ctx, c.id, history[c.pipeline])
		if err != nil {
			return n, err
		}
		if c.now.Add(time.Duration(remaining * float64(time.Second))).Before(c.deadline) {
			continue
		}
		b, _ := json.Marshal(map[string]any{"expected_remaining_seconds": remaining, "estimated": estimated,
			"deadline_at": c.deadline.UTC()})
		tag, err := s.pool.Exec(ctx, `
			WITH marked AS (
			    UPDATE workflow_runs SET deadline_at_risk_at = now()
			    WHERE id = $1 AND deadline_at_risk_at IS NULL AND status IN ('PENDING', 'RUNNING')
			    RETURNING id)
			INSERT INTO events (run_id, type, payload) SELECT id, 'run.deadline_at_risk', $2 FROM marked`, c.id, b)
		if err != nil {
			return n, err
		}
		n += int(tag.RowsAffected())
	}
	return n, nil
}

// p95Durations: per task name, the 95th percentile of successful attempt
// durations (start → finish) over the pipeline's last 30 days, if enough samples.
func (s *Store) p95Durations(ctx context.Context, pipeline string) (map[string]float64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.name, percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM a.finished_at - a.started_at)), count(*)
		FROM workflow_runs r JOIN tasks t ON t.run_id = r.id JOIN task_attempts a ON a.task_id = t.id
		WHERE r.pipeline_name = $1 AND r.created_at > now() - interval '30 days'
		  AND a.status = 'SUCCEEDED' AND a.started_at IS NOT NULL
		GROUP BY t.name`, pipeline)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var name string
		var p95 float64
		var samples int
		if err := rows.Scan(&name, &p95, &samples); err != nil {
			return nil, err
		}
		if samples >= minHistorySamples {
			out[name] = p95
		}
	}
	return out, rows.Err()
}

// expectedRemaining = critical path over unfinished tasks; a running task
// counts only its remaining part. estimated=true if any task used its declared
// estimate (or had none) instead of measured history.
func (s *Store) expectedRemaining(ctx context.Context, runID string, p95 map[string]float64) (float64, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.name, t.status, coalesce(t.estimate_seconds, 0),
		       coalesce(extract(epoch FROM now() - a.started_at), 0),
		       coalesce((SELECT array_agg(p.name) FROM task_edges e JOIN tasks p ON p.id = e.parent_task_id
		                 WHERE e.child_task_id = t.id), '{}')
		FROM tasks t LEFT JOIN task_attempts a ON a.id = t.current_attempt_id
		WHERE t.run_id = $1`, runID)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	var tasks []domain.PathTask
	estimated := false
	for rows.Next() {
		var name, status string
		var estimate, elapsed float64
		var deps []string
		if err := rows.Scan(&name, &status, &estimate, &elapsed, &deps); err != nil {
			return 0, false, err
		}
		d, ok := p95[name]
		if !ok {
			d, estimated = estimate, true
		}
		rem := d
		switch domain.State(status) {
		case domain.Succeeded, domain.FailedTerminal, domain.Canceled:
			rem = 0
		case domain.Running:
			rem = max(0, d-elapsed)
		}
		tasks = append(tasks, domain.PathTask{Name: name, Deps: deps, Remaining: rem})
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	return domain.CriticalPath(tasks), estimated, nil
}

func (s *Store) runEvent(ctx context.Context, runID, typ string, payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("event payload: %w", err)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO events (run_id, type, payload) VALUES ($1, $2, $3)`, runID, typ, b)
	return err
}
