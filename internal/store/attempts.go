// Claim, start, complete and fail: the worker side of a task's life (Slice A2).
//
// Why it exists: these four transactions are the only code that moves a task
// out of READY. Each one validates the move with domain.Next, writes the task,
// its attempt row, an event and the derived run status in ONE transaction.
// Who calls it: internal/api (POST /claim, /attempts/{id}/start|complete|fail).
//
// Invariants protected:
//   - A task is never leased to two live attempts: the claim locks the task
//     row with FOR UPDATE SKIP LOCKED, and task_attempts_one_live backs it up.
//   - Every attempt is recorded, never overwritten: one task_attempts row per
//     claim; later steps only fill in its own finish fields.
//   - Fencing: start/complete/fail must present the attempt id AND fencing
//     token that currently hold the task; anything else is ErrStaleAttempt.
//     (Leases and the reaper that expire stale attempts arrive in A3.)
//   - Terminal states are final: domain.Next refuses any event on them.
//   - Run status is derived, never set: syncRunStatus recomputes it from all
//     task states in the same transaction.
//
// Lock order (deadlock freedom): every transaction here locks the TASK row
// first and the RUN row second. Two transactions can therefore never wait for
// each other in a cycle.
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

// ErrStaleAttempt: the attempt no longer holds the task (wrong fencing token,
// or another attempt has since claimed it). HTTP 409.
var ErrStaleAttempt = errors.New("stale attempt: fencing token or attempt does not hold the task")

const (
	defaultLeaseSeconds = 300
	minLeaseSeconds     = 10
	maxLeaseSeconds     = 3600
)

// Outcome classes for an attempt (CLAUDE.md): what Atlas knows about the
// external effect of an attempt.
var outcomes = map[string]bool{"succeeded": true, "failed_definite": true, "unknown": true}

type ClaimRequest struct {
	WorkerID     string `json:"worker_id"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

// Claim is what a worker needs to run one task attempt.
type Claim struct {
	AttemptID         string          `json:"attempt_id"`
	FencingToken      int64           `json:"fencing_token"`
	AttemptNumber     int             `json:"attempt_number"`
	LeaseExpiresAt    time.Time       `json:"lease_expires_at"`
	TaskID            string          `json:"task_id"`
	TaskName          string          `json:"task"`
	RunID             string          `json:"run_id"`
	PipelineName      string          `json:"pipeline"`
	PipelineVersion   int             `json:"pipeline_version"`
	PartitionKey      string          `json:"partition_key"`
	DataIntervalStart time.Time       `json:"data_interval_start"`
	DataIntervalEnd   time.Time       `json:"data_interval_end"`
	PriorityClass     string          `json:"priority_class"`
	AssetOutputs      []string        `json:"asset_outputs"`
	Checkpoint        json.RawMessage `json:"checkpoint"` // heartbeat details arrive in A3; null until then
}

// Claim leases the next READY task in claim order, or returns ok=false when
// nothing is ready.
func (s *Store) Claim(ctx context.Context, req ClaimRequest) (Claim, bool, error) {
	if req.WorkerID == "" || len(req.WorkerID) > 200 {
		return Claim{}, false, fmt.Errorf("%w: worker_id is required (max 200 chars)", ErrInvalid)
	}
	if req.LeaseSeconds == 0 {
		req.LeaseSeconds = defaultLeaseSeconds
	}
	if req.LeaseSeconds < minLeaseSeconds || req.LeaseSeconds > maxLeaseSeconds {
		return Claim{}, false, fmt.Errorf("%w: lease_seconds must be %d..%d", ErrInvalid, minLeaseSeconds, maxLeaseSeconds)
	}
	next, err := domain.Next(domain.Ready, domain.EvClaim)
	if err != nil {
		return Claim{}, false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Claim{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var c Claim
	var attemptCount int
	var token int64
	// No double claims: FOR NO KEY UPDATE locks the chosen row until COMMIT
	// (it conflicts with itself, so no second claimer can hold it), and SKIP
	// LOCKED makes a concurrent claimer take the next unlocked row instead of
	// waiting. NO KEY UPDATE rather than UPDATE: it doesn't conflict with the
	// KEY SHARE locks that foreign-key inserts (attempts, events) take, which
	// would otherwise deadlock (see syncRunStatus). Under READ COMMITTED, Postgres re-checks `status = 'READY'`
	// on the locked row's latest version, so a row another claimer just leased
	// is never returned. The ORDER BY matches the tasks_claim partial index:
	// live before ad-hoc before backfill, then earliest deadline, then FIFO.
	// available_at <= now() uses the database clock, the only clock Atlas trusts.
	err = tx.QueryRow(ctx, `
		SELECT id, run_id, name, attempt_count, fencing_token, priority_class, asset_outputs
		FROM tasks
		WHERE status = 'READY' AND available_at <= now()
		ORDER BY priority_rank, deadline_at NULLS LAST, available_at, id
		LIMIT 1
		FOR NO KEY UPDATE SKIP LOCKED`,
	).Scan(&c.TaskID, &c.RunID, &c.TaskName, &attemptCount, &token, &c.PriorityClass, &c.AssetOutputs)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, err
	}

	c.AttemptNumber, c.FencingToken = attemptCount+1, token+1
	if err := tx.QueryRow(ctx, `
		INSERT INTO task_attempts (task_id, run_id, attempt_number, fencing_token, worker_id, status, lease_expires_at)
		VALUES ($1, $2, $3, $4, $5, 'LEASED', now() + make_interval(secs => $6))
		RETURNING id, lease_expires_at`,
		c.TaskID, c.RunID, c.AttemptNumber, c.FencingToken, req.WorkerID, req.LeaseSeconds,
	).Scan(&c.AttemptID, &c.LeaseExpiresAt); err != nil {
		return Claim{}, false, err
	}
	// The row is locked by this transaction, so these increments can't race.
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status = $2, attempt_count = $3, fencing_token = $4, current_attempt_id = $5,
		       lease_expires_at = $6, updated_at = now()
		WHERE id = $1`,
		c.TaskID, string(next), c.AttemptNumber, c.FencingToken, c.AttemptID, c.LeaseExpiresAt); err != nil {
		return Claim{}, false, err
	}
	if err := taskEvent(ctx, tx, c.RunID, c.TaskID, "task.claimed", map[string]any{
		"attempt_id": c.AttemptID, "attempt_number": c.AttemptNumber, "fencing_token": c.FencingToken,
		"worker_id": req.WorkerID}); err != nil {
		return Claim{}, false, err
	}
	if _, err := syncRunStatus(ctx, tx, c.RunID); err != nil {
		return Claim{}, false, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT pipeline_name, pipeline_version, partition_key, data_interval_start, data_interval_end
		FROM workflow_runs WHERE id = $1`, c.RunID,
	).Scan(&c.PipelineName, &c.PipelineVersion, &c.PartitionKey, &c.DataIntervalStart, &c.DataIntervalEnd); err != nil {
		return Claim{}, false, err
	}
	c.Checkpoint = json.RawMessage("null")
	if err := tx.Commit(ctx); err != nil {
		return Claim{}, false, err
	}
	return c, true, nil
}

// CheckResult mirrors the Python task contract (pipelines/.../core/task.py).
type CheckResult struct {
	CheckName string          `json:"check_name"`
	Severity  string          `json:"severity"`
	Passed    bool            `json:"passed"`
	Metrics   json.RawMessage `json:"metrics,omitempty"`
}

type StartRequest struct {
	FencingToken int64 `json:"fencing_token"`
}

type CompleteRequest struct {
	FencingToken int64 `json:"fencing_token"`
	// Recorded in the task.completed event until the assets tables arrive (A8).
	Materializations []json.RawMessage `json:"materializations"`
	Checks           []CheckResult     `json:"checks"`
}

type FailRequest struct {
	FencingToken      int64   `json:"fencing_token"`
	ErrorClass        string  `json:"error_class"`
	Detail            string  `json:"detail,omitempty"`
	Outcome           string  `json:"outcome"`
	Retryable         bool    `json:"retryable"`
	RetryAfterSeconds float64 `json:"retry_after_seconds,omitempty"`
}

// AttemptResult is returned by start/complete/fail.
type AttemptResult struct {
	AttemptID     string           `json:"attempt_id"`
	AttemptStatus string           `json:"attempt_status"`
	TaskStatus    domain.State     `json:"task_status"`
	RunStatus     domain.RunStatus `json:"run_status"`
	Replay        bool             `json:"replay"` // true: this exact report was already applied
}

// heldTask is the task row locked through one of its attempts.
type heldTask struct {
	taskID, runID  string
	status         domain.State
	currentAttempt *string
	token          int64
	attemptNumber  int
	maxAttempts    int
	attemptStatus  string
	attemptToken   int64
}

// lockByAttempt locks the task row an attempt belongs to (lock order: task first).
func lockByAttempt(ctx context.Context, tx pgx.Tx, attemptID string) (heldTask, error) {
	if !uuidRE.MatchString(attemptID) {
		return heldTask{}, fmt.Errorf("attempt %q: %w", attemptID, ErrNotFound)
	}
	var h heldTask
	var st string
	err := tx.QueryRow(ctx, `
		SELECT t.id, t.run_id, t.status, t.current_attempt_id, t.fencing_token, t.max_attempts,
		       a.attempt_number, a.status, a.fencing_token
		FROM task_attempts a JOIN tasks t ON t.id = a.task_id
		WHERE a.id = $1
		FOR NO KEY UPDATE OF t`, attemptID,
	).Scan(&h.taskID, &h.runID, &st, &h.currentAttempt, &h.token, &h.maxAttempts,
		&h.attemptNumber, &h.attemptStatus, &h.attemptToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return heldTask{}, fmt.Errorf("attempt %s: %w", attemptID, ErrNotFound)
	}
	h.status = domain.State(st)
	return h, err
}

// holds reports whether (attemptID, token) is the task's current lease holder.
// This is the fencing check: the token was incremented by the claim that
// created the current attempt, so any older attempt's token is smaller.
func (h heldTask) holds(attemptID string, token int64) bool {
	return h.currentAttempt != nil && *h.currentAttempt == attemptID && h.token == token
}

func (s *Store) Start(ctx context.Context, attemptID string, req StartRequest) (AttemptResult, error) {
	return s.transition(ctx, attemptID, req.FencingToken, "RUNNING", func(ctx context.Context, tx pgx.Tx, h heldTask) (domain.State, error) {
		next, err := domain.Next(h.status, domain.EvStart)
		if err != nil {
			return "", err
		}
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET status = 'RUNNING', started_at = now() WHERE id = $1`, attemptID)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE tasks SET status = $2, updated_at = now() WHERE id = $1`, h.taskID, string(next))
		}
		if err == nil {
			err = taskEvent(ctx, tx, h.runID, h.taskID, "task.started", map[string]any{"attempt_id": attemptID})
		}
		return next, err
	})
}

func (s *Store) Complete(ctx context.Context, attemptID string, req CompleteRequest) (AttemptResult, error) {
	for _, c := range req.Checks {
		if c.CheckName == "" || (c.Severity != "blocking" && c.Severity != "warn") {
			return AttemptResult{}, fmt.Errorf("%w: check %q severity %q", ErrInvalid, c.CheckName, c.Severity)
		}
	}
	ev, attemptStatus := domain.EvComplete, "SUCCEEDED"
	var failed []string
	for _, c := range req.Checks {
		if c.Severity == "blocking" && !c.Passed {
			failed = append(failed, c.CheckName)
		}
	}
	if len(failed) > 0 {
		// Re-fetching identical bad data won't fix it: terminal, no retry (ADR-003).
		ev, attemptStatus = domain.EvCompleteCheckFailed, "CHECK_FAILED"
	}
	return s.transition(ctx, attemptID, req.FencingToken, attemptStatus, func(ctx context.Context, tx pgx.Tx, h heldTask) (domain.State, error) {
		next, err := domain.Next(h.status, ev)
		if err != nil {
			return "", err
		}
		errClass := any(nil)
		if len(failed) > 0 {
			errClass = "data_check_failed"
		}
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts SET status = $2, outcome = 'succeeded', error_class = $3, finished_at = now()
			WHERE id = $1`, attemptID, attemptStatus, errClass)
		if err == nil {
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET status = $2, current_attempt_id = NULL, lease_expires_at = NULL, updated_at = now()
				WHERE id = $1`, h.taskID, string(next))
		}
		if err == nil {
			mats := req.Materializations
			if mats == nil {
				mats = []json.RawMessage{}
			}
			err = taskEvent(ctx, tx, h.runID, h.taskID, "task.completed", map[string]any{
				"attempt_id": attemptID, "status": attemptStatus, "failed_blocking_checks": failed,
				"materializations": mats, "checks": req.Checks})
		}
		return next, err
	})
}

func (s *Store) Fail(ctx context.Context, attemptID string, req FailRequest) (AttemptResult, error) {
	if req.ErrorClass == "" || !outcomes[req.Outcome] || req.RetryAfterSeconds < 0 || req.RetryAfterSeconds > 86400 {
		return AttemptResult{}, fmt.Errorf("%w: fail needs error_class, outcome in succeeded|failed_definite|unknown, retry_after_seconds 0..86400", ErrInvalid)
	}
	return s.transition(ctx, attemptID, req.FencingToken, "FAILED", func(ctx context.Context, tx pgx.Tx, h heldTask) (domain.State, error) {
		// The attempt number comes from the locked row, so AttemptsRemain can't be stale.
		ev := domain.FailEvent(h.attemptNumber, h.maxAttempts, req.Retryable)
		next, err := domain.Next(h.status, ev)
		if err != nil {
			return "", err
		}
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts SET status = 'FAILED', outcome = $2, error_class = $3, error_detail = $4,
			       retryable = $5, finished_at = now()
			WHERE id = $1`, attemptID, req.Outcome, req.ErrorClass, truncate(req.Detail, 8000), req.Retryable)
		if err == nil {
			// Retry: READY again, not before retry_after (backoff policies arrive in A4).
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET status = $2, current_attempt_id = NULL, lease_expires_at = NULL,
				       available_at = CASE WHEN $2 = 'READY' THEN now() + make_interval(secs => $3) ELSE available_at END,
				       updated_at = now()
				WHERE id = $1`, h.taskID, string(next), req.RetryAfterSeconds)
		}
		if err == nil {
			err = taskEvent(ctx, tx, h.runID, h.taskID, "task.failed", map[string]any{
				"attempt_id": attemptID, "error_class": req.ErrorClass, "outcome": req.Outcome,
				"retryable": req.Retryable, "next_state": next})
		}
		return next, err
	})
}

// transition runs one attempt report: lock, fence, apply, derive run status.
// A report repeated with the same token after it was applied (e.g. the HTTP
// response was lost) is answered as a replay instead of an error (ADR-001).
func (s *Store) transition(ctx context.Context, attemptID string, token int64, wantAttemptStatus string,
	apply func(context.Context, pgx.Tx, heldTask) (domain.State, error)) (AttemptResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AttemptResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	h, err := lockByAttempt(ctx, tx, attemptID)
	if err != nil {
		return AttemptResult{}, err
	}
	res := AttemptResult{AttemptID: attemptID}
	if !h.holds(attemptID, token) {
		if h.attemptToken == token && h.attemptStatus == wantAttemptStatus {
			res.AttemptStatus, res.TaskStatus, res.Replay = h.attemptStatus, h.status, true
			res.RunStatus, err = currentRunStatus(ctx, tx, h.runID)
			return res, err
		}
		return AttemptResult{}, ErrStaleAttempt
	}
	if h.attemptStatus == wantAttemptStatus { // start repeated while still holding the lease
		res.AttemptStatus, res.TaskStatus, res.Replay = h.attemptStatus, h.status, true
		res.RunStatus, err = currentRunStatus(ctx, tx, h.runID)
		return res, err
	}
	next, err := apply(ctx, tx, h)
	if err != nil {
		return AttemptResult{}, err
	}
	run, err := syncRunStatus(ctx, tx, h.runID)
	if err != nil {
		return AttemptResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AttemptResult{}, err
	}
	return AttemptResult{AttemptID: attemptID, AttemptStatus: wantAttemptStatus, TaskStatus: next, RunStatus: run}, nil
}

// syncRunStatus recomputes the run's status from all of its tasks and stores
// it if it changed. It locks the run row (lock order: task, then run) with
// FOR NO KEY UPDATE, not FOR UPDATE: every attempt/event insert earlier in the
// transaction takes a KEY SHARE lock on the run row through its foreign key,
// and FOR UPDATE conflicts with KEY SHARE, so two transitions of the same run
// would deadlock (found by TestConcurrentClaimersNeverDoubleClaim). NO KEY
// UPDATE still conflicts with itself, so transitions of one run serialise here,
// so concurrent transitions of the same run's tasks are serialised here. In
// READ COMMITTED the task SELECT below takes a new snapshot after the lock is
// granted, so it sees every task transition committed before ours: two
// workers finishing a run's last two tasks at once can't both see "one still
// running" and leave the run RUNNING forever.
func syncRunStatus(ctx context.Context, tx pgx.Tx, runID string) (domain.RunStatus, error) {
	var current string
	if err := tx.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1 FOR NO KEY UPDATE`, runID).Scan(&current); err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx, `SELECT status FROM tasks WHERE run_id = $1`, runID)
	if err != nil {
		return "", err
	}
	states, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.State, error) {
		var s string
		err := r.Scan(&s)
		return domain.State(s), err
	})
	if err != nil {
		return "", err
	}
	derived, err := domain.DeriveRunStatus(states)
	if err != nil {
		return "", err
	}
	if string(derived) != current {
		if _, err := tx.Exec(ctx, `UPDATE workflow_runs SET status = $2, updated_at = now() WHERE id = $1`,
			runID, string(derived)); err != nil {
			return "", err
		}
		payload, _ := json.Marshal(map[string]string{"from": current, "to": string(derived)})
		if _, err := tx.Exec(ctx, `INSERT INTO events (run_id, type, payload) VALUES ($1, 'run.status_changed', $2)`,
			runID, payload); err != nil {
			return "", err
		}
	}
	return derived, nil
}

func currentRunStatus(ctx context.Context, tx pgx.Tx, runID string) (domain.RunStatus, error) {
	var st string
	err := tx.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1`, runID).Scan(&st)
	return domain.RunStatus(st), err
}

func taskEvent(ctx context.Context, tx pgx.Tx, runID, taskID, typ string, payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (run_id, task_id, type, payload) VALUES ($1, $2, $3, $4)`, runID, taskID, typ, b)
	return err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
