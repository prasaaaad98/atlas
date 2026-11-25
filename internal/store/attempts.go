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
//   - Fencing: start/heartbeat/complete/fail must present the attempt id AND
//     fencing token that currently hold the task, while its lease is live on
//     the database clock; anything else is ErrStaleAttempt (heartbeat answers
//     cancel_requested instead). A lease that lapsed is dead even before the
//     reaper (reaper.go) has re-queued the task, so there is no window in
//     which an expired attempt can still win.
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
	defaultLeaseSeconds = 30 // ADR-004; workers heartbeat every lease/3
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
	Checkpoint        json.RawMessage `json:"checkpoint"` // last heartbeat checkpoint of a previous attempt, or null
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
	var checkpoint []byte
	var limitKey *string
	full := []string{} // limit keys found at their limit in this claim; skip their tasks
	for {
		if len(full) > maxKeysPerClaim {
			return Claim{}, false, nil // many saturated keys: report "nothing ready", the worker polls again
		}
		if _, err := tx.Exec(ctx, `SAVEPOINT candidate`); err != nil {
			return Claim{}, false, err
		}
		// No double claims: FOR NO KEY UPDATE locks the chosen row until COMMIT
		// (it conflicts with itself, so no second claimer can hold it), and SKIP
		// LOCKED makes a concurrent claimer take the next unlocked row instead of
		// waiting. NO KEY UPDATE rather than UPDATE: it doesn't conflict with the
		// KEY SHARE locks that foreign-key inserts (attempts, events) take, which
		// would otherwise deadlock (ADR-015). Under READ COMMITTED, Postgres
		// re-checks `status = 'READY'` on the locked row's latest version, so a
		// row another claimer just leased is never returned. The ORDER BY matches
		// the tasks_claim partial index: live before ad-hoc before backfill, then
		// earliest deadline, then FIFO. available_at <= now() and paused_until
		// use the database clock, the only clock Atlas trusts.
		err = tx.QueryRow(ctx, `
			SELECT t.id, t.run_id, t.name, t.attempt_count, t.fencing_token, t.priority_class, t.asset_outputs,
			       t.checkpoint, t.limit_key
			FROM tasks t
			WHERE t.status = 'READY' AND t.available_at <= now()
			  -- coalesce: a NULL array would make "NOT (key = ANY(NULL))" NULL and hide every keyed task.
			  AND (t.limit_key IS NULL OR (NOT (t.limit_key = ANY(coalesce($1::text[], '{}')))
			       AND NOT EXISTS (SELECT 1 FROM limit_keys k WHERE k.key = t.limit_key AND k.paused_until > now())))
			ORDER BY t.priority_rank, t.deadline_at NULLS LAST, t.available_at, t.id
			LIMIT 1
			FOR NO KEY UPDATE OF t SKIP LOCKED`, full,
		).Scan(&c.TaskID, &c.RunID, &c.TaskName, &attemptCount, &token, &c.PriorityClass, &c.AssetOutputs,
			&checkpoint, &limitKey)
		if errors.Is(err, pgx.ErrNoRows) {
			return Claim{}, false, nil
		}
		if err != nil {
			return Claim{}, false, err
		}
		if limitKey == nil {
			break
		}
		admitted, err := admit(ctx, tx, *limitKey)
		if err != nil {
			return Claim{}, false, err
		}
		if admitted {
			break
		}
		// Key at its limit: release this task's row lock (rolling back to the
		// savepoint undoes the lock taken after it) and look past that key.
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT candidate`); err != nil {
			return Claim{}, false, err
		}
		full = append(full, *limitKey)
	}

	c.AttemptNumber, c.FencingToken = attemptCount+1, token+1
	if err := tx.QueryRow(ctx, `
		-- clock_timestamp(), not now(): now() is this transaction's START, which
		-- can be well before the lease is granted when the claim waited for a
		-- limit-key lock. The lease (and the history) start when it's granted.
		INSERT INTO task_attempts (task_id, run_id, attempt_number, fencing_token, worker_id, status,
		                           leased_at, lease_expires_at, limit_key)
		VALUES ($1, $2, $3, $4, $5, 'LEASED', clock_timestamp(), clock_timestamp() + make_interval(secs => $6), $7)
		RETURNING id, lease_expires_at`,
		c.TaskID, c.RunID, c.AttemptNumber, c.FencingToken, req.WorkerID, req.LeaseSeconds, limitKey,
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
	if checkpoint != nil {
		c.Checkpoint = checkpoint
	}
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
	leaseLive      bool // lease_expires_at > now() on the database clock
	policy         domain.RetryPolicy
	limitKey       *string
}

// lockByAttempt locks the task row an attempt belongs to (lock order: task first).
func lockByAttempt(ctx context.Context, tx pgx.Tx, attemptID string) (heldTask, error) {
	if !uuidRE.MatchString(attemptID) {
		return heldTask{}, fmt.Errorf("attempt %q: %w", attemptID, ErrNotFound)
	}
	var h heldTask
	var st string
	var policy []byte
	err := tx.QueryRow(ctx, `
		SELECT t.id, t.run_id, t.status, t.current_attempt_id, t.fencing_token, t.max_attempts,
		       a.attempt_number, a.status, a.fencing_token,
		       coalesce(t.lease_expires_at > now(), false), t.retry_policy, t.limit_key
		FROM task_attempts a JOIN tasks t ON t.id = a.task_id
		WHERE a.id = $1
		FOR NO KEY UPDATE OF t`, attemptID,
	).Scan(&h.taskID, &h.runID, &st, &h.currentAttempt, &h.token, &h.maxAttempts,
		&h.attemptNumber, &h.attemptStatus, &h.attemptToken, &h.leaseLive, &policy, &h.limitKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return heldTask{}, fmt.Errorf("attempt %s: %w", attemptID, ErrNotFound)
	}
	if err != nil {
		return heldTask{}, err
	}
	h.status = domain.State(st)
	h.policy, err = decodePolicy(policy)
	return h, err
}

// holds reports whether (attemptID, token) is the task's current, live lease
// holder. This is the fencing check: the token was incremented by the claim
// that created the current attempt, so any older attempt's token is smaller;
// and an attempt whose lease lapsed on the database clock no longer holds the
// task even if the reaper hasn't run yet.
func (h heldTask) holds(attemptID string, token int64) bool {
	return h.currentAttempt != nil && *h.currentAttempt == attemptID && h.token == token && h.leaseLive
}

type HeartbeatRequest struct {
	FencingToken int64           `json:"fencing_token"`
	Checkpoint   json.RawMessage `json:"checkpoint,omitempty"` // omitted or null: keep the stored one
	LeaseSeconds int             `json:"lease_seconds,omitempty"`
}

type HeartbeatResult struct {
	CancelRequested bool       `json:"cancel_requested"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at,omitempty"`
}

// Heartbeat extends a live lease and optionally stores a checkpoint. An
// attempt that no longer holds the task (replaced, reaped, canceled, lapsed)
// gets cancel_requested=true and changes nothing, so a stale worker can never
// overwrite a newer attempt's checkpoint or revive its own lease.
func (s *Store) Heartbeat(ctx context.Context, attemptID string, req HeartbeatRequest) (HeartbeatResult, error) {
	if req.LeaseSeconds == 0 {
		req.LeaseSeconds = defaultLeaseSeconds
	}
	if req.LeaseSeconds < minLeaseSeconds || req.LeaseSeconds > maxLeaseSeconds {
		return HeartbeatResult{}, fmt.Errorf("%w: lease_seconds must be %d..%d", ErrInvalid, minLeaseSeconds, maxLeaseSeconds)
	}
	cp := req.Checkpoint
	if len(cp) > 0 && string(cp) != "null" {
		var obj map[string]any
		if len(cp) > 8192 || json.Unmarshal(cp, &obj) != nil {
			return HeartbeatResult{}, fmt.Errorf("%w: checkpoint must be a JSON object of at most 8 KB", ErrInvalid)
		}
	} else {
		cp = nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return HeartbeatResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	h, err := lockByAttempt(ctx, tx, attemptID)
	if err != nil {
		return HeartbeatResult{}, err
	}
	if !h.holds(attemptID, req.FencingToken) || (h.status != domain.Leased && h.status != domain.Running) {
		return HeartbeatResult{CancelRequested: true}, nil
	}
	var until time.Time
	// Fenced write: the task row is locked and holds() was checked under that lock.
	if err := tx.QueryRow(ctx, `
		UPDATE tasks SET lease_expires_at = now() + make_interval(secs => $2),
		       checkpoint = coalesce($3::jsonb, checkpoint), updated_at = now()
		WHERE id = $1 RETURNING lease_expires_at`, h.taskID, req.LeaseSeconds, cp).Scan(&until); err != nil {
		return HeartbeatResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE task_attempts SET lease_expires_at = $2 WHERE id = $1`, attemptID, until); err != nil {
		return HeartbeatResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, err
	}
	return HeartbeatResult{LeaseExpiresAt: &until}, nil
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
		if err == nil {
			// Same transaction (ADR-003 decision 6): dependents become READY, or
			// are canceled after a failed blocking check.
			err = afterTaskEnded(ctx, tx, h.runID, h.taskID, next)
		}
		return next, err
	})
}

func (s *Store) Fail(ctx context.Context, attemptID string, req FailRequest) (AttemptResult, error) {
	if req.ErrorClass == "" || !outcomes[req.Outcome] || req.RetryAfterSeconds < 0 || req.RetryAfterSeconds > 86400 {
		return AttemptResult{}, fmt.Errorf("%w: fail needs error_class, outcome in succeeded|failed_definite|unknown, retry_after_seconds 0..86400", ErrInvalid)
	}
	return s.transition(ctx, attemptID, req.FencingToken, "FAILED", func(ctx context.Context, tx pgx.Tx, h heldTask) (domain.State, error) {
		// Classifier (ADR-005): the worker's hint, vetoed by terminal error
		// classes and the task's policy. The attempt number comes from the
		// locked row, so AttemptsRemain can't be stale.
		retryable := h.policy.Retryable(req.ErrorClass, req.Retryable)
		ev := domain.FailEvent(h.attemptNumber, h.maxAttempts, retryable)
		next, err := domain.Next(h.status, ev)
		if err != nil {
			return "", err
		}
		delay := h.policy.Backoff(h.attemptNumber, s.jitter(), req.RetryAfterSeconds).Seconds()
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts SET status = 'FAILED', outcome = $2, error_class = $3, error_detail = $4,
			       retryable = $5, finished_at = now()
			WHERE id = $1`, attemptID, req.Outcome, req.ErrorClass, truncate(req.Detail, 8000), retryable)
		if err == nil {
			// Retry: READY again, claimable only after the backoff (database clock).
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET status = $2, current_attempt_id = NULL, lease_expires_at = NULL,
				       available_at = CASE WHEN $2 = 'READY' THEN now() + make_interval(secs => $3) ELSE available_at END,
				       updated_at = now()
				WHERE id = $1`, h.taskID, string(next), delay)
		}
		if err == nil {
			err = taskEvent(ctx, tx, h.runID, h.taskID, "task.failed", map[string]any{
				"attempt_id": attemptID, "error_class": req.ErrorClass, "outcome": req.Outcome,
				"worker_retryable": req.Retryable, "retryable": retryable, "next_state": next,
				"retry_in_seconds": map[bool]any{true: delay, false: nil}[next == domain.Ready]})
		}
		if err == nil && req.ErrorClass == rateLimitedErrorClass && h.limitKey != nil {
			// The source said "too many requests": stop every claim on this key,
			// not only this task's retry (ADR-008).
			err = pauseKey(ctx, tx, h.runID, h.taskID, *h.limitKey, req.RetryAfterSeconds)
		}
		if err == nil {
			err = afterTaskEnded(ctx, tx, h.runID, h.taskID, next)
		}
		return next, err
	})
}

// transition runs one attempt report: lock, fence, apply, derive run status.
// A report repeated with the same token after it was applied (e.g. the HTTP
// response was lost) is answered as a replay instead of an error (ADR-001).
// A deadlock with CancelRun (which locks many tasks) is retried whole.
func (s *Store) transition(ctx context.Context, attemptID string, token int64, wantAttemptStatus string,
	apply func(context.Context, pgx.Tx, heldTask) (domain.State, error)) (AttemptResult, error) {
	var res AttemptResult
	err := retryDeadlock(ctx, func() error {
		var err error
		res, err = s.transitionOnce(ctx, attemptID, token, wantAttemptStatus, apply)
		return err
	})
	return res, err
}

func (s *Store) transitionOnce(ctx context.Context, attemptID string, token int64, wantAttemptStatus string,
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
// UPDATE still conflicts with itself, so transitions of one run serialise here. In
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
	// A run that has started stays RUNNING while a task waits for a retry:
	// DeriveRunStatus sees only states, and a re-queued task looks READY like
	// one that never ran.
	if derived == domain.RunPending && current == string(domain.RunRunning) {
		derived = domain.RunRunning
	}
	if string(derived) != current {
		// finished_at uses clock_timestamp(): the moment this transition is
		// applied, not the transaction's start, so deadline slack is honest.
		if _, err := tx.Exec(ctx, `
			UPDATE workflow_runs SET status = $2, updated_at = now(),
			       finished_at = CASE WHEN $2 IN ('SUCCEEDED', 'FAILED', 'CANCELED') THEN clock_timestamp() END
			WHERE id = $1`, runID, string(derived)); err != nil {
			return "", err
		}
		if err := missedAtFinish(ctx, tx, runID); err != nil {
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

// decodePolicy reads tasks.retry_policy; NULL means the default policy.
func decodePolicy(raw []byte) (domain.RetryPolicy, error) {
	if raw == nil {
		return domain.DefaultRetryPolicy, nil
	}
	var p domain.RetryPolicy
	err := json.Unmarshal(raw, &p)
	return p, err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
