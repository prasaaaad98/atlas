// Read models for observability: the metrics snapshot, run timelines, the
// asset freshness grid, and LISTEN/NOTIFY wake-ups for long-poll claims (Slice A9).
//
// Why it exists: every number /metrics and the UI pages show is derived from
// the same tables the guarantees live in, so dashboards can't disagree with
// the state machine.
// Who calls it: internal/api (/metrics, /ui/*, POST /claim?wait=), cmd/atlas (Listen).
package store

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// SlackBuckets (seconds, deadline − finish; negative = late) for the slack histogram.
var SlackBuckets = []float64{-3600, -600, -60, -10, 0, 10, 60, 600, 3600, 21600, 86400}

type MetricsSnapshot struct {
	QueueReady                                map[string]int    // READY and claimable now, by priority class
	QueueDelayed                              map[string]int    // READY but waiting for available_at (backoff), by class
	Running                                   int               // LEASED or RUNNING
	Attempts                                  map[[2]string]int // finished attempts by (status, outcome)
	DeadlineAtRisk, DeadlineMissed, AssetLate int
	Slack                                     []uint64 // cumulative counts per SlackBuckets
	SlackSum                                  float64
	SlackCount                                uint64
	FreshnessLag                              map[string]float64 // asset → seconds since its oldest late partition's deadline
	LimitPaused                               map[string]bool
	LimitPauses                               map[string]int
}

func (s *Store) Metrics(ctx context.Context) (MetricsSnapshot, error) {
	m := MetricsSnapshot{QueueReady: map[string]int{}, QueueDelayed: map[string]int{}, Attempts: map[[2]string]int{},
		FreshnessLag: map[string]float64{}, LimitPaused: map[string]bool{}, LimitPauses: map[string]int{}}
	rows, err := s.pool.Query(ctx, `
		SELECT priority_class, count(*) FILTER (WHERE available_at <= now()), count(*) FILTER (WHERE available_at > now())
		FROM tasks WHERE status = 'READY' GROUP BY 1`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var class string
		var ready, delayed int
		if err := rows.Scan(&class, &ready, &delayed); err != nil {
			rows.Close()
			return m, err
		}
		m.QueueReady[class], m.QueueDelayed[class] = ready, delayed
	}
	rows.Close()
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status IN ('LEASED', 'RUNNING')`).Scan(&m.Running); err != nil {
		return m, err
	}
	rows, err = s.pool.Query(ctx, `
		SELECT status, coalesce(outcome, 'none'), count(*) FROM task_attempts
		WHERE status NOT IN ('LEASED', 'RUNNING') GROUP BY 1, 2`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var st, oc string
		var n int
		if err := rows.Scan(&st, &oc, &n); err != nil {
			rows.Close()
			return m, err
		}
		m.Attempts[[2]string{st, oc}] = n
	}
	rows.Close()
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE type = 'run.deadline_at_risk'), count(*) FILTER (WHERE type = 'run.deadline_missed'),
		       count(*) FILTER (WHERE type = 'asset.late')
		FROM events WHERE type IN ('run.deadline_at_risk', 'run.deadline_missed', 'asset.late')`).Scan(
		&m.DeadlineAtRisk, &m.DeadlineMissed, &m.AssetLate); err != nil {
		return m, err
	}
	// Slack histogram over every finished run with a deadline (durable, monotonic).
	rows, err = s.pool.Query(ctx, `
		WITH slack AS (SELECT extract(epoch FROM deadline_at - finished_at)::float8 AS v FROM workflow_runs
		               WHERE deadline_at IS NOT NULL AND finished_at IS NOT NULL)
		SELECT b, (SELECT count(*) FROM slack WHERE v <= b), (SELECT coalesce(sum(v), 0) FROM slack), (SELECT count(*) FROM slack)
		FROM unnest($1::float8[]) AS b ORDER BY b`, SlackBuckets)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var b float64
		var c uint64
		if err := rows.Scan(&b, &c, &m.SlackSum, &m.SlackCount); err != nil {
			rows.Close()
			return m, err
		}
		m.Slack = append(m.Slack, c)
	}
	rows.Close()
	rows, err = s.pool.Query(ctx, `
		SELECT asset_key, max(extract(epoch FROM now() - late_since))::float8 FROM asset_partitions
		WHERE status = 'late' AND late_since IS NOT NULL GROUP BY 1`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var k string
		var lag float64
		if err := rows.Scan(&k, &lag); err != nil {
			rows.Close()
			return m, err
		}
		m.FreshnessLag[k] = lag
	}
	rows.Close()
	rows, err = s.pool.Query(ctx, `
		SELECT k.key, coalesce(k.paused_until > now(), false),
		       (SELECT count(*) FROM events e WHERE e.type = 'limit_key.paused' AND e.payload->>'key' = k.key)
		FROM limit_keys k`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var k string
		var paused bool
		var n int
		if err := rows.Scan(&k, &paused, &n); err != nil {
			rows.Close()
			return m, err
		}
		m.LimitPaused[k], m.LimitPauses[k] = paused, n
	}
	rows.Close()
	return m, nil
}

// --- run timeline -----------------------------------------------------------------

type TimelineEvent struct {
	At       time.Time `json:"at"`
	Type     string    `json:"type"`
	TaskName string    `json:"task,omitempty"`
	Payload  string    `json:"payload"`
}

type TimelineAttempt struct {
	TaskName      string     `json:"task"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      string     `json:"worker_id"`
	Status        string     `json:"status"`
	Outcome       *string    `json:"outcome,omitempty"`
	ErrorClass    *string    `json:"error_class,omitempty"`
	LeasedAt      time.Time  `json:"leased_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

func (s *Store) Timeline(ctx context.Context, runID string) ([]TimelineEvent, []TimelineAttempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.created_at, e.type, coalesce(t.name, ''), e.payload::text FROM events e LEFT JOIN tasks t ON t.id = e.task_id
		WHERE e.run_id = $1 ORDER BY e.id`, runID)
	if err != nil {
		return nil, nil, err
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (TimelineEvent, error) {
		var e TimelineEvent
		err := r.Scan(&e.At, &e.Type, &e.TaskName, &e.Payload)
		return e, err
	})
	if err != nil {
		return nil, nil, err
	}
	rows, err = s.pool.Query(ctx, `
		SELECT t.name, a.attempt_number, a.worker_id, a.status, a.outcome, a.error_class, a.leased_at, a.started_at, a.finished_at
		FROM task_attempts a JOIN tasks t ON t.id = a.task_id WHERE a.run_id = $1 ORDER BY a.leased_at`, runID)
	if err != nil {
		return nil, nil, err
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (TimelineAttempt, error) {
		var a TimelineAttempt
		err := r.Scan(&a.TaskName, &a.AttemptNumber, &a.WorkerID, &a.Status, &a.Outcome, &a.ErrorClass, &a.LeasedAt, &a.StartedAt, &a.FinishedAt)
		return a, err
	})
	return events, attempts, err
}

// --- LISTEN/NOTIFY wake-ups -------------------------------------------------------

const readyChannel = "atlas_task_ready"

// notifyReady is called inside transactions that make a task READY. Postgres
// delivers NOTIFY only at COMMIT, so a waiter can never wake for a task that
// isn't visible yet; and a rolled-back transaction notifies nobody.
func notifyReady(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, readyChannel)
	return err
}

// readySignal is closed and replaced whenever a notification arrives, so any
// number of waiting claims wake at once (a broadcast).
type readySignal struct {
	mu sync.Mutex
	ch chan struct{}
}

func (r *readySignal) current() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch == nil {
		r.ch = make(chan struct{})
	}
	return r.ch
}

func (r *readySignal) fire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch != nil {
		close(r.ch)
	}
	r.ch = make(chan struct{})
}

// ReadySignal returns a channel closed at the next "task ready" notification.
// Take it BEFORE trying to claim, so a notification that arrives between the
// failed claim and the wait isn't lost.
func (s *Store) ReadySignal() <-chan struct{} { return s.ready.current() }

// Listen holds one dedicated connection on LISTEN atlas_task_ready and fires
// the ready signal per notification, reconnecting with backoff. Without it,
// long-poll claims still work: they re-check every second (see the API).
func (s *Store) Listen(ctx context.Context, onError func(error)) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := s.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		onError(err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

func (s *Store) listenOnce(ctx context.Context) error {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// Take the connection out of the pool for good: a LISTENing session must
	// not be handed to ordinary queries, and is closed when we stop.
	conn := pc.Hijack()
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN "+readyChannel); err != nil {
		return err
	}
	s.ready.fire() // anything that happened while we weren't listening: let waiters re-check
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		s.ready.fire()
	}
}
