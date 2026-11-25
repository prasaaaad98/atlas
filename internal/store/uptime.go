// Uptime record and the schedule report (B7, 14-day schedule run).
//
// Why it exists: the plan's exit item "schedules run for 14 days with measured
// freshness lag" runs on the owner's laptop, which sleeps. A late partition
// can mean "the source published late" or "Atlas wasn't running". The uptime
// record separates the two: every report row says what share of its waiting
// time the control plane was up with at least one worker.
// Who calls it: the uptime loop in cmd/atlas (RecordUptime), the API
// (GET /reports/schedule), tests.
package store

import (
	"context"
	"sync"
	"time"
)

// WorkerSeen collects distinct worker ids that claimed since the last flush.
type WorkerSeen struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func (w *WorkerSeen) Saw(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ids == nil {
		w.ids = map[string]struct{}{}
	}
	w.ids[id] = struct{}{}
}

// Flush returns the count seen since the previous flush and resets it.
func (w *WorkerSeen) Flush() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.ids)
	w.ids = nil
	return n
}

// RecordUptime writes this minute's row (database clock). Several writes in
// one minute keep the highest worker count, so a short flush interval or a
// second control plane can't under-count.
func (s *Store) RecordUptime(ctx context.Context, instance string, workers int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO uptime_minutes (minute, instance, workers) VALUES (date_trunc('minute', now()), $1, $2)
		ON CONFLICT (minute) DO UPDATE SET workers = greatest(uptime_minutes.workers, EXCLUDED.workers)`,
		instance, workers)
	return err
}

// ScheduleReportRow is one scheduled run: how long from the schedule firing
// to the run finishing, how much of that was spent waiting for the source,
// and whether Atlas could have done better (was it up the whole time?).
type ScheduleReportRow struct {
	PartitionKey     string     `json:"partition_key"`
	Status           string     `json:"status"`
	FiredAt          time.Time  `json:"fired_at"` // run created by the schedule tick
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	LagSeconds       *float64   `json:"lag_seconds,omitempty"` // finished − fired
	NotReadyAttempts int        `json:"not_ready_attempts"`    // source answered "not published yet"
	LastNotReadyAt   *time.Time `json:"last_not_ready_at,omitempty"`
	FirstSuccessAt   *time.Time `json:"first_success_at,omitempty"` // first successful attempt of any task
	// Published between LastNotReadyAt and FirstSuccessAt (when both exist).
	UptimeMinutes  int     `json:"uptime_minutes"`  // minutes with control plane + ≥1 worker, fired..finished
	WindowMinutes  int     `json:"window_minutes"`  // whole minutes fired..finished (or now)
	UptimeCoverage float64 `json:"uptime_coverage"` // UptimeMinutes / WindowMinutes
	FullyUp        bool    `json:"fully_up"`        // coverage ≥ 0.99: the lag is the source's (and Atlas's), not downtime
	TotalAttempts  int     `json:"total_attempts"`
}

func (s *Store) ScheduleReport(ctx context.Context, pipeline string, since time.Time) ([]ScheduleReportRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.partition_key, r.status, r.created_at, r.finished_at,
		       extract(epoch FROM r.finished_at - r.created_at)::float8,
		       count(a.id) FILTER (WHERE a.error_class = 'source_not_ready'),
		       max(a.finished_at) FILTER (WHERE a.error_class = 'source_not_ready'),
		       min(a.finished_at) FILTER (WHERE a.status = 'SUCCEEDED'),
		       count(a.id),
		       (SELECT count(*) FROM uptime_minutes u
		        WHERE u.workers > 0 AND u.minute >= date_trunc('minute', r.created_at)
		          AND u.minute < coalesce(r.finished_at, now())),
		       greatest(1, ceil(extract(epoch FROM coalesce(r.finished_at, now()) - date_trunc('minute', r.created_at)) / 60))::int
		FROM workflow_runs r LEFT JOIN task_attempts a ON a.run_id = r.id
		WHERE r.pipeline_name = $1 AND r.trigger = 'schedule' AND r.created_at >= $2
		GROUP BY r.id ORDER BY r.partition_key`, pipeline, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScheduleReportRow
	for rows.Next() {
		var x ScheduleReportRow
		if err := rows.Scan(&x.PartitionKey, &x.Status, &x.FiredAt, &x.FinishedAt, &x.LagSeconds, &x.NotReadyAttempts,
			&x.LastNotReadyAt, &x.FirstSuccessAt, &x.TotalAttempts, &x.UptimeMinutes, &x.WindowMinutes); err != nil {
			return nil, err
		}
		x.UptimeCoverage = float64(min(x.UptimeMinutes, x.WindowMinutes)) / float64(x.WindowMinutes)
		x.FullyUp = x.UptimeCoverage >= 0.99
		out = append(out, x)
	}
	return out, rows.Err()
}
