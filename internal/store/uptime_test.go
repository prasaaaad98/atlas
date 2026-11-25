// Tests for the uptime record and the schedule report (B7 14-day run).
package store

import (
	"context"
	"testing"
	"time"
)

func TestUptimeOneRowPerMinuteKeepsMaxWorkers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	s.workersSeen.Saw("a")
	s.workersSeen.Saw("b")
	s.workersSeen.Saw("a")
	if n := s.FlushWorkersSeen(); n != 2 {
		t.Fatalf("distinct workers %d, want 2", n)
	}
	if n := s.FlushWorkersSeen(); n != 0 {
		t.Fatal("flush didn't reset")
	}
	for _, w := range []int{2, 0, 1} { // same minute, several writes
		if err := s.RecordUptime(ctx, "test", w); err != nil {
			t.Fatal(err)
		}
	}
	var rows, workers int
	_ = s.pool.QueryRow(ctx, `SELECT count(*), max(workers) FROM uptime_minutes`).Scan(&rows, &workers)
	if rows != 1 || workers != 2 {
		t.Fatalf("rows %d workers %d, want 1 and 2", rows, workers)
	}
}

// A scheduled run that waited twice for the source, then succeeded. The
// report counts the waits, brackets the publication time, and measures how
// much of the window Atlas was up with a worker.
func TestScheduleReportSeparatesSourceWaitsFromDowntime(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, retryDef(nil, 5))
	run := mustSubmit(t, s, "flaky", "2026-11-27", "live") // trigger "schedule"
	for range 2 {
		c := mustClaim(t, s, "w1")
		failAttempt(t, s, c, "source_not_ready", "succeeded", true, 0)
	}
	c := mustClaim(t, s, "w1")
	_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken})
	if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
		t.Fatal(err)
	}
	// Pretend the run was created 10 minutes ago and Atlas was up for 6 of those
	// minutes (test-only SQL standing in for the clock and the uptime loop).
	_, _ = s.pool.Exec(ctx, `UPDATE workflow_runs SET created_at = now() - interval '10 minutes', finished_at = now() WHERE id = $1`, run.ID)
	_, _ = s.pool.Exec(ctx, `INSERT INTO uptime_minutes (minute, instance, workers)
		SELECT date_trunc('minute', now()) - make_interval(mins => g), 'test', 1 FROM generate_series(1, 6) g`)

	rows, err := s.ScheduleReport(ctx, "flaky", time.Now().AddDate(0, 0, -1))
	if err != nil || len(rows) != 1 {
		t.Fatalf("report %+v %v", rows, err)
	}
	r := rows[0]
	if r.NotReadyAttempts != 2 || r.TotalAttempts != 3 || r.LastNotReadyAt == nil || r.FirstSuccessAt == nil ||
		r.FirstSuccessAt.Before(*r.LastNotReadyAt) || r.Status != "SUCCEEDED" {
		t.Fatalf("row %+v", r)
	}
	if r.WindowMinutes < 10 || r.WindowMinutes > 11 || r.UptimeMinutes != 6 || r.FullyUp {
		t.Fatalf("uptime %d/%d fully_up=%v", r.UptimeMinutes, r.WindowMinutes, r.FullyUp)
	}
	if r.LagSeconds == nil || *r.LagSeconds < 590 {
		t.Fatalf("lag %v", r.LagSeconds)
	}
}
