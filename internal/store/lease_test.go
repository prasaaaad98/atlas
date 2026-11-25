// Integration tests for leases, heartbeats, checkpoints and the reaper (Slice A3).
//
// Time: leases are judged on the database clock. Instead of sleeping for a
// whole lease, tests move one task's lease into the past with expireLease()
// (test-only SQL, equivalent to the clock passing it). Production code never
// writes lease_expires_at except through claim and heartbeat.
//
// These prove:
//   - a crashed worker's task is re-run (reaper → READY → new attempt);
//   - a paused worker that resumes after expiry can't complete, heartbeat or
//     fail, and the new attempt's result stands (the SIGSTOP scenario);
//   - an expired-but-not-yet-reaped attempt is already fenced out;
//   - a heartbeat extends the lease; a stale heartbeat changes nothing;
//   - a checkpoint survives into the next attempt's claim;
//   - expiries count as attempts: max_attempts reached → FAILED_TERMINAL;
//   - the reaper skips a task row another transaction holds.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func expireLease(t *testing.T, s *Store, taskID string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
}

func reap(t *testing.T, s *Store) int {
	t.Helper()
	n, err := s.ReapExpired(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCrashedWorkerTaskIsReRun(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	mustSubmit(t, s, "wide", "2026-11-24", "live")

	dead := mustClaim(t, s, "w-crashed")
	_, _ = s.Start(ctx, dead.AttemptID, StartRequest{FencingToken: dead.FencingToken})
	if n := reap(t, s); n != 0 {
		t.Fatalf("reaped %d live leases", n)
	}
	expireLease(t, s, dead.TaskID)
	if n := reap(t, s); n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	var st, outcome string
	_ = s.pool.QueryRow(ctx, `SELECT status, outcome FROM task_attempts WHERE id = $1`, dead.AttemptID).Scan(&st, &outcome)
	if st != "LEASE_EXPIRED" || outcome != "unknown" {
		t.Fatalf("expired attempt recorded as %s/%s, want LEASE_EXPIRED/unknown", st, outcome)
	}
	again := mustClaim(t, s, "w2")
	if again.TaskID != dead.TaskID || again.AttemptNumber != 2 || again.FencingToken != 2 {
		t.Fatalf("re-run claim = %+v", again)
	}
}

// The SIGSTOP scenario: worker 1 stops (no heartbeats), its lease lapses, the
// task is re-run by worker 2, then worker 1 wakes up and reports.
func TestPausedWorkerCannotOverwriteNewAttempt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	run := mustSubmit(t, s, "wide", "2026-11-24", "live")

	paused := mustClaim(t, s, "w1")
	_, _ = s.Start(ctx, paused.AttemptID, StartRequest{FencingToken: 1})
	expireLease(t, s, paused.TaskID)
	reap(t, s)
	fresh := mustClaim(t, s, "w2")
	_, _ = s.Start(ctx, fresh.AttemptID, StartRequest{FencingToken: fresh.FencingToken})

	// Worker 1 resumes: every report is refused, its heartbeat says stop.
	if hb, err := s.Heartbeat(ctx, paused.AttemptID, HeartbeatRequest{FencingToken: 1, Checkpoint: json.RawMessage(`{"stale":true}`)}); err != nil || !hb.CancelRequested {
		t.Fatalf("stale heartbeat: %+v %v", hb, err)
	}
	if _, err := s.Complete(ctx, paused.AttemptID, CompleteRequest{FencingToken: 1}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale complete: %v", err)
	}
	if _, err := s.Fail(ctx, paused.AttemptID, FailRequest{FencingToken: 1, ErrorClass: "x", Outcome: "unknown"}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale fail: %v", err)
	}
	// The new attempt's result stands.
	if r, err := s.Complete(ctx, fresh.AttemptID, CompleteRequest{FencingToken: fresh.FencingToken}); err != nil || r.RunStatus != domain.RunSucceeded {
		t.Fatalf("fresh complete: %+v %v", r, err)
	}
	var cp []byte
	_ = s.pool.QueryRow(ctx, `SELECT checkpoint FROM tasks WHERE id = $1`, fresh.TaskID).Scan(&cp)
	if cp != nil {
		t.Fatalf("stale heartbeat wrote a checkpoint: %s (run %s)", cp, run.ID)
	}
}

func TestExpiredLeaseIsFencedBeforeTheReaperRuns(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	c := mustClaim(t, s, "w1")
	_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: 1})
	expireLease(t, s, c.TaskID) // lapsed, not yet reaped
	if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: 1}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("complete on a lapsed lease: %v, want ErrStaleAttempt", err)
	}
	if hb, _ := s.Heartbeat(ctx, c.AttemptID, HeartbeatRequest{FencingToken: 1}); !hb.CancelRequested {
		t.Fatal("a heartbeat revived a lapsed lease")
	}
}

func TestHeartbeatExtendsLeaseAndCheckpointReachesNextAttempt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	c := mustClaim(t, s, "w1")
	if string(c.Checkpoint) != "null" {
		t.Fatalf("first claim checkpoint = %s, want null", c.Checkpoint)
	}
	_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: 1})
	hb, err := s.Heartbeat(ctx, c.AttemptID, HeartbeatRequest{FencingToken: 1, LeaseSeconds: 600,
		Checkpoint: json.RawMessage(`{"last_chunk_done": 2}`)})
	if err != nil || hb.CancelRequested || hb.LeaseExpiresAt.Before(time.Now().Add(500*time.Second)) {
		t.Fatalf("heartbeat: %+v %v", hb, err)
	}
	// A heartbeat without a checkpoint keeps the stored one.
	_, _ = s.Heartbeat(ctx, c.AttemptID, HeartbeatRequest{FencingToken: 1})
	if n := reap(t, s); n != 0 {
		t.Fatal("reaped a task with a live, extended lease")
	}
	expireLease(t, s, c.TaskID) // the worker is killed after the heartbeat
	reap(t, s)
	next := mustClaim(t, s, "w2")
	var got map[string]int
	if err := json.Unmarshal(next.Checkpoint, &got); err != nil || got["last_chunk_done"] != 2 {
		t.Fatalf("next attempt checkpoint = %s", next.Checkpoint)
	}
	for _, bad := range []string{`[1,2]`, `"x"`, `{"a":"` + string(make([]byte, 9000)) + `"}`} {
		if _, err := s.Heartbeat(ctx, next.AttemptID, HeartbeatRequest{FencingToken: next.FencingToken, Checkpoint: json.RawMessage(bad)}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("checkpoint %.10q: err=%v, want ErrInvalid", bad, err)
		}
	}
}

func TestExpiriesCountAsAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 2))
	run := mustSubmit(t, s, "wide", "2026-11-24", "live")
	for range 2 {
		c := mustClaim(t, s, "w")
		expireLease(t, s, c.TaskID)
		reap(t, s)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Tasks[0].Status != domain.FailedTerminal || got.Status != domain.RunFailed {
		t.Fatalf("after 2 expiries with max_attempts 2: task %s run %s", got.Tasks[0].Status, got.Status)
	}
}

func TestReaperSkipsALockedTask(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	c := mustClaim(t, s, "w1")
	expireLease(t, s, c.TaskID)

	tx, err := s.pool.Begin(ctx) // a report in flight holds the task row
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT 1 FROM tasks WHERE id = $1 FOR NO KEY UPDATE`, c.TaskID); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { n, _ := s.ReapExpired(ctx, 10); done <- n }()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("reaped a locked task: %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reaper blocked on a locked task row")
	}
	_ = tx.Rollback(ctx)
	if n := reap(t, s); n != 1 {
		t.Fatalf("after unlock reaped %d, want 1", n)
	}
}
