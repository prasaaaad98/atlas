// Integration tests for claim/start/complete/fail (Slice A2), against real
// PostgreSQL. These prove:
//   - 20 concurrent claimers x 1,000 tasks: every task claimed exactly once,
//     repeated over several rounds (no double claims);
//   - live tasks are claimed before backfill tasks, whatever their age;
//   - fencing: a report with an old token, or from a replaced attempt, is rejected;
//     a repeated report (lost HTTP response) is a replay, not a second effect;
//   - every attempt is a new row; a retry gets attempt 2 and token 2;
//   - two workers completing a run's last two tasks at once leave the run
//     SUCCEEDED (the run-row lock in syncRunStatus), repeated many times;
//   - the claim query uses the tasks_claim partial index.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prasaaaad98/atlas/internal/domain"
)

// wideDef: n independent tasks, all READY at submit.
func wideDef(name string, n, maxAttempts int) PipelineDef {
	d := PipelineDef{Name: name, PartitionKind: "daily", PartitionTZ: "UTC"}
	for i := range n {
		d.Tasks = append(d.Tasks, domain.TaskSpec{Name: fmt.Sprintf("t%03d", i), MaxAttempts: maxAttempts})
	}
	return d
}

func mustRegister(t *testing.T, s *Store, d PipelineDef) {
	t.Helper()
	if _, _, err := s.RegisterPipeline(context.Background(), d); err != nil {
		t.Fatalf("register %s: %v", d.Name, err)
	}
}

func mustSubmit(t *testing.T, s *Store, pipeline, partition, class string) Run {
	t.Helper()
	trigger := map[string]string{"live": "schedule", "backfill": "backfill", "adhoc": "manual"}[class]
	r, _, err := s.SubmitRun(context.Background(), SubmitRequest{
		IdempotencyKey: pipeline + ":" + partition, PipelineName: pipeline, PartitionKey: partition,
		Trigger: trigger, PriorityClass: class,
	})
	if err != nil {
		t.Fatalf("submit %s %s: %v", pipeline, partition, err)
	}
	return r
}

func mustClaim(t *testing.T, s *Store, worker string) Claim {
	t.Helper()
	c, ok, err := s.Claim(context.Background(), ClaimRequest{WorkerID: worker})
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	return c
}

func TestClaimStartCompleteHappyPath(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 1))
	run := mustSubmit(t, s, "wide", "2026-11-24", "live")

	c := mustClaim(t, s, "w1")
	if c.RunID != run.ID || c.AttemptNumber != 1 || c.FencingToken != 1 || c.PartitionKey != "2026-11-24" ||
		c.PipelineName != "wide" || c.TaskName != "t000" || !c.LeaseExpiresAt.After(c.DataIntervalStart) {
		t.Fatalf("claim = %+v", c)
	}
	if got, _ := s.GetRun(ctx, run.ID); got.Status != domain.RunRunning || got.Tasks[0].Status != domain.Leased {
		t.Fatalf("after claim: run %s task %s", got.Status, got.Tasks[0].Status)
	}
	if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w2"}); ok {
		t.Fatal("a leased task was claimed again")
	}
	if r, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: 1}); err != nil || r.TaskStatus != domain.Running {
		t.Fatalf("start: %+v %v", r, err)
	}
	res, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: 1,
		Checks: []CheckResult{{CheckName: "schema", Severity: "blocking", Passed: true}}})
	if err != nil || res.TaskStatus != domain.Succeeded || res.RunStatus != domain.RunSucceeded {
		t.Fatalf("complete: %+v %v", res, err)
	}

	var attempts int
	var types string
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE run_id = $1 AND status = 'SUCCEEDED' AND outcome = 'succeeded'`, run.ID).Scan(&attempts)
	_ = s.pool.QueryRow(ctx, `SELECT string_agg(type, ',' ORDER BY id) FROM events WHERE run_id = $1`, run.ID).Scan(&types)
	if attempts != 1 || types != "run.created,task.claimed,run.status_changed,task.started,task.completed,run.status_changed" {
		t.Fatalf("attempts=%d events=%s", attempts, types)
	}
}

func TestBlockingCheckFailureIsTerminalNotRetried(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 3))
	run := mustSubmit(t, s, "wide", "2026-11-24", "live")
	c := mustClaim(t, s, "w1")
	_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken})
	res, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken,
		Checks: []CheckResult{{CheckName: "interval_completeness", Severity: "blocking", Passed: false},
			{CheckName: "source_revision", Severity: "warn", Passed: false}}})
	if err != nil || res.TaskStatus != domain.FailedTerminal || res.RunStatus != domain.RunFailed || res.AttemptStatus != "CHECK_FAILED" {
		t.Fatalf("complete with failed check: %+v %v (run %s)", res, err, run.ID)
	}
	if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w1"}); ok {
		t.Fatal("a data_check_failed task was retried despite attempts remaining")
	}
}

func TestLiveIsClaimedBeforeBackfill(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 2, 1))
	// Backfill submitted first (older available_at), live and ad-hoc later.
	for _, p := range []string{"2023-01-01", "2023-01-02"} {
		mustSubmit(t, s, "wide", p, "backfill")
	}
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	mustSubmit(t, s, "wide", "2026-11-20", "adhoc")

	var order []string
	for {
		c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: "w"})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		order = append(order, c.PriorityClass)
	}
	want := "live,live,adhoc,adhoc,backfill,backfill,backfill,backfill"
	if got := strings.Join(order, ","); got != want {
		t.Fatalf("claim order %s, want %s", got, want)
	}
}

func TestStaleTokenAndReplacedAttemptAreRejected(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 2))
	mustSubmit(t, s, "wide", "2026-11-24", "live")

	first := mustClaim(t, s, "w1")
	if _, err := s.Start(ctx, first.AttemptID, StartRequest{FencingToken: first.FencingToken + 1}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("wrong token: err=%v, want ErrStaleAttempt", err)
	}
	_, _ = s.Start(ctx, first.AttemptID, StartRequest{FencingToken: first.FencingToken})
	// Retryable failure, attempts remain: READY again, a new attempt claims it.
	if r, err := s.Fail(ctx, first.AttemptID, FailRequest{FencingToken: 1, ErrorClass: "network", Outcome: "unknown", Retryable: true}); err != nil || r.TaskStatus != domain.Ready {
		t.Fatalf("fail retry: %+v %v", r, err)
	}
	second := mustClaim(t, s, "w2")
	if second.AttemptNumber != 2 || second.FencingToken != 2 || second.TaskID != first.TaskID {
		t.Fatalf("second claim = %+v", second)
	}
	// The first attempt can't complete any more, with either token.
	for _, tok := range []int64{1, 2} {
		if _, err := s.Complete(ctx, first.AttemptID, CompleteRequest{FencingToken: tok}); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("old attempt complete (token %d): err=%v, want ErrStaleAttempt", tok, err)
		}
	}
	_, _ = s.Start(ctx, second.AttemptID, StartRequest{FencingToken: 2})
	if _, err := s.Complete(ctx, second.AttemptID, CompleteRequest{FencingToken: 2}); err != nil {
		t.Fatal(err)
	}
	// A repeated completion (response lost) is a replay; a fail after it is rejected.
	if r, err := s.Complete(ctx, second.AttemptID, CompleteRequest{FencingToken: 2}); err != nil || !r.Replay {
		t.Fatalf("replayed complete: %+v %v", r, err)
	}
	if _, err := s.Fail(ctx, second.AttemptID, FailRequest{FencingToken: 2, ErrorClass: "x", Outcome: "unknown"}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("fail after success: err=%v", err)
	}
	var rows int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE task_id = $1`, first.TaskID).Scan(&rows)
	if rows != 2 {
		t.Fatalf("attempt rows = %d, want 2 (every attempt recorded)", rows)
	}
}

func TestAttemptsExhaustedIsTerminal(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 2))
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	for i, want := range []domain.State{domain.Ready, domain.FailedTerminal} {
		c := mustClaim(t, s, "w")
		r, err := s.Fail(ctx, c.AttemptID, FailRequest{FencingToken: c.FencingToken, ErrorClass: "server_error",
			Outcome: "failed_definite", Retryable: true})
		if err != nil || r.TaskStatus != want {
			t.Fatalf("attempt %d: %+v %v, want %s", i+1, r, err, want)
		}
	}
}

func TestStartIsReplayableAndCompleteNeedsStart(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 1, 1))
	mustSubmit(t, s, "wide", "2026-11-24", "live")
	c := mustClaim(t, s, "w")
	if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: 1}); !errors.Is(err, domain.ErrIllegalTransition) {
		t.Fatalf("complete before start: err=%v, want illegal transition", err)
	}
	_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: 1})
	if r, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: 1}); err != nil || !r.Replay {
		t.Fatalf("second start: %+v %v", r, err)
	}
	if _, err := s.Start(ctx, "00000000-0000-0000-0000-000000000000", StartRequest{FencingToken: 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown attempt: %v", err)
	}
}

// The A2 acceptance test: 20 concurrent claimers x 1,000 tasks, repeated.
func TestConcurrentClaimersNeverDoubleClaim(t *testing.T) {
	const claimers, runs, perRun, rounds = 20, 100, 10, 3
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", perRun, 1))

	for round := range rounds {
		for i := range runs {
			mustSubmit(t, s, "wide", fmt.Sprintf("%d-01-01", 2000+round*runs+i), "backfill")
		}
		var mu sync.Mutex
		claimed := map[string]int{}
		var wg sync.WaitGroup
		errs := make(chan error, claimers)
		for w := range claimers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: fmt.Sprintf("w%d", w)})
					if err != nil {
						errs <- err
						return
					}
					if !ok {
						return
					}
					mu.Lock()
					claimed[c.TaskID]++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("round %d: claim error: %v", round, err)
		}
		if len(claimed) != runs*perRun {
			t.Fatalf("round %d: %d distinct tasks claimed, want %d", round, len(claimed), runs*perRun)
		}
		for id, n := range claimed {
			if n != 1 {
				t.Fatalf("round %d: task %s claimed %d times", round, id, n)
			}
		}
		var dup int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT task_id FROM task_attempts GROUP BY task_id HAVING count(*) > 1) d`).Scan(&dup)
		if dup != 0 {
			t.Fatalf("round %d: %d tasks have more than one attempt", round, dup)
		}
	}
}

// Race: two workers finish the last two tasks of a run at the same moment.
// Without the run-row lock, each transaction sees the other task still
// RUNNING and the run is left RUNNING with every task SUCCEEDED.
func TestConcurrentLastCompletionsFinishTheRun(t *testing.T) {
	const iterations = 40
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("pair", 2, 1))
	for i := range iterations {
		run := mustSubmit(t, s, "pair", fmt.Sprintf("%d-01-01", 2100+i), "live")
		a, b := mustClaim(t, s, "w1"), mustClaim(t, s, "w2")
		for _, c := range []Claim{a, b} {
			if _, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken}); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, c := range []Claim{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		got, _ := s.GetRun(ctx, run.ID)
		if got.Status != domain.RunSucceeded {
			t.Fatalf("iteration %d: run %s, want SUCCEEDED (tasks %s, %s)", i, got.Status, got.Tasks[0].Status, got.Tasks[1].Status)
		}
	}
}

func TestClaimUsesPartialIndex(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("wide", 10, 1))
	for i := range 200 {
		mustSubmit(t, s, "wide", fmt.Sprintf("%d-01-01", 1800+i), "backfill")
	}
	// Realistic shape: most tasks finished, a few READY. (Test setup only:
	// production code never writes status directly.)
	if _, err := s.pool.Exec(ctx, `UPDATE tasks SET status = 'SUCCEEDED' WHERE ctid IN (SELECT ctid FROM tasks LIMIT 1980)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `ANALYZE tasks`); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.pool.Query(ctx, `EXPLAIN SELECT id FROM tasks WHERE status = 'READY' AND available_at <= now()
		ORDER BY priority_rank, deadline_at NULLS LAST, available_at, id LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED`)
	var all []string
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		all = append(all, line)
	}
	rows.Close()
	text := strings.Join(all, "\n")
	if !strings.Contains(text, "tasks_claim") {
		t.Fatalf("claim plan does not use tasks_claim:\n%s", text)
	}
	t.Logf("claim plan:\n%s", text)
}

func TestClaimValidation(t *testing.T) {
	s := newStore(t)
	for _, req := range []ClaimRequest{{}, {WorkerID: "w", LeaseSeconds: 5}, {WorkerID: "w", LeaseSeconds: 4000}} {
		if _, _, err := s.Claim(context.Background(), req); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%+v: err=%v, want ErrInvalid", req, err)
		}
	}
	if _, _, err := s.Claim(context.Background(), ClaimRequest{WorkerID: "w"}); err != nil {
		t.Fatalf("empty queue: %v", err)
	}
}
