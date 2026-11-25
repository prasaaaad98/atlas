// Integration tests for DAG readiness, fail-fast, cancellation and limit keys (Slice A5).
// These prove:
//   - a child becomes READY exactly when all parents succeeded, also when two
//     parents complete concurrently (repeated);
//   - a failed, check-failed or exhausted parent cancels all BLOCKED descendants;
//   - cancel stops pending and running work; the running worker is fenced out;
//   - 100 partitions of a diamond DAG with random worker deaths all succeed,
//     with no task left BLOCKED;
//   - a limit key with concurrency 1 is never exceeded under 20 workers;
//   - rpm is enforced; a 429 pauses the key for everyone and claims resume after it.
package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
)

// diamond: a → (b, c) → d
func diamondDef(maxAttempts int, limitKey string) PipelineDef {
	return PipelineDef{Name: "diamond", PartitionKind: "daily", PartitionTZ: "UTC", Tasks: []domain.TaskSpec{
		{Name: "a", MaxAttempts: maxAttempts, LimitKey: limitKey},
		{Name: "b", Deps: []string{"a"}, MaxAttempts: maxAttempts},
		{Name: "c", Deps: []string{"a"}, MaxAttempts: maxAttempts},
		{Name: "d", Deps: []string{"b", "c"}, MaxAttempts: maxAttempts},
	}}
}

func states(t *testing.T, s *Store, runID string) map[string]domain.State {
	t.Helper()
	r, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]domain.State{}
	for _, tk := range r.Tasks {
		out[tk.Name] = tk.Status
	}
	return out
}

func runOne(t *testing.T, s *Store, want string) Claim {
	t.Helper()
	ctx := context.Background()
	c := mustClaim(t, s, "w")
	if want != "" && c.TaskName != want {
		t.Fatalf("claimed %s, want %s", c.TaskName, want)
	}
	if _, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken}); err != nil {
		t.Fatal(err)
	}
	return c
}

func complete(t *testing.T, s *Store, c Claim) {
	t.Helper()
	if _, err := s.Complete(context.Background(), c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
		t.Fatal(err)
	}
}

func TestChildBecomesReadyWhenAllParentsSucceed(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, diamondDef(1, ""))
	run := mustSubmit(t, s, "diamond", "2026-11-24", "live")

	complete(t, s, runOne(t, s, "a"))
	if st := states(t, s, run.ID); st["b"] != domain.Ready || st["c"] != domain.Ready || st["d"] != domain.Blocked {
		t.Fatalf("after a: %v", st)
	}
	b, c := runOne(t, s, ""), runOne(t, s, "")
	complete(t, s, b)
	if st := states(t, s, run.ID); st["d"] != domain.Blocked {
		t.Fatalf("d ready with one parent pending: %v", st)
	}
	complete(t, s, c)
	if st := states(t, s, run.ID); st["d"] != domain.Ready {
		t.Fatalf("after b and c: %v", st)
	}
	complete(t, s, runOne(t, s, "d"))
	if r, _ := s.GetRun(context.Background(), run.ID); r.Status != domain.RunSucceeded {
		t.Fatalf("run %s", r.Status)
	}
}

func TestConcurrentParentCompletionsReadyTheChild(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, diamondDef(1, ""))
	for i := range 30 {
		run := mustSubmit(t, s, "diamond", fmt.Sprintf("%d-01-01", 2100+i), "live")
		complete(t, s, runOne(t, s, "a"))
		b, c := runOne(t, s, ""), runOne(t, s, "")
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, x := range []Claim{b, c} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := s.Complete(context.Background(), x.AttemptID, CompleteRequest{FencingToken: x.FencingToken}); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if st := states(t, s, run.ID); st["d"] != domain.Ready {
			t.Fatalf("iteration %d: d is %s after both parents succeeded concurrently", i, st["d"])
		}
		complete(t, s, runOne(t, s, "d"))
	}
}

func TestFailedParentCancelsDescendants(t *testing.T) {
	ctx := context.Background()
	for _, how := range []string{"fail_final", "check_failed", "lease_exhausted"} {
		t.Run(how, func(t *testing.T) {
			s := newStore(t)
			mustRegister(t, s, diamondDef(1, ""))
			run := mustSubmit(t, s, "diamond", "2026-11-24", "live")
			a := runOne(t, s, "a")
			switch how {
			case "fail_final":
				_, _ = s.Fail(ctx, a.AttemptID, FailRequest{FencingToken: 1, ErrorClass: "auth", Outcome: "failed_definite"})
			case "check_failed":
				_, _ = s.Complete(ctx, a.AttemptID, CompleteRequest{FencingToken: 1,
					Checks: []CheckResult{{CheckName: "x", Severity: "blocking", Passed: false}}})
			case "lease_exhausted":
				expireLease(t, s, a.TaskID)
				reap(t, s)
			}
			st := states(t, s, run.ID)
			if st["a"] != domain.FailedTerminal || st["b"] != domain.Canceled || st["c"] != domain.Canceled || st["d"] != domain.Canceled {
				t.Fatalf("%s: %v", how, st)
			}
			if r, _ := s.GetRun(ctx, run.ID); r.Status != domain.RunFailed {
				t.Fatalf("run %s, want FAILED", r.Status)
			}
		})
	}
}

func TestCancelRunStopsPendingAndRunningWork(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, diamondDef(3, ""))
	run := mustSubmit(t, s, "diamond", "2026-11-24", "live")
	complete(t, s, runOne(t, s, "a"))
	running := runOne(t, s, "") // b or c running; the other READY; d BLOCKED

	got, err := s.CancelRun(ctx, run.ID)
	if err != nil || got.Status != domain.RunCanceled {
		t.Fatalf("cancel: %v %v", got.Status, err)
	}
	st := states(t, s, run.ID)
	if st["a"] != domain.Succeeded || st["b"] != domain.Canceled || st["c"] != domain.Canceled || st["d"] != domain.Canceled {
		t.Fatalf("after cancel: %v", st)
	}
	if hb, _ := s.Heartbeat(ctx, running.AttemptID, HeartbeatRequest{FencingToken: running.FencingToken}); !hb.CancelRequested {
		t.Fatal("running worker not told to stop")
	}
	if _, err := s.Complete(ctx, running.AttemptID, CompleteRequest{FencingToken: running.FencingToken}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("completion after cancel: %v", err)
	}
	if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w"}); ok {
		t.Fatal("a canceled run's task was claimed")
	}
	var attemptStatus, outcome string
	_ = s.pool.QueryRow(ctx, `SELECT status, outcome FROM task_attempts WHERE id = $1`, running.AttemptID).Scan(&attemptStatus, &outcome)
	if attemptStatus != "CANCELED" || outcome != "unknown" {
		t.Fatalf("canceled attempt recorded as %s/%s", attemptStatus, outcome)
	}
	if again, err := s.CancelRun(ctx, run.ID); err != nil || again.Status != domain.RunCanceled {
		t.Fatalf("second cancel: %v %v", again.Status, err)
	}
	// The partition is free again: a new run can be submitted.
	if _, _, err := s.SubmitRun(ctx, submitReqFor("diamond", "2026-11-24", "again")); err != nil {
		t.Fatalf("resubmit after cancel: %v", err)
	}
}

func submitReqFor(pipeline, partition, key string) SubmitRequest {
	return SubmitRequest{IdempotencyKey: key, PipelineName: pipeline, PartitionKey: partition,
		Trigger: "manual", PriorityClass: "adhoc"}
}

// A5 acceptance: 100 partitions, 8 workers, each attempt "dies" (abandons its
// lease) with probability 0.2. A reaper goroutine expires abandoned leases
// (test-only SQL standing in for the clock) and reaps. Everything must finish
// SUCCEEDED with no BLOCKED task left.
func TestHundredPartitionsWithRandomWorkerDeaths(t *testing.T) {
	const partitions, workers = 100, 8
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, diamondDef(20, ""))
	for i := range partitions {
		mustSubmit(t, s, "diamond", fmt.Sprintf("%d-01-01", 2100+i), "backfill")
	}
	abandoned := make(chan string, 10_000)
	var deaths atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for {
				select {
				case <-done:
					return
				default:
				}
				c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: fmt.Sprintf("w%d", w)})
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if r.Float64() < 0.2 { // the worker dies: no more reports
					deaths.Add(1)
					abandoned <- c.TaskID
					continue
				}
				if _, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken}); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() { // the reaper, with dead workers' leases run out
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case id := <-abandoned:
				expireLease(t, s, id)
				if _, err := s.ReapExpired(ctx, 100); err != nil {
					t.Error(err)
					return
				}
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	deadline := time.Now().Add(90 * time.Second)
	for {
		var active int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_runs WHERE status IN ('PENDING', 'RUNNING')`).Scan(&active)
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			close(done)
			wg.Wait()
			t.Fatalf("%d runs still active after 90 s", active)
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(done)
	wg.Wait()

	var succeeded, blocked, attempts int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_runs WHERE status = 'SUCCEEDED'`).Scan(&succeeded)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status <> 'SUCCEEDED'`).Scan(&blocked)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts`).Scan(&attempts)
	if succeeded != partitions || blocked != 0 {
		t.Fatalf("succeeded runs %d/%d, unfinished tasks %d", succeeded, partitions, blocked)
	}
	t.Logf("%d tasks, %d attempts, %d worker deaths", partitions*4, attempts, deaths.Load())
	if deaths.Load() == 0 || attempts != partitions*4+int(deaths.Load()) {
		t.Fatalf("attempts %d != tasks %d + deaths %d", attempts, partitions*4, deaths.Load())
	}
}

func oneTaskKeyed(name, key string) PipelineDef {
	return PipelineDef{Name: name, PartitionKind: "daily", PartitionTZ: "UTC",
		Tasks: []domain.TaskSpec{{Name: "fetch", LimitKey: key, MaxAttempts: 3}}}
}

func TestLimitKeyConcurrencyNeverExceeded(t *testing.T) {
	const tasks, workers = 60, 20
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.UpsertLimitKey(ctx, LimitKey{Key: "src", MaxConcurrency: 1, RPM: 100_000}); err != nil {
		t.Fatal(err)
	}
	mustRegister(t, s, oneTaskKeyed("keyed", "src"))
	for i := range tasks {
		mustSubmit(t, s, "keyed", fmt.Sprintf("%d-01-01", 2100+i), "backfill")
	}
	var held, maxHeld, finished atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for finished.Load() < tasks {
				c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: fmt.Sprintf("w%d", w)})
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				// Between this claim's commit and the completion's commit the slot is
				// held; counting inside that window can only under-count.
				if n := held.Add(1); n > maxHeld.Load() {
					maxHeld.Store(n)
				}
				_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken})
				time.Sleep(2 * time.Millisecond)
				held.Add(-1)
				if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
					t.Error(err)
					return
				}
				finished.Add(1)
			}
		}()
	}
	wg.Wait()
	if maxHeld.Load() != 1 {
		t.Fatalf("max concurrent holders of a concurrency-1 key: %d", maxHeld.Load())
	}
	// Independent check from attempt history: no two attempts' [leased, finished] overlap.
	var overlaps int
	_ = s.pool.QueryRow(ctx, `
		SELECT count(*) FROM task_attempts a JOIN task_attempts b ON a.id < b.id
		WHERE a.limit_key = 'src' AND b.limit_key = 'src'
		  AND a.leased_at < b.finished_at AND b.leased_at < a.finished_at`).Scan(&overlaps)
	if overlaps != 0 {
		t.Fatalf("%d overlapping attempts on a concurrency-1 key", overlaps)
	}
}

func TestLimitKeyRPM(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _ = s.UpsertLimitKey(ctx, LimitKey{Key: "src", MaxConcurrency: 10, RPM: 3})
	mustRegister(t, s, oneTaskKeyed("keyed", "src"))
	for i := range 5 {
		mustSubmit(t, s, "keyed", fmt.Sprintf("%d-01-01", 2100+i), "backfill")
	}
	claimed := 0
	for range 5 {
		if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w"}); ok {
			claimed++
		}
	}
	if claimed != 3 {
		t.Fatalf("claimed %d in one minute with rpm 3", claimed)
	}
	k, _ := s.GetLimitKey(ctx, "src")
	if k.LastMinute != 3 || k.Live != 3 {
		t.Fatalf("limit key state %+v", k)
	}
}

func TestRateLimitedFailurePausesKeyAndRecovers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _ = s.UpsertLimitKey(ctx, LimitKey{Key: "src", MaxConcurrency: 5, RPM: 1000})
	mustRegister(t, s, oneTaskKeyed("keyed", "src"))
	mustRegister(t, s, wideDef("free", 1, 1))
	for i := range 3 {
		mustSubmit(t, s, "keyed", fmt.Sprintf("%d-01-01", 2100+i), "live")
	}
	c := runOne(t, s, "fetch")
	if _, err := s.Fail(ctx, c.AttemptID, FailRequest{FencingToken: c.FencingToken, ErrorClass: "rate_limited",
		Outcome: "failed_definite", Retryable: true, RetryAfterSeconds: 120}); err != nil {
		t.Fatal(err)
	}
	if k, _ := s.GetLimitKey(ctx, "src"); k.PausedUntil == nil || time.Until(*k.PausedUntil) < 100*time.Second {
		t.Fatalf("key not paused for ~120 s: %+v", k)
	}
	// Every task on the key waits; a task without the key still runs.
	mustSubmit(t, s, "free", "2026-11-24", "backfill")
	got := mustClaim(t, s, "w")
	if got.PipelineName != "free" {
		t.Fatalf("claimed %s.%s while its key is paused", got.PipelineName, got.TaskName)
	}
	if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w"}); ok {
		t.Fatal("claimed a task on a paused key")
	}
	// The pause ends (test-only SQL standing in for the clock): claims resume.
	_, _ = s.pool.Exec(ctx, `UPDATE limit_keys SET paused_until = now() - interval '1 second' WHERE key = 'src'`)
	if again := mustClaim(t, s, "w"); again.PipelineName != "keyed" {
		t.Fatalf("after the pause: claimed %s", again.PipelineName)
	}
}

func TestRegisterRejectsUnknownLimitKey(t *testing.T) {
	s := newStore(t)
	if _, _, err := s.RegisterPipeline(context.Background(), oneTaskKeyed("keyed", "nope")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown limit key: %v", err)
	}
	if _, err := s.UpsertLimitKey(context.Background(), LimitKey{Key: "Bad-Key", MaxConcurrency: 1, RPM: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad key name: %v", err)
	}
}
