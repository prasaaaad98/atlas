// Integration tests for retry policies, backoff and classification (Slice A4).
// These prove:
//   - fail twice, then succeed → 3 attempt rows with the right outcomes;
//   - terminal error classes and the policy's non-retryable list are never
//     retried, even when the worker says "retryable";
//   - a retry is not claimable before its backoff (database clock), the
//     backoff follows the policy, and a source's Retry-After wins when longer;
//   - a lapsed lease is retried after a backoff too.
package store

import (
	"context"
	"testing"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func retryDef(policy *domain.RetryPolicy, maxAttempts int) PipelineDef {
	return PipelineDef{Name: "flaky", PartitionKind: "daily", PartitionTZ: "UTC",
		Tasks: []domain.TaskSpec{{Name: "fetch", MaxAttempts: maxAttempts, Retry: policy}}}
}

func failAttempt(t *testing.T, s *Store, c Claim, class, outcome string, retryable bool, retryAfter float64) AttemptResult {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Fail(ctx, c.AttemptID, FailRequest{FencingToken: c.FencingToken, ErrorClass: class,
		Outcome: outcome, Retryable: retryable, RetryAfterSeconds: retryAfter})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFailTwiceThenSucceedRecordsThreeAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, retryDef(&domain.RetryPolicy{InitialIntervalSeconds: 0, BackoffCoefficient: 2, MaxIntervalSeconds: 0}, 3))
	run := mustSubmit(t, s, "flaky", "2026-11-24", "live")

	if r := failAttempt(t, s, mustClaim(t, s, "w"), "network", "unknown", true, 0); r.RunStatus != domain.RunRunning {
		t.Fatalf("run after a retryable failure: %s, want RUNNING (it has started)", r.RunStatus)
	}
	failAttempt(t, s, mustClaim(t, s, "w"), "server_error", "failed_definite", true, 0)
	last := mustClaim(t, s, "w")
	_, _ = s.Start(ctx, last.AttemptID, StartRequest{FencingToken: last.FencingToken})
	if r, err := s.Complete(ctx, last.AttemptID, CompleteRequest{FencingToken: last.FencingToken}); err != nil || r.RunStatus != domain.RunSucceeded {
		t.Fatalf("third attempt: %+v %v", r, err)
	}

	rows, _ := s.pool.Query(ctx, `SELECT attempt_number, fencing_token, status, outcome, coalesce(error_class, '')
		FROM task_attempts WHERE run_id = $1 ORDER BY attempt_number`, run.ID)
	type row struct {
		n, tok               int
		status, outcome, cls string
	}
	var got []row
	for rows.Next() {
		var r row
		_ = rows.Scan(&r.n, &r.tok, &r.status, &r.outcome, &r.cls)
		got = append(got, r)
	}
	rows.Close()
	want := []row{{1, 1, "FAILED", "unknown", "network"}, {2, 2, "FAILED", "failed_definite", "server_error"},
		{3, 3, "SUCCEEDED", "succeeded", ""}}
	if len(got) != 3 {
		t.Fatalf("attempt rows: %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attempt %d = %+v, want %+v", i+1, got[i], want[i])
		}
	}
}

func TestNonRetryableClassesAreNotRetried(t *testing.T) {
	cases := []struct{ class string }{{"auth"}, {"bad_request"}, {"definition_mismatch"}, {"quota_exhausted"}}
	for _, c := range cases {
		t.Run(c.class, func(t *testing.T) {
			s := newStore(t)
			mustRegister(t, s, retryDef(&domain.RetryPolicy{InitialIntervalSeconds: 0, BackoffCoefficient: 2,
				MaxIntervalSeconds: 0, NonRetryableErrorClasses: []string{"quota_exhausted"}}, 5))
			mustSubmit(t, s, "flaky", "2026-11-24", "live")
			// The worker claims the error is transient; the classifier vetoes it.
			r := failAttempt(t, s, mustClaim(t, s, "w"), c.class, "failed_definite", true, 0)
			if r.TaskStatus != domain.FailedTerminal {
				t.Fatalf("%s: task %s, want FAILED_TERMINAL", c.class, r.TaskStatus)
			}
		})
	}
}

func TestFailBackoffFollowsPolicy(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	s.jitter = func() float64 { return 0.5 }
	mustRegister(t, s, retryDef(&domain.RetryPolicy{InitialIntervalSeconds: 40, BackoffCoefficient: 3, MaxIntervalSeconds: 100}, 5))
	mustSubmit(t, s, "flaky", "2026-11-24", "live")

	delayOf := func(c Claim) float64 {
		var d float64
		_ = s.pool.QueryRow(ctx, `SELECT extract(epoch FROM available_at - now()) FROM tasks WHERE id = $1`, c.TaskID).Scan(&d)
		return d
	}
	first := mustClaim(t, s, "w")
	failAttempt(t, s, first, "network", "unknown", true, 0)
	// attempt 1: cap 40 → 0.5 * 40 = 20 s
	if d := delayOf(first); d < 18 || d > 20.5 {
		t.Fatalf("delay after attempt 1 = %.1f s, want about 20", d)
	}
	if _, ok, _ := s.Claim(ctx, ClaimRequest{WorkerID: "w"}); ok {
		t.Fatal("a retry was claimable before its backoff")
	}
	// Retry-After from the source (90 s) is longer than the backoff: it wins.
	makeClaimable(t, s, first.TaskID)
	second := mustClaim(t, s, "w")
	failAttempt(t, s, second, "rate_limited", "failed_definite", true, 90)
	if d := delayOf(second); d < 88 || d > 90.5 {
		t.Fatalf("delay with Retry-After 90 = %.1f s", d)
	}
	// attempt 3: cap min(100, 40*9) = 100 → 50 s
	makeClaimable(t, s, first.TaskID)
	third := mustClaim(t, s, "w")
	failAttempt(t, s, third, "network", "unknown", true, 0)
	if d := delayOf(third); d < 48 || d > 50.5 {
		t.Fatalf("delay after attempt 3 = %.1f s, want about 50 (max interval)", d)
	}
}

func TestLeaseExpiryRetryUsesBackoff(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	s.jitter = func() float64 { return 0.5 }
	mustRegister(t, s, retryDef(&domain.RetryPolicy{InitialIntervalSeconds: 60, BackoffCoefficient: 2, MaxIntervalSeconds: 600}, 3))
	mustSubmit(t, s, "flaky", "2026-11-24", "live")
	c := mustClaim(t, s, "w")
	expireLease(t, s, c.TaskID)
	reap(t, s)
	var d float64
	_ = s.pool.QueryRow(ctx, `SELECT extract(epoch FROM available_at - now()) FROM tasks WHERE id = $1`, c.TaskID).Scan(&d)
	if d < 28 || d > 30.5 {
		t.Fatalf("re-run after lease expiry in %.1f s, want about 30", d)
	}
}

func TestRegisterRejectsBadRetryPolicy(t *testing.T) {
	s := newStore(t)
	_, _, err := s.RegisterPipeline(context.Background(),
		retryDef(&domain.RetryPolicy{InitialIntervalSeconds: 10, BackoffCoefficient: 0.5, MaxIntervalSeconds: 5}, 3))
	if err == nil {
		t.Fatal("bad retry policy accepted")
	}
}

// makeClaimable moves a task's backoff into the past (test-only SQL,
// equivalent to the database clock passing it).
func makeClaimable(t *testing.T, s *Store, taskID string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE tasks SET available_at = now() - interval '1 second' WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
}
