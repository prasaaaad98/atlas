// Integration tests for deadline-awareness (Slice A7). These prove:
//   - a pipeline's deadline rule sets deadline_at at submit (an explicit one wins);
//   - within a priority class, ready tasks are claimed earliest-deadline-first
//     (no deadline last), and class still comes before deadline;
//   - every run that is late (still active after its deadline, or finishing
//     after it) has exactly one run.deadline_missed event, whichever notices
//     first, also when the evaluator and the completion race;
//   - late_policy=cancel cancels the run and the fallback pipeline is submitted;
//   - at-risk fires before the miss from declared estimates, and measured
//     history replaces the estimate once there are enough samples.
package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func deadlineDef(name string, rule *domain.DeadlineRule, estimate float64) PipelineDef {
	return PipelineDef{Name: name, PartitionKind: "daily", PartitionTZ: "Europe/Brussels", Deadline: rule,
		Tasks: []domain.TaskSpec{{Name: "fetch", EstimateSeconds: estimate}}}
}

func submitWithDeadline(t *testing.T, s *Store, pipeline, partition, class string, dl *time.Time) Run {
	t.Helper()
	trigger := map[string]string{"live": "schedule", "backfill": "backfill", "adhoc": "manual"}[class]
	r, _, err := s.SubmitRun(context.Background(), SubmitRequest{IdempotencyKey: pipeline + partition, PipelineName: pipeline,
		PartitionKey: partition, Trigger: trigger, PriorityClass: class, DeadlineAt: dl})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func missedEvents(t *testing.T, s *Store, runID string) int {
	t.Helper()
	var n int
	_ = s.pool.QueryRow(context.Background(), `SELECT count(*) FROM events WHERE run_id = $1 AND type = 'run.deadline_missed'`, runID).Scan(&n)
	return n
}

func evaluate(t *testing.T, s *Store) DeadlineReport {
	t.Helper()
	r, err := s.EvaluateDeadlines(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDeadlineRuleSetsDeadlineAtSubmit(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, deadlineDef("da", &domain.DeadlineRule{RelativeTo: "partition_start", OffsetMinutes: -600}, 0))
	r := mustSubmit(t, s, "da", "2026-11-25", "live")
	// Brussels day 2026-11-25 starts 2026-11-24T23:00Z; minus 10 h = 13:00Z on the 24th.
	if want := time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC); r.DeadlineAt == nil || !r.DeadlineAt.Equal(want) {
		t.Fatalf("deadline_at = %v, want %v", r.DeadlineAt, want)
	}
	explicit := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	if r2 := submitWithDeadline(t, s, "da", "2026-11-26", "live", &explicit); !r2.DeadlineAt.Equal(explicit) {
		t.Fatalf("explicit deadline lost: %v", r2.DeadlineAt)
	}
}

func TestEarliestDeadlineFirstWithinClass(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("edf", 1, 1))
	base := time.Now().Add(time.Hour)
	r := rand.New(rand.NewPCG(3, 4))
	for i, k := range r.Perm(30) {
		dl := base.Add(time.Duration(k) * time.Minute)
		var p *time.Time
		if i%7 != 0 {
			p = &dl // every 7th run has no deadline
		}
		submitWithDeadline(t, s, "edf", fmt.Sprintf("%d-01-01", 2100+i), "live", p)
	}
	early := base.Add(-30 * time.Minute) // earliest deadline overall, but backfill class
	submitWithDeadline(t, s, "edf", "2099-01-01", "backfill", &early)

	var prev *time.Time
	sawNil := false
	for i := 0; ; i++ {
		c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: "w"})
		if err != nil || !ok {
			break
		}
		var dl *time.Time
		_ = s.pool.QueryRow(ctx, `SELECT deadline_at FROM tasks WHERE id = $1`, c.TaskID).Scan(&dl)
		if c.PriorityClass == "backfill" {
			if i != 30 {
				t.Fatalf("backfill claimed at position %d, before live runs finished", i)
			}
			continue
		}
		switch {
		case dl == nil:
			sawNil = true
		case sawNil:
			t.Fatalf("a task with deadline %v was claimed after one without", dl)
		case prev != nil && dl.Before(*prev):
			t.Fatalf("claim %d: deadline %v before previous %v (not EDF)", i, dl, prev)
		}
		if dl != nil {
			prev = dl
		}
	}
}

func TestEveryLateRunHasExactlyOneMissedEvent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("late", 1, 1))
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	// (a) evaluator first, then the run finishes late.
	a := submitWithDeadline(t, s, "late", "2100-01-01", "live", &past)
	if rep := evaluate(t, s); rep.Missed != 1 {
		t.Fatalf("evaluator missed %d", rep.Missed)
	}
	complete(t, s, runOne(t, s, ""))
	evaluate(t, s)
	// (b) finishes late before any evaluator pass.
	b := submitWithDeadline(t, s, "late", "2100-01-02", "live", &past)
	complete(t, s, runOne(t, s, ""))
	evaluate(t, s)
	// (c) on time: no miss.
	c := submitWithDeadline(t, s, "late", "2100-01-03", "live", &future)
	complete(t, s, runOne(t, s, ""))
	evaluate(t, s)
	for _, x := range []struct {
		run  Run
		want int
	}{{a, 1}, {b, 1}, {c, 0}} {
		if n := missedEvents(t, s, x.run.ID); n != x.want {
			t.Errorf("run %s: %d missed events, want %d", x.run.PartitionKey, n, x.want)
		}
	}
	// (d) the evaluator and the completion race, 20 times.
	for i := range 20 {
		r := submitWithDeadline(t, s, "late", fmt.Sprintf("2101-01-%02d", i+1), "live", &past)
		cl := runOne(t, s, "")
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, _ = s.EvaluateDeadlines(ctx) }()
		go func() {
			defer wg.Done()
			<-start
			_, _ = s.Complete(ctx, cl.AttemptID, CompleteRequest{FencingToken: cl.FencingToken})
		}()
		close(start)
		wg.Wait()
		evaluate(t, s)
		if n := missedEvents(t, s, r.ID); n != 1 {
			t.Fatalf("race %d: %d missed events, want exactly 1", i, n)
		}
	}
}

func TestLatePolicyCancelSubmitsFallback(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("fallback_src", 1, 1))
	mustRegister(t, s, deadlineDef("strict", &domain.DeadlineRule{RelativeTo: "partition_start", OffsetMinutes: 0,
		LatePolicy: "cancel", FallbackPipeline: "fallback_src"}, 0))
	past := time.Now().Add(-time.Minute)
	r := submitWithDeadline(t, s, "strict", "2100-01-01", "live", &past)
	rep := evaluate(t, s)
	if rep.Missed != 1 || rep.Canceled != 1 || rep.Fallbacks != 1 {
		t.Fatalf("report %+v", rep)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.Status != domain.RunCanceled || got.DeadlineMissedAt == nil {
		t.Fatalf("strict run: %s missed=%v", got.Status, got.DeadlineMissedAt)
	}
	var fbPipeline, fbPartition string
	_ = s.pool.QueryRow(ctx, `SELECT f.pipeline_name, f.partition_key FROM workflow_runs r JOIN workflow_runs f ON f.id = r.fallback_run_id
		WHERE r.id = $1`, r.ID).Scan(&fbPipeline, &fbPartition)
	if fbPipeline != "fallback_src" || fbPartition != "2100-01-01" {
		t.Fatalf("fallback %s %s", fbPipeline, fbPartition)
	}
	if n := missedEvents(t, s, r.ID); n != 1 {
		t.Fatalf("%d missed events after cancel", n)
	}
	evaluate(t, s) // idempotent: no second fallback
	var fallbacks int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_runs WHERE pipeline_name = 'fallback_src'`).Scan(&fallbacks)
	if fallbacks != 1 {
		t.Fatalf("%d fallback runs", fallbacks)
	}
}

func TestAtRiskFiresBeforeMissFromEstimateThenHistory(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, deadlineDef("risky", nil, 600)) // declared estimate: 10 minutes
	soon := time.Now().Add(5 * time.Minute)
	later := time.Now().Add(time.Hour)
	tight := submitWithDeadline(t, s, "risky", "2100-01-01", "live", &soon)
	roomy := submitWithDeadline(t, s, "risky", "2100-01-02", "live", &later)
	if rep := evaluate(t, s); rep.AtRisk != 1 || rep.Missed != 0 {
		t.Fatalf("report %+v, want 1 at risk, 0 missed", rep)
	}
	var payload string
	_ = s.pool.QueryRow(ctx, `SELECT payload::text FROM events WHERE run_id = $1 AND type = 'run.deadline_at_risk'`, tight.ID).Scan(&payload)
	if payload == "" {
		t.Fatal("tight run not flagged")
	}
	if got, _ := s.GetRun(ctx, roomy.ID); got.DeadlineAtRiskAt != nil {
		t.Fatal("roomy run flagged")
	}
	if evaluate(t, s).AtRisk != 0 {
		t.Fatal("at-risk fired twice")
	}
	t.Logf("at-risk payload: %s", payload)

	// Measured history: 3 quick successful runs make p95 ≈ 0 s, which replaces
	// the 600 s estimate, so a new tight run is no longer at risk.
	for _, r := range []Run{tight, roomy} {
		_, _ = s.CancelRun(ctx, r.ID)
	}
	for i := range 3 {
		mustSubmit(t, s, "risky", fmt.Sprintf("2100-02-%02d", i+1), "live")
		complete(t, s, runOne(t, s, ""))
	}
	submitWithDeadline(t, s, "risky", "2100-03-01", "live", &soon)
	if rep := evaluate(t, s); rep.AtRisk != 0 {
		t.Fatalf("with fast history the run was flagged: %+v", rep)
	}
}
