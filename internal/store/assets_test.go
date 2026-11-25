// Integration tests for assets, materializations, check-gated pointers and
// freshness (Slice A8). These prove:
//   - a successful completion records its materialization and checks, and the
//     current pointer moves to it; a re-run moves it to the newer attempt;
//   - a failed blocking check leaves the pointer where it was (failed_check),
//     the task FAILED_TERMINAL and dependents canceled; a warn check moves it
//     and is recorded;
//   - a fenced-out attempt's materialization never becomes current (nothing
//     of it is recorded);
//   - outputs for undeclared assets or the wrong partition fail the task;
//   - freshness: an asset turns late at its policy deadline (injected clock),
//     exactly once, and fresh again when the partition is materialized.
package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
)

const (
	hashA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func assetDef(freshness map[string]FreshnessPolicy) PipelineDef {
	return PipelineDef{Name: "prices", PartitionKind: "daily", PartitionTZ: "UTC", Freshness: freshness,
		Tasks: []domain.TaskSpec{
			{Name: "ingest", AssetOutputs: []string{"silver.power_prices__x"}, MaxAttempts: 3},
			{Name: "gold", Deps: []string{"ingest"}, AssetOutputs: []string{"gold.spreads__x"}},
		}}
}

func mat(partition, hash string) Materialization {
	return Materialization{AssetKey: "silver.power_prices__x", PartitionKey: partition,
		URI: "silver/power_prices__x/date=" + partition + "/attempt=a/part-0.parquet", RowCount: 96,
		ContentHash: hash, SchemaVersion: "power_prices-1", CodeVersion: "test-1"}
}

func currentHash(t *testing.T, s *Store, partition string) (string, string) {
	t.Helper()
	ps, err := s.AssetPartitions(context.Background(), "silver.power_prices__x", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.PartitionKey == partition {
			if p.Current == nil {
				return "", p.Status
			}
			return p.Current.ContentHash, p.Status
		}
	}
	return "", "absent"
}

func completeWith(t *testing.T, s *Store, c Claim, mats []Materialization, checks []CheckResult) AttemptResult {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken, Materializations: mats, Checks: checks})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSuccessfulCompletionPublishesAndRerunMovesThePointer(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, assetDef(nil))
	mustSubmit(t, s, "prices", "2026-11-24", "live")
	c := mustClaim(t, s, "w")
	completeWith(t, s, c, []Materialization{mat("2026-11-24", hashA)},
		[]CheckResult{{CheckName: "schema", Severity: "blocking", Passed: true}, {CheckName: "revision", Severity: "warn", Passed: false}})
	if h, st := currentHash(t, s, "2026-11-24"); h != hashA || st != "fresh" {
		t.Fatalf("after first run: %s %s", h, st)
	}
	var checks int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM asset_checks WHERE attempt_id = $1`, c.AttemptID).Scan(&checks)
	if checks != 2 {
		t.Fatalf("%d checks recorded, want 2 (the warn check too)", checks)
	}
	// A re-run (new run after the first finished) publishes a new version.
	complete(t, s, runOne(t, s, "gold"))
	_, _, _ = s.SubmitRun(ctx, submitReqFor("prices", "2026-11-24", "rerun"))
	completeWith(t, s, mustClaim(t, s, "w"), []Materialization{mat("2026-11-24", hashB)}, nil)
	if h, _ := currentHash(t, s, "2026-11-24"); h != hashB {
		t.Fatalf("re-run didn't move the pointer: %s", h)
	}
	var history int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM materializations WHERE partition_key = '2026-11-24'`).Scan(&history)
	if history != 2 {
		t.Fatalf("materialization history %d, want 2 (append-only)", history)
	}
}

func TestFailedBlockingCheckLeavesPointerAndCancelsDependents(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, assetDef(nil))
	mustSubmit(t, s, "prices", "2026-11-24", "live")
	completeWith(t, s, mustClaim(t, s, "w"), []Materialization{mat("2026-11-24", hashA)}, nil)
	complete(t, s, runOne(t, s, "gold"))

	run, _, _ := s.SubmitRun(ctx, submitReqFor("prices", "2026-11-24", "bad-rerun"))
	r := completeWith(t, s, mustClaim(t, s, "w"), []Materialization{mat("2026-11-24", hashB)},
		[]CheckResult{{CheckName: "interval_completeness", Severity: "blocking", Passed: false}})
	if r.TaskStatus != domain.FailedTerminal {
		t.Fatalf("task %s", r.TaskStatus)
	}
	if h, st := currentHash(t, s, "2026-11-24"); h != hashA || st != "failed_check" {
		t.Fatalf("pointer %s status %s, want the old %s and failed_check", h, st, hashA)
	}
	if st := states(t, s, run.ID); st["gold"] != domain.Canceled {
		t.Fatalf("dependent of failed check: %s", st["gold"])
	}
}

func TestFencedOutAttemptsOutputNeverBecomesCurrent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, assetDef(nil))
	mustSubmit(t, s, "prices", "2026-11-24", "live")
	stale := mustClaim(t, s, "w1")
	_, _ = s.Start(ctx, stale.AttemptID, StartRequest{FencingToken: stale.FencingToken})
	expireLease(t, s, stale.TaskID)
	reap(t, s)
	fresh := mustClaim(t, s, "w2")
	completeWith(t, s, fresh, []Materialization{mat("2026-11-24", hashB)}, nil)
	// The paused worker wakes up and reports its (different) output.
	_, err := s.Complete(ctx, stale.AttemptID, CompleteRequest{FencingToken: stale.FencingToken,
		Materializations: []Materialization{mat("2026-11-24", hashA)}})
	if !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale completion: %v", err)
	}
	if h, _ := currentHash(t, s, "2026-11-24"); h != hashB {
		t.Fatalf("current = %s, want the fresh attempt's %s", h, hashB)
	}
	var staleRows int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM materializations WHERE attempt_id = $1`, stale.AttemptID).Scan(&staleRows)
	if staleRows != 0 {
		t.Fatalf("%d materializations recorded for the fenced-out attempt", staleRows)
	}
}

func TestInvalidOutputsFailTheTask(t *testing.T) {
	cases := map[string]Materialization{
		"undeclared asset": {AssetKey: "silver.other", PartitionKey: "2026-11-24", URI: "x", RowCount: 1, ContentHash: hashA, SchemaVersion: "1", CodeVersion: "1"},
		"wrong partition":  mat("2026-11-23", hashA),
		"bad hash":         mat("2026-11-24", "md5:123"),
		"escaping uri":     func() Materialization { m := mat("2026-11-24", hashA); m.URI = "../../etc/passwd"; return m }(),
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			mustRegister(t, s, assetDef(nil))
			mustSubmit(t, s, "prices", "2026-11-24", "live")
			c := mustClaim(t, s, "w")
			r := completeWith(t, s, c, []Materialization{m}, nil)
			if r.TaskStatus != domain.FailedTerminal || r.AttemptStatus != "CHECK_FAILED" {
				t.Fatalf("%s: %+v", name, r)
			}
			var cls string
			_ = s.pool.QueryRow(context.Background(), `SELECT error_class FROM task_attempts WHERE id = $1`, c.AttemptID).Scan(&cls)
			if cls != "materialization_invalid" {
				t.Fatalf("error_class %s", cls)
			}
			if h, st := currentHash(t, s, "2026-11-24"); h != "" || st != "absent" {
				t.Fatalf("invalid output recorded: %s %s", h, st)
			}
		})
	}
}

func TestFreshnessTurnsLateAtPolicyDeadline(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// Day-ahead style: by 12:00 UTC each day, tomorrow's partition must be fresh.
	policy := FreshnessPolicy{DeadlineCron: "0 12 * * *", TZ: "UTC", ExpectedPartitionOffset: 1, Severity: "page"}
	mustRegister(t, s, assetDef(map[string]FreshnessPolicy{"silver.power_prices__x": policy}))
	// The schedule delivered 2026-11-25 (fetched on the 24th), then stopped.
	mustSubmit(t, s, "prices", "2026-11-25", "live")
	completeWith(t, s, mustClaim(t, s, "w"), []Materialization{mat("2026-11-25", hashA)}, nil)
	complete(t, s, runOne(t, s, "gold"))

	at := func(day, hour, minute int) time.Time { return time.Date(2026, 11, day, hour, minute, 0, 0, time.UTC) }
	if n, err := s.EvaluateFreshness(ctx, at(25, 11, 59)); err != nil || n != 0 {
		t.Fatalf("before the 25th's deadline (expects the 25th, fresh): late=%d err=%v", n, err)
	}
	if n, _ := s.EvaluateFreshness(ctx, at(25, 12, 1)); n != 1 {
		t.Fatalf("after the deadline (expects the 26th, missing): late=%d, want 1", n)
	}
	if _, st := currentHash(t, s, "2026-11-26"); st != "late" {
		t.Fatalf("2026-11-26 status %s", st)
	}
	if n, _ := s.EvaluateFreshness(ctx, at(25, 13, 0)); n != 0 {
		t.Fatal("asset.late fired twice")
	}
	var events int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE type = 'asset.late' AND payload->>'partition_key' = '2026-11-26'`).Scan(&events)
	if events != 1 {
		t.Fatalf("%d asset.late events", events)
	}
	// It arrives late: fresh again, pointer set.
	mustSubmit(t, s, "prices", "2026-11-26", "live")
	completeWith(t, s, mustClaim(t, s, "w"), []Materialization{mat("2026-11-26", hashB)}, nil)
	if h, st := currentHash(t, s, "2026-11-26"); h != hashB || st != "fresh" {
		t.Fatalf("after late arrival: %s %s", h, st)
	}
}

func TestFreshnessPolicyValidated(t *testing.T) {
	s := newStore(t)
	for name, f := range map[string]map[string]FreshnessPolicy{
		"undeclared asset": {"silver.nope": {DeadlineCron: "0 12 * * *", TZ: "UTC"}},
		"bad cron":         {"silver.power_prices__x": {DeadlineCron: "noon", TZ: "UTC"}},
	} {
		if _, _, err := s.RegisterPipeline(context.Background(), assetDef(f)); err == nil || !strings.Contains(err.Error(), "freshness") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
