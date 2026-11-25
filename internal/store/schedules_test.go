// Integration tests for schedules, catchup and backfills (Slice A6).
// These prove:
//   - without catchup, only the latest due partition is submitted;
//   - with catchup, exactly the missed partitions since start_partition;
//   - a tick that crashes before saving its cursor, or two overlapping
//     ticks, still create each partition once;
//   - the partition offset maps a fire to the right business date;
//   - a 1,600-partition backfill never exceeds max_concurrent_runs, runs
//     oldest first, finishes, and never delays a live run;
//   - a backfill waits for (never skips) a partition another run holds.
package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func dbNow(t *testing.T, s *Store) time.Time {
	t.Helper()
	var now time.Time
	if err := s.pool.QueryRow(context.Background(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func partitionsOf(t *testing.T, s *Store, pipeline string) []string {
	t.Helper()
	rows, err := s.pool.Query(context.Background(),
		`SELECT partition_key FROM workflow_runs WHERE pipeline_name = $1 ORDER BY partition_key`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		var p string
		_ = rows.Scan(&p)
		out = append(out, p)
	}
	rows.Close()
	return out
}

func tick(t *testing.T, s *Store) int {
	t.Helper()
	n, err := s.TickSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestScheduleWithoutCatchupSubmitsOnlyTheLatestPartition(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, wideDef("daily", 1, 1))
	if _, err := s.SetSchedule(context.Background(), Schedule{PipelineName: "daily", Cron: "0 6 * * *", TZ: "UTC", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if n := tick(t, s); n != 1 {
		t.Fatalf("first tick submitted %d, want 1", n)
	}
	c, _ := domain.ParseCron("0 6 * * *")
	last, _ := c.Prev(dbNow(t, s), time.UTC, 48*time.Hour)
	if got := partitionsOf(t, s, "daily"); len(got) != 1 || got[0] != last.Format("2006-01-02") {
		t.Fatalf("partitions %v, want [%s]", got, last.Format("2006-01-02"))
	}
	if n := tick(t, s); n != 0 {
		t.Fatalf("second tick submitted %d, want 0", n)
	}
	var trigger, class string
	_ = s.pool.QueryRow(context.Background(), `SELECT trigger, priority_class FROM workflow_runs`).Scan(&trigger, &class)
	if trigger != "schedule" || class != "live" {
		t.Fatalf("scheduled run is %s/%s", trigger, class)
	}
}

func TestCatchupCreatesExactlyTheMissedPartitions(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("daily", 1, 1))
	now := dbNow(t, s)
	start := now.AddDate(0, 0, -9).Format("2006-01-02")
	_, err := s.SetSchedule(ctx, Schedule{PipelineName: "daily", Cron: "0 6 * * *", TZ: "UTC", Catchup: true,
		StartPartition: &start, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	tick(t, s)
	// Independent expectation: every UTC date from start through the date of the last 06:00 fire.
	c, _ := domain.ParseCron("0 6 * * *")
	last, _ := c.Prev(now, time.UTC, 48*time.Hour)
	var want []string
	for d, _ := time.Parse("2006-01-02", start); !d.After(last); d = d.AddDate(0, 0, 1) {
		want = append(want, d.Format("2006-01-02"))
	}
	got := partitionsOf(t, s, "daily")
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("catchup partitions\n got %v\nwant %v", got, want)
	}
	tick(t, s)
	if again := partitionsOf(t, s, "daily"); len(again) != len(want) {
		t.Fatalf("second tick changed partitions: %d", len(again))
	}
}

func TestTickCrashOrOverlapCreatesEachPartitionOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("daily", 1, 1))
	start := dbNow(t, s).AddDate(0, 0, -5).Format("2006-01-02")
	_, _ = s.SetSchedule(ctx, Schedule{PipelineName: "daily", Cron: "0 * * * *", TZ: "Europe/Brussels",
		Catchup: true, StartPartition: &start, Enabled: true})
	tick(t, s)
	before := len(partitionsOf(t, s, "daily"))
	// Crash after the submissions but before the cursor update: simulate by losing the cursor.
	_, _ = s.pool.Exec(ctx, `UPDATE schedules SET cursor = NULL`)
	tick(t, s)
	// Overlapping ticks (e.g. two control planes during a restart).
	_, _ = s.pool.Exec(ctx, `UPDATE schedules SET cursor = NULL`)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.TickSchedules(ctx) }()
	}
	wg.Wait()
	var runs, distinct int
	_ = s.pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT partition_key) FROM workflow_runs`).Scan(&runs, &distinct)
	// Hourly cron → 24 fires per local day, all mapping to that day: one run per partition.
	if runs != distinct || runs != before || before < 5 {
		t.Fatalf("runs %d, distinct partitions %d, first tick %d", runs, distinct, before)
	}
}

func TestPartitionOffsetMapsFireToBusinessDate(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, wideDef("dayahead", 1, 1))
	_, _ = s.SetSchedule(context.Background(), Schedule{PipelineName: "dayahead", Cron: "0 13 * * *", TZ: "Europe/Brussels",
		PartitionOffsetDays: 1, Enabled: true})
	tick(t, s)
	c, _ := domain.ParseCron("0 13 * * *")
	loc, _ := time.LoadLocation("Europe/Brussels")
	last, _ := c.Prev(dbNow(t, s), loc, 48*time.Hour)
	want := last.In(loc).AddDate(0, 0, 1).Format("2006-01-02")
	if got := partitionsOf(t, s, "dayahead"); len(got) != 1 || got[0] != want {
		t.Fatalf("partition %v, want delivery date %s", got, want)
	}
}

func TestScheduleValidation(t *testing.T) {
	s := newStore(t)
	mustRegister(t, s, wideDef("daily", 1, 1))
	for _, bad := range []Schedule{
		{PipelineName: "daily", Cron: "bad", TZ: "UTC"},
		{PipelineName: "daily", Cron: "0 6 * * *", TZ: "Mars/Olympus"},
		{PipelineName: "daily", Cron: "0 6 * * *", TZ: "UTC", Catchup: true},
		{PipelineName: "nope", Cron: "0 6 * * *", TZ: "UTC"},
	} {
		if _, err := s.SetSchedule(context.Background(), bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

// A6 acceptance: a 1,600-partition backfill with max 4 concurrent runs. A
// "worker" drains whatever is claimable each round. Midway a live run is
// submitted; it must be the very next task claimed.
func TestBackfillConcurrencyOrderAndLivePriority(t *testing.T) {
	const partitions, maxRuns = 1600, 4
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("hist", 1, 1))
	mustRegister(t, s, wideDef("live", 1, 1))
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, partitions-1)
	b, err := s.CreateBackfill(ctx, BackfillRequest{PipelineName: "hist", FromPartition: from.Format("2006-01-02"),
		ToPartition: to.Format("2006-01-02"), MaxConcurrentRuns: maxRuns})
	if err != nil {
		t.Fatal(err)
	}
	maxActive, rounds, liveChecked := 0, 0, false
	var order []string
	for {
		if _, err := s.AdvanceBackfills(ctx); err != nil {
			t.Fatal(err)
		}
		var active int
		_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_runs WHERE backfill_id = $1 AND status IN ('PENDING','RUNNING')`, b.ID).Scan(&active)
		maxActive = max(maxActive, active)
		if rounds == 100 {
			mustSubmit(t, s, "live", "2026-11-24", "live")
		}
		claimedAny := false
		for {
			c, ok, err := s.Claim(ctx, ClaimRequest{WorkerID: "w"})
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if rounds == 100 && !liveChecked {
				if c.PipelineName != "live" {
					t.Fatalf("first claim after the live submit was %s %s, want the live run", c.PipelineName, c.PartitionKey)
				}
				liveChecked = true
			}
			if c.PipelineName == "hist" {
				order = append(order, c.PartitionKey)
			}
			claimedAny = true
			_, _ = s.Start(ctx, c.AttemptID, StartRequest{FencingToken: c.FencingToken})
			if _, err := s.Complete(ctx, c.AttemptID, CompleteRequest{FencingToken: c.FencingToken}); err != nil {
				t.Fatal(err)
			}
		}
		rounds++
		got, _ := s.GetBackfill(ctx, b.ID)
		if got.Status == "SUCCEEDED" {
			if got.Runs.Succeeded != partitions || got.Runs.Total != partitions {
				t.Fatalf("finished with %+v", got.Runs)
			}
			break
		}
		if !claimedAny && rounds > partitions+10 {
			t.Fatalf("stuck after %d rounds: %+v", rounds, got)
		}
	}
	if maxActive > maxRuns {
		t.Fatalf("backfill had %d active runs, max %d", maxActive, maxRuns)
	}
	if !liveChecked {
		t.Fatal("live run never checked")
	}
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			t.Fatalf("not oldest first: %s after %s", order[i], order[i-1])
		}
	}
	t.Logf("1,600 partitions in %d rounds, max %d active", rounds, maxActive)
}

func TestBackfillWaitsForAPartitionHeldByAnotherRun(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustRegister(t, s, wideDef("hist", 1, 1))
	held := mustSubmit(t, s, "hist", "2020-01-02", "adhoc") // e.g. a manual re-run in progress
	b, _ := s.CreateBackfill(ctx, BackfillRequest{PipelineName: "hist", FromPartition: "2020-01-01",
		ToPartition: "2020-01-03", MaxConcurrentRuns: 5})
	if _, err := s.AdvanceBackfills(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetBackfill(ctx, b.ID)
	if got.Runs.Total != 1 || got.NextPartition != "2020-01-02" {
		t.Fatalf("backfill skipped or passed the held partition: %+v next=%s", got.Runs, got.NextPartition)
	}
	// The held run finishes (cancel here); the backfill then takes the partition.
	_, _ = s.CancelRun(ctx, held.ID)
	_, _ = s.AdvanceBackfills(ctx)
	got, _ = s.GetBackfill(ctx, b.ID)
	if got.Runs.Total != 3 || got.NextPartition != "2020-01-04" {
		t.Fatalf("after the holder finished: %+v next=%s", got.Runs, got.NextPartition)
	}
	cancelled, err := s.CancelBackfill(ctx, b.ID)
	if err != nil || cancelled.Status != "CANCELED" || cancelled.Runs.Active != 0 {
		t.Fatalf("cancel backfill: %+v %v", cancelled, err)
	}
}
