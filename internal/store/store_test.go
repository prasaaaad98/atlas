// Integration tests for the submit and registration transactions, against real
// PostgreSQL (internal/testdb). These prove the A1 guarantees:
//   - a duplicate submission doesn't create a second run (concurrent too);
//   - at most one active run per (pipeline, partition);
//   - same idempotency key with a different request is a conflict, not a replay;
//   - run, tasks, edges and the created event are written together.
package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/domain"
	"github.com/prasaaaad98/atlas/internal/testdb"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, testdb.New(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func demoDef() PipelineDef {
	return PipelineDef{
		Name:          "demo_grid",
		PartitionKind: "daily",
		PartitionTZ:   "Europe/Brussels",
		Tasks: []domain.TaskSpec{
			{Name: "fetch", AssetOutputs: []string{"bronze.demo_raw"}},
			{Name: "parse", Deps: []string{"fetch"}, AssetOutputs: []string{"silver.demo"}},
		},
	}
}

func register(t *testing.T, s *Store) Pipeline {
	t.Helper()
	p, _, err := s.RegisterPipeline(context.Background(), demoDef())
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return p
}

func submitReq(key, partition string) SubmitRequest {
	return SubmitRequest{
		IdempotencyKey: key,
		PipelineName:   "demo_grid",
		PartitionKey:   partition,
		Trigger:        "manual",
		PriorityClass:  "adhoc",
	}
}

func TestRegisterIsIdempotentAndVersioned(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p1, created, err := s.RegisterPipeline(ctx, demoDef())
	if err != nil || !created || p1.Version != 1 {
		t.Fatalf("first register: %+v created=%v err=%v", p1, created, err)
	}
	again, created, err := s.RegisterPipeline(ctx, demoDef())
	if err != nil || created || again.Version != 1 {
		t.Fatalf("same definition should return v1 unchanged: %+v created=%v err=%v", again, created, err)
	}
	changed := demoDef()
	changed.Tasks = append(changed.Tasks, domain.TaskSpec{Name: "check", Deps: []string{"parse"}})
	p2, created, err := s.RegisterPipeline(ctx, changed)
	if err != nil || !created || p2.Version != 2 {
		t.Fatalf("changed definition should be v2: %+v created=%v err=%v", p2, created, err)
	}
}

// Race: concurrent registrations of the same new definition produce one version.
func TestConcurrentRegisterCreatesOneVersion(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	versions := make(chan int, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _, err := s.RegisterPipeline(context.Background(), demoDef())
			if err != nil {
				t.Errorf("register: %v", err)
				return
			}
			versions <- p.Version
		}()
	}
	wg.Wait()
	close(versions)
	for v := range versions {
		if v != 1 {
			t.Errorf("got version %d, want 1 for every caller", v)
		}
	}
}

func TestRegisterRejectsBadDefinitions(t *testing.T) {
	s := newStore(t)
	bad := demoDef()
	bad.Tasks[0].Deps = []string{"parse"} // cycle fetch <-> parse
	if _, _, err := s.RegisterPipeline(context.Background(), bad); !errors.Is(err, ErrInvalid) {
		t.Errorf("cycle accepted: %v", err)
	}
	badTZ := demoDef()
	badTZ.PartitionTZ = "Mars/Olympus"
	if _, _, err := s.RegisterPipeline(context.Background(), badTZ); !errors.Is(err, ErrInvalid) {
		t.Errorf("bad time zone accepted: %v", err)
	}
}

func TestSubmitWritesRunTasksEdgesAndEvent(t *testing.T) {
	s := newStore(t)
	register(t, s)
	ctx := context.Background()

	run, created, err := s.SubmitRun(ctx, submitReq("k1", "2025-10-26"))
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	if run.Status != domain.RunPending || run.PipelineVersion != 1 {
		t.Errorf("run: %+v", run)
	}
	// Fall-back day in CET: 25-hour data interval computed by Atlas.
	if got := run.DataIntervalEnd.Sub(run.DataIntervalStart); got != 25*time.Hour {
		t.Errorf("interval %v, want 25h", got)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	status := map[string]domain.State{}
	for _, task := range got.Tasks {
		status[task.Name] = task.Status
	}
	if status["fetch"] != domain.Ready || status["parse"] != domain.Blocked {
		t.Errorf("task states %v, want fetch READY, parse BLOCKED", status)
	}
	var edges, events int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM task_edges WHERE run_id = $1`, run.ID).Scan(&edges)
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE run_id = $1 AND type = 'run.created'`, run.ID).Scan(&events)
	if edges != 1 || events != 1 {
		t.Errorf("edges=%d events=%d, want 1 and 1", edges, events)
	}
}

// Race: N concurrent identical submits create exactly one run, and every caller
// gets that run back.
func TestConcurrentDuplicateSubmitsCreateOneRun(t *testing.T) {
	s := newStore(t)
	register(t, s)
	const n = 20
	ids := make(chan string, n)
	createdCount := make(chan bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, created, err := s.SubmitRun(context.Background(), submitReq("same-key", "2026-11-18"))
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			ids <- run.ID
			createdCount <- created
		}()
	}
	wg.Wait()
	close(ids)
	close(createdCount)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Errorf("callers got different runs: %s vs %s", id, first)
		}
	}
	nCreated := 0
	for c := range createdCount {
		if c {
			nCreated++
		}
	}
	var rows int
	_ = s.pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&rows)
	if rows != 1 || nCreated != 1 {
		t.Errorf("rows=%d created=%d, want 1 and 1", rows, nCreated)
	}
}

func TestSameKeyDifferentRequestIsAConflict(t *testing.T) {
	s := newStore(t)
	register(t, s)
	ctx := context.Background()
	if _, _, err := s.SubmitRun(ctx, submitReq("k1", "2026-11-18")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SubmitRun(ctx, submitReq("k1", "2026-11-19")); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Errorf("got %v, want ErrIdempotencyMismatch", err)
	}
}

// Duplicate active run: a different key for the same active partition is rejected,
// naming the run that holds the partition. Once that run is finished, the
// partition can be submitted again.
func TestOneActiveRunPerPartition(t *testing.T) {
	s := newStore(t)
	register(t, s)
	ctx := context.Background()
	first, _, err := s.SubmitRun(ctx, submitReq("schedule:demo_grid:2026-11-18", "2026-11-18"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.SubmitRun(ctx, submitReq("manual-1", "2026-11-18"))
	var active *ActiveRunError
	if !errors.As(err, &active) || !errors.Is(err, ErrActiveRunExists) || active.RunID != first.ID {
		t.Fatalf("got %v, want ActiveRunError naming %s", err, first.ID)
	}
	// Another partition is unaffected.
	if _, _, err := s.SubmitRun(ctx, submitReq("manual-2", "2026-11-19")); err != nil {
		t.Errorf("other partition rejected: %v", err)
	}
	// Finish the first run (state changes arrive with A2; set directly here).
	if _, err := s.pool.Exec(ctx, `UPDATE workflow_runs SET status = 'SUCCEEDED' WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, created, err := s.SubmitRun(ctx, submitReq("manual-3", "2026-11-18")); err != nil || !created {
		t.Errorf("re-run after finish: created=%v err=%v", created, err)
	}
}

// Race: concurrent submits with different keys for one partition → one winner.
func TestConcurrentDifferentKeysOneWinner(t *testing.T) {
	s := newStore(t)
	register(t, s)
	var wg sync.WaitGroup
	results := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.SubmitRun(context.Background(), submitReq("key-"+string(rune('a'+i)), "2026-11-18"))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrActiveRunExists):
			conflicts++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if wins != 1 || conflicts != 9 {
		t.Errorf("wins=%d conflicts=%d, want 1 and 9", wins, conflicts)
	}
}

func TestSubmitValidation(t *testing.T) {
	s := newStore(t)
	register(t, s)
	ctx := context.Background()
	cases := map[string]SubmitRequest{
		"no key":        {PipelineName: "demo_grid", PartitionKey: "2026-11-18", Trigger: "manual", PriorityClass: "adhoc"},
		"bad partition": submitReq("k", "2026/11/18"),
		"bad trigger":   {IdempotencyKey: "k", PipelineName: "demo_grid", PartitionKey: "2026-11-18", Trigger: "cron", PriorityClass: "adhoc"},
		"bad priority":  {IdempotencyKey: "k", PipelineName: "demo_grid", PartitionKey: "2026-11-18", Trigger: "manual", PriorityClass: "urgent"},
	}
	for name, req := range cases {
		if _, _, err := s.SubmitRun(ctx, req); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
	unknown := submitReq("k", "2026-11-18")
	unknown.PipelineName = "nope"
	if _, _, err := s.SubmitRun(ctx, unknown); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown pipeline: got %v, want ErrNotFound", err)
	}
	if _, err := s.GetRun(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown run: got %v", err)
	}
	if _, err := s.GetRun(ctx, "not-a-uuid"); !errors.Is(err, ErrNotFound) {
		t.Errorf("malformed id: got %v", err)
	}
}
