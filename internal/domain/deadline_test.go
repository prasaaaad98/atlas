package domain

import (
	"testing"
	"time"
)

func TestDeadlineFor(t *testing.T) {
	start := time.Date(2026, 11, 23, 23, 0, 0, 0, time.UTC) // Brussels midnight, 24 Nov
	end := start.Add(24 * time.Hour)
	r := DeadlineRule{RelativeTo: "partition_start", OffsetMinutes: -600}
	if got := r.DeadlineFor(start, end); !got.Equal(start.Add(-10 * time.Hour)) {
		t.Errorf("start-10h = %v", got)
	}
	r = DeadlineRule{RelativeTo: "partition_end", OffsetMinutes: 180}
	if got := r.DeadlineFor(start, end); !got.Equal(end.Add(3 * time.Hour)) {
		t.Errorf("end+3h = %v", got)
	}
	for _, bad := range []DeadlineRule{{RelativeTo: "noon"}, {RelativeTo: "partition_end", LatePolicy: "panic"},
		{RelativeTo: "partition_end", OffsetMinutes: 1 << 20}, {RelativeTo: "partition_end", FallbackPipeline: "Bad Name"}} {
		if bad.Validate() == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestCriticalPath(t *testing.T) {
	// a(10) → b(5), c(20) → d(1): longest = 10 + 20 + 1 = 31.
	tasks := []PathTask{
		{Name: "a", Remaining: 10}, {Name: "b", Deps: []string{"a"}, Remaining: 5},
		{Name: "c", Deps: []string{"a"}, Remaining: 20}, {Name: "d", Deps: []string{"b", "c"}, Remaining: 1},
	}
	if got := CriticalPath(tasks); got != 31 {
		t.Fatalf("critical path = %v, want 31", got)
	}
	tasks[0].Remaining, tasks[2].Remaining = 0, 0 // a and c done
	if got := CriticalPath(tasks); got != 6 {
		t.Fatalf("after a and c finished = %v, want 6 (b then d)", got)
	}
	if CriticalPath(nil) != 0 {
		t.Fatal("empty DAG")
	}
}
