package domain

import (
	"errors"
	"testing"
	"time"
)

func TestValidateDAG(t *testing.T) {
	ok := []TaskSpec{{Name: "fetch"}, {Name: "parse", Deps: []string{"fetch"}}, {Name: "check", Deps: []string{"parse", "fetch"}}}
	if err := ValidateDAG(ok); err != nil {
		t.Fatalf("valid DAG rejected: %v", err)
	}
	bad := map[string][]TaskSpec{
		"empty":         {},
		"duplicate":     {{Name: "a"}, {Name: "a"}},
		"unknown dep":   {{Name: "a", Deps: []string{"zzz"}}},
		"self dep":      {{Name: "a", Deps: []string{"a"}}},
		"cycle":         {{Name: "a", Deps: []string{"c"}}, {Name: "b", Deps: []string{"a"}}, {Name: "c", Deps: []string{"b"}}},
		"bad name":      {{Name: "a b"}},
		"duplicate dep": {{Name: "a"}, {Name: "b", Deps: []string{"a", "a"}}},
		"bad asset key": {{Name: "a", AssetOutputs: []string{"Silver.Prices"}}},
	}
	for name, specs := range bad {
		if err := ValidateDAG(specs); !errors.Is(err, ErrInvalidDefinition) {
			t.Errorf("%s: got %v, want ErrInvalidDefinition", name, err)
		}
	}
}

func TestDailyInterval(t *testing.T) {
	cases := []struct {
		key, tz string
		start   string
		hours   int
	}{
		{"2026-11-24", "Asia/Kolkata", "2026-11-23T18:30:00Z", 24},    // IST: no DST
		{"2026-03-29", "Europe/Brussels", "2026-03-28T23:00:00Z", 23}, // spring forward
		{"2025-10-26", "Europe/Brussels", "2025-10-25T22:00:00Z", 25}, // fall back
		{"2026-03-08", "America/Chicago", "2026-03-08T06:00:00Z", 23}, // US Central spring forward
	}
	for _, c := range cases {
		start, end, err := DailyInterval(c.key, c.tz)
		if err != nil {
			t.Fatalf("%s %s: %v", c.key, c.tz, err)
		}
		if got := start.Format(time.RFC3339); got != c.start {
			t.Errorf("%s %s: start %s, want %s", c.key, c.tz, got, c.start)
		}
		if got := int(end.Sub(start).Hours()); got != c.hours {
			t.Errorf("%s %s: %d hours, want %d", c.key, c.tz, got, c.hours)
		}
	}
}

// Failure test: wrong-day formats must never be silently reinterpreted.
func TestDailyIntervalRejectsBadInput(t *testing.T) {
	for _, key := range []string{"20261124", "2026-11-24T00:00", "2026-13-01", "2026-02-30", ""} {
		if _, _, err := DailyInterval(key, "UTC"); !errors.Is(err, ErrInvalidPartition) {
			t.Errorf("key %q accepted: %v", key, err)
		}
	}
	if _, _, err := DailyInterval("2026-11-24", "Mars/Olympus"); !errors.Is(err, ErrInvalidPartition) {
		t.Errorf("unknown zone accepted: %v", err)
	}
}

func TestTriggerAndPriority(t *testing.T) {
	for _, v := range []string{"schedule", "backfill", "manual"} {
		if !ValidTrigger(v) {
			t.Errorf("trigger %q rejected", v)
		}
	}
	for _, v := range []string{"live", "backfill", "adhoc"} {
		if !ValidPriorityClass(v) {
			t.Errorf("priority %q rejected", v)
		}
	}
	if ValidTrigger("cron") || ValidPriorityClass("urgent") {
		t.Error("unknown values accepted")
	}
}
