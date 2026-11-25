package domain

import (
	"testing"
	"time"
)

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestParseCronRejectsBadExpressions(t *testing.T) {
	for _, bad := range []string{"", "* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *",
		"* * * * 8", "*/0 * * * *", "5-1 * * * *", "a * * * *"} {
		if _, err := ParseCron(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestCronNextInTimeZone(t *testing.T) {
	brussels := mustLoc(t, "Europe/Brussels")
	c, _ := ParseCron("0 14 * * *")
	// 14:00 Brussels in winter is 13:00 UTC; in summer 12:00 UTC.
	got, _ := c.Next(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC), brussels)
	if want := time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("winter: %v, want %v", got, want)
	}
	got, _ = c.Next(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC), brussels)
	if want := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("summer: %v, want %v (12:00 UTC on Jul 1 is exactly 14:00 local, not after)", got, want)
	}
	if PartitionFor(got, brussels, 1) != "2026-07-03" {
		t.Errorf("partition with +1 day = %s", PartitionFor(got, brussels, 1))
	}
}

func TestCronFieldsListsRangesStepsAndDayRule(t *testing.T) {
	utc := time.UTC
	c, _ := ParseCron("*/15 9-10 * * 1-5") // every 15 min, 09-10h, weekdays
	fires := c.Between(time.Date(2026, 11, 27, 8, 59, 0, 0, utc), time.Date(2026, 11, 30, 23, 0, 0, 0, utc), utc, 100)
	// Fri 27th: 8 fires; Sat/Sun none; Mon 30th: 8 fires.
	if len(fires) != 16 || fires[0].Minute() != 0 || fires[1].Minute() != 15 || fires[8].Day() != 30 {
		t.Fatalf("fires = %v", fires)
	}
	// dom and dow both restricted: OR (1st of the month, or any Sunday).
	c, _ = ParseCron("0 0 1 * 0")
	fires = c.Between(time.Date(2026, 11, 1, 0, 0, 0, 0, utc), time.Date(2026, 12, 1, 0, 0, 0, 0, utc), utc, 100)
	// Sundays after Nov 1 (a Sunday itself, excluded by "after"): 8, 15, 22, 29; then Dec 1.
	if len(fires) != 5 || fires[4].Day() != 1 {
		t.Fatalf("OR rule fires = %v", fires)
	}
	c, _ = ParseCron("0 0 29 2 *")
	if n, ok := c.Next(time.Date(2026, 3, 1, 0, 0, 0, 0, utc), utc); !ok || n.Year() != 2028 {
		t.Fatalf("Feb 29 next = %v %v", n, ok)
	}
}

func TestCronAcrossDST(t *testing.T) {
	brussels := mustLoc(t, "Europe/Brussels")
	daily, _ := ParseCron("0 14 * * *")
	// One fire per local day across both DST changes: 14:00 local each day.
	fires := daily.Between(time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC), brussels, 10)
	if len(fires) != 4 {
		t.Fatalf("spring week fires = %v", fires)
	}
	for _, f := range fires {
		if f.In(brussels).Hour() != 14 {
			t.Errorf("fire %v is %v local", f, f.In(brussels))
		}
	}
	// 02:30 doesn't exist on 2026-03-29 in Brussels: no fire that day (documented).
	c, _ := ParseCron("30 2 * * *")
	fires = c.Between(time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC), brussels, 10)
	if len(fires) != 1 || fires[0].In(brussels).Day() != 30 {
		t.Fatalf("spring-forward 02:30 fires = %v", fires)
	}
	// 02:30 happens twice on 2025-10-26: two fires, one hour apart (documented).
	fires = c.Between(time.Date(2025, 10, 25, 12, 0, 0, 0, time.UTC), time.Date(2025, 10, 26, 12, 0, 0, 0, time.UTC), brussels, 10)
	if len(fires) != 2 || fires[1].Sub(fires[0]) != time.Hour {
		t.Fatalf("fall-back 02:30 fires = %v", fires)
	}
}

func TestCronPrev(t *testing.T) {
	c, _ := ParseCron("0 6 * * *")
	at := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)
	p, ok := c.Prev(at, time.UTC, 48*time.Hour)
	if !ok || !p.Equal(time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)) {
		t.Fatalf("Prev = %v %v", p, ok)
	}
}
