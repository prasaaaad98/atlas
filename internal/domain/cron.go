// A small 5-field cron parser and fire-time calculator (Slice A6, ADR-011).
//
// Why it exists: schedules are cron expressions in a pipeline's own time zone
// (e.g. "0 14 * * *" in Europe/Brussels). The standard library has no cron, and
// the subset Atlas needs (numbers, *, lists, ranges, steps) is small enough to
// own and test, instead of adding a dependency (CLAUDE.md: no new dependency
// without need).
// Who calls it: internal/store (schedule ticker, freshness policies).
//
// Semantics (documented and tested, because DST makes them matter):
//   - Fields: minute hour day-of-month month day-of-week (0-6, Sunday = 0; 7 is Sunday too).
//   - If both day-of-month and day-of-week are restricted, a day matches if
//     EITHER matches (Vixie cron rule).
//   - Fire times are found by walking real instants minute by minute and
//     reading their local wall time. Consequence: a local time that doesn't
//     exist (spring forward, e.g. 02:30 in Brussels on the last Sunday of
//     March) never fires that day, and a local time that occurs twice (fall
//     back) fires twice. Schedules should avoid 02:00-03:00 local in DST zones;
//     a double fire is harmless because scheduled submissions are idempotent.
package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCron = errors.New("invalid cron expression")

type Cron struct {
	expr                         string
	minute, hour, dom, month     []bool
	dow                          []bool
	domRestricted, dowRestricted bool
}

func (c Cron) String() string { return c.expr }

func ParseCron(expr string) (Cron, error) {
	f := strings.Fields(expr)
	if len(f) != 5 {
		return Cron{}, fmt.Errorf("%w: %q needs 5 fields (minute hour day month weekday)", ErrInvalidCron, expr)
	}
	c := Cron{expr: strings.Join(f, " ")}
	var err error
	if c.minute, err = field(f[0], 0, 59); err != nil {
		return Cron{}, fmt.Errorf("%w: minute: %v", ErrInvalidCron, err)
	}
	if c.hour, err = field(f[1], 0, 23); err != nil {
		return Cron{}, fmt.Errorf("%w: hour: %v", ErrInvalidCron, err)
	}
	if c.dom, err = field(f[2], 1, 31); err != nil {
		return Cron{}, fmt.Errorf("%w: day of month: %v", ErrInvalidCron, err)
	}
	if c.month, err = field(f[3], 1, 12); err != nil {
		return Cron{}, fmt.Errorf("%w: month: %v", ErrInvalidCron, err)
	}
	if c.dow, err = field(f[4], 0, 7); err != nil {
		return Cron{}, fmt.Errorf("%w: day of week: %v", ErrInvalidCron, err)
	}
	if c.dow[7] {
		c.dow[0] = true
	}
	c.domRestricted, c.dowRestricted = f[2] != "*", f[4] != "*"
	return c, nil
}

// field parses one comma-separated cron field into a membership table.
func field(s string, lo, hi int) ([]bool, error) {
	out := make([]bool, hi+1)
	for _, part := range strings.Split(s, ",") {
		step := 1
		if i := strings.Index(part, "/"); i >= 0 {
			n, err := strconv.Atoi(part[i+1:])
			if err != nil || n < 1 {
				return nil, fmt.Errorf("bad step in %q", part)
			}
			step, part = n, part[:i]
		}
		from, to := lo, hi
		switch {
		case part == "*":
		case strings.Contains(part, "-"):
			a, b, _ := strings.Cut(part, "-")
			x, err1 := strconv.Atoi(a)
			y, err2 := strconv.Atoi(b)
			if err1 != nil || err2 != nil || x > y {
				return nil, fmt.Errorf("bad range %q", part)
			}
			from, to = x, y
		default:
			x, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("bad value %q", part)
			}
			from, to = x, x
			if step > 1 { // "5/15" means 5, 20, 35, 50
				to = hi
			}
		}
		if from < lo || to > hi {
			return nil, fmt.Errorf("%q outside %d-%d", part, lo, hi)
		}
		for v := from; v <= to; v += step {
			out[v] = true
		}
	}
	return out, nil
}

func (c Cron) matches(t time.Time) bool {
	if !c.minute[t.Minute()] || !c.hour[t.Hour()] || !c.month[int(t.Month())] {
		return false
	}
	domOK, dowOK := c.dom[t.Day()], c.dow[int(t.Weekday())]
	if c.domRestricted && c.dowRestricted {
		return domOK || dowOK
	}
	return domOK && dowOK
}

// maxSearch bounds Next: the rarest valid schedule ("0 0 29 2 *") fires within 8 years.
const maxSearch = 8 * 366 * 24 * 60

// Next returns the first fire time strictly after `after`, judged in loc.
func (c Cron) Next(after time.Time, loc *time.Location) (time.Time, bool) {
	t := after.UTC().Truncate(time.Minute).Add(time.Minute)
	for range maxSearch {
		if c.matches(t.In(loc)) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

// Between returns fire times in (from, to], at most limit of them, oldest first.
func (c Cron) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	var out []time.Time
	t := from
	for len(out) < limit {
		n, ok := c.Next(t, loc)
		if !ok || n.After(to) {
			break
		}
		out = append(out, n)
		t = n
	}
	return out
}

// Prev returns the last fire time at or before `at`, searching back up to `window`.
func (c Cron) Prev(at time.Time, loc *time.Location, window time.Duration) (time.Time, bool) {
	fires := c.Between(at.Add(-window), at, loc, 1<<20)
	if len(fires) == 0 {
		return time.Time{}, false
	}
	return fires[len(fires)-1], true
}

// PartitionFor maps a fire time to the daily partition it produces: the local
// date of the fire plus offsetDays (e.g. +1: a 14:00 run fetches tomorrow's
// day-ahead delivery date).
func PartitionFor(fire time.Time, loc *time.Location, offsetDays int) string {
	return fire.In(loc).AddDate(0, 0, offsetDays).Format("2006-01-02")
}
