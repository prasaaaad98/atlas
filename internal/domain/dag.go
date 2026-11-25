// Pipeline definitions and partitions: pure validation the control plane runs
// before anything is written.
//
// Why it exists: pipeline definitions arrive from clients (the Python SDK).
// Atlas must not trust them: a cycle or a dangling dependency would leave tasks
// BLOCKED forever. Partition keys must map to exactly one UTC data interval,
// computed by Atlas from the pipeline's time zone (DST days are 23/25 hours).
// Who calls it: internal/store (pipeline registration, run submission).
package domain

import (
	"errors"
	"fmt"
	"regexp"
	"time"
	_ "time/tzdata" // embed the IANA database: Windows has none, and results must not depend on the host
)

var (
	ErrInvalidDefinition = errors.New("invalid pipeline definition")
	ErrInvalidPartition  = errors.New("invalid partition")
)

var (
	nameRE     = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)
	assetKeyRE = regexp.MustCompile(`^(bronze|silver|gold|demo)\.[a-z0-9_]+$`)
	dateRE     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

// TaskSpec is one task in a pipeline definition.
type TaskSpec struct {
	Name         string   `json:"name"`
	Deps         []string `json:"deps,omitempty"`
	AssetOutputs []string `json:"asset_outputs,omitempty"`
	MaxAttempts  int      `json:"max_attempts,omitempty"` // 0 means 1; retry policies arrive in A4
}

// ValidateDAG checks names, dependencies and acyclicity.
func ValidateDAG(tasks []TaskSpec) error {
	if len(tasks) == 0 {
		return fmt.Errorf("%w: no tasks", ErrInvalidDefinition)
	}
	deps := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		if !nameRE.MatchString(t.Name) {
			return fmt.Errorf("%w: task name %q", ErrInvalidDefinition, t.Name)
		}
		if _, dup := deps[t.Name]; dup {
			return fmt.Errorf("%w: duplicate task %q", ErrInvalidDefinition, t.Name)
		}
		if t.MaxAttempts < 0 {
			return fmt.Errorf("%w: task %q max_attempts < 0", ErrInvalidDefinition, t.Name)
		}
		for _, a := range t.AssetOutputs {
			if !assetKeyRE.MatchString(a) {
				return fmt.Errorf("%w: task %q asset key %q", ErrInvalidDefinition, t.Name, a)
			}
		}
		deps[t.Name] = t.Deps
	}
	for _, t := range tasks {
		seen := map[string]bool{}
		for _, d := range t.Deps {
			if _, ok := deps[d]; !ok {
				return fmt.Errorf("%w: task %q depends on unknown %q", ErrInvalidDefinition, t.Name, d)
			}
			if seen[d] {
				return fmt.Errorf("%w: task %q lists %q twice", ErrInvalidDefinition, t.Name, d)
			}
			seen[d] = true
		}
	}
	// Depth-first search with three colours; a grey node reached again is a cycle.
	const white, grey, black = 0, 1, 2
	colour := map[string]int{}
	var visit func(string) error
	visit = func(n string) error {
		switch colour[n] {
		case grey:
			return fmt.Errorf("%w: dependency cycle through %q", ErrInvalidDefinition, n)
		case black:
			return nil
		}
		colour[n] = grey
		for _, d := range deps[n] {
			if err := visit(d); err != nil {
				return err
			}
		}
		colour[n] = black
		return nil
	}
	for _, t := range tasks {
		if err := visit(t.Name); err != nil {
			return err
		}
	}
	return nil
}

// DailyInterval returns the UTC bounds of a daily partition: local midnight to
// the next local midnight in tz. An IST day 2026-11-24 is
// 2026-11-23T18:30Z → 2026-11-24T18:30Z; a CET fall-back day is 25 hours.
func DailyInterval(partitionKey, tz string) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil || tz == "" || tz == "Local" {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: time zone %q", ErrInvalidPartition, tz)
	}
	if !dateRE.MatchString(partitionKey) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: key %q is not YYYY-MM-DD", ErrInvalidPartition, partitionKey)
	}
	d, err := time.ParseInLocation("2006-01-02", partitionKey, loc)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: key %q: %v", ErrInvalidPartition, partitionKey, err)
	}
	next := time.Date(d.Year(), d.Month(), d.Day()+1, 0, 0, 0, 0, loc)
	return d.UTC(), next.UTC(), nil
}

// ValidTrigger and ValidPriorityClass mirror the CHECK constraints in migration 000001.
func ValidTrigger(s string) bool { return s == "schedule" || s == "backfill" || s == "manual" }

func ValidPriorityClass(s string) bool { return s == "live" || s == "backfill" || s == "adhoc" }
