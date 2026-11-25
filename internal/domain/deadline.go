// Deadline rules and critical-path estimates (Slice A7, ADR-012).
//
// Why it exists: data for a market is only useful if it lands before a
// business deadline (e.g. before the next day's bids). Atlas computes each
// run's deadline from its partition, orders claims earliest-deadline-first
// within a priority class, and flags runs that are at risk or missed. It is
// deadline-AWARE scheduling, best effort: a deadline can still be missed, and
// then the miss is detected and recorded, never hidden.
// Who calls it: internal/store (submit, the deadline evaluator).
package domain

import (
	"fmt"
	"regexp"
	"time"
)

// DeadlineRule is part of a pipeline definition.
type DeadlineRule struct {
	// RelativeTo is "partition_start" or "partition_end" (the run's UTC data interval).
	RelativeTo string `json:"relative_to"`
	// OffsetMinutes is added to that instant; negative = before it (e.g. -600:
	// ten hours before the delivery day starts).
	OffsetMinutes int `json:"offset_minutes"`
	// LatePolicy on a miss: "continue" keeps running, "cancel" cancels the run.
	LatePolicy string `json:"late_policy,omitempty"`
	// FallbackPipeline, if set, is submitted for the same partition on a miss
	// (e.g. a cheaper source or a "carry forward and flag" pipeline).
	FallbackPipeline string `json:"fallback_pipeline,omitempty"`
}

var pipelineNameRE = regexp.MustCompile(`^[a-z0-9_]+$`)

func (d DeadlineRule) Validate() error {
	switch {
	case d.RelativeTo != "partition_start" && d.RelativeTo != "partition_end":
		return fmt.Errorf("%w: deadline relative_to must be partition_start or partition_end", ErrInvalidDefinition)
	case d.OffsetMinutes < -60*24*31 || d.OffsetMinutes > 60*24*31:
		return fmt.Errorf("%w: deadline offset_minutes must be within ±31 days", ErrInvalidDefinition)
	case d.LatePolicy != "" && d.LatePolicy != "continue" && d.LatePolicy != "cancel":
		return fmt.Errorf("%w: late_policy must be continue or cancel", ErrInvalidDefinition)
	case d.FallbackPipeline != "" && !pipelineNameRE.MatchString(d.FallbackPipeline):
		return fmt.Errorf("%w: fallback_pipeline %q", ErrInvalidDefinition, d.FallbackPipeline)
	}
	return nil
}

// DeadlineFor computes a run's deadline from its data interval.
func (d DeadlineRule) DeadlineFor(start, end time.Time) time.Time {
	base := start
	if d.RelativeTo == "partition_end" {
		base = end
	}
	return base.Add(time.Duration(d.OffsetMinutes) * time.Minute)
}

// PathTask is one task's remaining work for the critical-path estimate.
type PathTask struct {
	Name      string
	Deps      []string
	Remaining float64 // seconds still needed (0 for finished tasks)
}

// CriticalPath returns the longest chain of remaining work through the DAG:
// finish(t) = remaining(t) + max(finish(parent)). Unknown deps count as 0.
// The DAG is acyclic (validated at registration), so memoised recursion ends.
func CriticalPath(tasks []PathTask) float64 {
	by := make(map[string]PathTask, len(tasks))
	for _, t := range tasks {
		by[t.Name] = t
	}
	memo := map[string]float64{}
	var finish func(string) float64
	finish = func(n string) float64 {
		if v, ok := memo[n]; ok {
			return v
		}
		t, ok := by[n]
		if !ok {
			return 0
		}
		longest := 0.0
		for _, d := range t.Deps {
			longest = max(longest, finish(d))
		}
		memo[n] = t.Remaining + longest
		return memo[n]
	}
	out := 0.0
	for _, t := range tasks {
		out = max(out, finish(t.Name))
	}
	return out
}
