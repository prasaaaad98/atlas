// The task state machine: the single place that decides which task state
// transitions are legal.
//
// Why it exists: every status write to Postgres must first ask Next whether
// the move is allowed, so "terminal states are final" and "only a running
// attempt can complete" are decided in one pure, exhaustively tested function.
// It does no I/O (no database, clock or network).
//
// Who calls it: internal/store and internal/scheduler (from Slice A1 on),
// before writing a task's new status. The SQL also guards terminal states with
// `WHERE status NOT IN (...)`; this function and that clause must agree.
//
// Invariants protected (ADR-003):
//   - SUCCEEDED, FAILED_TERMINAL and CANCELED accept no event at all.
//   - Facts Next cannot see (attempt counts, dependency counts) are turned
//     into split events by the helpers below, never re-implemented by callers.
package domain

import (
	"errors"
	"fmt"
)

// State is a task's stored status. The string values are the exact values
// stored in the tasks.status column.
type State string

const (
	Created        State = "CREATED" // in memory only, before the submit transaction inserts the row
	Blocked        State = "BLOCKED"
	Ready          State = "READY"
	Leased         State = "LEASED"
	Running        State = "RUNNING"
	Succeeded      State = "SUCCEEDED"
	FailedTerminal State = "FAILED_TERMINAL"
	Canceled       State = "CANCELED"
)

// Event is something that happened to a task. Events whose outcome depends on
// facts outside (state, event) are split in two (ADR-003 decision 1).
type Event string

const (
	EvSubmitReady         Event = "submit_ready"          // task has no dependencies
	EvSubmitBlocked       Event = "submit_blocked"        // task waits for parents
	EvDepsSatisfied       Event = "deps_satisfied"        // every parent succeeded with passing blocking checks
	EvDepFailed           Event = "dep_failed"            // a parent ended FAILED_TERMINAL or CANCELED
	EvClaim               Event = "claim"                 // a worker leased the task
	EvStart               Event = "start"                 // the leased worker began executing
	EvComplete            Event = "complete"              // succeeded, blocking checks passed
	EvCompleteCheckFailed Event = "complete_check_failed" // ran, but a blocking data check failed
	EvFailRetry           Event = "fail_retry"            // retryable failure, attempts remain
	EvFailFinal           Event = "fail_final"            // non-retryable, or attempts exhausted
	EvLeaseExpiredRetry   Event = "lease_expired_retry"   // reaper: lease lapsed, attempts remain
	EvLeaseExpiredFinal   Event = "lease_expired_final"   // reaper: lease lapsed, attempts exhausted
	EvCancel              Event = "cancel"                // owner or run cancellation
)

// ErrIllegalTransition is returned for every (state, event) pair not in the
// table. Callers check it with errors.Is and map it to HTTP 409.
var ErrIllegalTransition = errors.New("illegal task state transition")

// transitions is the whole state machine. A pair missing from this table is
// illegal. Terminal states have no entry, so they reject every event.
var transitions = map[State]map[Event]State{
	Created: {
		EvSubmitReady:   Ready,
		EvSubmitBlocked: Blocked,
	},
	Blocked: {
		EvDepsSatisfied: Ready,
		// A dependent of a failed task never ran, so it is CANCELED rather
		// than FAILED_TERMINAL: "failed" is reserved for tasks with a failed attempt.
		EvDepFailed: Canceled,
		EvCancel:    Canceled,
	},
	Ready: {
		EvClaim:  Leased,
		EvCancel: Canceled,
	},
	Leased: {
		EvStart: Running,
		// A worker may fail before start (e.g. it cannot load the task code).
		EvFailRetry:         Ready,
		EvFailFinal:         FailedTerminal,
		EvLeaseExpiredRetry: Ready,
		EvLeaseExpiredFinal: FailedTerminal,
		EvCancel:            Canceled,
	},
	Running: {
		EvComplete: Succeeded,
		// Re-fetching identical bad data will not fix it, so no automatic retry.
		EvCompleteCheckFailed: FailedTerminal,
		EvFailRetry:           Ready,
		EvFailFinal:           FailedTerminal,
		EvLeaseExpiredRetry:   Ready,
		EvLeaseExpiredFinal:   FailedTerminal,
		// Cancel takes effect immediately (ADR-003 decision 3). The running
		// worker learns about it from its next heartbeat; its completion is
		// then rejected because the task is no longer RUNNING.
		EvCancel: Canceled,
	},
}

// Next returns the state a task moves to when event e happens in state s.
// On an illegal pair it returns s unchanged and ErrIllegalTransition.
func Next(s State, e Event) (State, error) {
	if to, ok := transitions[s][e]; ok {
		return to, nil
	}
	return s, fmt.Errorf("%w: %s on %s", ErrIllegalTransition, e, s)
}

// Terminal reports whether no further transition is possible.
func (s State) Terminal() bool {
	return s == Succeeded || s == FailedTerminal || s == Canceled
}

// AllStates and AllEvents let tests walk the full (state, event) grid.
func AllStates() []State {
	return []State{Created, Blocked, Ready, Leased, Running, Succeeded, FailedTerminal, Canceled}
}

func AllEvents() []Event {
	return []Event{
		EvSubmitReady, EvSubmitBlocked, EvDepsSatisfied, EvDepFailed, EvClaim, EvStart,
		EvComplete, EvCompleteCheckFailed, EvFailRetry, EvFailFinal,
		EvLeaseExpiredRetry, EvLeaseExpiredFinal, EvCancel,
	}
}

// AttemptsRemain is the only implementation of "may this task be retried?".
// attempt is the 1-based number of the attempt that just ended. A max of 0 or
// less is treated as "no retries" so a bad config can never loop forever.
//
// The caller must read attempt and max from the task row it has locked in the
// same transaction as the status write, or the answer can be stale.
func AttemptsRemain(attempt, max int) bool {
	return attempt < max
}

// FailEvent picks the split event for a failure reported by a worker.
func FailEvent(attempt, max int, retryable bool) Event {
	if retryable && AttemptsRemain(attempt, max) {
		return EvFailRetry
	}
	return EvFailFinal
}

// LeaseExpiredEvent picks the split event for the reaper. A lapsed lease is
// always retryable: the outcome of the attempt is unknown, not failed.
func LeaseExpiredEvent(attempt, max int) Event {
	if AttemptsRemain(attempt, max) {
		return EvLeaseExpiredRetry
	}
	return EvLeaseExpiredFinal
}
