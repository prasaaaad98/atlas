// Tests for the task state machine.
//
// Why: the state machine is the first line of defence for "terminal states are
// final" and "a task is only completed by the attempt that is running it".
// These tests walk every (state, event) pair, so adding a state or event
// without deciding its transitions makes the suite fail.
package domain

import (
	"errors"
	"testing"
)

// legal is the complete list of allowed transitions. Anything not listed here
// must be rejected with ErrIllegalTransition.
var legal = map[State]map[Event]State{
	Created: {
		EvSubmitReady:   Ready,
		EvSubmitBlocked: Blocked,
	},
	Blocked: {
		EvDepsSatisfied: Ready,
		EvDepFailed:     Canceled,
		EvCancel:        Canceled,
	},
	Ready: {
		EvClaim:  Leased,
		EvCancel: Canceled,
	},
	Leased: {
		EvStart:             Running,
		EvFailRetry:         Ready,
		EvFailFinal:         FailedTerminal,
		EvLeaseExpiredRetry: Ready,
		EvLeaseExpiredFinal: FailedTerminal,
		EvCancel:            Canceled,
	},
	Running: {
		EvComplete:            Succeeded,
		EvCompleteCheckFailed: FailedTerminal,
		EvFailRetry:           Ready,
		EvFailFinal:           FailedTerminal,
		EvLeaseExpiredRetry:   Ready,
		EvLeaseExpiredFinal:   FailedTerminal,
		EvCancel:              Canceled,
	},
}

func TestNextEveryPair(t *testing.T) {
	for _, s := range AllStates() {
		for _, e := range AllEvents() {
			want, ok := legal[s][e]
			got, err := Next(s, e)
			if ok {
				if err != nil {
					t.Errorf("Next(%s, %s): unexpected error %v", s, e, err)
				} else if got != want {
					t.Errorf("Next(%s, %s) = %s, want %s", s, e, got, want)
				}
				continue
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("Next(%s, %s) = (%s, %v), want ErrIllegalTransition", s, e, got, err)
			}
			if got != s {
				t.Errorf("Next(%s, %s) on error returned %s, want unchanged state", s, e, got)
			}
		}
	}
}

// Failure test: a late completion (stale worker, canceled task) must never
// move a terminal task anywhere.
func TestTerminalStatesRejectEverything(t *testing.T) {
	for _, s := range []State{Succeeded, FailedTerminal, Canceled} {
		if !s.Terminal() {
			t.Fatalf("%s should be terminal", s)
		}
		for _, e := range AllEvents() {
			if _, err := Next(s, e); !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("terminal %s accepted %s", s, e)
			}
		}
	}
}

func TestUnknownStateAndEventRejected(t *testing.T) {
	if _, err := Next(State("BOGUS"), EvClaim); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("unknown state accepted: %v", err)
	}
	if _, err := Next(Ready, Event("bogus")); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("unknown event accepted: %v", err)
	}
}

// The boundary of the "attempts left" rule (ADR-003 decision 1): with
// max_attempts = 3, attempts 1 and 2 retry, attempt 3 is final.
func TestAttemptsRemainBoundaries(t *testing.T) {
	cases := []struct {
		attempt, max int
		want         bool
	}{
		{1, 3, true},
		{2, 3, true},  // max - 1
		{3, 3, false}, // max
		{4, 3, false}, // over the limit (should not happen, must still be final)
		{1, 1, false}, // no retries configured
		{1, 0, false}, // invalid config treated as "no retries", never infinite
	}
	for _, c := range cases {
		if got := AttemptsRemain(c.attempt, c.max); got != c.want {
			t.Errorf("AttemptsRemain(%d, %d) = %v, want %v", c.attempt, c.max, got, c.want)
		}
	}
}

func TestFailureEvents(t *testing.T) {
	if got := FailEvent(1, 3, true); got != EvFailRetry {
		t.Errorf("retryable with attempts left: got %s", got)
	}
	if got := FailEvent(3, 3, true); got != EvFailFinal {
		t.Errorf("retryable, attempts exhausted: got %s", got)
	}
	if got := FailEvent(1, 3, false); got != EvFailFinal {
		t.Errorf("non-retryable must be final: got %s", got)
	}
	if got := LeaseExpiredEvent(2, 3); got != EvLeaseExpiredRetry {
		t.Errorf("lease expired with attempts left: got %s", got)
	}
	if got := LeaseExpiredEvent(3, 3); got != EvLeaseExpiredFinal {
		t.Errorf("lease expired, attempts exhausted: got %s", got)
	}
}

func TestDeriveRunStatus(t *testing.T) {
	cases := []struct {
		name  string
		tasks []State
		want  RunStatus
	}{
		{"all waiting", []State{Ready, Blocked}, RunPending},
		{"one leased", []State{Leased, Blocked}, RunRunning},
		{"one done, one waiting", []State{Succeeded, Ready}, RunRunning},
		{"all succeeded", []State{Succeeded, Succeeded}, RunSucceeded},
		{"failure wins over cancel", []State{Succeeded, FailedTerminal, Canceled}, RunFailed},
		{"failure but branch still running", []State{FailedTerminal, Running}, RunRunning},
		{"all canceled", []State{Canceled, Canceled}, RunCanceled},
		{"succeeded and canceled", []State{Succeeded, Canceled}, RunCanceled},
	}
	for _, c := range cases {
		got, err := DeriveRunStatus(c.tasks)
		if err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestDeriveRunStatusRejectsEmptyAndUnknown(t *testing.T) {
	if _, err := DeriveRunStatus(nil); err == nil {
		t.Error("empty run accepted")
	}
	if _, err := DeriveRunStatus([]State{Created}); err == nil {
		t.Error("CREATED task in a stored run accepted")
	}
	if _, err := DeriveRunStatus([]State{"BOGUS"}); err == nil {
		t.Error("unknown state accepted")
	}
}
