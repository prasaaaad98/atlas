// OWNERSHIP: owner
//
// The task state machine: the single place that decides which task state
// transitions are legal. It is pure (no database, clock or network) so every
// (state, event) pair can be tested exhaustively.
// Who calls it: later, internal/store and internal/scheduler, before they write
// a new task status to Postgres.
package domain

// TODO(owner): define a State type and one constant per task state
// (BLOCKED, READY, LEASED, RUNNING, SUCCEEDED, FAILED_TERMINAL, CANCELED).

// TODO(owner): define an Event type and one constant per event
// (submit, parent succeeded, claim, start, complete, fail, lease expired, cancel).

// TODO(owner): define a sentinel error for illegal transitions, so callers can
// check it with errors.Is.

// TODO(owner): write Next(state, event) that returns the new state, or the
// sentinel error if the transition is not allowed. Terminal states reject everything.
