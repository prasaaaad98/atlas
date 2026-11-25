// Retry policies, full-jitter backoff and the error classifier (Slice A4, ADR-005).
//
// Why it exists: when an attempt fails or its lease lapses, two questions
// decide what happens next: may it be retried, and when. Both answers live
// here as pure functions so they're tested exhaustively and statistically,
// and the store only applies them.
// Who calls it: internal/store (Fail, the reaper, pipeline registration).
//
// Invariants protected:
//   - An error class that can't be fixed by trying again (bad credentials, a
//     bad request, failed data checks, code that doesn't match the registered
//     definition) is never retried, whatever the worker says.
//   - Backoff never exceeds the policy's max interval, and is randomised over
//     [0, cap) ("full jitter") so many failed tasks don't retry in lockstep.
package domain

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// RetryPolicy is stored per task (tasks.retry_policy). Field names follow
// Temporal's retry policy; max attempts stays on TaskSpec.MaxAttempts.
type RetryPolicy struct {
	InitialIntervalSeconds   float64  `json:"initial_interval_seconds"`
	BackoffCoefficient       float64  `json:"backoff_coefficient"`
	MaxIntervalSeconds       float64  `json:"max_interval_seconds"`
	NonRetryableErrorClasses []string `json:"non_retryable_error_classes,omitempty"`
}

// DefaultRetryPolicy applies when a task declares none. Configuration, not a
// measurement: revisit with real failure data (ADR-005).
var DefaultRetryPolicy = RetryPolicy{InitialIntervalSeconds: 5, BackoffCoefficient: 2, MaxIntervalSeconds: 300}

// TerminalErrorClasses are never retried. Each one means "the same attempt
// again would fail the same way".
var TerminalErrorClasses = []string{
	"auth",                    // HTTP 401/403: credentials won't fix themselves
	"bad_request",             // HTTP 4xx other than 429: the request itself is wrong
	"data_check_failed",       // re-fetching identical bad data won't fix it (ADR-003)
	"unknown_task",            // the worker has no such task
	"definition_mismatch",     // worker code differs from the registered definition
	"materialization_invalid", // task reported files that don't exist or don't match
}

func (p RetryPolicy) Validate() error {
	switch {
	case p.InitialIntervalSeconds < 0 || p.InitialIntervalSeconds > 86400:
		return fmt.Errorf("%w: initial_interval_seconds must be 0..86400", ErrInvalidDefinition)
	case p.BackoffCoefficient < 1 || p.BackoffCoefficient > 100:
		return fmt.Errorf("%w: backoff_coefficient must be 1..100", ErrInvalidDefinition)
	case p.MaxIntervalSeconds < p.InitialIntervalSeconds || p.MaxIntervalSeconds > 86400:
		return fmt.Errorf("%w: max_interval_seconds must be initial..86400", ErrInvalidDefinition)
	}
	return nil
}

// Cap is the upper bound of the backoff after the given 1-based attempt:
// min(max, initial * coefficient^(attempt-1)).
func (p RetryPolicy) Cap(attempt int) float64 {
	if attempt < 1 {
		attempt = 1
	}
	c := p.InitialIntervalSeconds * math.Pow(p.BackoffCoefficient, float64(attempt-1))
	if math.IsInf(c, 0) || math.IsNaN(c) || c > p.MaxIntervalSeconds {
		return p.MaxIntervalSeconds
	}
	return c
}

// Backoff is the delay before the next attempt after `attempt` failed:
// uniform over [0, Cap(attempt)) ("full jitter", AWS Architecture Blog,
// "Exponential Backoff And Jitter", 2015). u must be uniform in [0, 1).
// A source's Retry-After wins when it is longer.
func (p RetryPolicy) Backoff(attempt int, u float64, retryAfterSeconds float64) time.Duration {
	d := u * p.Cap(attempt)
	if retryAfterSeconds > d {
		d = retryAfterSeconds
	}
	return time.Duration(d * float64(time.Second))
}

// Retryable classifies a failure. The worker's hint says whether the task
// itself considers the error transient; the policy and the terminal classes
// can only veto a retry, never force one.
func (p RetryPolicy) Retryable(errorClass string, workerSaysRetryable bool) bool {
	if !workerSaysRetryable {
		return false
	}
	return !slices.Contains(TerminalErrorClasses, errorClass) && !slices.Contains(p.NonRetryableErrorClasses, errorClass)
}
