// Run-level status, derived from the states of a run's tasks.
//
// Why it exists: a run's status is never set directly; it is recomputed from
// its tasks in the same transaction as the last task transition (ADR-003), so
// run and task status can never disagree after a crash.
// Who calls it: internal/store, inside every task-transition transaction (A1+).
package domain

import "fmt"

// RunStatus values are the exact values stored in workflow_runs.status.
// PENDING and RUNNING are the "active" statuses covered by the partial unique
// index on (pipeline_name, partition_key).
type RunStatus string

const (
	RunPending   RunStatus = "PENDING"
	RunRunning   RunStatus = "RUNNING"
	RunSucceeded RunStatus = "SUCCEEDED"
	RunFailed    RunStatus = "FAILED"
	RunCanceled  RunStatus = "CANCELED"
)

// DeriveRunStatus computes a run's status from its tasks' stored states.
//
// Rules, in order:
//  1. Any task not yet terminal: PENDING if no task has been claimed or
//     finished, otherwise RUNNING. (A failed branch does not end the run
//     while an independent branch is still working.)
//  2. All terminal: FAILED if any task failed, else CANCELED if any task was
//     canceled, else SUCCEEDED.
func DeriveRunStatus(tasks []State) (RunStatus, error) {
	if len(tasks) == 0 {
		return "", fmt.Errorf("run has no tasks")
	}
	var active, started, failed, canceled bool
	for _, s := range tasks {
		switch s {
		case Blocked, Ready:
			active = true
		case Leased, Running:
			active, started = true, true
		case Succeeded:
			started = true
		case FailedTerminal:
			started, failed = true, true
		case Canceled:
			started, canceled = true, true
		default:
			// CREATED never reaches the database; anything else is corruption.
			return "", fmt.Errorf("task state %q cannot appear in a stored run", s)
		}
	}
	switch {
	case active && !started:
		return RunPending, nil
	case active:
		return RunRunning, nil
	case failed:
		return RunFailed, nil
	case canceled:
		return RunCanceled, nil
	default:
		return RunSucceeded, nil
	}
}
