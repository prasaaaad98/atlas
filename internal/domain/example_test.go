// Runnable trace of a task's life, used by `make demo-a0`.
//
// Why: it shows the state machine doing what the reaper and workers will do
// in later slices: a lease lapses (worker killed), the task is retried, the
// second attempt completes, and a stale completion from the first worker is
// rejected because SUCCEEDED is terminal. `go test` checks the printed output.
package domain_test

import (
	"fmt"

	"github.com/prasaaaad98/atlas/internal/domain"
)

func ExampleNext() {
	const maxAttempts = 3
	s := domain.Created

	step := func(e domain.Event) {
		next, err := domain.Next(s, e)
		if err != nil {
			fmt.Printf("%-16s --%s--> rejected (%v)\n", s, e, err)
			return
		}
		fmt.Printf("%-16s --%s--> %s\n", s, e, next)
		s = next
	}

	step(domain.EvSubmitReady)
	step(domain.EvClaim) // attempt 1
	step(domain.EvStart)
	// Worker 1 is killed; the reaper sees its lease lapse.
	step(domain.LeaseExpiredEvent(1, maxAttempts))
	step(domain.EvClaim) // attempt 2
	step(domain.EvStart)
	step(domain.EvComplete)
	// Worker 1 wakes up and tries to complete its old attempt.
	step(domain.EvComplete)

	// Output:
	// CREATED          --submit_ready--> READY
	// READY            --claim--> LEASED
	// LEASED           --start--> RUNNING
	// RUNNING          --lease_expired_retry--> READY
	// READY            --claim--> LEASED
	// LEASED           --start--> RUNNING
	// RUNNING          --complete--> SUCCEEDED
	// SUCCEEDED        --complete--> rejected (illegal task state transition: complete on SUCCEEDED)
}
