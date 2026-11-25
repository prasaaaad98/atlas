// Schedules, catchup and backfills (Slice A6, ADR-010, ADR-011).
//
// Why it exists: data pipelines run per business date (the partition), on a
// clock in the market's time zone, and history is rebuilt by backfills. Both
// create runs through SubmitRun, so every run keeps the A1 guarantees.
// Who calls it: the scheduler loop in cmd/atlas, the API, tests.
//
// Invariants protected:
//   - A scheduled partition is submitted once, across restarts: the key
//     "schedule:<pipeline>:<partition>" is deterministic, and the cursor is
//     advanced only after the submissions committed. A crash before the cursor
//     update re-submits the same keys next tick, which are replays.
//   - Catchup creates exactly the missed partitions from start_partition;
//     without catchup only the latest due partition is submitted.
//   - A backfill never has more than max_concurrent_runs active runs, runs
//     oldest partition first at priority `backfill` (so live runs are claimed
//     first), and waits instead of skipping when another run holds a partition.
//   - All time decisions use the database clock (SELECT now()).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/prasaaaad98/atlas/internal/domain"
)

const (
	maxFiresPerTick       = 500   // catchup over years proceeds in chunks, oldest first
	maxBackfillPartitions = 20000 // ~55 years of daily partitions
)

type Schedule struct {
	PipelineName        string     `json:"pipeline"`
	Cron                string     `json:"cron"`
	TZ                  string     `json:"tz"`
	PartitionOffsetDays int        `json:"partition_offset_days"`
	Catchup             bool       `json:"catchup"`
	StartPartition      *string    `json:"start_partition,omitempty"`
	Enabled             bool       `json:"enabled"`
	Cursor              *time.Time `json:"cursor,omitempty"` // last fire time submitted
}

// SetSchedule creates or replaces a pipeline's schedule. The cursor is kept
// on update, so changing the cron doesn't re-submit history.
func (s *Store) SetSchedule(ctx context.Context, sch Schedule) (Schedule, error) {
	if _, err := domain.ParseCron(sch.Cron); err != nil {
		return Schedule{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if _, _, err := domain.DailyInterval("2000-01-01", sch.TZ); err != nil {
		return Schedule{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if sch.PartitionOffsetDays < -31 || sch.PartitionOffsetDays > 31 {
		return Schedule{}, fmt.Errorf("%w: partition_offset_days must be -31..31", ErrInvalid)
	}
	if sch.Catchup && sch.StartPartition == nil {
		return Schedule{}, fmt.Errorf("%w: catchup needs start_partition", ErrInvalid)
	}
	if sch.StartPartition != nil {
		if _, _, err := domain.DailyInterval(*sch.StartPartition, "UTC"); err != nil {
			return Schedule{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pipelines WHERE name = $1)`, sch.PipelineName).Scan(&exists); err != nil {
		return Schedule{}, err
	}
	if !exists {
		return Schedule{}, fmt.Errorf("pipeline %q: %w", sch.PipelineName, ErrNotFound)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO schedules (pipeline_name, cron, tz, partition_offset_days, catchup, start_partition, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (pipeline_name) DO UPDATE SET cron = $2, tz = $3, partition_offset_days = $4, catchup = $5,
		       start_partition = $6, enabled = $7, updated_at = now()`,
		sch.PipelineName, sch.Cron, sch.TZ, sch.PartitionOffsetDays, sch.Catchup, sch.StartPartition, sch.Enabled); err != nil {
		return Schedule{}, err
	}
	return s.GetSchedule(ctx, sch.PipelineName)
}

func (s *Store) GetSchedule(ctx context.Context, pipeline string) (Schedule, error) {
	var sch Schedule
	err := s.pool.QueryRow(ctx, `
		SELECT pipeline_name, cron, tz, partition_offset_days, catchup, start_partition, enabled, cursor
		FROM schedules WHERE pipeline_name = $1`, pipeline).Scan(&sch.PipelineName, &sch.Cron, &sch.TZ,
		&sch.PartitionOffsetDays, &sch.Catchup, &sch.StartPartition, &sch.Enabled, &sch.Cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, fmt.Errorf("schedule %q: %w", pipeline, ErrNotFound)
	}
	return sch, err
}

// TickSchedules submits every due scheduled partition and returns how many
// submissions it made (new or replayed).
func (s *Store) TickSchedules(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil { // the database clock
		return 0, err
	}
	// SKIP LOCKED: a second control plane (or an overlapping tick) leaves a
	// schedule another tick is handling alone. NO KEY UPDATE: doesn't conflict
	// with key-share locks.
	rows, err := tx.Query(ctx, `
		SELECT pipeline_name, cron, tz, partition_offset_days, catchup, start_partition, cursor
		FROM schedules WHERE enabled ORDER BY pipeline_name FOR NO KEY UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	var due []Schedule
	for rows.Next() {
		var sch Schedule
		if err := rows.Scan(&sch.PipelineName, &sch.Cron, &sch.TZ, &sch.PartitionOffsetDays, &sch.Catchup,
			&sch.StartPartition, &sch.Cursor); err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, sch)
	}
	rows.Close()

	submitted := 0
	for _, sch := range due {
		fires, err := dueFires(sch, now)
		if err != nil {
			return submitted, err
		}
		if len(fires) == 0 {
			continue
		}
		loc, _ := time.LoadLocation(sch.TZ)
		for _, fire := range fires {
			partition := domain.PartitionFor(fire, loc, sch.PartitionOffsetDays)
			if sch.StartPartition != nil && partition < *sch.StartPartition {
				continue
			}
			_, _, err := s.SubmitRun(ctx, SubmitRequest{
				IdempotencyKey: "schedule:" + sch.PipelineName + ":" + partition,
				PipelineName:   sch.PipelineName, PartitionKey: partition,
				Trigger: "schedule", PriorityClass: "live",
			})
			if err != nil && !errors.Is(err, ErrActiveRunExists) {
				return submitted, fmt.Errorf("schedule %s partition %s: %w", sch.PipelineName, partition, err)
			}
			// ErrActiveRunExists: another run (manual, backfill) holds the
			// partition right now; it is being built, which is what the
			// schedule wanted. Not submitted again later.
			submitted++
		}
		// Advance only after every submission above committed (crash-safety, see header).
		if _, err := tx.Exec(ctx, `UPDATE schedules SET cursor = $2, updated_at = now() WHERE pipeline_name = $1`,
			sch.PipelineName, fires[len(fires)-1]); err != nil {
			return submitted, err
		}
	}
	return submitted, tx.Commit(ctx)
}

// dueFires returns the fire times a tick must submit, oldest first.
func dueFires(sch Schedule, now time.Time) ([]time.Time, error) {
	c, err := domain.ParseCron(sch.Cron)
	if err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation(sch.TZ)
	if err != nil {
		return nil, err
	}
	switch {
	case sch.Cursor != nil:
		return c.Between(*sch.Cursor, now, loc, maxFiresPerTick), nil
	case sch.Catchup:
		// Fires on the local date that produces start_partition, onwards.
		start, err := time.ParseInLocation("2006-01-02", *sch.StartPartition, loc)
		if err != nil {
			return nil, err
		}
		from := start.AddDate(0, 0, -sch.PartitionOffsetDays).Add(-time.Minute)
		return c.Between(from, now, loc, maxFiresPerTick), nil
	default:
		// No catchup: only the latest fire that is due now.
		if last, ok := c.Prev(now, loc, 400*24*time.Hour); ok {
			return []time.Time{last}, nil
		}
		return nil, nil
	}
}

// --- backfills ----------------------------------------------------------------

type BackfillRequest struct {
	PipelineName      string `json:"pipeline"`
	FromPartition     string `json:"from_partition"`
	ToPartition       string `json:"to_partition"`
	MaxConcurrentRuns int    `json:"max_concurrent_runs"`
}

type Backfill struct {
	ID                string     `json:"id"`
	PipelineName      string     `json:"pipeline"`
	FromPartition     string     `json:"from_partition"`
	ToPartition       string     `json:"to_partition"`
	NextPartition     string     `json:"next_partition"`
	MaxConcurrentRuns int        `json:"max_concurrent_runs"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	Runs              struct {
		Total, Active, Succeeded, Failed, Canceled int
	} `json:"runs"`
}

func (s *Store) CreateBackfill(ctx context.Context, req BackfillRequest) (Backfill, error) {
	from, _, err1 := domain.DailyInterval(req.FromPartition, "UTC")
	to, _, err2 := domain.DailyInterval(req.ToPartition, "UTC")
	switch {
	case err1 != nil || err2 != nil:
		return Backfill{}, fmt.Errorf("%w: from/to must be YYYY-MM-DD", ErrInvalid)
	case to.Before(from):
		return Backfill{}, fmt.Errorf("%w: to_partition before from_partition", ErrInvalid)
	case to.Sub(from) > maxBackfillPartitions*24*time.Hour:
		return Backfill{}, fmt.Errorf("%w: at most %d partitions per backfill", ErrInvalid, maxBackfillPartitions)
	case req.MaxConcurrentRuns < 1 || req.MaxConcurrentRuns > 1000:
		return Backfill{}, fmt.Errorf("%w: max_concurrent_runs must be 1..1000", ErrInvalid)
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pipelines WHERE name = $1)`, req.PipelineName).Scan(&exists); err != nil {
		return Backfill{}, err
	}
	if !exists {
		return Backfill{}, fmt.Errorf("pipeline %q: %w", req.PipelineName, ErrNotFound)
	}
	var id string
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO backfills (pipeline_name, from_partition, to_partition, next_partition, max_concurrent_runs, status)
		VALUES ($1, $2, $3, $2, $4, 'RUNNING') RETURNING id`,
		req.PipelineName, req.FromPartition, req.ToPartition, req.MaxConcurrentRuns).Scan(&id); err != nil {
		return Backfill{}, err
	}
	return s.GetBackfill(ctx, id)
}

func (s *Store) GetBackfill(ctx context.Context, id string) (Backfill, error) {
	if !uuidRE.MatchString(id) {
		return Backfill{}, fmt.Errorf("backfill %q: %w", id, ErrNotFound)
	}
	var b Backfill
	err := s.pool.QueryRow(ctx, `
		SELECT b.id, b.pipeline_name, b.from_partition, b.to_partition, b.next_partition, b.max_concurrent_runs,
		       b.status, b.created_at, b.finished_at,
		       count(r.id), count(r.id) FILTER (WHERE r.status IN ('PENDING', 'RUNNING')),
		       count(r.id) FILTER (WHERE r.status = 'SUCCEEDED'), count(r.id) FILTER (WHERE r.status = 'FAILED'),
		       count(r.id) FILTER (WHERE r.status = 'CANCELED')
		FROM backfills b LEFT JOIN workflow_runs r ON r.backfill_id = b.id
		WHERE b.id = $1 GROUP BY b.id`, id).Scan(&b.ID, &b.PipelineName, &b.FromPartition, &b.ToPartition,
		&b.NextPartition, &b.MaxConcurrentRuns, &b.Status, &b.CreatedAt, &b.FinishedAt,
		&b.Runs.Total, &b.Runs.Active, &b.Runs.Succeeded, &b.Runs.Failed, &b.Runs.Canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return Backfill{}, fmt.Errorf("backfill %s: %w", id, ErrNotFound)
	}
	return b, err
}

// AdvanceBackfills tops up every running backfill to max_concurrent_runs
// active runs, oldest partition first, and finishes backfills that are done.
func (s *Store) AdvanceBackfills(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	// NO KEY UPDATE: runs inserted by SubmitRun (another connection) reference
	// the backfill row by foreign key (KEY SHARE); FOR UPDATE would block them
	// behind this very transaction.
	rows, err := tx.Query(ctx, `
		SELECT id, pipeline_name, to_partition, next_partition, max_concurrent_runs,
		       (SELECT count(*) FROM workflow_runs r WHERE r.backfill_id = b.id AND r.status IN ('PENDING', 'RUNNING'))
		FROM backfills b WHERE status = 'RUNNING' ORDER BY created_at FOR NO KEY UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	type bf struct {
		id, pipeline, to, next string
		max, active            int
	}
	var list []bf
	for rows.Next() {
		var b bf
		if err := rows.Scan(&b.id, &b.pipeline, &b.to, &b.next, &b.max, &b.active); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, b)
	}
	rows.Close()

	submitted := 0
	for _, b := range list {
		for b.active < b.max && b.next <= b.to {
			_, _, err := s.SubmitRun(ctx, SubmitRequest{
				IdempotencyKey: "backfill:" + b.id + ":" + b.next, PipelineName: b.pipeline, PartitionKey: b.next,
				Trigger: "backfill", PriorityClass: "backfill", backfillID: b.id,
			})
			if errors.Is(err, ErrActiveRunExists) {
				break // another run is building this day: wait for it, retry next tick (never skip)
			}
			if err != nil {
				return submitted, fmt.Errorf("backfill %s partition %s: %w", b.id, b.next, err)
			}
			submitted++
			b.active++
			d, _ := time.Parse("2006-01-02", b.next)
			b.next = d.AddDate(0, 0, 1).Format("2006-01-02")
		}
		status := "RUNNING"
		if b.next > b.to && b.active == 0 {
			status = "SUCCEEDED" // every partition submitted and none still active
		}
		if _, err := tx.Exec(ctx, `
			UPDATE backfills SET next_partition = $2, status = $3,
			       finished_at = CASE WHEN $3 = 'RUNNING' THEN NULL ELSE now() END
			WHERE id = $1`, b.id, b.next, status); err != nil {
			return submitted, err
		}
	}
	return submitted, tx.Commit(ctx)
}

// CancelBackfill stops creating runs and cancels the backfill's active runs.
func (s *Store) CancelBackfill(ctx context.Context, id string) (Backfill, error) {
	if !uuidRE.MatchString(id) {
		return Backfill{}, fmt.Errorf("backfill %q: %w", id, ErrNotFound)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE backfills SET status = 'CANCELED', finished_at = now()
		WHERE id = $1 AND status = 'RUNNING'`, id)
	if err != nil {
		return Backfill{}, err
	}
	if tag.RowsAffected() > 0 {
		rows, err := s.pool.Query(ctx, `SELECT id FROM workflow_runs WHERE backfill_id = $1 AND status IN ('PENDING', 'RUNNING')`, id)
		if err != nil {
			return Backfill{}, err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return Backfill{}, err
		}
		for _, r := range ids {
			if _, err := s.CancelRun(ctx, r); err != nil {
				return Backfill{}, err
			}
		}
	}
	return s.GetBackfill(ctx, id)
}
