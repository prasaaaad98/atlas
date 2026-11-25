// Package store is the only code that reads and writes orchestration state in
// PostgreSQL.
//
// Why it exists: every Atlas guarantee is enforced here, in SQL transactions
// and constraints, not in memory. A1 provides pipeline registration and the
// submit transaction (run + tasks + edges + event, all or nothing) and run lookup.
// Who calls it: internal/api handlers; later the scheduler, reaper and evaluators.
//
// Invariants protected (A1):
//   - An acknowledged submission is durable: SubmitRun returns only after COMMIT.
//   - A duplicate submission never creates a second run: unique
//     (tenant_id, idempotency_key); a replay returns the original run.
//   - Same key + different request is rejected, never silently merged.
//   - At most one active run per (pipeline, partition): partial unique index.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/prasaaaad98/atlas/internal/domain"
)

var (
	ErrInvalid             = errors.New("invalid request")
	ErrNotFound            = errors.New("not found")
	ErrActiveRunExists     = errors.New("an active run already exists for this pipeline partition")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// ActiveRunError names the run currently holding the partition.
type ActiveRunError struct{ RunID string }

func (e *ActiveRunError) Error() string { return ErrActiveRunExists.Error() + ": " + e.RunID }
func (e *ActiveRunError) Unwrap() error { return ErrActiveRunExists }

const defaultTenant = "default"

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Ping reports whether the database answers (used by /healthz).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// --- pipelines -------------------------------------------------------------

type PipelineDef struct {
	Name          string            `json:"name"`
	PartitionKind string            `json:"partition_kind"`
	PartitionTZ   string            `json:"partition_tz"`
	Tasks         []domain.TaskSpec `json:"tasks"`
}

type Pipeline struct {
	Name      string      `json:"name"`
	Version   int         `json:"version"`
	Def       PipelineDef `json:"definition"`
	CreatedAt time.Time   `json:"created_at"`
}

// canonical normalises a definition so the same DAG always hashes the same
// (dependency and output order don't matter; task order is kept).
func canonical(def PipelineDef) ([]byte, string) {
	c := def
	c.Tasks = make([]domain.TaskSpec, len(def.Tasks))
	for i, t := range def.Tasks {
		t.Deps = slices.Sorted(slices.Values(t.Deps))
		t.AssetOutputs = slices.Sorted(slices.Values(t.AssetOutputs))
		if t.MaxAttempts == 0 {
			t.MaxAttempts = 1
		}
		c.Tasks[i] = t
	}
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func validateDef(def PipelineDef) error {
	if def.Name == "" || !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(def.Name) {
		return fmt.Errorf("%w: pipeline name %q (lower_snake)", ErrInvalid, def.Name)
	}
	if def.PartitionKind != "daily" {
		return fmt.Errorf("%w: partition_kind %q (only daily in A1)", ErrInvalid, def.PartitionKind)
	}
	if _, _, err := domain.DailyInterval("2000-01-01", def.PartitionTZ); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := domain.ValidateDAG(def.Tasks); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return nil
}

// RegisterPipeline stores a new version when the definition changed, or
// returns the existing version for an identical definition (created=false).
func (s *Store) RegisterPipeline(ctx context.Context, def PipelineDef) (Pipeline, bool, error) {
	if err := validateDef(def); err != nil {
		return Pipeline{}, false, err
	}
	body, hash := canonical(def)
	var stored PipelineDef
	_ = json.Unmarshal(body, &stored)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Pipeline{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	// Serialises registrations of one pipeline name, so two concurrent
	// registrations can't both compute the same "next version" (or both insert
	// the same definition). Lock is released at commit/rollback.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, def.Name); err != nil {
		return Pipeline{}, false, err
	}
	p := Pipeline{Name: def.Name, Def: stored}
	err = tx.QueryRow(ctx,
		`SELECT version, created_at FROM pipelines WHERE name = $1 AND definition_hash = $2`,
		def.Name, hash).Scan(&p.Version, &p.CreatedAt)
	if err == nil {
		return p, false, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Pipeline{}, false, err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO pipelines (name, version, definition, definition_hash, partition_kind, partition_tz)
		SELECT $1, coalesce(max(version), 0) + 1, $2, $3, $4, $5 FROM pipelines WHERE name = $1
		RETURNING version, created_at`,
		def.Name, body, hash, def.PartitionKind, def.PartitionTZ).Scan(&p.Version, &p.CreatedAt)
	if err != nil {
		return Pipeline{}, false, err
	}
	return p, true, tx.Commit(ctx)
}

// --- runs ------------------------------------------------------------------

type SubmitRequest struct {
	TenantID        string     `json:"tenant_id,omitempty"`
	IdempotencyKey  string     `json:"idempotency_key"`
	PipelineName    string     `json:"pipeline"`
	PipelineVersion int        `json:"pipeline_version,omitempty"` // 0 = latest at submit time
	PartitionKey    string     `json:"partition_key"`
	Trigger         string     `json:"trigger"`
	PriorityClass   string     `json:"priority_class"`
	DeadlineAt      *time.Time `json:"deadline_at,omitempty"` // computed from deadline_rule in A7
}

type Task struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Status       domain.State `json:"status"`
	AttemptCount int          `json:"attempt_count"`
	MaxAttempts  int          `json:"max_attempts"`
	AssetOutputs []string     `json:"asset_outputs"`
	Deps         []string     `json:"deps"`
}

type Run struct {
	ID                string           `json:"id"`
	TenantID          string           `json:"tenant_id"`
	IdempotencyKey    string           `json:"idempotency_key"`
	PipelineName      string           `json:"pipeline"`
	PipelineVersion   int              `json:"pipeline_version"`
	PartitionKey      string           `json:"partition_key"`
	DataIntervalStart time.Time        `json:"data_interval_start"`
	DataIntervalEnd   time.Time        `json:"data_interval_end"`
	Trigger           string           `json:"trigger"`
	PriorityClass     string           `json:"priority_class"`
	Status            domain.RunStatus `json:"status"`
	DeadlineAt        *time.Time       `json:"deadline_at,omitempty"`
	CreatedAt         time.Time        `json:"created_at"`
	Tasks             []Task           `json:"tasks,omitempty"`

	requestHash string // only set on idempotency lookups; never serialised
}

func validateSubmit(r *SubmitRequest) error {
	if r.TenantID == "" {
		r.TenantID = defaultTenant
	}
	switch {
	case r.IdempotencyKey == "" || len(r.IdempotencyKey) > 200:
		return fmt.Errorf("%w: idempotency_key is required (max 200 chars)", ErrInvalid)
	case r.PipelineName == "":
		return fmt.Errorf("%w: pipeline is required", ErrInvalid)
	case r.PipelineVersion < 0:
		return fmt.Errorf("%w: pipeline_version < 0", ErrInvalid)
	case !domain.ValidTrigger(r.Trigger):
		return fmt.Errorf("%w: trigger %q", ErrInvalid, r.Trigger)
	case !domain.ValidPriorityClass(r.PriorityClass):
		return fmt.Errorf("%w: priority_class %q", ErrInvalid, r.PriorityClass)
	}
	if _, _, err := domain.DailyInterval(r.PartitionKey, "UTC"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return nil
}

// requestHash fingerprints everything that defines the run, so a replay with
// the same key is recognised and a different request with that key is refused.
func requestHash(r SubmitRequest) string {
	b, _ := json.Marshal([]any{r.PipelineName, r.PipelineVersion, r.PartitionKey, r.Trigger, r.PriorityClass, r.DeadlineAt})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// SubmitRun creates a run and all of its tasks in one transaction, or returns
// the existing run for a replayed idempotency key (created=false).
func (s *Store) SubmitRun(ctx context.Context, req SubmitRequest) (Run, bool, error) {
	if err := validateSubmit(&req); err != nil {
		return Run{}, false, err
	}
	hash := requestHash(req)

	run, created, err := s.submitTx(ctx, req, hash)
	if err == nil {
		return run, created, nil
	}
	// A unique violation can come from either index, including when the
	// "conflicting" run is our own duplicate committed a moment earlier by a
	// concurrent caller. Always re-check the key first: a replay wins.
	if errors.Is(err, ErrActiveRunExists) || errors.Is(err, errRetryLookup) {
		existing, lookupErr := s.runByKey(ctx, req.TenantID, req.IdempotencyKey)
		if lookupErr == nil {
			return replay(existing, hash)
		}
		if !errors.Is(lookupErr, ErrNotFound) {
			return Run{}, false, lookupErr
		}
		if errors.Is(err, errRetryLookup) {
			return Run{}, false, fmt.Errorf("idempotency key conflict but no run found: %w", ErrInvalid)
		}
	}
	return Run{}, false, err
}

var errRetryLookup = errors.New("idempotency key already used")

func replay(existing Run, hash string) (Run, bool, error) {
	if existing.requestHash != hash {
		return Run{}, false, ErrIdempotencyMismatch
	}
	existing.requestHash = ""
	return existing, false, nil
}

func (s *Store) submitTx(ctx context.Context, req SubmitRequest, hash string) (Run, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var p Pipeline
	var defJSON []byte
	q := `SELECT name, version, definition FROM pipelines WHERE name = $1 ORDER BY version DESC LIMIT 1`
	args := []any{req.PipelineName}
	if req.PipelineVersion > 0 {
		q = `SELECT name, version, definition FROM pipelines WHERE name = $1 AND version = $2`
		args = append(args, req.PipelineVersion)
	}
	if err := tx.QueryRow(ctx, q, args...).Scan(&p.Name, &p.Version, &defJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Run{}, false, fmt.Errorf("pipeline %q version %d: %w", req.PipelineName, req.PipelineVersion, ErrNotFound)
		}
		return Run{}, false, err
	}
	if err := json.Unmarshal(defJSON, &p.Def); err != nil {
		return Run{}, false, err
	}
	start, end, err := domain.DailyInterval(req.PartitionKey, p.Def.PartitionTZ)
	if err != nil {
		return Run{}, false, fmt.Errorf("%w: %v", ErrInvalid, err)
	}

	run := Run{
		TenantID: req.TenantID, IdempotencyKey: req.IdempotencyKey,
		PipelineName: p.Name, PipelineVersion: p.Version, PartitionKey: req.PartitionKey,
		DataIntervalStart: start, DataIntervalEnd: end,
		Trigger: req.Trigger, PriorityClass: req.PriorityClass, DeadlineAt: req.DeadlineAt,
		Status: domain.RunPending, // DeriveRunStatus of fresh READY/BLOCKED tasks; checked below
	}
	// ON CONFLICT on the idempotency index makes a concurrent duplicate wait for
	// the first insert and then do nothing (no second run). A violation of the
	// partial "one active run per partition" index is NOT absorbed here: it
	// surfaces as an error and becomes ActiveRunError after the key re-check.
	err = tx.QueryRow(ctx, `
		INSERT INTO workflow_runs (tenant_id, idempotency_key, request_hash, pipeline_name, pipeline_version,
		    partition_key, data_interval_start, data_interval_end, trigger, priority_class, status, deadline_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id, created_at`,
		run.TenantID, run.IdempotencyKey, hash, run.PipelineName, run.PipelineVersion, run.PartitionKey,
		start, end, run.Trigger, run.PriorityClass, string(run.Status), run.DeadlineAt,
	).Scan(&run.ID, &run.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, false, errRetryLookup // key exists: caller resolves replay vs mismatch
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "runs_one_active_per_partition" {
			_ = tx.Rollback(ctx)
			return Run{}, false, s.activeRunError(ctx, req.PipelineName, req.PartitionKey)
		}
		return Run{}, false, err
	}

	// Tasks: initial state comes from the state machine, never hard-coded.
	ids := map[string]string{}
	states := make([]domain.State, 0, len(p.Def.Tasks))
	for _, t := range p.Def.Tasks {
		ev := domain.EvSubmitReady
		if len(t.Deps) > 0 {
			ev = domain.EvSubmitBlocked
		}
		st, err := domain.Next(domain.Created, ev)
		if err != nil {
			return Run{}, false, err
		}
		maxAttempts := max(t.MaxAttempts, 1)
		var id string
		if err := tx.QueryRow(ctx, `
			INSERT INTO tasks (run_id, name, status, priority_class, deadline_at, max_attempts, asset_outputs)
			VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
			run.ID, t.Name, string(st), run.PriorityClass, run.DeadlineAt, maxAttempts, nonNil(t.AssetOutputs),
		).Scan(&id); err != nil {
			return Run{}, false, err
		}
		ids[t.Name] = id
		states = append(states, st)
		run.Tasks = append(run.Tasks, Task{ID: id, Name: t.Name, Status: st, MaxAttempts: maxAttempts,
			AssetOutputs: nonNil(t.AssetOutputs), Deps: nonNil(t.Deps)})
	}
	for _, t := range p.Def.Tasks {
		for _, d := range t.Deps {
			if _, err := tx.Exec(ctx,
				`INSERT INTO task_edges (run_id, parent_task_id, child_task_id) VALUES ($1, $2, $3)`,
				run.ID, ids[d], ids[t.Name]); err != nil {
				return Run{}, false, err
			}
		}
	}
	if derived, err := domain.DeriveRunStatus(states); err != nil || derived != run.Status {
		return Run{}, false, fmt.Errorf("derived run status %q (%v), stored %q", derived, err, run.Status)
	}
	payload, _ := json.Marshal(map[string]any{"partition_key": run.PartitionKey, "trigger": run.Trigger,
		"pipeline_version": run.PipelineVersion})
	if _, err := tx.Exec(ctx, `INSERT INTO events (run_id, type, payload) VALUES ($1, 'run.created', $2)`,
		run.ID, payload); err != nil {
		return Run{}, false, err
	}
	// Durability: the caller acknowledges (HTTP 201) only after this returns.
	if err := tx.Commit(ctx); err != nil {
		return Run{}, false, err
	}
	return run, true, nil
}

func (s *Store) activeRunError(ctx context.Context, pipeline, partition string) error {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM workflow_runs
		WHERE pipeline_name = $1 AND partition_key = $2 AND status IN ('PENDING', 'RUNNING')`,
		pipeline, partition).Scan(&id)
	if err != nil {
		// The holder finished between our insert and this read; report the conflict anyway.
		return ErrActiveRunExists
	}
	return &ActiveRunError{RunID: id}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// --- reads -----------------------------------------------------------------

const runColumns = `id, tenant_id, idempotency_key, request_hash, pipeline_name, pipeline_version, partition_key,
	data_interval_start, data_interval_end, trigger, priority_class, status, deadline_at, created_at`

func scanRun(row pgx.Row) (Run, string, error) {
	var r Run
	var hash, status string
	err := row.Scan(&r.ID, &r.TenantID, &r.IdempotencyKey, &hash, &r.PipelineName, &r.PipelineVersion,
		&r.PartitionKey, &r.DataIntervalStart, &r.DataIntervalEnd, &r.Trigger, &r.PriorityClass, &status,
		&r.DeadlineAt, &r.CreatedAt)
	r.Status = domain.RunStatus(status)
	r.DataIntervalStart, r.DataIntervalEnd = r.DataIntervalStart.UTC(), r.DataIntervalEnd.UTC()
	return r, hash, err
}

func (s *Store) runByKey(ctx context.Context, tenant, key string) (Run, error) {
	r, hash, err := scanRun(s.pool.QueryRow(ctx,
		`SELECT `+runColumns+` FROM workflow_runs WHERE tenant_id = $1 AND idempotency_key = $2`, tenant, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, err
	}
	if err := s.loadTasks(ctx, &r); err != nil {
		return Run{}, err
	}
	r.requestHash = hash
	return r, nil
}

// GetRun returns a run with its tasks.
func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	if !uuidRE.MatchString(id) {
		return Run{}, ErrNotFound
	}
	r, _, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM workflow_runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, err
	}
	return r, s.loadTasks(ctx, &r)
}

func (s *Store) loadTasks(ctx context.Context, r *Run) error {
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.name, t.status, t.attempt_count, t.max_attempts, t.asset_outputs,
		       coalesce(array_agg(p.name ORDER BY p.name) FILTER (WHERE p.name IS NOT NULL), '{}')
		FROM tasks t
		LEFT JOIN task_edges e ON e.child_task_id = t.id
		LEFT JOIN tasks p ON p.id = e.parent_task_id
		WHERE t.run_id = $1
		GROUP BY t.id
		ORDER BY t.created_at, t.name`, r.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	r.Tasks = nil
	for rows.Next() {
		var t Task
		var status string
		if err := rows.Scan(&t.ID, &t.Name, &status, &t.AttemptCount, &t.MaxAttempts, &t.AssetOutputs, &t.Deps); err != nil {
			return err
		}
		t.Status = domain.State(status)
		r.Tasks = append(r.Tasks, t)
	}
	return rows.Err()
}
