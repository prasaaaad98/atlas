// Assets, materializations, check-gated current pointers, freshness (Slice A8, ADR-013).
//
// Why it exists: the most important data invariant. A re-run writes to a new
// attempt path; only the winning, fenced, checked attempt becomes current.
// Who calls it: Complete (recordOutputs, inside the fenced transaction),
// RegisterPipeline (registerAssets), the freshness loop, the API.
//
// Invariants protected:
//   - Fenced materialization: rows are inserted only from Complete after the
//     fencing check passed, in the same transaction as SUCCEEDED. A stale
//     attempt's report is refused before anything is written.
//   - The current pointer moves only when every blocking check passed; a
//     failed blocking check leaves the pointer where it was (status
//     failed_check) and the task FAILED_TERMINAL, so dependents are canceled.
//   - A materialization must be for an asset the task declared and for the
//     run's partition; anything else fails the task (materialization_invalid).
//   - asset.late fires once per (asset, partition) when it isn't fresh by its
//     policy deadline.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/prasaaaad98/atlas/internal/domain"
)

// Materialization mirrors the Python task contract (core/task.py).
type Materialization struct {
	AssetKey      string `json:"asset_key"`
	PartitionKey  string `json:"partition_key"`
	URI           string `json:"uri"`
	RowCount      int64  `json:"row_count"`
	ContentHash   string `json:"content_hash"`
	SchemaVersion string `json:"schema_version"`
	CodeVersion   string `json:"code_version"`
}

// FreshnessPolicy: by each fire of DeadlineCron (in TZ), the partition
// "local fire date + ExpectedPartitionOffset days" must be fresh.
type FreshnessPolicy struct {
	DeadlineCron            string `json:"deadline_cron"`
	TZ                      string `json:"tz"`
	ExpectedPartitionOffset int    `json:"expected_partition_offset"`
	Severity                string `json:"severity,omitempty"` // "warn" (default) or "page": a label for alerts
}

func (f FreshnessPolicy) validate() error {
	if _, err := domain.ParseCron(f.DeadlineCron); err != nil {
		return err
	}
	if _, err := time.LoadLocation(f.TZ); err != nil || f.TZ == "" {
		return fmt.Errorf("freshness tz %q", f.TZ)
	}
	if f.ExpectedPartitionOffset < -31 || f.ExpectedPartitionOffset > 31 {
		return fmt.Errorf("expected_partition_offset must be -31..31")
	}
	if f.Severity != "" && f.Severity != "warn" && f.Severity != "page" {
		return fmt.Errorf("severity must be warn or page")
	}
	return nil
}

var hashRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// validateMaterializations returns "" or why the reported outputs are invalid.
func validateMaterializations(mats []Materialization, declared []string, partition string) string {
	seen := map[string]bool{}
	for _, m := range mats {
		switch {
		case !slices.Contains(declared, m.AssetKey):
			return fmt.Sprintf("undeclared asset %q (declared %v)", m.AssetKey, declared)
		case m.PartitionKey != partition:
			return fmt.Sprintf("%s: partition %q != run partition %q", m.AssetKey, m.PartitionKey, partition)
		case seen[m.AssetKey]:
			return fmt.Sprintf("%s reported twice", m.AssetKey)
		case m.URI == "" || strings.Contains(m.URI, "..") || strings.HasPrefix(m.URI, "/") || strings.Contains(m.URI, `\`):
			return fmt.Sprintf("%s: uri %q must be lake-relative", m.AssetKey, m.URI)
		case !hashRE.MatchString(m.ContentHash):
			return fmt.Sprintf("%s: content_hash must be sha256:<64 hex>", m.AssetKey)
		case m.RowCount < 0 || m.SchemaVersion == "" || m.CodeVersion == "":
			return fmt.Sprintf("%s: row_count, schema_version and code_version are required", m.AssetKey)
		}
		seen[m.AssetKey] = true
	}
	return ""
}

// recordOutputs runs inside Complete after the fencing check: insert
// materializations and checks, then move (or don't move) the pointers.
func recordOutputs(ctx context.Context, tx pgx.Tx, h heldTask, attemptID string, mats []Materialization,
	checks []CheckResult, blockingFailed bool) error {
	for _, c := range checks {
		metrics := c.Metrics
		if len(metrics) == 0 {
			metrics = json.RawMessage("{}")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO asset_checks (attempt_id, run_id, partition_key, check_name, severity, passed, metrics)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, attemptID, h.runID, h.partitionKey, c.CheckName, c.Severity, c.Passed, metrics); err != nil {
			return err
		}
	}
	for _, m := range mats {
		// The asset row exists (registered with the pipeline); upsert keeps old
		// definitions working if a task's outputs were registered before A8.
		if _, err := tx.Exec(ctx, `
			INSERT INTO assets (key, pipeline_name) SELECT $1, r.pipeline_name FROM workflow_runs r WHERE r.id = $2
			ON CONFLICT (key) DO NOTHING`, m.AssetKey, h.runID); err != nil {
			return err
		}
		var matID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO materializations (asset_key, partition_key, run_id, task_id, attempt_id, fencing_token, uri,
			                              row_count, content_hash, schema_version, code_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
			m.AssetKey, m.PartitionKey, h.runID, h.taskID, attemptID, h.token, m.URI, m.RowCount, m.ContentHash,
			m.SchemaVersion, m.CodeVersion).Scan(&matID); err != nil {
			return err
		}
		if blockingFailed {
			// Pointer stays; the partition shows that its latest attempt failed checks.
			if _, err := tx.Exec(ctx, `
				INSERT INTO asset_partitions (asset_key, partition_key, status) VALUES ($1, $2, 'failed_check')
				ON CONFLICT (asset_key, partition_key) DO UPDATE SET status = 'failed_check', updated_at = now()`,
				m.AssetKey, m.PartitionKey); err != nil {
				return err
			}
			if err := taskEvent(ctx, tx, h.runID, h.taskID, "asset.check_failed", map[string]any{
				"asset_key": m.AssetKey, "partition_key": m.PartitionKey, "materialization_id": matID}); err != nil {
				return err
			}
			continue
		}
		// Publish: the pointer moves to this materialization (write-then-publish).
		if _, err := tx.Exec(ctx, `
			INSERT INTO asset_partitions (asset_key, partition_key, current_materialization_id, status)
			VALUES ($1, $2, $3, 'fresh')
			ON CONFLICT (asset_key, partition_key) DO UPDATE
			SET current_materialization_id = $3, status = 'fresh', late_since = NULL, updated_at = now()`,
			m.AssetKey, m.PartitionKey, matID); err != nil {
			return err
		}
		if err := taskEvent(ctx, tx, h.runID, h.taskID, "materialization.created", map[string]any{
			"asset_key": m.AssetKey, "partition_key": m.PartitionKey, "materialization_id": matID, "current": true}); err != nil {
			return err
		}
	}
	return nil
}

// registerAssets upserts the assets a pipeline declares, with freshness policies.
func registerAssets(ctx context.Context, tx pgx.Tx, def PipelineDef) error {
	for _, t := range def.Tasks {
		for _, a := range t.AssetOutputs {
			var policy []byte
			if p, ok := def.Freshness[a]; ok {
				policy, _ = json.Marshal(p)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO assets (key, pipeline_name, freshness_policy) VALUES ($1, $2, $3)
				ON CONFLICT (key) DO UPDATE SET pipeline_name = $2, freshness_policy = $3, updated_at = now()`,
				a, def.Name, policy); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- reads -------------------------------------------------------------------------

type Asset struct {
	Key             string           `json:"key"`
	PipelineName    string           `json:"pipeline"`
	FreshnessPolicy *FreshnessPolicy `json:"freshness_policy,omitempty"`
	Partitions      int              `json:"partitions"`
	Fresh           int              `json:"fresh"`
	Late            int              `json:"late"`
	FailedCheck     int              `json:"failed_check"`
}

type AssetPartition struct {
	PartitionKey   string           `json:"partition_key"`
	Status         string           `json:"status"`
	LateSince      *time.Time       `json:"late_since,omitempty"`
	UpdatedAt      time.Time        `json:"updated_at"`
	Current        *Materialization `json:"current,omitempty"`
	CurrentID      *string          `json:"current_materialization_id,omitempty"`
	CurrentAttempt *string          `json:"current_attempt_id,omitempty"`
}

func (s *Store) ListAssets(ctx context.Context) ([]Asset, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.key, a.pipeline_name, a.freshness_policy, count(p.partition_key),
		       count(*) FILTER (WHERE p.status = 'fresh'), count(*) FILTER (WHERE p.status = 'late'),
		       count(*) FILTER (WHERE p.status = 'failed_check')
		FROM assets a LEFT JOIN asset_partitions p ON p.asset_key = a.key
		GROUP BY a.key ORDER BY a.key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Asset
	for rows.Next() {
		var a Asset
		var policy []byte
		if err := rows.Scan(&a.Key, &a.PipelineName, &policy, &a.Partitions, &a.Fresh, &a.Late, &a.FailedCheck); err != nil {
			return nil, err
		}
		if policy != nil {
			a.FreshnessPolicy = &FreshnessPolicy{}
			_ = json.Unmarshal(policy, a.FreshnessPolicy)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AssetPartition returns one partition's current pointer: what a downstream
// task reads (worker resolve_input). ErrNotFound if Atlas has no record of it.
func (s *Store) AssetPartition(ctx context.Context, key, partition string) (AssetPartition, error) {
	ps, err := s.assetPartitions(ctx, key, partition, 1)
	if err != nil {
		return AssetPartition{}, err
	}
	if len(ps) == 0 {
		return AssetPartition{}, fmt.Errorf("asset %s partition %s: %w", key, partition, ErrNotFound)
	}
	return ps[0], nil
}

func (s *Store) AssetPartitions(ctx context.Context, key string, limit int) ([]AssetPartition, error) {
	// Up to a full backfill's worth (maxBackfillPartitions): the catalog reads every current pointer.
	if limit <= 0 || limit > maxBackfillPartitions {
		limit = 100
	}
	return s.assetPartitions(ctx, key, "", limit)
}

func (s *Store) assetPartitions(ctx context.Context, key, partition string, limit int) ([]AssetPartition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.partition_key, p.status, p.late_since, p.updated_at, p.current_materialization_id, m.attempt_id,
		       m.asset_key, m.partition_key, m.uri, m.row_count, m.content_hash, m.schema_version, m.code_version
		FROM asset_partitions p LEFT JOIN materializations m ON m.id = p.current_materialization_id
		WHERE p.asset_key = $1 AND ($2 = '' OR p.partition_key = $2)
		ORDER BY p.partition_key DESC LIMIT $3`, key, partition, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AssetPartition
	for rows.Next() {
		var p AssetPartition
		var ak, pk, uri, hash, sv, cv *string
		var rc *int64
		if err := rows.Scan(&p.PartitionKey, &p.Status, &p.LateSince, &p.UpdatedAt, &p.CurrentID, &p.CurrentAttempt,
			&ak, &pk, &uri, &rc, &hash, &sv, &cv); err != nil {
			return nil, err
		}
		if ak != nil {
			p.Current = &Materialization{AssetKey: *ak, PartitionKey: *pk, URI: *uri, RowCount: *rc, ContentHash: *hash,
				SchemaVersion: *sv, CodeVersion: *cv}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// --- freshness -----------------------------------------------------------------------

// DBNow is the database clock, the only clock Atlas uses for time decisions.
func (s *Store) DBNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&now)
	return now, err
}

// EvaluateFreshness marks expected-but-not-fresh partitions late as of `now`
// (the database clock in production; injectable for tests) and returns how
// many became late in this pass.
func (s *Store) EvaluateFreshness(ctx context.Context, now time.Time) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, freshness_policy FROM assets WHERE freshness_policy IS NOT NULL ORDER BY key`)
	if err != nil {
		return 0, err
	}
	type item struct {
		key    string
		policy FreshnessPolicy
	}
	var items []item
	for rows.Next() {
		var it item
		var raw []byte
		if err := rows.Scan(&it.key, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		if json.Unmarshal(raw, &it.policy) == nil {
			items = append(items, it)
		}
	}
	rows.Close()
	late := 0
	for _, it := range items {
		c, err := domain.ParseCron(it.policy.DeadlineCron)
		if err != nil {
			continue
		}
		loc, err := time.LoadLocation(it.policy.TZ)
		if err != nil {
			continue
		}
		deadline, ok := c.Prev(now, loc, 400*24*time.Hour)
		if !ok {
			continue
		}
		expected := domain.PartitionFor(deadline, loc, it.policy.ExpectedPartitionOffset)
		// Becomes late at most once: only a missing/failed_check partition (or
		// none) changes to late; fresh stays fresh, late stays late.
		var marked bool
		err = s.pool.QueryRow(ctx, `
			WITH up AS (
			    INSERT INTO asset_partitions (asset_key, partition_key, status, late_since)
			    VALUES ($1, $2, 'late', $3)
			    ON CONFLICT (asset_key, partition_key) DO UPDATE SET status = 'late', late_since = $3, updated_at = now()
			    WHERE asset_partitions.status IN ('missing', 'failed_check')
			    RETURNING 1)
			SELECT EXISTS (SELECT 1 FROM up)`, it.key, expected, deadline).Scan(&marked)
		if err != nil {
			return late, err
		}
		if marked {
			b, _ := json.Marshal(map[string]any{"asset_key": it.key, "partition_key": expected, "deadline": deadline.UTC(),
				"severity": it.policy.Severity})
			if _, err := s.pool.Exec(ctx, `INSERT INTO events (type, payload) VALUES ('asset.late', $1)`, b); err != nil {
				return late, err
			}
			late++
		}
	}
	return late, nil
}
