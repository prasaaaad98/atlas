// Limit keys: source politeness enforced at claim (Slice A5, ADR-008).
//
// Why it exists: tasks that call the same external source (e.g. the ENTSO-E
// API) share a limit key. Atlas admits a claim only while the key is under
// max_concurrency live attempts and rpm claims in the last minute, and not
// paused after a 429. This bounds the requests Atlas makes (best effort for
// the source: Atlas can't see other clients of the same source).
// Who calls it: Claim (admit), Fail (pause on rate_limited), the API (upsert/get).
//
// Invariant: concurrency and rpm are checked while holding the key's row lock
// (FOR NO KEY UPDATE), so two claimers of the same key are counted one after
// the other and can't both take the last slot. Lock order: task row (already
// held by the claim), then the limit key, then the run.
package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	maxKeysPerClaim       = 20
	defaultPauseSeconds   = 60 // 429 without Retry-After; configuration, not a measurement
	rateLimitedErrorClass = "rate_limited"
)

var limitKeyRE = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

type LimitKey struct {
	Key            string     `json:"key"`
	MaxConcurrency int        `json:"max_concurrency"`
	RPM            int        `json:"rpm"`
	PausedUntil    *time.Time `json:"paused_until,omitempty"`
	Live           int        `json:"live"`        // attempts LEASED/RUNNING now
	LastMinute     int        `json:"last_minute"` // claims in the last 60 s
}

// UpsertLimitKey creates or updates a key's limits (pause state is kept).
func (s *Store) UpsertLimitKey(ctx context.Context, k LimitKey) (LimitKey, error) {
	if !limitKeyRE.MatchString(k.Key) || k.MaxConcurrency < 1 || k.RPM < 1 {
		return LimitKey{}, fmt.Errorf("%w: limit key needs a lower_snake key, max_concurrency >= 1, rpm >= 1", ErrInvalid)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO limit_keys (key, max_concurrency, rpm) VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET max_concurrency = $2, rpm = $3, updated_at = now()`,
		k.Key, k.MaxConcurrency, k.RPM); err != nil {
		return LimitKey{}, err
	}
	return s.GetLimitKey(ctx, k.Key)
}

func (s *Store) GetLimitKey(ctx context.Context, key string) (LimitKey, error) {
	var k LimitKey
	err := s.pool.QueryRow(ctx, `
		SELECT key, max_concurrency, rpm, CASE WHEN paused_until > now() THEN paused_until END,
		       (SELECT count(*) FROM tasks WHERE limit_key = $1 AND status IN ('LEASED', 'RUNNING')),
		       (SELECT count(*) FROM task_attempts WHERE limit_key = $1 AND leased_at > now() - interval '1 minute')
		FROM limit_keys WHERE key = $1`, key).Scan(&k.Key, &k.MaxConcurrency, &k.RPM, &k.PausedUntil, &k.Live, &k.LastMinute)
	if errors.Is(err, pgx.ErrNoRows) {
		return LimitKey{}, fmt.Errorf("limit key %q: %w", key, ErrNotFound)
	}
	return k, err
}

// admit decides, under the key's row lock, whether one more claim fits.
func admit(ctx context.Context, tx pgx.Tx, key string) (bool, error) {
	var maxC, rpm int
	var paused bool
	// The row lock serialises claimers of this key until our COMMIT, so the
	// counts below can't be raced by another claimer of the same key.
	if err := tx.QueryRow(ctx, `
		SELECT max_concurrency, rpm, coalesce(paused_until > now(), false)
		FROM limit_keys WHERE key = $1 FOR NO KEY UPDATE`, key).Scan(&maxC, &rpm, &paused); err != nil {
		return false, err
	}
	if paused {
		return false, nil
	}
	var live, recent int
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM tasks WHERE limit_key = $1 AND status IN ('LEASED', 'RUNNING')),
		       (SELECT count(*) FROM task_attempts WHERE limit_key = $1 AND leased_at > clock_timestamp() - interval '1 minute')`,
		key).Scan(&live, &recent); err != nil {
		return false, err
	}
	return live < maxC && recent < rpm, nil
}

// pauseKey stops claims for a key after the source said "too many requests".
// The pause only ever extends (greatest), so a short Retry-After can't cut a
// longer pause short.
func pauseKey(ctx context.Context, tx pgx.Tx, runID, taskID, key string, seconds float64) error {
	if seconds <= 0 {
		seconds = defaultPauseSeconds
	}
	var until time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE limit_keys SET paused_until = greatest(coalesce(paused_until, now()), now() + make_interval(secs => $2)),
		       updated_at = now()
		WHERE key = $1 RETURNING paused_until`, key, seconds).Scan(&until); err != nil {
		return err
	}
	return taskEvent(ctx, tx, runID, taskID, "limit_key.paused", map[string]any{"key": key, "paused_until": until})
}
