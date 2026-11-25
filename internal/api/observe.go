// Observability endpoints and the long-poll claim (Slice A9).
//
// Why it exists: an operator must see, without SQL, what Atlas is doing:
// queue depth per class, claim latency, attempt outcomes, deadline misses and
// slack, freshness lag, limit-key pauses (/metrics); one run's history as a
// timeline (/ui/runs/{id}); and which asset partitions are fresh (/ui/assets).
// Who calls it: api.go (Handler), Prometheus, a browser.
//
//	GET  /metrics            Prometheus text format
//	GET  /ui/runs/{id}       run timeline (events + attempts as bars)
//	GET  /ui/assets          freshness grid: assets × last 14 partitions
//	POST /claim?wait=N       long-poll: wait up to N s (max 30) for a task,
//	                         woken by LISTEN/NOTIFY, re-checking every second
package api

import (
	"context"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/prasaaaad98/atlas/internal/metrics"
	"github.com/prasaaaad98/atlas/internal/store"
)

type observer struct {
	claimLatency *metrics.Histogram // server-side duration of one Claim transaction
}

func newObserver() *observer {
	return &observer{claimLatency: metrics.NewHistogram([]float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})}
}

const maxClaimWait = 30 * time.Second

// claim runs one Claim, or long-polls for up to `wait`. The ready signal is
// taken BEFORE each attempt, so a NOTIFY between a failed attempt and the
// wait still wakes us. The 1 s re-check covers retries whose backoff ends
// without a notification, and a lost LISTEN connection.
func (o *observer) claim(ctx context.Context, s Store, req store.ClaimRequest, wait time.Duration) (store.Claim, bool, error) {
	deadline := time.Now().Add(wait)
	for {
		signal := s.ReadySignal()
		start := time.Now()
		c, ok, err := s.Claim(ctx, req)
		o.claimLatency.Observe(time.Since(start).Seconds())
		if err != nil || ok {
			return c, ok, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return store.Claim{}, false, nil
		}
		t := time.NewTimer(min(remaining, time.Second))
		select {
		case <-ctx.Done():
			t.Stop()
			return store.Claim{}, false, nil
		case <-signal:
		case <-t.C:
		}
		t.Stop()
	}
}

func claimWait(r *http.Request) time.Duration {
	secs, err := strconv.ParseFloat(r.URL.Query().Get("wait"), 64)
	if err != nil || secs <= 0 {
		return 0
	}
	return min(time.Duration(secs*float64(time.Second)), maxClaimWait)
}

func (o *observer) metricsHandler(s Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap, err := s.Metrics(r.Context())
		if err != nil {
			log.Error("metrics snapshot failed", "err", err)
			http.Error(w, "metrics unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		mw := metrics.NewWriter(w)
		classes := []string{"live", "adhoc", "backfill"}
		mw.Header("atlas_queue_depth", "gauge", "READY tasks claimable now, by priority class")
		for _, c := range classes {
			mw.Sample("atlas_queue_depth", float64(snap.QueueReady[c]), "class", c)
		}
		mw.Header("atlas_queue_delayed", "gauge", "READY tasks waiting for a retry backoff, by priority class")
		for _, c := range classes {
			mw.Sample("atlas_queue_delayed", float64(snap.QueueDelayed[c]), "class", c)
		}
		mw.Header("atlas_tasks_running", "gauge", "Tasks LEASED or RUNNING")
		mw.Sample("atlas_tasks_running", float64(snap.Running))
		bounds, cum, sum, n := o.claimLatency.Snapshot()
		mw.HistogramFamily("atlas_claim_duration_seconds", "Server-side duration of one claim transaction (this process)", bounds, cum, sum, n)
		mw.Header("atlas_attempts_total", "counter", "Finished task attempts by status and outcome class")
		keys := make([][2]string, 0, len(snap.Attempts))
		for k := range snap.Attempts {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
		for _, k := range keys {
			mw.Sample("atlas_attempts_total", float64(snap.Attempts[k]), "status", k[0], "outcome", k[1])
		}
		mw.Header("atlas_deadline_at_risk_total", "counter", "Runs flagged at risk of missing their deadline")
		mw.Sample("atlas_deadline_at_risk_total", float64(snap.DeadlineAtRisk))
		mw.Header("atlas_deadline_missed_total", "counter", "Runs that missed their deadline (one per late run)")
		mw.Sample("atlas_deadline_missed_total", float64(snap.DeadlineMissed))
		mw.HistogramFamily("atlas_deadline_slack_seconds", "Deadline minus finish time of finished runs (negative = late)",
			store.SlackBuckets, snap.Slack, snap.SlackSum, snap.SlackCount)
		mw.Header("atlas_asset_late_total", "counter", "Asset partitions that turned late")
		mw.Sample("atlas_asset_late_total", float64(snap.AssetLate))
		mw.Header("atlas_asset_freshness_lag_seconds", "gauge", "Seconds since the deadline of the oldest late partition, per asset")
		for _, k := range sortedKeys(snap.FreshnessLag) {
			mw.Sample("atlas_asset_freshness_lag_seconds", snap.FreshnessLag[k], "asset", k)
		}
		mw.Header("atlas_limit_key_paused", "gauge", "1 while a limit key is paused after a 429")
		for _, k := range sortedKeys(snap.LimitPaused) {
			v := 0.0
			if snap.LimitPaused[k] {
				v = 1
			}
			mw.Sample("atlas_limit_key_paused", v, "key", k)
		}
		mw.Header("atlas_limit_key_pauses_total", "counter", "Times a limit key was paused")
		for _, k := range sortedKeys(snap.LimitPauses) {
			mw.Sample("atlas_limit_key_pauses_total", float64(snap.LimitPauses[k]), "key", k)
		}
		if mw.Err() != nil {
			log.Error("writing metrics", "err", mw.Err())
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- pages (html/template escapes every value) ------------------------------------

var page = template.Must(template.New("base").Funcs(template.FuncMap{
	"ts": func(t time.Time) string { return t.UTC().Format("15:04:05.000") },
	"pct": func(v float64) string {
		return strconv.FormatFloat(max(0, min(100, v)), 'f', 2, 64) + "%"
	},
}).Parse(`<!doctype html><html><head><meta charset="utf-8"><title>{{.Title}}</title>
<style>body{font:14px system-ui,sans-serif;margin:24px;color:#222}table{border-collapse:collapse}
td,th{border:1px solid #ddd;padding:4px 8px;text-align:left;vertical-align:top}th{background:#f4f4f4}
.bar{position:relative;height:14px;background:#f0f0f0;width:420px}.bar span{position:absolute;height:14px}
.SUCCEEDED,.fresh{background:#3a9d5d}.FAILED,.CHECK_FAILED,.failed_check{background:#d9534f}.LEASE_EXPIRED,.late{background:#f0ad4e}
.CANCELED{background:#999}.RUNNING,.LEASED{background:#5b8def}.missing{background:#eee}code{font-size:12px}</style></head><body>
{{template "body" .}}</body></html>`))

var timelinePage = template.Must(template.Must(page.Clone()).Parse(`{{define "body"}}
<h2>Run {{.Run.PipelineName}} / {{.Run.PartitionKey}}</h2>
<p>status <b>{{.Run.Status}}</b> · trigger {{.Run.Trigger}} · class {{.Run.PriorityClass}} · created {{ts .Run.CreatedAt}} UTC
{{if .Run.DeadlineAt}} · deadline {{ts .Run.DeadlineAt.UTC}}{{end}}{{if .Run.DeadlineMissedAt}} · <b>deadline missed</b>{{end}}</p>
<h3>Attempts</h3><table><tr><th>task</th><th>#</th><th>worker</th><th>status</th><th>outcome</th><th>error</th><th>time</th></tr>
{{range .Bars}}<tr><td>{{.A.TaskName}}</td><td>{{.A.AttemptNumber}}</td><td>{{.A.WorkerID}}</td><td>{{.A.Status}}</td>
<td>{{with .A.Outcome}}{{.}}{{end}}</td><td>{{with .A.ErrorClass}}{{.}}{{end}}</td>
<td><div class="bar"><span class="{{.A.Status}}" style="left:{{pct .Left}};width:{{pct .Width}}"></span></div></td></tr>{{end}}</table>
<h3>Events</h3><table><tr><th>at (UTC)</th><th>type</th><th>task</th><th>payload</th></tr>
{{range .Events}}<tr><td>{{ts .At}}</td><td>{{.Type}}</td><td>{{.TaskName}}</td><td><code>{{.Payload}}</code></td></tr>{{end}}</table>
{{end}}`))

var assetsPage = template.Must(template.Must(page.Clone()).Parse(`{{define "body"}}
<h2>Asset freshness</h2><p>Last 14 partitions per asset. Green fresh · orange late · red failed check · grey missing.</p>
<table><tr><th>asset</th>{{range .Partitions}}<th>{{.}}</th>{{end}}</tr>
{{range .Rows}}<tr><td>{{.Key}}</td>{{range .Cells}}<td class="{{.}}" title="{{.}}"></td>{{end}}</tr>{{end}}</table>
{{end}}`))

type bar struct {
	A           store.TimelineAttempt
	Left, Width float64
}

func uiRun(s Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		run, err := s.GetRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writeErr(w, log, err)
			return
		}
		events, attempts, err := s.Timeline(r.Context(), run.ID)
		if err != nil {
			writeErr(w, log, err)
			return
		}
		start, end := run.CreatedAt, run.CreatedAt.Add(time.Second)
		for _, a := range attempts {
			if a.FinishedAt != nil && a.FinishedAt.After(end) {
				end = *a.FinishedAt
			}
		}
		if n := time.Now(); run.FinishedAt == nil && n.After(end) {
			end = n
		}
		span := end.Sub(start).Seconds()
		bars := make([]bar, 0, len(attempts))
		for _, a := range attempts {
			stop := time.Now()
			if a.FinishedAt != nil {
				stop = *a.FinishedAt
			}
			bars = append(bars, bar{A: a, Left: a.LeasedAt.Sub(start).Seconds() / span * 100,
				Width: max(0.5, stop.Sub(a.LeasedAt).Seconds()/span*100)})
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := timelinePage.Execute(w, map[string]any{"Title": "Run " + run.ID, "Run": run, "Bars": bars, "Events": events}); err != nil {
			log.Error("render timeline", "err", err)
		}
	}
}

func uiAssets(s Store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assets, err := s.ListAssets(r.Context())
		if err != nil {
			writeErr(w, log, err)
			return
		}
		type row struct {
			Key   string
			Cells []string
		}
		seen := map[string]bool{}
		status := map[string]map[string]string{}
		for _, a := range assets {
			ps, err := s.AssetPartitions(r.Context(), a.Key, 14)
			if err != nil {
				writeErr(w, log, err)
				return
			}
			status[a.Key] = map[string]string{}
			for _, p := range ps {
				status[a.Key][p.PartitionKey] = p.Status
				seen[p.PartitionKey] = true
			}
		}
		parts := sortedKeys(seen)
		sort.Sort(sort.Reverse(sort.StringSlice(parts)))
		if len(parts) > 14 {
			parts = parts[:14]
		}
		var rows []row
		for _, a := range assets {
			cells := make([]string, len(parts))
			for i, p := range parts {
				cells[i] = status[a.Key][p]
				if cells[i] == "" {
					cells[i] = "missing"
				}
			}
			rows = append(rows, row{a.Key, cells})
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := assetsPage.Execute(w, map[string]any{"Title": "Asset freshness", "Partitions": parts, "Rows": rows}); err != nil {
			log.Error("render assets", "err", err)
		}
	}
}
