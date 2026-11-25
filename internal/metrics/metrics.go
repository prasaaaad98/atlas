// Package metrics writes Prometheus text exposition by hand (Slice A9, ADR-006).
//
// Why it exists: Atlas exposes queue depth, claim latency, attempt outcomes,
// deadline slack, freshness lag and limit-key pauses at GET /metrics. The text
// format is simple and stable (https://prometheus.io/docs/instrumenting/exposition_formats/),
// so a hand-written writer avoids a dependency (CLAUDE.md).
// Who calls it: internal/api (/metrics handler, claim timing).
//
// Two kinds of metric:
//   - in-process (claim latency histogram): reset when the process restarts,
//     which Prometheus handles as a counter reset;
//   - derived from Postgres at scrape time (attempt counts, deadline misses,
//     slack histogram, queue depth): durable across restarts, because the
//     tables they count are append-only or current-state.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
)

// Histogram is a cumulative-bucket histogram (seconds).
type Histogram struct {
	mu      sync.Mutex
	bounds  []float64
	counts  []uint64 // per bucket, not cumulative
	sum     float64
	samples uint64
}

func NewHistogram(bounds []float64) *Histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return &Histogram{bounds: b, counts: make([]uint64, len(b))}
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.samples++
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
			return
		}
	}
}

// Snapshot returns cumulative counts per bound, the sum and the total count.
func (h *Histogram) Snapshot() ([]float64, []uint64, float64, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum := make([]uint64, len(h.counts))
	var run uint64
	for i, c := range h.counts {
		run += c
		cum[i] = run
	}
	return append([]float64(nil), h.bounds...), cum, h.sum, h.samples
}

// Writer emits one metric family at a time.
type Writer struct {
	w   io.Writer
	err error
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

func (w *Writer) Err() error { return w.err }

func (w *Writer) printf(format string, args ...any) {
	if w.err == nil {
		_, w.err = fmt.Fprintf(w.w, format, args...)
	}
}

func (w *Writer) Header(name, typ, help string) {
	w.printf("# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

// Sample writes one value line. labels are key, value pairs.
func (w *Writer) Sample(name string, value float64, labels ...string) {
	w.printf("%s%s %s\n", name, labelString(labels), formatValue(value))
}

// HistogramFamily writes cumulative buckets, +Inf, _sum and _count.
func (w *Writer) HistogramFamily(name, help string, bounds []float64, cumulative []uint64, sum float64, count uint64) {
	w.Header(name, "histogram", help)
	for i, b := range bounds {
		w.Sample(name+"_bucket", float64(cumulative[i]), "le", formatValue(b))
	}
	w.Sample(name+"_bucket", float64(count), "le", "+Inf")
	w.Sample(name+"_sum", sum)
	w.Sample(name+"_count", float64(count))
}

func labelString(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, kv[i], escape(kv[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

// escape per the exposition format: backslash, double quote and newline.
func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
