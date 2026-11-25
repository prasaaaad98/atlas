package metrics

import (
	"strings"
	"testing"
)

func TestHistogramIsCumulative(t *testing.T) {
	h := NewHistogram([]float64{0.01, 0.1, 1})
	for _, v := range []float64{0.005, 0.05, 0.05, 0.5, 5} {
		h.Observe(v)
	}
	bounds, cum, sum, n := h.Snapshot()
	if len(bounds) != 3 || cum[0] != 1 || cum[1] != 3 || cum[2] != 4 || n != 5 || sum < 5.6 || sum > 5.61 {
		t.Fatalf("bounds %v cum %v sum %v n %d", bounds, cum, sum, n)
	}
	var b strings.Builder
	w := NewWriter(&b)
	w.HistogramFamily("x_seconds", "help", bounds, cum, sum, n)
	out := b.String()
	for _, want := range []string{
		"# TYPE x_seconds histogram", `x_seconds_bucket{le="0.01"} 1`, `x_seconds_bucket{le="1"} 4`,
		`x_seconds_bucket{le="+Inf"} 5`, "x_seconds_count 5",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}

func TestLabelEscaping(t *testing.T) {
	var b strings.Builder
	NewWriter(&b).Sample("m", 1, "k", "a\"b\\c\nd")
	if got := b.String(); got != `m{k="a\"b\\c\nd"} 1`+"\n" {
		t.Fatalf("got %q", got)
	}
}
