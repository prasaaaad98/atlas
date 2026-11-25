package domain

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestBackoffCapGrowsAndIsBounded(t *testing.T) {
	p := RetryPolicy{InitialIntervalSeconds: 2, BackoffCoefficient: 3, MaxIntervalSeconds: 100}
	want := []float64{2, 6, 18, 54, 100, 100}
	for i, w := range want {
		if got := p.Cap(i + 1); got != w {
			t.Errorf("Cap(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := p.Cap(10_000); got != 100 { // overflow to +Inf must still cap
		t.Errorf("Cap(10000) = %v", got)
	}
	if p.Backoff(1, 0.999999, 0) >= 2*time.Second || p.Backoff(1, 0, 0) != 0 {
		t.Error("backoff outside [0, cap)")
	}
	if p.Backoff(1, 0.5, 30) != 30*time.Second {
		t.Error("Retry-After longer than backoff must win")
	}
}

// Full jitter: delays after attempt n are uniform over [0, cap). Checked over
// 20,000 samples with a fixed seed: every sample in range, mean within 2% of
// cap/2, and each tenth of the range holds 10% ± 1.5% of the samples (a
// coarse uniformity test; the 1.5% band is about 7 standard deviations at
// n = 20,000, so a fixed-seed run can't flake).
func TestBackoffIsFullJitterUniform(t *testing.T) {
	p := RetryPolicy{InitialIntervalSeconds: 1, BackoffCoefficient: 2, MaxIntervalSeconds: 60}
	r := rand.New(rand.NewPCG(1, 2))
	const n, attempt = 20_000, 4 // cap = 8 s
	limit := p.Cap(attempt)
	var sum float64
	bins := make([]int, 10)
	for range n {
		d := p.Backoff(attempt, r.Float64(), 0).Seconds()
		if d < 0 || d >= limit {
			t.Fatalf("sample %v outside [0, %v)", d, limit)
		}
		sum += d
		bins[int(d/limit*10)]++
	}
	if mean := sum / n; math.Abs(mean-limit/2) > 0.02*limit/2 {
		t.Errorf("mean %v, want %v ± 2%%", mean, limit/2)
	}
	for i, c := range bins {
		if share := float64(c) / n; math.Abs(share-0.1) > 0.015 {
			t.Errorf("bin %d holds %.3f of samples, want 0.100 ± 0.015", i, share)
		}
	}
}

func TestRetryableClassifier(t *testing.T) {
	p := RetryPolicy{InitialIntervalSeconds: 1, BackoffCoefficient: 2, MaxIntervalSeconds: 10,
		NonRetryableErrorClasses: []string{"quota_exhausted"}}
	cases := []struct {
		class string
		hint  bool
		want  bool
	}{
		{"network", true, true},
		{"server_error", true, true},
		{"rate_limited", true, true},
		{"source_not_ready", true, true},
		{"network", false, false},    // worker says permanent
		{"auth", true, false},        // terminal whatever the worker says
		{"bad_request", true, false}, //
		{"data_check_failed", true, false},
		{"definition_mismatch", true, false},
		{"quota_exhausted", true, false}, // policy veto
	}
	for _, c := range cases {
		if got := p.Retryable(c.class, c.hint); got != c.want {
			t.Errorf("Retryable(%q, %v) = %v, want %v", c.class, c.hint, got, c.want)
		}
	}
}

func TestRetryPolicyValidate(t *testing.T) {
	if err := DefaultRetryPolicy.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []RetryPolicy{
		{InitialIntervalSeconds: -1, BackoffCoefficient: 2, MaxIntervalSeconds: 10},
		{InitialIntervalSeconds: 1, BackoffCoefficient: 0.5, MaxIntervalSeconds: 10},
		{InitialIntervalSeconds: 10, BackoffCoefficient: 2, MaxIntervalSeconds: 5},
	} {
		if bad.Validate() == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}
