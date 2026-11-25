// atlas-load: a load harness for the Atlas control plane (Slice A9, ADR-006, ADR-007).
//
// Why it exists: claims about Atlas's performance must be measured, with the
// hardware, data and method stated. This harness drives a running control
// plane over HTTP exactly as workers do and reports the numbers.
// Who calls it: make load-throughput, make load-dispatch; results go to
// docs/measurements/a9.md.
//
// Scenarios:
//
//	throughput  submit R runs × T independent tasks up front, then W workers
//	            claim → start → complete as fast as they can. Reports tasks/s,
//	            client-side claim round-trip percentiles, and checks that no task
//	            was claimed twice.
//	dispatch    W idle workers; N single-task runs submitted one every I ms.
//	            Reports submit-to-claim latency percentiles, for
//	            -mode poll (sleep -poll between empty claims) or
//	            -mode notify (long-poll POST /claim?wait=20, woken by LISTEN/NOTIFY).
//
// Measures the control plane + Postgres only: tasks do no work.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var client = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}

func post(url string, body any) (int, map[string]any, error) {
	b, _ := json.Marshal(body)
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out, nil
}

// mustPost posts and fails the run unless the status is one of want (default 200/201).
func mustPost(url string, body any, want ...int) map[string]any {
	code, out, err := post(url, body)
	if err != nil {
		fail("request: %v", err)
	}
	if len(want) == 0 {
		want = []int{200, 201}
	}
	for _, w := range want {
		if code == w {
			return out
		}
	}
	fail("unexpected HTTP %d: %v", code, out)
	return nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "atlas-load: "+format+"\n", args...)
	os.Exit(1)
}

type report struct {
	Scenario   string             `json:"scenario"`
	Mode       string             `json:"mode,omitempty"`
	Params     map[string]any     `json:"params"`
	Tasks      int                `json:"tasks"`
	Seconds    float64            `json:"seconds"`
	TasksPerS  float64            `json:"tasks_per_second,omitempty"`
	LatencyMS  map[string]float64 `json:"latency_ms"`
	LatencyOf  string             `json:"latency_of"`
	DoubleClms int                `json:"double_claims"`
	// Claim transactions the server ran during the scenario (from /metrics),
	// per task: the database cost of finding work, including empty polls.
	ClaimsPerTask float64 `json:"claims_per_task"`
}

// claimCount reads atlas_claim_duration_seconds_count from /metrics.
func claimCount(url string) float64 {
	resp, err := client.Get(url + "/metrics")
	if err != nil {
		fail("metrics: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		var v float64
		if _, err := fmt.Sscanf(string(line), "atlas_claim_duration_seconds_count %g", &v); err == nil {
			return v
		}
	}
	return 0
}

func percentiles(d []time.Duration) map[string]float64 {
	if len(d) == 0 {
		return nil
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(p float64) float64 {
		i := int(p*float64(len(d)-1) + 0.5)
		return float64(d[i].Microseconds()) / 1000
	}
	return map[string]float64{"p50": at(0.50), "p95": at(0.95), "p99": at(0.99), "max": at(1)}
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "control plane URL")
	scenario := flag.String("scenario", "throughput", "throughput | dispatch")
	runs := flag.Int("runs", 100, "throughput: runs to submit")
	tasks := flag.Int("tasks", 10, "throughput: independent tasks per run")
	workers := flag.Int("workers", 20, "concurrent workers")
	mode := flag.String("mode", "poll", "dispatch: poll | notify")
	pollEvery := flag.Duration("poll", time.Second, "dispatch poll mode: sleep between empty claims")
	n := flag.Int("n", 100, "dispatch: runs to submit")
	interval := flag.Duration("interval", 100*time.Millisecond, "dispatch: time between submits")
	flag.Parse()

	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	base := time.Date(3000+rand.IntN(5000), 1, 1, 0, 0, 0, 0, time.UTC) // partitions unused by real pipelines
	partition := func(i int) string { return base.AddDate(0, 0, i).Format("2006-01-02") }

	switch *scenario {
	case "throughput":
		throughput(*url, tag, partition, *runs, *tasks, *workers)
	case "dispatch":
		dispatch(*url, tag, partition, *n, *workers, *mode, *pollEvery, *interval)
	default:
		fail("unknown scenario %q", *scenario)
	}
}

func register(url, name string, tasks int) {
	def := map[string]any{"name": name, "partition_kind": "daily", "partition_tz": "UTC"}
	var ts []map[string]any
	for i := range tasks {
		ts = append(ts, map[string]any{"name": fmt.Sprintf("t%03d", i)})
	}
	def["tasks"] = ts
	mustPost(url+"/pipelines", def)
}

func submit(url, tag, pipeline, partition string) string {
	out := mustPost(url+"/runs", map[string]any{"idempotency_key": "load:" + tag + ":" + partition, "pipeline": pipeline,
		"partition_key": partition, "trigger": "manual", "priority_class": "adhoc"}, 201)
	return out["id"].(string)
}

func work(url string, c map[string]any) {
	id, tok := c["attempt_id"].(string), c["fencing_token"]
	mustPost(url+"/attempts/"+id+"/start", map[string]any{"fencing_token": tok}, 200)
	mustPost(url+"/attempts/"+id+"/complete", map[string]any{"fencing_token": tok}, 200)
}

func throughput(url, tag string, partition func(int) string, runs, tasks, workers int) {
	pipeline := fmt.Sprintf("load_wide_%d", tasks)
	register(url, pipeline, tasks)
	for i := range runs {
		submit(url, tag, pipeline, partition(i))
	}
	total := runs * tasks
	claims0 := claimCount(url)
	var mu sync.Mutex
	var lat []time.Duration
	claimed := map[string]int{}
	var done atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for done.Load() < int64(total) {
				t0 := time.Now()
				code, c, err := post(url+"/claim", map[string]any{"worker_id": fmt.Sprintf("load-%d", w)})
				d := time.Since(t0)
				if err != nil {
					fail("claim: %v", err)
				}
				if code == 204 {
					if done.Load() >= int64(total) {
						return
					}
					time.Sleep(5 * time.Millisecond) // the remaining tasks are held by other workers
					continue
				}
				if code != 200 {
					fail("claim HTTP %d %v", code, c)
				}
				work(url, c)
				mu.Lock()
				lat = append(lat, d)
				claimed[c["task_id"].(string)]++
				mu.Unlock()
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	doubles := 0
	for _, k := range claimed {
		if k > 1 {
			doubles++
		}
	}
	emit(report{Scenario: "throughput", Params: map[string]any{"runs": runs, "tasks_per_run": tasks, "workers": workers},
		Tasks: total, Seconds: secs, TasksPerS: float64(total) / secs, LatencyMS: percentiles(lat),
		LatencyOf: "client-side POST /claim round trip (successful claims)", DoubleClms: doubles,
		ClaimsPerTask: (claimCount(url) - claims0) / float64(total)})
}

func dispatch(url, tag string, partition func(int) string, n, workers int, mode string, pollEvery, interval time.Duration) {
	register(url, "load_one", 1)
	var submittedAt sync.Map // run id → time the 201 was received
	var mu sync.Mutex
	var lat []time.Duration
	var got atomic.Int64
	stop := make(chan struct{})
	claimPath := url + "/claim"
	if mode == "notify" {
		claimPath += "?wait=20"
	} else if mode != "poll" {
		fail("mode must be poll or notify")
	}
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				code, c, err := post(claimPath, map[string]any{"worker_id": fmt.Sprintf("load-%d", w)})
				if err != nil {
					fail("claim: %v", err)
				}
				if code == 204 {
					if mode == "poll" {
						time.Sleep(pollEvery)
					}
					continue
				}
				at := time.Now()
				runID := c["run_id"].(string)
				var sub any
				for range 1000 { // the submit's 201 may land a moment after the claim
					if v, ok := submittedAt.Load(runID); ok {
						sub = v
						break
					}
					time.Sleep(time.Millisecond)
				}
				if sub != nil {
					mu.Lock()
					lat = append(lat, max(0, at.Sub(sub.(time.Time))))
					mu.Unlock()
				}
				work(url, c)
				got.Add(1)
			}
		}()
	}
	time.Sleep(500 * time.Millisecond) // workers are idle-waiting before the first submit
	claims0 := claimCount(url)
	start := time.Now()
	for i := range n {
		id := submit(url, tag, "load_one", partition(i))
		submittedAt.Store(id, time.Now())
		time.Sleep(interval)
	}
	for got.Load() < int64(n) {
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	// Workers in a long poll return within 20 s; don't wait for them to report.
	secs := time.Since(start).Seconds()
	params := map[string]any{"runs": n, "workers": workers, "submit_interval_ms": interval.Milliseconds()}
	if mode == "poll" {
		params["poll_interval_ms"] = pollEvery.Milliseconds()
	}
	emit(report{Scenario: "dispatch", Mode: mode, Params: params, Tasks: n, Seconds: secs, LatencyMS: percentiles(lat),
		LatencyOf: "submit 201 received → claim response received, per run", ClaimsPerTask: (claimCount(url) - claims0) / float64(n)})
}

func emit(r report) {
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
}
