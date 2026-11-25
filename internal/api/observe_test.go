// HTTP tests for /metrics, the UI pages and the long-poll claim (Slice A9).
package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/store"
	"github.com/prasaaaad98/atlas/internal/testdb"
)

// serverWithListen also runs the LISTEN loop, as cmd/atlas does.
func serverWithListen(t *testing.T) *httptest.Server {
	t.Helper()
	s, err := store.Open(context.Background(), testdb.New(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go s.Listen(ctx, func(error) {})
	srv := httptest.NewServer(Handler(s, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() { srv.Close(); cancel(); s.Close() })
	return srv
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

const onePipeline = `{"name":"one","partition_kind":"daily","partition_tz":"UTC","tasks":[{"name":"t","asset_outputs":["demo.one"]}]}`

func oneRun(key, partition string) string {
	return `{"idempotency_key":"` + key + `","pipeline":"one","partition_key":"` + partition + `","trigger":"manual","priority_class":"adhoc"}`
}

func TestMetricsReflectActivity(t *testing.T) {
	srv := serverWithListen(t)
	call(t, srv, "POST", "/pipelines", onePipeline)
	call(t, srv, "POST", "/runs", oneRun("a", "2026-11-24"))
	call(t, srv, "POST", "/runs", oneRun("b", "2026-11-25"))
	_, c := call(t, srv, "POST", "/claim", `{"worker_id":"w"}`)
	id := c["attempt_id"].(string)
	call(t, srv, "POST", "/attempts/"+id+"/start", `{"fencing_token":1}`)
	call(t, srv, "POST", "/attempts/"+id+"/complete", `{"fencing_token":1}`)

	code, ctype, body := get(t, srv, "/metrics")
	if code != 200 || !strings.HasPrefix(ctype, "text/plain") {
		t.Fatalf("metrics: %d %s", code, ctype)
	}
	for _, want := range []string{
		`atlas_queue_depth{class="adhoc"} 1`,
		`atlas_attempts_total{status="SUCCEEDED",outcome="succeeded"} 1`,
		`atlas_claim_duration_seconds_count 1`,
		`atlas_deadline_slack_seconds_bucket{le="+Inf"} 0`,
		"# TYPE atlas_deadline_missed_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestLongPollClaimWakesOnNotify(t *testing.T) {
	srv := serverWithListen(t)
	call(t, srv, "POST", "/pipelines", onePipeline)
	if code, _ := call(t, srv, "POST", "/claim", `{"worker_id":"w"}`); code != 204 {
		t.Fatalf("no wait, nothing ready: %d", code)
	}
	time.Sleep(200 * time.Millisecond) // let LISTEN start
	type result struct {
		code int
		at   time.Time
	}
	done := make(chan result, 1)
	go func() {
		code, _ := call(t, srv, "POST", "/claim?wait=10", `{"worker_id":"w"}`)
		done <- result{code, time.Now()}
	}()
	time.Sleep(300 * time.Millisecond) // the claim is now waiting
	submitted := time.Now()
	call(t, srv, "POST", "/runs", oneRun("x", "2026-11-24"))
	r := <-done
	latency := r.at.Sub(submitted)
	// The 1 s re-check would answer ~700 ms after the submit; NOTIFY answers at once.
	if r.code != 200 || latency > 400*time.Millisecond {
		t.Fatalf("long-poll claim: %d after %v", r.code, latency)
	}
	t.Logf("woken %v after the submit", latency)
	start := time.Now()
	if code, _ := call(t, srv, "POST", "/claim?wait=0.3", `{"worker_id":"w"}`); code != 204 || time.Since(start) < 250*time.Millisecond {
		t.Fatalf("wait expiry: %d after %v", code, time.Since(start))
	}
}

func TestUIPagesRenderAndEscape(t *testing.T) {
	srv := serverWithListen(t)
	call(t, srv, "POST", "/pipelines", onePipeline)
	_, run := call(t, srv, "POST", "/runs", oneRun("a", "2026-11-24"))
	_, c := call(t, srv, "POST", "/claim", `{"worker_id":"<script>alert(1)</script>"}`)
	id := c["attempt_id"].(string)
	call(t, srv, "POST", "/attempts/"+id+"/start", `{"fencing_token":1}`)
	call(t, srv, "POST", "/attempts/"+id+"/complete", `{"fencing_token":1,"materializations":[{"asset_key":"demo.one",
		"partition_key":"2026-11-24","uri":"demo/one/x.json","row_count":1,
		"content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"1","code_version":"1"}]}`)

	code, ctype, body := get(t, srv, "/ui/runs/"+run["id"].(string))
	if code != 200 || !strings.HasPrefix(ctype, "text/html") {
		t.Fatalf("timeline: %d %s", code, ctype)
	}
	for _, want := range []string{"task.claimed", "task.completed", "SUCCEEDED", "&lt;script&gt;"} {
		if !strings.Contains(body, want) {
			t.Errorf("timeline missing %q", want)
		}
	}
	if strings.Contains(body, "<script>alert") {
		t.Fatal("worker_id rendered unescaped")
	}
	code, _, body = get(t, srv, "/ui/assets")
	if code != 200 || !strings.Contains(body, "demo.one") || !strings.Contains(body, `class="fresh"`) {
		t.Fatalf("assets page: %d\n%s", code, body)
	}
	if code, _, _ := get(t, srv, "/ui/runs/00000000-0000-0000-0000-000000000000"); code != 404 {
		t.Fatalf("unknown run page: %d", code)
	}
}
