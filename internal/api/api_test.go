// HTTP-level tests against a real store: status codes and error bodies are the
// contract clients and the Python SDK rely on.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prasaaaad98/atlas/internal/store"
	"github.com/prasaaaad98/atlas/internal/testdb"
)

func server(t *testing.T) *httptest.Server {
	t.Helper()
	s, err := store.Open(context.Background(), testdb.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	srv := httptest.NewServer(Handler(s, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

const pipelineJSON = `{"name":"demo_grid","partition_kind":"daily","partition_tz":"Asia/Kolkata",
  "tasks":[{"name":"fetch"},{"name":"parse","deps":["fetch"]}]}`

func runJSON(key, partition string) string {
	return `{"idempotency_key":"` + key + `","pipeline":"demo_grid","partition_key":"` + partition +
		`","trigger":"manual","priority_class":"adhoc"}`
}

func TestHTTPContract(t *testing.T) {
	srv := server(t)

	if code, _ := call(t, srv, "POST", "/pipelines", pipelineJSON); code != 201 {
		t.Fatalf("register: %d", code)
	}
	if code, _ := call(t, srv, "POST", "/pipelines", pipelineJSON); code != 200 {
		t.Errorf("re-register identical: %d, want 200", code)
	}

	code, run := call(t, srv, "POST", "/runs", runJSON("k1", "2026-11-24"))
	if code != 201 {
		t.Fatalf("submit: %d %v", code, run)
	}
	if run["data_interval_start"] != "2026-11-23T18:30:00Z" {
		t.Errorf("IST interval start %v", run["data_interval_start"])
	}
	id := run["id"].(string)

	if code, again := call(t, srv, "POST", "/runs", runJSON("k1", "2026-11-24")); code != 200 || again["id"] != id {
		t.Errorf("replay: %d %v", code, again["id"])
	}
	if code, body := call(t, srv, "POST", "/runs", runJSON("k2", "2026-11-24")); code != 409 ||
		body["error"] != "active_run_exists" || body["active_run_id"] != id {
		t.Errorf("second active run: %d %v", code, body)
	}
	if code, body := call(t, srv, "POST", "/runs", runJSON("k1", "2026-11-25")); code != 409 ||
		body["error"] != "idempotency_key_reused" {
		t.Errorf("key reuse: %d %v", code, body)
	}
	if code, got := call(t, srv, "GET", "/runs/"+id, ""); code != 200 || len(got["tasks"].([]any)) != 2 {
		t.Errorf("get: %d %v", code, got)
	}
	if code, _ := call(t, srv, "GET", "/runs/00000000-0000-0000-0000-000000000000", ""); code != 404 {
		t.Errorf("unknown run: %d", code)
	}
	if code, _ := call(t, srv, "POST", "/runs", `{"idempotency_key":"x","pipelin":"typo"}`); code != 400 {
		t.Errorf("unknown field: %d, want 400", code)
	}
	if code, _ := call(t, srv, "POST", "/runs", runJSON("k3", "24-11-2026")); code != 400 {
		t.Errorf("bad partition: %d, want 400", code)
	}
	if code, _ := call(t, srv, "GET", "/healthz", ""); code != 200 {
		t.Errorf("healthz: %d", code)
	}
}
