// Fault test for the A1 durability guarantee: "an acknowledged submission is
// durable". It runs the real atlas binary as a separate process, submits a
// run, kills the process (SIGKILL / TerminateProcess, no graceful shutdown)
// immediately after the 201, starts a new process, and reads the run back.
//
// Run with: make demo-a1 (verbose) or go test ./tests/integration.
package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prasaaaad98/atlas/internal/testdb"
)

func buildAtlas(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "atlas")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "github.com/prasaaaad98/atlas/cmd/atlas")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

// start launches atlas on a free port and waits for its "listening" line.
func start(t *testing.T, bin, dbURL string) (*exec.Cmd, string) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "DATABASE_URL="+dbURL, "ATLAS_ADDR=127.0.0.1:0")
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	lines := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if addr, ok := strings.CutPrefix(sc.Text(), "atlas listening on "); ok {
				lines <- addr
				return
			}
		}
	}()
	select {
	case addr := <-lines:
		return cmd, "http://" + addr
	case <-time.After(20 * time.Second):
		t.Fatal("atlas did not start")
		return nil, ""
	}
}

func post(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestKillAfterAckRunIsDurable(t *testing.T) {
	dbURL := testdb.New(t)
	bin := buildAtlas(t)

	proc, base := start(t, bin, dbURL)
	t.Logf("step 1: atlas started (pid %d) at %s", proc.Process.Pid, base)

	code, _ := post(t, base+"/pipelines", `{"name":"demo_grid","partition_kind":"daily",
		"partition_tz":"Europe/Brussels","tasks":[{"name":"fetch"},{"name":"parse","deps":["fetch"]}]}`)
	if code != 201 {
		t.Fatalf("register: %d", code)
	}
	runReq := `{"idempotency_key":"demo-a1","pipeline":"demo_grid","partition_key":"2026-11-18",
		"trigger":"manual","priority_class":"adhoc"}`
	code, run := post(t, base+"/runs", runReq)
	if code != 201 {
		t.Fatalf("submit: %d %v", code, run)
	}
	id := run["id"].(string)
	t.Logf("step 2: POST /runs acknowledged with 201, run %s", id)

	// Kill without any chance to flush or shut down cleanly.
	if err := proc.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = proc.Wait()
	t.Logf("step 3: killed atlas immediately after the ack")

	_, base2 := start(t, bin, dbURL)
	t.Logf("step 4: started a new atlas process at %s", base2)

	resp, err := http.Get(base2 + "/runs/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if resp.StatusCode != 200 || got["id"] != id || len(got["tasks"].([]any)) != 2 {
		t.Fatalf("run lost after kill: %d %v", resp.StatusCode, got)
	}
	t.Logf("step 5: GET /runs/%s -> %d, status %v, %d tasks", id, resp.StatusCode, got["status"], len(got["tasks"].([]any)))

	// The client's safe retry after an unclear outcome: same key, same request.
	code, again := post(t, base2+"/runs", runReq)
	if code != 200 || again["id"] != id {
		t.Fatalf("retry with same key: %d %v", code, again)
	}
	t.Logf("step 6: retry with the same idempotency key -> %d, same run (no duplicate)", code)
	fmt.Println("demo-a1: acknowledged run survived a kill; retry returned the same run")
}
