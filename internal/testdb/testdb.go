// Package testdb gives each integration test its own fresh PostgreSQL database
// with every migration applied.
//
// Why it exists: Atlas's guarantees live in SQL constraints and transactions,
// so they must be tested against real Postgres, never a mock. A fresh database
// per test keeps tests independent and lets them run in parallel.
// Who calls it: *_test.go files in internal/store, internal/api, tests/.
//
// Configuration:
//
//	ATLAS_TEST_DATABASE_URL  admin connection (default: the docker-compose database)
//	ATLAS_REQUIRE_DB=1       fail instead of skip when Postgres is unreachable (set in CI,
//	                         so integration tests can never be skipped silently there)
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultURL = "postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable"

// New creates a database, applies migrations/*.up.sql in order, and returns
// its connection URL. The database is dropped when the test ends.
func New(t testing.TB) string {
	t.Helper()
	admin := os.Getenv("ATLAS_TEST_DATABASE_URL")
	if admin == "" {
		admin = defaultURL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		if os.Getenv("ATLAS_REQUIRE_DB") == "1" {
			t.Fatalf("postgres required but unreachable: %v", err)
		}
		t.Skipf("postgres not reachable (run `make up`): %v", err)
	}
	defer conn.Close(ctx)

	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "atlas_test_" + hex.EncodeToString(b)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, admin)
		if err != nil {
			return
		}
		defer c.Close(ctx)
		_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Path = "/" + name
	dbURL := u.String()

	db, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test db: %v", err)
	}
	defer db.Close(ctx)
	for _, f := range upMigrations(t) {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := db.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
	return dbURL
}

func upMigrations(t testing.TB) []string {
	_, here, _, _ := runtime.Caller(0) // internal/testdb/testdb.go
	dir := filepath.Join(filepath.Dir(here), "..", "..", "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found in %s", dir)
	}
	sort.Strings(files) // 000001_, 000002_, ... apply in order
	return files
}
