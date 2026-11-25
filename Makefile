# OWNERSHIP: assisted
#
# One-word commands for everything you run often. Each target is a thin wrapper
# so you can read exactly what it does.

COMPOSE      := docker compose -f deploy/compose/docker-compose.yml
DATABASE_URL ?= postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable

# Git Bash on Windows rewrites arguments like /migrations into Windows paths before
# Docker sees them; this turns that off (no effect on Linux/CI).
export MSYS_NO_PATHCONV := 1

# golang-migrate runs from its Docker image, so it adds no Go dependency.
# host.docker.internal lets the container reach the Postgres port published on your machine.
MIGRATE := docker run --rm -v "$(CURDIR)/migrations:/migrations" migrate/migrate:v4.18.3 \
	-path=/migrations -database "postgres://atlas:atlas@host.docker.internal:5432/atlas?sslmode=disable"

.PHONY: up down reset migrate migrate-down psql build test test-race vet fmt demo-a0 	pipelines-venv pipelines-test pipelines-run lake-catalog demo-b0

# Python: use the project venv if it exists (`make pipelines-venv`), else `python` on PATH.
VENV_PY := $(firstword $(wildcard pipelines/.venv/Scripts/python.exe pipelines/.venv/bin/python))
PY      ?= $(if $(VENV_PY),$(VENV_PY),python)
ATLAS   := $(PY) -m atlas_pipelines

up:            ## start Postgres and wait until it is healthy
	$(COMPOSE) up -d --wait

down:          ## stop containers, keep data
	$(COMPOSE) down

reset:         ## stop containers and DELETE local database data
	$(COMPOSE) down -v

migrate:       ## apply all pending migrations
	$(MIGRATE) up

migrate-down:  ## roll back the most recent migration
	$(MIGRATE) down 1

psql:          ## open a SQL shell inside the Postgres container
	$(COMPOSE) exec postgres psql -U atlas -d atlas

build:
	go build -o bin/atlas ./cmd/atlas

test:          ## unit + integration tests
	go test ./...

# The race detector needs cgo. On Windows that means gcc on PATH (WinLibs via
# `winget install BrechtSanders.WinLibs.POSIX.UCRT`); cgo is forced on here
# because Go silently disables it when it can't find a compiler.
test-race: export CGO_ENABLED = 1
test-race:     ## tests with the race detector (needs a C compiler)
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

run:           ## run the control plane against the compose database (make up && make migrate first)
	go run ./cmd/atlas

demo-a1:       ## kill atlas right after a 201, restart, show the run survived (needs make up)
	ATLAS_REQUIRE_DB=1 go test -count=1 -v -run TestKillAfterAck ./tests/integration

demo-a0:       ## state machine trace: lease lapses, retry succeeds, stale completion rejected
	go test ./internal/domain -run Example -v

pipelines-venv: ## create pipelines/.venv and install the package with pinned deps
	python -m venv pipelines/.venv
	$(firstword $(wildcard pipelines/.venv/Scripts/python.exe pipelines/.venv/bin/python)) -m pip install -e "pipelines[dev]"

pipelines-test: ## Python unit, golden and property tests (no network)
	$(PY) -m pytest -q -c pipelines/pyproject.toml pipelines/tests

pipelines-run: ## local runner, e.g. make pipelines-run P=demo_grid_iex_dam PART=2026-11-24
	$(ATLAS) run $(P) --partition $(PART)

lake-catalog:  ## rebuild <lake>/catalog.duckdb views over current partitions
	$(ATLAS) catalog

demo-b0:       ## run a partition twice (same hash, new attempt), rebuild catalog, query it
	$(ATLAS) markets
	$(ATLAS) run demo_grid_iex_dam --partition 2026-11-24
	$(ATLAS) run demo_grid_iex_dam --partition 2026-11-24
	$(ATLAS) catalog
	$(ATLAS) show demo.grid_iex_dam
