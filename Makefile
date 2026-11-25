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

.PHONY: up down reset migrate migrate-down psql build test test-race vet fmt demo-a0 	pipelines-venv pipelines-test pipelines-run lake-catalog demo-b0 fixture-openmeteo \
	demo-b1 fixture-entsoe-a01 ingest-entsoe demo-a2 demo-a3 demo-a4 demo-a5 demo-a6 demo-a7 demo-a8 demo-a9 load-throughput load-dispatch demo-b7 b7-backfill b7-schedule fault fixture-eia calendar demo-b4e demo-e1 demo-e2 ingest-eia

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

demo-a2:       ## real control plane + two Python workers: claim order, attempts, stale report rejected (needs make up, make migrate)
	bash scripts/demo-a2.sh

demo-a3:       ## kill -9 a worker mid-task: reaper re-queues, new worker resumes from the checkpoint (needs make up, make migrate)
	bash scripts/demo-a3.sh

demo-a4:       ## retries: flaky task succeeds on attempt 3 after jittered backoff; auth failure never retried (needs make up, make migrate)
	bash scripts/demo-a4.sh

demo-a5:       ## DAG with a killed worker, cancel a running run, concurrency-1 limit key (needs make up, make migrate)
	bash scripts/demo-a5.sh

demo-a6:       ## catchup schedule across a control-plane kill -9; bounded backfill with a live run (needs make up, make migrate)
	bash scripts/demo-a6.sh

demo-a7:       ## EDF claim order, at-risk before miss, late policy cancel + fallback (needs make up, make migrate)
	bash scripts/demo-a7.sh

demo-a8:       ## check-gated current pointer, failed re-run keeps old version, freshness late then fresh (needs make up, make migrate)
	bash scripts/demo-a8.sh

demo-a9:       ## metrics, run timeline and freshness pages with long-polling workers (needs make up, make migrate)
	bash scripts/demo-a9.sh

LOAD_URL ?= http://127.0.0.1:8080
load-throughput: ## measure: 20 workers x 1,000 tasks against $(LOAD_URL) (start it with make run)
	go run ./cmd/atlas-load -url $(LOAD_URL) -scenario throughput -runs 100 -tasks 10 -workers 20

load-dispatch: ## measure: submit-to-claim latency, polling 1 s vs long-poll NOTIFY, against $(LOAD_URL)
	go run ./cmd/atlas-load -url $(LOAD_URL) -scenario dispatch -mode poll -poll 1s -n 200 -workers 4
	go run ./cmd/atlas-load -url $(LOAD_URL) -scenario dispatch -mode notify -n 200 -workers 4

demo-b7:       ## offline: real ENTSO-E files through Atlas + workers (kill -9, deadline miss), gold spreads, catalog from Atlas
	bash scripts/demo-b7.sh

b7-backfill:   ## OWNER-RUN (network, ENTSOE_TOKEN): ENTSO-E history FROM..TO through Atlas (CHAOS=1 kills workers)
	bash scripts/b7-backfill.sh

b7-schedule:   ## OWNER-RUN: leave running; daily 12:00 Brussels fetch of the next delivery day + uptime record
	bash scripts/b7-schedule.sh

fixture-eia:   ## OWNER-RUN (network, EIA_API_KEY in .env): EIA reachability, key placement, route discovery, raw fixtures (E0)
	$(PY) scripts/eia_reachability.py

calendar:      ## build silver.calendar for every market, 2022 through next year (offline)
	@for y in $$(seq 2022 $$(( $$(date +%Y) + 1 ))); do $(ATLAS) run calendar_build --partition $$y-01-01 | grep -E '^run|FAIL'; done
	$(ATLAS) catalog

demo-b4e:      ## calendar: build 2022..next year, then show 2026 holidays, weekends and DST days per zone
	$(MAKE) calendar
	$(PY) scripts/calendar_show.py 2026 $(or $(LAKE_ROOT),lake)

demo-e1:       ## offline: real saved EIA responses through the client (paging, truncation, failures, key handling)
	$(PY) -m pytest -v -c pipelines/pyproject.toml pipelines/tests/unit/test_eia_client.py

demo-e2:       ## offline: real EIA-930 responses -> silver.demand / silver.interchange (DST, vintages, gaps)
	$(PY) -m pytest -v -c pipelines/pyproject.toml pipelines/tests/unit/test_eia_region.py

EIA_FROM ?= $(shell date -u -d '30 days ago' +%Y-%m-%d)
EIA_TO   ?= $(shell date -u -d '2 days ago' +%Y-%m-%d)
ingest-eia:    ## OWNER-RUN (network, EIA_API_KEY): EIA-930 demand + interchange for every BA, EIA_FROM..EIA_TO
	$(ATLAS) run eia930_region_daily --partition $(EIA_FROM) --to $(EIA_TO)
	$(ATLAS) catalog

FAULT_TESTS = ^(TestKillAfterAckRunIsDurable|TestConcurrentDuplicateSubmitsCreateOneRun|TestConcurrentDifferentKeysOneWinner|TestConcurrentClaimersNeverDoubleClaim|TestConcurrentLastCompletionsFinishTheRun|TestStaleTokenAndReplacedAttemptAreRejected|TestCrashedWorkerTaskIsReRun|TestPausedWorkerCannotOverwriteNewAttempt|TestExpiredLeaseIsFencedBeforeTheReaperRuns|TestReaperSkipsALockedTask|TestExpiriesCountAsAttempts|TestHundredPartitionsWithRandomWorkerDeaths|TestConcurrentParentCompletionsReadyTheChild|TestLimitKeyConcurrencyNeverExceeded|TestRateLimitedFailurePausesKeyAndRecovers|TestTickCrashOrOverlapCreatesEachPartitionOnce|TestEveryLateRunHasExactlyOneMissedEvent|TestFencedOutAttemptsOutputNeverBecomesCurrent|TestFailedBlockingCheckLeavesPointerAndCancelsDependents|TestLongPollClaimWakesOnNotify)$$
fault:         ## every crash/race/stale-worker/fault scenario in one command (Go with -race, then Python)
	@mkdir -p bin; go test -race -count=1 -v -run '$(FAULT_TESTS)' ./... > bin/fault-go.log 2>&1; rc=$$?; 	  grep -E '^(--- |ok|FAIL|panic)' bin/fault-go.log; exit $$rc
	$(PY) -m pytest -q -c pipelines/pyproject.toml pipelines/tests -k "not_published or http_failures or lost_response or stale or unreachable or missing_token or wrong_day or missing_interval or duplicate_position or unknown_series or tampered"

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

fixture-openmeteo: ## OWNER-RUN (network): save real Open-Meteo responses for golden tests
	$(ATLAS) fetch-fixture openmeteo --location entsoe:DE_LU --date 2026-11-18
	$(ATLAS) fetch-fixture openmeteo --location entsoe:DE_LU --date 2026-03-29
	$(ATLAS) fetch-fixture openmeteo --location entsoe:DE_LU --date 2025-10-26
	$(ATLAS) fetch-fixture openmeteo --location entsoe:ES --date 2026-11-18
	$(ATLAS) fetch-fixture openmeteo --location entsoe:SE3 --date 2026-11-18

demo-b1:       ## offline: real ENTSO-E files through parser, checks and the pipeline (no network, no token)
	$(PY) -m pytest -v -c pipelines/pyproject.toml pipelines/tests/golden/test_entsoe_golden.py \
		pipelines/tests/unit/test_entsoe.py -k "fixture or compressed or selection or pipeline or not_published or revision"

fixture-entsoe-a01: ## OWNER-RUN (network, ENTSOE_TOKEN): re-fetch ES fixtures with contract type A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2026-11-18 --label normal_A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2025-09-30 --label last_hourly_A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2025-10-01 --label first_15min_A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2025-10-26 --label fall_dst_A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2026-03-29 --label spring_dst_A01
	$(ATLAS) fetch-fixture entsoe --zone ES --date 2025-03-30 --label spring_hourly_A01

FROM ?= 2026-11-17
TO   ?= 2026-11-23
ingest-entsoe: ## OWNER-RUN (network, ENTSOE_TOKEN): ingest FROM..TO for all zones into the local lake
	$(ATLAS) run prices_entsoe_daily --partition $(FROM) --to $(TO)
	$(ATLAS) catalog
