# Atlas

A Go + PostgreSQL control plane for multi-step CPU and LLM workflows. Atlas V1 is designed to survive worker crashes, coordinate shared provider quotas, apply cost-budget admission control, and record every attempt of every task.

## Goals

1. **Understanding.** Every file, decision and failure mode can be explained.
2. **Correctness.** Invariants hold under injected crashes, races and duplicate messages.
3. **Evidence.** Measured results, reproducible demos and stated limitations.

## Scope

**In V1:** durable submit and query; claim with leases and fencing; heartbeats; a lease reaper; retries with backoff and jitter; error and outcome classification; attempt records; a static DAG with bounded fan-out; best-effort cancellation; an LLM adapter for one hosted provider; coordinated rate limiting (concurrency + requests per minute per limit key); per-workflow budget admission (reserve → reconcile); outbox fixture tests; Prometheus metrics; a timeline page; a load test; a hosted-vs-vLLM inference comparison; Docker Compose; CI.

**Deferred to V2:** token-per-minute limiting, weighted multi-tenant fairness, multiple control-plane replicas, Kubernetes/Helm, Ray Serve, Terraform, a web UI, Kafka, Redis, dynamic workflows, human approval steps.

## Guarantees

Atlas makes guarantees about **its own state**. For external systems (LLM providers, APIs) it **records, bounds and surfaces** what happened; it does not control what a provider actually did.

| Property | Level | Enforced by | Tested by |
|---|---|---|---|
| An acknowledged submission is durable | Atlas guarantees | Commit before HTTP 2xx | Kill the control plane after the ack; the run is still present |
| A duplicate submission doesn't create a second run | Atlas guarantees | Unique `(tenant_id, idempotency_key)` | Two concurrent identical submits |
| A task is never leased to two live attempts at once | Atlas guarantees | `FOR UPDATE SKIP LOCKED` in a single transaction | Many concurrent claimers, repeated runs |
| A stale worker can't overwrite results | Atlas guarantees | Fencing token checked in a conditional `UPDATE` | Pause a worker past lease expiry, resume it |
| Every attempt is recorded, never overwritten | Atlas guarantees | Append-only `task_attempts` | Retry 3×, expect 3 attempt rows |
| Terminal states are final | Atlas guarantees | Transition function + `WHERE status NOT IN (terminal)` | Late completion on a canceled task is rejected |
| Task execution | At-least-once | Lease expiry → re-execution | Kill a worker mid-task; the task runs again |
| External call outcomes | Recorded and classified, not controlled | Outcome class on every attempt | Timeout after send → `unknown` |
| Provider rate limits | Coordinated, best effort | Admission at claim time + pause on 429 | 429 count, naive vs. coordinated |
| Workflow budget | Admission control, given the configured price table | Reserve worst-case estimate, reconcile on completion | Budget exhaustion defers tasks; `unknown` keeps the reservation |

**Never claimed:** exactly-once side effects, billing guarantees, prevention of all 429s, cancellation of in-flight external calls.

## Architecture

```
Python SDK / CLI ──HTTP──▶ Go control plane (single binary)
                             ├─ internal/api        submit, query, cancel, timeline, worker endpoints
                             ├─ internal/domain     pure state machine, error/outcome types (no I/O)
                             ├─ internal/store      all SQL and transactions
                             ├─ internal/scheduler  claim, reaper, retry timer, DAG readiness
                             ├─ internal/limits     rate-limit admission per limit key
                             ├─ internal/budget     reserve / reconcile
                             └─ internal/metrics    Prometheus
                                      │
                                      ▼
                                 PostgreSQL  (only source of truth)
                                      ▲
Python workers ──HTTP: claim / start / heartbeat / complete / fail──┘
   ├─ cpu worker   (parse, format)
   └─ llm worker   (hosted adapter; vLLM endpoint for the comparison)
```

- **One binary, internal packages.** Module boundaries without network boundaries.
- **Workers call the API, not the database.** One place enforces fencing, validation and auth; workers stay thin.
- **`internal/domain` is pure.** The state machine is a function, tested exhaustively without a database.
- **Generic core, LLM adapter at the edge.** Rate limits use *limit keys*, budgets use *cost units*, attempts carry *outcome classes*. Only the LLM adapter knows about tokens, models and prices, so the same mechanisms work for any external API.
- **PostgreSQL is the only source of truth.** No Kafka, Redis or consensus layer in V1.

## Task lifecycle

| From | To | Trigger | Condition / effect |
|---|---|---|---|
| (new) | READY | submit | No dependencies; same transaction as the run insert |
| (new) | BLOCKED | submit | Has dependencies |
| BLOCKED | READY | parent succeeded | Last pending dependency resolved in the parent's completion transaction |
| READY | LEASED | claim | Due, limit-key capacity and budget reservation OK; new attempt row; fencing token incremented |
| LEASED | RUNNING | worker start | Fencing token matches |
| LEASED/RUNNING | READY | lease expired | Attempt outcome `unknown`; attempts remain; retry after backoff |
| LEASED/RUNNING | FAILED_TERMINAL | lease expired | No attempts remain |
| RUNNING | SUCCEEDED | complete | Fencing token matches; children's readiness updated in the same transaction |
| RUNNING | READY | retryable failure | Attempts remain; backoff with jitter |
| RUNNING | FAILED_TERMINAL | terminal failure or attempts exhausted | Dependents canceled (fail-fast) |
| BLOCKED/READY | CANCELED | cancel | Immediate |
| LEASED/RUNNING | CANCELED | cancel | Worker observes the request on heartbeat; best effort |
| SUCCEEDED / FAILED_TERMINAL / CANCELED | — | any | Rejected: terminal states are final |

## Outcome classification

Every external call attempt gets one outcome class: `succeeded`, `failed_definite` or `unknown`. Adapters map provider signals into these generic classes.

| Signal | Outcome | Retry? |
|---|---|---|
| Validation / 4xx client error | failed_definite | No |
| Auth error (401/403) | failed_definite | No, surfaced loudly |
| 429 | failed_definite | Yes, after `Retry-After`; pauses the whole limit key |
| Connection failed before the request was sent | failed_definite | Yes |
| 5xx | Adapter decides | Yes |
| Timeout / reset after the request was sent | unknown | Idempotent operations only; budget reservation kept |
| Worker crash / lease expiry | unknown | Yes (at-least-once) |
| Malformed response | failed_definite | Limited |

## Data model

| Table | Purpose |
|---|---|
| `workflow_runs` | One row per submitted workflow, with its budget limit, reserved and spent amounts; unique per `(tenant_id, idempotency_key)` |
| `tasks` | Task status, priority, attempt limits, next-available time, pending dependencies, limit key, fencing token |
| `task_edges` | Static DAG edges (parent → child) |
| `task_attempts` | Append-only record of every attempt: worker, lease, fencing token, outcome, error class |
| `usage_records` | Generic usage units and cost per attempt, estimated or confirmed, with the price-table version |
| `budget_reservations` | Held, reconciled or kept-on-unknown reservations per attempt |
| `limit_keys` | Max concurrency, requests per minute and pause state per shared quota |
| `workers` | Registered workers, capabilities, last heartbeat |
| `events` | Transition log and outbox |

## Flagship demo

A **document pipeline**: parse and format documents on CPU workers, then run an LLM step through the hosted adapter, as a DAG with fan-out. It is run with random worker kills to show crash recovery, and again against a self-hosted vLLM endpoint to compare latency, throughput and cost per document.

All published measurements state the hardware, method and what they do *not* prove.

## Repository layout

```
cmd/atlas/                     control plane entry point
internal/{api,domain,store,scheduler,limits,budget,metrics}/
migrations/                    SQL migrations (golang-migrate)
workers/python/atlas_worker/   worker loop + adapters
sdk/python/atlas/              Python SDK / CLI
examples/document-pipeline/
tests/{integration,fault}/
bench/                         load-test configs, scripts, raw results
deploy/compose/                Docker Compose stack
config/pricing/                versioned price tables
```

## Tech stack

Go, PostgreSQL 17, Python (workers and SDK), Docker Compose, Prometheus, GitHub Actions, vLLM (inference comparison).

## Local development

Requires Go, Docker and GNU Make.

```bash
make up          # start Postgres via Docker Compose
make migrate     # apply migrations
make test        # unit + integration tests
make test-race   # race detector (needs cgo; runs in CI on Linux)
make psql        # SQL shell in the Postgres container
make down        # stop containers, keep data
make reset       # stop containers and delete local data
```
