# OWNERSHIP: assisted
#
# One-word commands for everything you run often. Each target is a thin wrapper
# so you can read exactly what it does.

COMPOSE      := docker compose -f deploy/compose/docker-compose.yml
DATABASE_URL ?= postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable

# golang-migrate runs from its Docker image, so it adds no Go dependency.
# host.docker.internal lets the container reach the Postgres port published on your machine.
MIGRATE := docker run --rm -v "$(CURDIR)/migrations:/migrations" migrate/migrate:v4.18.3 \
	-path=/migrations -database "postgres://atlas:atlas@host.docker.internal:5432/atlas?sslmode=disable"

.PHONY: up down reset migrate migrate-down psql build test test-race vet fmt

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

test-race:     ## tests with the race detector (needs cgo: Linux/CI, or a C compiler on Windows)
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .
