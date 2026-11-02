// OWNERSHIP: assisted
//
// Entry point for the Atlas control plane binary.
// Why it exists: Atlas is a modular monolith, so there is exactly one binary;
// every internal package (api, store, scheduler, ...) is wired together here.
// Who calls it: `go run ./cmd/atlas`, and later the Docker Compose service.
//
// Slice 0: it only prints a startup line. Slice 1 adds config, the Postgres
// pool and the HTTP server.
package main

import "log"

func main() {
	log.Println("atlas control plane: nothing to serve yet (Slice 0)")
}
