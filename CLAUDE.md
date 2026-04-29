# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Velociraptor (v0.76.1) — an endpoint visibility and collection tool by Rapid7. Go-based client/server architecture with a React web UI. Licensed under AGPLv3.

Module path: `www.velocidex.com/golang/velociraptor`

## Build Environment

A Docker-based build environment lives in `Docker/build/` for building Linux amd64
binaries without installing Go locally. The repo root is bind-mounted into the container
so host edits are immediately buildable and `output/` lands back on the host.

```bash
cd Docker/build
docker compose build             # build the image once (re-run if go.mod changes)
docker compose run --rm build    # interactive shell at /src with full toolchain
docker compose run --rm build make linux   # one-shot build
```

Named volumes (`go-mod-cache`, `go-build-cache`) keep incremental builds fast across runs.

## Build Commands

```bash
make                    # Build dev binary (runs: go run make.go -v autoDev)
make darwin             # macOS binary (no yara)
make darwin_m1          # macOS Apple Silicon
make linux              # Linux binary
make windows            # Windows dev binary
make release            # Release binaries for all platforms
make assets             # Build GUI assets (React frontend)
make clean              # Clean build artifacts
```

Build output goes to `output/` directory. Cross-compilation uses `go run make.go` with mage-based targets defined in `magefile.go`.

## Testing

```bash
make test               # All tests with race detector: go test -race -v --tags server_vql ./...
make test_light         # All tests without race detector
go test -v --tags server_vql ./path/to/package/...   # Single package
go test -v --tags server_vql -run TestName ./path/to/package/...  # Single test
```

**Build tag `server_vql` is required** for all test runs — it enables server-side VQL functionality.

### Golden Tests

```bash
make golden                     # Run golden/reference tests
GOLDEN=filter make golden       # Run specific golden test by filter
```

Golden test data lives in `artifacts/testdata/`. Golden tests require a built binary at `output/velociraptor`.

## Linting

```bash
make lint               # golangci-lint run
make check              # staticcheck ./...
```

Lint build tags: `server_vql`, `extras`, `release`, `yara`, `codeanalysis` (see `.golangci.yml`).

## Architecture

### Client/Server Model
- **Server**: Manages clients, serves the web UI, runs server-side VQL queries, stores collected artifacts
- **Client**: Runs on endpoints, executes collection artifacts, communicates back to server via HTTP
- **Entry point**: `bin/main.go` — uses kingpin for CLI command dispatch with a `command_handlers` pattern

### Key Subsystems

- **VQL Engine** (`vql/`): Custom query language for endpoint data collection. Platform-specific implementations in `vql/darwin/`, `vql/linux/`, `vql/windows/`. Functions in `vql/functions/`, parsers in `vql/parsers/`.
- **Artifacts** (`artifacts/definitions/`): YAML-defined collection tasks organized by platform (Windows/, Linux/, MacOS/, Admin/, etc.). This is the primary extension mechanism.
- **Services** (`services/`): ~64 service packages (acl_manager, client_info, client_monitoring, audit_manager, etc.) implementing server-side functionality. Services register via an init pattern.
- **API** (`api/`): gRPC/REST API layer with protocol buffer definitions in `api/proto/`.
- **Datastore** (`datastore/`): Pluggable persistence layer with multiple backend implementations.
- **File Store** (`file_store/`): Artifact result and file storage management.
- **Flows** (`flows/`): Orchestration of artifact collection across clients.
- **Config** (`config/`): Configuration loading with a builder/loader pattern supporting file, env, embedded, and API configs.
- **Crypto** (`crypto/`): Client-server communication encryption and authentication.
- **GUI** (`gui/velociraptor/`): React frontend (separate npm project). Build with `cd gui/velociraptor && npm install && npm run build`.

### Code Generation

```bash
make generate           # Regenerates VQL Windows bindings and API mocks
```

Runs `go generate` on `vql/windows/` and `api/mock/`.

### Platform-Specific Code

Platform code uses build tags and is organized into platform-specific directories (e.g., `vql/windows/`, `vql/darwin/`, `vql/linux/`). Many packages have `_windows.go`, `_linux.go`, `_darwin.go` file variants.

### Velocidex Dependencies

The project heavily depends on `github.com/Velocidex/*` packages — these are companion libraries maintained by the same team (json, yaml, zip, yara bindings, ETW, etc.).
