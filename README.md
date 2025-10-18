# Meshpipe (Meshtastic MQTT capture in Go)

Meshpipe is a Go-based capture service for Meshtastic MQTT networks. It ingests Meshtastic traffic, decrypts/decodes protobuf payloads, and persists packet history plus node metadata to SQLite for downstream analytics and UI workloads. Meshworks Malla uses Meshpipe as its reference deployment, but the binary is suitable for any Meshtastic installation that needs a lightweight MQTT→SQLite pipeline.

## Goals
- High-throughput, low-latency ingest with predictable memory usage.
- First-class observability (structured logs, Prometheus metrics, health probes).
- Compatible schema for `packet_history` / `node_info` so existing dashboards or applications (including Meshworks Malla) continue to work.
- Configurable via YAML + `MESHPIPE_*` environment overrides (`MALLA_*` remains supported for legacy deployments).
- Safe rollout strategy (dual-run, diff checks, feature flag).

## Project Layout
```
cmd/meshpipe/           # main entrypoint wiring config, telemetry, and the capture pipeline
internal/config/        # configuration loading (YAML + env overrides)
internal/observability/ # structured logging, Prometheus metrics, /healthz server
internal/pipeline/      # MQTT -> decode -> storage orchestration
internal/storage/       # SQLite writer, schema migrations, queue management
docs/                   # design docs, operational playbooks, migration notes
```

Additional packages (MQTT client, protobuf decode, replay tooling) evolve alongside the implementation.

### Utilities

- `cmd/meshpipe-replay`: replays `packet_history.raw_service_envelope` data from an existing capture SQLite database through the Go pipeline, producing a fresh SQLite output for regression comparisons.
- `cmd/meshpipe-diff`: compares two capture SQLite databases and reports row-level differences in `packet_history` / `node_info` with sample fingerprints—useful for validating schema migrations or new decoder logic.

### Container

A multi-stage Dockerfile is provided and builds a CGO-enabled binary inside `debian:bookworm-slim`.

```
docker build -t meshpipe-go:dev .
docker run --rm \
  -v $PWD/.data:/data \
  -e MESHPIPE_CONFIG_FILE=/config/config.yaml \
  -e MESHPIPE_DATABASE_FILE=/data/meshtastic_history.db \
  -v $PWD/config.yaml:/config/config.yaml:ro \
  meshpipe-go:dev
```

The image defines a `/data` volume for the SQLite file and exposes a healthcheck that runs `PRAGMA integrity_check`.

## Getting Started
1. Create a configuration file (`config.yaml`) or export the relevant `MESHPIPE_*` environment variables.
2. Run `go build ./cmd/meshpipe` to produce a local binary (or use the provided Dockerfile).
3. Start the service: `MESHPIPE_CONFIG_FILE=path/to/config.yaml ./meshpipe` (or mount the file when running the container).
4. Optional: run `cmd/meshpipe-smoke` to verify connectivity with your MQTT broker.

## Contributing
Use short-lived branches (e.g. `feat/go-config-loader`) and keep history tidy (1–3 commits per branch). No direct pushes to `main` without owner approval.

## CI Status
GitHub Actions (`.github/workflows/ci.yml`) runs gofmt, go test, staticcheck, and module tidy checks.

## Development
- Run `go test ./...` before pushing.
- Run static analysis with `staticcheck ./...` (CI enforces it).
- Use `gofmt` on Go files (CI enforces).
- Observability server listens on `MESHPIPE_OBSERVABILITY_ADDRESS` (default `:2112`) and exposes `/metrics` (Prometheus) + `/healthz`.
- SQLite maintenance runs automatically (`PRAGMA wal_checkpoint(TRUNCATE)` + `PRAGMA optimize` every `MESHPIPE_MAINTENANCE_INTERVAL_MINUTES`, default 360). On shutdown the service runs `VACUUM`/`ANALYZE` to keep the file compact.
- For regression checks: dump an existing SQLite, run `meshpipe-replay --source input.db --output meshpipe.db`, then `meshpipe-diff --old input.db --new meshpipe.db`; investigate any differences before promoting a new build.
- Guardrails: MQTT payloads > `MESHPIPE_MAX_ENVELOPE_BYTES` (default 256 KiB) drop early with metrics `messages_dropped_total`.
