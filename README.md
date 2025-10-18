# Meshtastic Capture Service (Go)

Go-based Meshtastic capture service for ingesting MQTT traffic, decrypting/decoding protobuf payloads, and persisting packet history plus node metadata to SQLite for downstream analytics and UI workloads. Meshworks Malla uses this project as its reference deployment, but the binary is suitable for any Meshtastic deployment that needs a lightweight capture pipeline.

## Goals
- High-throughput, low-latency ingest with predictable memory usage.
- First-class observability (structured logs, Prometheus metrics, health probes).
- Compatible schema for `packet_history` / `node_info` so existing dashboards or applications (including Meshworks Malla) continue to work.
- Configurable via YAML + `MALLA_*` environment overrides, matching the legacy Python tool.
- Safe rollout strategy (dual-run, diff checks, feature flag).

## Project Layout
```
cmd/malla-capture/      # main entrypoint wiring config, telemetry, and the capture pipeline
internal/config/        # configuration loading (YAML + env overrides)
internal/observability/ # structured logging, Prometheus metrics, /healthz server
internal/pipeline/      # MQTT -> decode -> storage orchestration
internal/storage/       # SQLite writer, schema migrations, queue management
docs/                   # design docs, operational playbooks, migration notes
```

Additional packages (MQTT client, protobuf decode, replay tooling) evolve alongside the implementation.

### Utilities

- `cmd/malla-replay`: replays `packet_history.raw_service_envelope` data from an existing capture SQLite database through the Go pipeline, producing a fresh SQLite output for parity comparisons.
- `cmd/malla-diff`: compares two capture SQLite databases (typically Python vs Go output) and reports row-level differences in `packet_history` / `node_info` with sample fingerprints.

### Container

A multi-stage Dockerfile is provided and builds a CGO-enabled binary inside `debian:bookworm-slim`.

```
docker build -t mw-malla-capture:dev .
docker run --rm \
  -v $PWD/.data:/data \
  -e MALLA_CONFIG_FILE=/config/config.yaml \
  -e MALLA_DATABASE_FILE=/data/meshtastic_history.db \
  -v $PWD/config.yaml:/config/config.yaml:ro \
  mw-malla-capture:dev
```

The image defines a `/data` volume for the SQLite file and exposes a healthcheck that runs `PRAGMA integrity_check`.

## Immediate Next Steps
1. Author the detailed architecture/design document under `docs/` (capture pipeline, storage layer, migration plan).
2. Scaffold internal packages (config loader, logging, metrics, storage interface, MQTT client wiring).
3. Set up CI (Go test, lint, static analysis) and container build workflow.
4. Build replay tooling to validate parity against the legacy Python capture.

## Contributing
Use short-lived branches (e.g. `feat/go-config-loader`) and keep history tidy (1–3 commits per branch). No direct pushes to `main` without owner approval.

## CI Status
GitHub Actions (`.github/workflows/ci.yml`) runs gofmt, go test, staticcheck, and module tidy checks.

## Development
- Run `go test ./...` before pushing.
- Run static analysis with `staticcheck ./...` (CI enforces it).
- Use `gofmt` on Go files (CI enforces).
- Observability server listens on `MALLA_OBSERVABILITY_ADDRESS` (default `:2112`) and exposes `/metrics` (Prometheus) + `/healthz`.
- SQLite maintenance runs automatically (`PRAGMA wal_checkpoint(TRUNCATE)` + `PRAGMA optimize` every `MALLA_MAINTENANCE_INTERVAL_MINUTES`, default 360). On shutdown the service runs `VACUUM`/`ANALYZE` to keep the file compact.
- For migration parity: dump the legacy SQLite, run `malla-replay --source legacy.db --output go.db`, затем `malla-diff --old legacy.db --new go.db` — расхождения должны быть нулевыми перед переключением.
- Guardrails: MQTT payloads > `MALLA_MAX_ENVELOPE_BYTES` (default 256 KiB) drop early with metrics `messages_dropped_total`.
