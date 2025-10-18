# Meshpipe (Meshtastic MQTT capture in Go)

[![CI](https://github.com/aminovpavel/meshpipe-go/actions/workflows/ci.yml/badge.svg)](https://github.com/aminovpavel/meshpipe-go/actions/workflows/ci.yml)
[![Container](https://img.shields.io/badge/ghcr.io-aminovpavel%2Fmeshpipe--go-1f6feb?logo=github)](https://github.com/aminovpavel/meshpipe-go/pkgs/container/meshpipe-go)

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

## Container Images
- `ghcr.io/aminovpavel/meshpipe-go:latest` – rolling image built from `main`.
- `ghcr.io/aminovpavel/meshpipe-go:sha-<git-sha>` – immutable image produced for every commit on `main`.
- `ghcr.io/aminovpavel/meshpipe-go:<tag>` – tagged release images (on git tags).

GitHub Actions (`.github/workflows/ci.yml`) builds, tests and publishes these images automatically.

## Configuration

Meshpipe can read settings from a YAML file or environment variables. A sample config lives in [`docs/config.example.yaml`](docs/config.example.yaml).

| Variable | Description | Default |
| --- | --- | --- |
| `MESHPIPE_NAME` | Friendly service name used in logs. | `Meshpipe` |
| `MESHPIPE_DATABASE_FILE` | Path to the SQLite database file. | `meshtastic_history.db` (in working dir) |
| `MESHPIPE_MQTT_BROKER_ADDRESS` / `MESHPIPE_MQTT_PORT` | MQTT broker host/port. | `127.0.0.1` / `1883` |
| `MESHPIPE_MQTT_USERNAME` / `MESHPIPE_MQTT_PASSWORD` | Optional credentials for the MQTT broker. | unset |
| `MESHPIPE_MQTT_TOPIC_PREFIX` / `MESHPIPE_MQTT_TOPIC_SUFFIX` | Subscription topic pattern (`prefix` + `suffix`). | `msh` / `/+/+/+/#` |
| `MESHPIPE_DEFAULT_CHANNEL_KEY` | Base64 encoded default channel key used for decrypt attempts (keep empty to rely on per-channel keys). | `""` |
| `MESHPIPE_CAPTURE_STORE_RAW` | Whether to persist `raw_service_envelope`. | `true` |
| `MESHPIPE_OBSERVABILITY_ADDRESS` | Address for `/metrics` and `/healthz`. | `:2112` |
| `MESHPIPE_MAX_ENVELOPE_BYTES` | Guardrail for incoming MQTT payload size. | `262144` (256 KiB) |
| `MESHPIPE_MAINTENANCE_INTERVAL_MINUTES` | Interval for WAL checkpoint + optimize. | `360` |

Legacy `MALLA_*` variables are still recognized for backwards compatibility.

## Docker Compose Example

The [`examples/docker-compose.yaml`](examples/docker-compose.yaml) file shows a minimal deployment that shares the SQLite volume with the host and exposes observability endpoints:

```
docker compose -f examples/docker-compose.yaml up -d
```

```
curl -sf http://localhost:2112/healthz
```

## Getting Started
1. Create a configuration file (`config.yaml`) or export the relevant `MESHPIPE_*` environment variables.
2. Run `go build ./cmd/meshpipe` to produce a local binary (or use the published container image).
3. Start the service: `MESHPIPE_CONFIG_FILE=path/to/config.yaml ./meshpipe` (or mount the file when running the container).
4. Optional: run `cmd/meshpipe-smoke` to verify connectivity with your MQTT broker.

## Observability
- `/metrics` (Prometheus format) and `/healthz` are exposed on `MESHPIPE_OBSERVABILITY_ADDRESS` (default `:2112`).
- Key metrics include `meshpipe_capture_messages_received_total`, `meshpipe_capture_decode_errors_total`, `meshpipe_capture_storage_queue_depth`, `meshpipe_capture_messages_dropped_total`.
- Health endpoint returns HTTP 200 unless recent decode/store errors flipped the internal health flag.

## CLI Utilities
- `cmd/meshpipe-replay`: replays `packet_history.raw_service_envelope` from an existing SQLite DB through the Go pipeline, producing a regression database for comparison.
- `cmd/meshpipe-diff`: compares two capture SQLite databases and reports row-level differences in `packet_history` / `node_info` with sample fingerprints—useful for validating schema migrations or new decoder logic.
- `cmd/meshpipe-smoke`: lightweight MQTT client that attaches to your broker and prints incoming frames for quick sanity checks.

## Contributing
Use short-lived branches (e.g. `feat/go-config-loader`) and keep history tidy (1–3 commits per branch). No direct pushes to `main` without owner approval.

## CI Status
GitHub Actions (`.github/workflows/ci.yml`) runs gofmt, go test, staticcheck, module tidy checks, and builds/pushes container images to GitHub Container Registry.

## Development
- Run `go test ./...` before pushing.
- Run static analysis with `staticcheck ./...` (CI enforces it).
- Use `gofmt` on Go files (CI enforces).
- Observability server listens on `MESHPIPE_OBSERVABILITY_ADDRESS` (default `:2112`) and exposes `/metrics` (Prometheus) + `/healthz`.
- SQLite maintenance runs automatically (`PRAGMA wal_checkpoint(TRUNCATE)` + `PRAGMA optimize` every `MESHPIPE_MAINTENANCE_INTERVAL_MINUTES`, default 360). On shutdown the service runs `VACUUM`/`ANALYZE` to keep the file compact.
- For regression checks: dump an existing SQLite, run `meshpipe-replay --source input.db --output meshpipe.db`, then `meshpipe-diff --old input.db --new meshpipe.db`; investigate any differences before promoting a new build.
- Guardrails: MQTT payloads > `MESHPIPE_MAX_ENVELOPE_BYTES` (default 256 KiB) drop early with metrics `messages_dropped_total`.
