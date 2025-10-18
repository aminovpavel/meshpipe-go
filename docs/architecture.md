# Meshtastic Capture Service — Architecture Draft

## Overview
The Go capture service ingests Meshtastic MQTT messages, applies optional decryption, decodes protobuf payloads, and writes normalized records into SQLite. Meshworks Malla deploys this build today, but the architecture is intentionally generic so any Meshtastic installation can reuse the same pipeline and schema.

```
+-----------+       +-----------------+       +-----------------+
| MQTT Broker| ---> | Capture Pipeline | ---> | SQLite (WAL)     |
+-----------+       |  - decoder      |       | packet_history   |
                    |  - decryptor    |       | node_info        |
                    |  - enricher     |       +-----------------+
                    +-----------------+
```

## Key Components (current)
- **cmd/malla-capture** – entrypoint, wiring config, logging, metrics, pipeline startup, graceful shutdown.
- **internal/config** – YAML + env loader matching the Python defaults. Ensures parity for compose/helm deployments.
- **internal/mqtt** – wrapper around paho.golang (v2) with resilient reconnect logic and backpressure-aware subscription handling.
- **internal/decode** – protobuf bindings generated from Meshtastic definitions, plus helpers for channel-key derivation, AES-CTR decryption, and payload enrichment.
- **internal/storage** – SQLite writer with connection tuning, PRAGMA management, schema migrations, and batch inserts backed by a bounded queue.
- **internal/observability** – structured logging helpers, Prometheus metrics registry, and `/metrics` + `/healthz` HTTP server.
- **internal/storage/node_cache** – in-memory node cache (mirrors `node_info`) с первичной/последней отметкой времени и merge-логикой, работает внутри SQLite writer.
- **internal/metrics** – Prometheus exporter, structured logging, health endpoints.
- **internal/replay** – utilities for offline packet replay / diffing against Python capture outputs (used for migration validation).

## Roadmap
1. **Config & logging scaffolding** – finalize config schema, structured logging, metrics skeleton.
2. **MQTT ingestion MVP** – subscribe, fan-out into pipeline, add graceful error handling + retry/backoff (unit tests for reconnect logic).
3. **Decode & decrypt** – port Meshtastic protobuf definitions, implement channel key derivation + AES-CTR decrypt, cover with fixtures.
4. **Storage layer** – schema migrations, WAL tuning, queue-based writer, parity tests vs Python output.
5. **Observability** – Prometheus metrics, `/healthz`, debug logging controls. **(Implemented core server + counters)**
6. **Replay tester** – feed recorded MQTT traffic through both implementations, diff SQLite outputs.
7. **Deployment** – container image, GitHub Actions CI (lint/test/build), GitOps integration.

## Migration Notes
- Run Go capture alongside Python (dual write) with feature flag before cutover.
- Keep SQLite schema immutable until both producers align; migrations live with the Go service but must be replayable by Python if rollback occurs.
- Update AGENTS/Runbooks once rollout plan is locked.

## Detailed Architecture

### 1. Config & Bootstrap
- `internal/config` loads YAML + environment overrides and returns `config.App`.
- `cmd/malla-capture/main.go` initialises logging/metrics/pipeline and handles graceful shutdown.
- Planned helper packages: `internal/logging`, `internal/metrics`, `internal/app` for dependency wiring.

**Startup sequence:**
1. `config.New` loads configuration.
2. Initialise structured logger and register Prometheus exporter.
3. Construct SQLite storage and preload caches.
4. Start MQTT pipeline (subscribe + launch worker goroutines).
5. Start HTTP endpoints (`/metrics`, `/healthz`).
6. Wait for signals; on shutdown close MQTT client, drain write queue, flush caches.

### 2. MQTT Ingest
- `internal/mqtt`: wrapper around client (evaluating paho.golang vs gmqtt).
- Reconnection: exponential backoff with jitter and retry limits.
- Subscription: multiple topics, configurable QoS.
- Messages flow into buffered channel feeding decoder worker pool.

```
MQTT -> ingress chan -> decode workers -> decrypt -> enrich -> storage queue -> SQLite
```

### 3. Decode & Decrypt
- `internal/meshtastic`: generated protobuf bindings (buf/protoc).
- `internal/decode`: parse ServiceEnvelope, resolve port, map to internal model.
- `internal/crypto`: key derivation + AES-CTR decrypt with fixture tests.
- Honour `CAPTURE_STORE_RAW` flag; derive `message_type` from topic.

### 4. Storage & Caching
- `internal/storage`: SQLite adapter (`modernc.org/sqlite`), migrations, PRAGMA configuration.
- `internal/storage/migrations`: SQL snippets embedded in Go migrate routine.
- `internal/storage/node_cache`: in-memory snapshot of `node_info` (first_seen/last_updated merge, channel metadata) to avoid per-packet SELECTs.
- Write queue: bounded channel + worker with prepared statements/transactions.
- Periodic maintenance: writer runs lightweight `wal_checkpoint(TRUNCATE)` + `PRAGMA optimize` on a schedule (configurable), and performs full `VACUUM`/`ANALYZE` during shutdown to keep the file healthy.
- Dockerfile builds a CGO-enabled binary via multi-stage (golang:1.24 → debian-slim) and ships a healthcheck that runs `PRAGMA integrity_check` against the configured SQLite path.
- Guardrails: MQTT payloads larger than `max_envelope_bytes` (default 256 KiB) are dropped before decode (`messages_dropped_total` metric) to avoid runaway memory/disk writes.
- `internal/replay`: helpers to stream existing packet_history rows (via raw ServiceEnvelope blobs) back through the Go pipeline, used by the replay CLI for parity checks.
- `internal/diff`: SQLite diff utilities for comparing packet_history/node_info footprints across databases (используются CLI).

### 5. Observability
- `internal/observability`: structured logging (`slog`), Prometheus metrics (ingest throughput, errors, queue depth, node upserts), and health endpoint wiring.
- `/metrics` served via `promhttp` on `MALLA_OBSERVABILITY_ADDRESS` (default `:2112`); `/healthz` returns 200 unless recent pipeline/storage errors mark the collector unhealthy.
- Structured logs (text or JSON) honour `MALLA_LOG_LEVEL`; pipeline/storage components accept injected loggers to ensure consistent context.

### 6. Testing Strategy
- Unit: config, crypto, decoder, storage (in-memory), node cache updates.
- Integration: embedded MQTT broker (mochi-co/mqtt), run pipeline, inspect SQLite output. `cmd/malla-replay` + `cmd/malla-diff` составляют baseline-поток «Python → Go → сравнить».
- Replay tool: CLI `cmd/malla-replay` to feed recorded frames.
- Benchmarks: `testing.B` + replay datasets.

### 7. Deployment & Rollout
- Dockerfile (multi-stage) built in CI.
- GitHub Actions: gofmt, staticcheck, unit/integration tests, Docker build.
- GitOps: add service to compose, dual-run via feature flag (`capture_impl`).
- Cutover: shadow mode, monitor metrics, flip env var, document rollback.

### Open Questions
- Final MQTT client choice.
- HTTP stack for metrics (`net/http` vs `chi`).
- SQLite driver trade-offs (modernc vs CGO) and performance tuning.
- Need real traffic dumps for replay.
- MQTT TLS / mTLS handling and certificate management.
