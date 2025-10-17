# MW Malla Capture — Architecture Draft

## Overview
The Go capture service ingests Meshtastic MQTT messages, applies optional decryption, decodes protobuf payloads, and writes normalized records into SQLite. The data model mirrors the legacy Python tool so the existing Malla web UI can continue reading from the same tables while we swap out the producer.

```
+-----------+       +-----------------+       +-----------------+
| MQTT Broker| ---> | Capture Pipeline | ---> | SQLite (WAL)     |
+-----------+       |  - decoder      |       | packet_history   |
                    |  - decryptor    |       | node_info        |
                    |  - enricher     |       +-----------------+
                    +-----------------+
```

## Key Components (planned)
- **cmd/malla-capture** – entrypoint, wiring config, logging, metrics, pipeline startup, graceful shutdown.
- **internal/config** – YAML + env loader matching the Python defaults. Ensures parity for compose/helm deployments.
- **internal/mqtt** – wrapper around Paho Go (or gmqtt) with resilient reconnect logic and backpressure-aware subscription handling.
- **internal/decode** – protobuf bindings generated from Meshtastic definitions, plus helpers for channel-key derivation and AES-CTR decryption.
- **internal/storage** – SQLite writer with connection pool tuning, PRAGMA management, schema migrations, and batch inserts under a write queue.
- **internal/cache** – in-memory node cache (mirrors `node_info`) with periodic refresh + real-time updates from NodeInfo packets.
- **internal/metrics** – Prometheus exporter, structured logging, health endpoints.
- **internal/replay** – utilities for offline packet replay / diffing against Python capture outputs (used for migration validation).

## Roadmap
1. **Config & logging scaffolding** – finalize config schema, structured logging, metrics skeleton.
2. **MQTT ingestion MVP** – subscribe, fan-out into pipeline, add graceful error handling + retry/backoff (unit tests for reconnect logic).
3. **Decode & decrypt** – port Meshtastic protobuf definitions, implement channel key derivation + AES-CTR decrypt, cover with fixtures.
4. **Storage layer** – schema migrations, WAL tuning, queue-based writer, parity tests vs Python output.
5. **Observability** – Prometheus metrics, `/healthz`, debug logging controls.
6. **Replay tester** – feed recorded MQTT traffic through both implementations, diff SQLite outputs.
7. **Deployment** – container image, GitHub Actions CI (lint/test/build), GitOps integration.

## Migration Notes
- Run Go capture alongside Python (dual write) with feature flag before cutover.
- Keep SQLite schema immutable until both producers align; migrations live with the Go service but must be replayable by Python if rollback occurs.
- Update AGENTS/Runbooks once rollout plan is locked.
