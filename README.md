# MW Malla Capture

Go-based capture service for Meshworks Malla. The service ingests Meshtastic MQTT traffic, decrypts/decodes protobuf payloads, and persists packet history plus node metadata to SQLite for downstream analytics and UI workloads.

## Goals
- High-throughput, low-latency ingest with predictable memory usage.
- First-class observability (structured logs, Prometheus metrics, health probes).
- Schema parity with existing `packet_history` / `node_info` tables so the current web/UI stack keeps working.
- Configurable via YAML + `MALLA_*` environment overrides, matching the legacy Python tool.
- Safe rollout strategy (dual-run, diff checks, feature flag).

## Project Layout
```
cmd/malla-capture/   # main entrypoint wiring config, telemetry, and the capture pipeline
internal/config/     # configuration loading (YAML + env overrides)
docs/                # design docs, operational playbooks, migration notes
```

We will flesh out additional internal packages for MQTT handling, protobuf decoding, decryption, storage, caching, and observability as the implementation proceeds.

## Immediate Next Steps
1. Author the detailed architecture/design document under `docs/` (capture pipeline, storage layer, migration plan).
2. Scaffold internal packages (config loader, logging, metrics, storage interface, MQTT client wiring).
3. Set up CI (Go test, lint, static analysis) and container build workflow.
4. Build replay tooling to validate parity against the legacy Python capture.

## Contributing
This repository is internal-only for now; coordinate via AGENTS directives. Use short-lived branches (e.g. `feat/go-config-loader`) and keep history tidy (1–3 commits per branch). No direct pushes to `main` without owner approval.
