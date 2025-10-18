# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [v0.1.0] - 2025-10-18
### Added
- Initial Meshpipe release: Meshtastic MQTT ingest pipeline with decode, decrypt and SQLite persistence.
- SQLite writer with node cache, WAL tuning, periodic maintenance and helper tables (`text_messages`, `positions`, `telemetry`).
- Observability stack: structured logging, Prometheus metrics and `/healthz` endpoint.
- CLI utilities: `meshpipe-smoke` (connectivity check), `meshpipe-replay` (rebuild database from raw envelopes) and `meshpipe-diff` (regression comparison).
- Documentation covering architecture, configuration, container images, storage schema and Docker Compose example.
- GitHub Actions workflow for fmt/vet/tests plus automatic Docker image publishing to GHCR.

[v0.1.0]: https://github.com/aminovpavel/meshpipe-go/releases/tag/v0.1.0
