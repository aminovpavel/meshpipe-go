# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [v0.2.0] - 2025-10-19
### Added
- Read-only gRPC data API (`meshpipe.v1.MeshpipeData`) with cursor pagination, streaming endpoints, and optional bearer token authentication.
- Persistence for module payloads (range test, store-and-forward, Paxcounter, traceroute hops) with matching SQLite tables and analytics views.
- Traceroute ingestion pipeline together with longest-path summaries and link aggregates exposed through the gRPC service.

### Changed
- Hardened SQLite access for gRPC consumers (query-only connections, tuned busy timeout, additional indexes) to keep read load isolated from the ingest writer.
- Decoder and integration tests extended to cover new payloads and node metadata updates surfaced via MapReport packets.

## [v0.1.0] - 2025-10-18
### Added
- Initial Meshpipe release: Meshtastic MQTT ingest pipeline with decode, decrypt and SQLite persistence.
- SQLite writer with node cache, WAL tuning, periodic maintenance and helper tables (`text_messages`, `positions`, `telemetry`).
- Observability stack: structured logging, Prometheus metrics and `/healthz` endpoint.
- CLI utilities: `meshpipe-smoke` (connectivity check), `meshpipe-replay` (rebuild database from raw envelopes) and `meshpipe-diff` (regression comparison).
- Documentation covering architecture, configuration, container images, storage schema and Docker Compose example.
- GitHub Actions workflow for fmt/vet/tests plus automatic Docker image publishing to GHCR.

[v0.2.0]: https://github.com/aminovpavel/meshpipe-go/releases/tag/v0.2.0
[v0.1.0]: https://github.com/aminovpavel/meshpipe-go/releases/tag/v0.1.0
