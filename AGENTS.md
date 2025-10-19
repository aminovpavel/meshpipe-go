# Meshpipe Expectations for External Agents

Meshpipe is the Meshtastic data capture service used across Meshworks. This
guide spells out what we expect from agents (human or automated) who contribute
changes to this repository.

## 1. Keep the pipeline stable
- The ingestion path is **MQTT → decode → SQLite → gRPC**. Do not change that
  sequence without opening an architecture discussion.
- `internal/storage/sqlite.go` owns schema migrations. Extend migrations
  atomically and keep them backward compatible. Every new table or view must be
  documented in `docs/storage.md`.
- Historical databases matter: provide a replay/backfill script or instructions
  whenever migrations require existing data to be rebuilt.

## 2. Run the checks before opening a PR
- `gofmt ./...`
- `go test ./...`
- `staticcheck ./...`
- `buf generate` (when proto definitions are touched)
- `docker build -t meshpipe-go:local .` for release-critical changes
- Stage smoke: run Meshpipe with `grpc_enabled: true` and execute\
  `GRPC_ADDR=<host:port> go run tmp/meshpipe-run/grpc_smoke.go`, capture its
  output, and verify `/metrics` exposes the `grpc_server_*` counters.

Attach the relevant logs or artefacts to the PR description so reviewers do not
have to re-run them from scratch.

## 3. Configuration contract
- Meshpipe reads both YAML and `MESHPIPE_*` environment variables. Keep new
  flags consistent across the configuration loader (`internal/config`),
  README tables, and `docs/config.example.yaml`.
- The default channel key is intentionally blank. If your feature relies on an
  encrypted channel, require the key through configuration instead of hardcoding
  values.

## 4. Observability and rollout
- Every new feature must export sufficient metrics or logs for stage and prod
  diagnostics. Use the existing prometheus registry in
  `internal/observability`.
- Before proposing a release, update `docs/release-checklist.md` if the process
  changes. Follow that checklist when cutting a tag.
- Meshpipe serves Meshworks Malla and other downstream clients. Document any
  behavioural change that requires client updates and mention it in the PR
  summary.

## 5. Docs and communication
- Update `CHANGELOG.md`, `README.md`, and `docs/storage.md` alongside code
  changes. Keep documentation in English and ASCII.
- Summaries in PRs must include: intent, risk, manual checks, and migration
  impact. Provide rollback guidance when behaviour changes.
- If you are unsure about scope or the change affects deployment strategies,
  open an issue first and wait for maintainer feedback.

Following these guidelines keeps the project deployable and makes releases
predictable for the Meshworks team. Thanks for contributing responsibly!
