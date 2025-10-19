# Release Checklist

Use this checklist when cutting a Meshpipe release.

1. **Update documentation**
   - Ensure `README.md`, `docs/architecture.md`, `docs/storage.md`, `docs/config.example.yaml`, and examples describe the current behaviour (gRPC API, traceroute analytics, module tables).
   - Update `CHANGELOG.md` with the new version number, highlights, and release date.
   - Capture required Malla UI changes (new analytics/chat/location RPCs, Envoy proxy expectations) in release notes.

2. **Regenerate APIs and manifests**
   - `buf generate` (keeps `internal/api/grpc/gen` in sync).
   - `go mod tidy` (no diff expected; fails the step if dependencies drifted).

3. **Run validation locally**
   - `gofmt -w ./`
   - `go test ./...`
   - `staticcheck ./...`
   - Optional: run `cmd/meshpipe-smoke` against a broker for a live sanity check.
   - Optional: regression comparison: `meshpipe-replay --source <baseline.db> --output meshpipe.db` followed by `meshpipe-diff --old <baseline.db> --new meshpipe.db`.

4. **Smoke-test the gRPC API**
   - Run Meshpipe locally or against stage with `grpc_enabled: true`.
   - Execute `GRPC_ADDR=<host:port> go run tmp/meshpipe-run/grpc_smoke.go` and capture the output as an artefact.
   - Verify Prometheus `/metrics` exposes `grpc_server_*` metrics and that pagination/streaming behave as expected.
   - If Envoy is used in the rollout, render the config (`scripts/render-envoy-config.sh`), run the proxy (`examples/docker-compose.yaml`), and probe it via `GRPC_PROXY_ADDRESS=<host:port> ./scripts/envoy-smoke.sh`. Confirm the admin interface (`/ready`, `/stats`) is reachable and the expected Envoy image tag matches the release notes.

5. **Verify container build**
   - `docker build -t meshpipe-go:local .`
   - `docker run --rm meshpipe-go:local --help` (or start with sample config).

6. **Tag the release**
   - Choose a version (`vX.Y.Z`).
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`

7. **Create GitHub Release**
   - Include highlights, configuration notes and links to docs.
   - CI will publish Docker images: `ghcr.io/aminovpavel/meshpipe-go:latest`, `:sha-<commit>`, and `:<tag>`.

8. **Post-release**
   - Update deployment repos/GitOps manifests to use the new tag (enable gRPC flag when ready).
   - Replay historical SQLite dumps or run the traceroute/module backfill if required.
   - Monitor metrics (`/metrics`) and logs after rollout.
   - Confirm downstream clients (Malla UI, Envoy sidecar) are pointing at the new RPCs and health/version probes.
