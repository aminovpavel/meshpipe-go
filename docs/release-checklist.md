# Release Checklist

Use this checklist when cutting a Meshpipe release.

1. **Update documentation**
   - Ensure `README.md`, `docs/architecture.md`, `docs/storage.md`, and examples reflect latest behaviour.
   - Add an entry to `CHANGELOG.md` with the new version number and date.

2. **Run validation locally**
   - `gofmt -w ./`
   - `go test ./...`
   - `$(go env GOPATH)/bin/staticcheck ./...`
   - Optional: run `cmd/meshpipe-smoke` against a broker for a live sanity check.
   - Optional: run regression comparison: `meshpipe-replay --source <baseline.db> --output meshpipe.db` followed by `meshpipe-diff --old <baseline.db> --new meshpipe.db`.

3. **Verify container build**
   - `docker build -t meshpipe-go:local .`
   - `docker run --rm meshpipe-go:local --help` (or start with sample config).

4. **Tag the release**
   - Choose a version (`vX.Y.Z`).
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`

5. **Create GitHub Release**
   - Include highlights, configuration notes and links to docs.
   - CI will publish Docker images: `ghcr.io/aminovpavel/meshpipe-go:latest`, `:sha-<commit>`, and `:<tag>`.

6. **Post-release**
   - Update deployment repos/GitOps manifests to use the new tag.
   - Monitor metrics (`/metrics`) and logs after rollout.
