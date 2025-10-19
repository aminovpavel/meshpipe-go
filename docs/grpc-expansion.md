# Meshpipe gRPC Expansion Plan

Context: Meshworks Malla is migrating off bespoke repositories (`LocationService`,
`ChatRepository`, `AnalyticsService`, etc.) to consume Meshpipe directly. The
current `meshpipe.v1.MeshpipeData` API exposes the raw packet stream and a few
aggregates but lacks the higher-level views Malla needs.

This document outlines the RPC, storage, and documentation work required to
close the gap.

## New RPCs

| RPC | Purpose | Notes |
| --- | --- | --- |
| `ListNodeLocations` | Serve the latest decoded coordinates per node. | Filters: `node_ids`, `start_time`, `gateway_id`. Response fields: latitude, longitude, altitude, sats, precision, age (hours), display name, primary channel, timestamp. |
| `GetChatWindow` | Group TEXT messages into chat windows, mirroring `ChatRepository`. | Filters: window start/size, `before`, `limit`, `audience`, `channel`, `sender`, `search`, `node_id`. Payload includes messages (id, timestamp, from/to, channel, text, gateways, processed flag), counters for 1/6/24h, and cursor metadata. |
| `GetGatewayOverview` | Top gateways snapshot analogous to `GatewayService.get_gateway_statistics`. | Return the top N (default 20) gateways with packet counts, unique sources, avg RSSI/SNR, last seen, diversity score, nodes seen, etc. |
| `GetAnalyticsSummary` | Single call that feeds AnalyticsService dashboards. | Sections: packet success stats, node activity buckets, signal quality, hourly histogram (24 buckets), top nodes (7 days), packet type distribution, gateway distribution. Filters: `gateway_id`, `from_node_id`, `hop_count`. |
| `GetNodeAnalytics` | Detailed per-node analytics for the node detail card. | Extends `GetNode` with 24h packet count, last packet timestamps, gateway packet distribution, per-role counts, most recent neighbors. |
| `ListTracerouteHops` | Return hop-by-hop traceroute observations. | Filters by `packet_id`, `origin_node_id`, `destination_node_id`, `gateway_id`, `request_id`, `direction`. Supports pagination. |
| `GetTracerouteGraph` | Graph view for traceroute data. | Returns nodes + edges with aggregate metrics (`avg_snr`, `packet_count`, hop depth). |
| `Healthz`, `GetVersion` | Let SPA detect Meshpipe readiness/version without an external backend. | `Healthz` returns status + optional message; `GetVersion` returns git SHA/semantic version. Recommended to expose as simple RPCs in `MeshpipeData`. |

## RPC Extensions

- **`ListPackets`**: add optional aggregation through `group_by_mesh_packet`. When enabled, return both the existing packet list and an aggregate block per `mesh_packet_id` (`reception_count`, `gateway_count`, min/max RSSI/SNR, hop range). Leave default behaviour unchanged.
- **`ListNodes`**: extend the existing `Node` message with 24h packet counts, last packet timestamp, most recent gateway stats, and role summaries. For heavier analytics expose `GetNodeAnalytics` (above).
- **`ListTraceroutes`**: consider optional inclusion of hop summaries when requested to reduce round-trips with the new traceroute RPCs.

## Storage & SQL

- Verify `positions` and `text_messages` tables already hold the necessary payloads. Create helper views/materialized queries if needed:
  - `latest_node_positions` view fetching the newest entry per node (with sat/precision).
  - `chat_messages` view joining `text_messages` with `packet_history` gateway reception/nodes.
- For grouped mesh packet stats, reuse `link_history` to compute per `mesh_packet_id` aggregates.
- Gateway overview can leverage `gateway_node_stats`, `gateway_stats`, and existing indexes.
- Analytics summary requires:
  - Success stats: derived from `packet_history.processed_successfully`.
  - Node activity buckets: count nodes grouped by packet volume thresholds.
  - Hourly histogram: bin timestamps into UTC hours for the last 24 hours (or window requested).
  - Top nodes: reuse `node_info` joined with packet counts limited to 7 days.
  - Packet/gateway distributions: group by `portnum_name` and `gateway_id`.
- Traceroute graph data should reuse `traceroute_hops`, `traceroute_longest_paths`, and `neighbor_history`. Consider new indexes on (`origin_node_id`, `destination_node_id`, `gateway_id`).
- Document any new views/indexes in `docs/storage.md` and add migrations to `internal/storage/sqlite.go`.

## Pagination & Limits

- Maintain cursor format based on `(timestamp, id)` for packet-derived lists.
- For new RPCs:
  - `ListNodeLocations`: support cursor by `(timestamp, node_id)`.
  - `GetChatWindow`: return explicit window metadata plus `next_cursor` for incremental fetch.
  - `ListTracerouteHops`: cursor by `(packet_id, hop_index, id)`.
- Expose `max_page_size` enforcement consistently; document effective limits in README.

## Testing

- Unit tests in `internal/api/grpcserver/service_test.go` for each new RPC and aggregation path.
- Extend the end-to-end integration test (`TestMeshpipeDataServiceEndToEnd`) to seed temp SQLite data covering positions, text messages, analytics, and traceroute hops.
- Add smoke snippets (where practical) to `tmp/meshpipe-run/grpc_smoke.go`.

## Documentation & Release Notes

- Update `README.md` (gRPC section) and `docs/architecture.md` to list the new RPCs and their payloads.
- Expand `docs/storage.md` with any new tables/views/indexes.
- Amend `docs/release-checklist.md` describing Malla UI migration steps and required stage validation.
- Mention configuration implications (e.g., health/version endpoints) in `docs/config.example.yaml` if new toggles are introduced.

## Envoy Sidecar Requirements

Meshworks wants to front Meshpipe with an Envoy proxy so UI clients only speak to the sidecar. Deliverables:

- **Image**: use `envoyproxy/envoy:distroless-v1.31-latest` (or pinned tag). Do not change Meshpipe’s Dockerfile.
- **Config template**: add `configs/envoy.meshpipe.yaml` exposing HTTP/2 ingress on port `8443` by default, proxying to Meshpipe (`localhost:7443`). Support gRPC, gRPC-Web, and JSON/HTTP. Forward the auth token from `MESHPIPE_GRPC_AUTH_TOKEN`.
- **Parameterisation**: honour `MESHPIPE_GRPC_PROXY_PORT`, `MESHPIPE_GRPC_UPSTREAM_HOST`, `MESHPIPE_GRPC_UPSTREAM_PORT`, `MESHPIPE_GRPC_WEB_ENABLED`. Provide an entrypoint or script (e.g., envsubst wrapper) that renders the config from env vars.
- **Compose example**: update `examples/docker-compose.yaml` to run two services—`meshpipe` (unchanged) and `meshpipe-proxy` (Envoy) with `depends_on`, config volume, port mapping `8443:8443`, and required environment variables.
- **Documentation**: explain in README/architecture that UIs should call Envoy, include a deployment diagram (MQTT → Meshpipe → Envoy → UI), mention health endpoints (`/ready`, `/stats`) and diagnostics.
- **Testing**: add an end-to-end smoke in scripts (e.g., shell task) that runs the compose stack and hits Envoy via `grpcurl -import-path proto -proto meshpipe/v1/data.proto -plaintext localhost:8443 meshpipe.v1.MeshpipeData.GetDashboardStats`.
- **Release checklist**: ensure the release doc includes Envoy version/port validation and notes that the proxy is optional (Meshpipe still listens on the direct gRPC port; proxy used when configured).

Implementation delivers:
- `configs/envoy.meshpipe.yaml.tmpl` – template rendered by `scripts/render-envoy-config.sh` into `configs/generated/`.
- `examples/docker-compose.yaml` – example stack with `meshpipe-proxy` (Envoy).
- `scripts/envoy-smoke.sh` – simple `grpcurl` probe against the proxy ingress.

## Next Steps

1. Update `proto/meshpipe/v1/data.proto` with messages/RPCs above and run `buf generate`.
2. Implement storage queries + server handlers with pagination/aggregation.
3. Add tests and documentation updates.
4. Coordinate with Malla team for UI rollout and verify acceptance criteria.
