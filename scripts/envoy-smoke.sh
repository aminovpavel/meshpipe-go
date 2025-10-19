#!/usr/bin/env bash
set -euo pipefail

ADDRESS="${GRPC_PROXY_ADDRESS:-localhost:8443}"
METHOD="${1:-meshpipe.v1.MeshpipeData.GetDashboardStats}"
PAYLOAD="${2:-}"
if [[ -z "${PAYLOAD}" ]]; then
  PAYLOAD='{}'
fi
PROTO_IMPORT_PATH="${GRPCURL_IMPORT_PATH:-proto}"
PROTO_FILE="${GRPCURL_PROTO_FILE:-meshpipe/v1/data.proto}"

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "grpcurl not found in PATH. Install it or set GO111MODULE=on go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest" >&2
  exit 1
fi

echo "Probing ${METHOD} via ${ADDRESS}"
ARGS=(-plaintext -import-path "${PROTO_IMPORT_PATH}" -proto "${PROTO_FILE}" -d "${PAYLOAD}")
if [[ -n "${MESHPIPE_GRPC_AUTH_TOKEN:-}" ]]; then
  ARGS+=(-H "authorization: Bearer ${MESHPIPE_GRPC_AUTH_TOKEN}")
fi
ARGS+=("${ADDRESS}" "${METHOD}")

grpcurl "${ARGS[@]}"
