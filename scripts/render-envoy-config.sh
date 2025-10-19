#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="${ROOT_DIR}/configs/envoy.meshpipe.yaml.tmpl"
OUTPUT="${ROOT_DIR}/configs/generated/envoy.meshpipe.yaml"

usage() {
  cat <<EOF
Usage: $0 [--template path] [--output path]

Renders the Envoy config template using environment variables.

Environment variables:
  MESHPIPE_GRPC_PROXY_PORT          Listener port exposed by Envoy (default 8443)
  MESHPIPE_GRPC_UPSTREAM_HOST       Upstream Meshpipe gRPC host (default 127.0.0.1)
  MESHPIPE_GRPC_UPSTREAM_PORT       Upstream Meshpipe gRPC port (default 7443)
  MESHPIPE_GRPC_PROXY_ADMIN_PORT    Envoy admin port for /ready and /stats (default 9901)
  MESHPIPE_GRPC_WEB_ENABLED         Enable gRPC-Web filter (default true)
  MESHPIPE_GRPC_AUTH_TOKEN          Optional bearer token forwarded to Meshpipe

Example:
  MESHPIPE_GRPC_UPSTREAM_HOST=meshpipe \\
  MESHPIPE_GRPC_AUTH_TOKEN=\"secret\" \\
  $0 --output configs/generated/envoy.meshpipe.stage.yaml
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --template)
      TEMPLATE="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v envsubst >/dev/null 2>&1; then
  echo "envsubst is required (install gettext)" >&2
  exit 1
fi

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"

bool_true() {
  local value="${1:-}"
  value="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

GRPC_PROXY_PORT="${MESHPIPE_GRPC_PROXY_PORT:-8443}"
GRPC_UPSTREAM_HOST="${MESHPIPE_GRPC_UPSTREAM_HOST:-127.0.0.1}"
GRPC_UPSTREAM_PORT="${MESHPIPE_GRPC_UPSTREAM_PORT:-7443}"
GRPC_PROXY_ADMIN_PORT="${MESHPIPE_GRPC_PROXY_ADMIN_PORT:-9901}"
GRPC_WEB="${MESHPIPE_GRPC_WEB_ENABLED:-true}"
AUTH_TOKEN="${MESHPIPE_GRPC_AUTH_TOKEN:-}"

if bool_true "$GRPC_WEB"; then
  GRPC_WEB_HTTP_FILTER=$'                  - name: envoy.filters.http.grpc_web
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.grpc_web.v3.GrpcWeb'
else
  GRPC_WEB_HTTP_FILTER=''
fi

if [[ -n "$AUTH_TOKEN" ]]; then
  ESCAPED_TOKEN=${AUTH_TOKEN//\"/\\\"}
  AUTH_HEADERS=$'                  request_headers_to_add:\n                    - header:\n                        key: authorization\n                        value: "Bearer '"$ESCAPED_TOKEN"$'"\n                      append_action: OVERWRITE_IF_EXISTS_OR_APPEND'
else
  AUTH_HEADERS=''
fi

export GRPC_PROXY_PORT
export GRPC_UPSTREAM_HOST
export GRPC_UPSTREAM_PORT
export GRPC_PROXY_ADMIN_PORT
export GRPC_WEB_HTTP_FILTER
export AUTH_HEADERS

envsubst '${GRPC_PROXY_PORT} ${GRPC_UPSTREAM_HOST} ${GRPC_UPSTREAM_PORT} ${GRPC_PROXY_ADMIN_PORT} ${GRPC_WEB_HTTP_FILTER} ${AUTH_HEADERS}' \
  < "$TEMPLATE" > "$OUTPUT"

echo "Rendered Envoy config -> $OUTPUT"
