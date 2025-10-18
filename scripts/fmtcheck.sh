#!/usr/bin/env bash
set -euo pipefail
fmt_out=$(gofmt -l ./)
if [[ -n "$fmt_out" ]]; then
  echo "Files need gofmt:" >&2
  echo "$fmt_out" >&2
  exit 1
fi
