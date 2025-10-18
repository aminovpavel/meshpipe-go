##########
# Builder stage
##########
FROM golang:1.24-bullseye AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# modernc.org/sqlite requires CGO.
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /out/malla-capture ./cmd/malla-capture

##########
# Runtime stage
##########
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    sqlite3 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/malla-capture /usr/local/bin/malla-capture

# store database and config outside of image by default
VOLUME ["/data"]
ENV MALLA_DATABASE_FILE=/data/meshtastic_history.db

HEALTHCHECK --interval=1m --timeout=5s --start-period=15s \
  CMD sqlite3 "$MALLA_DATABASE_FILE" 'PRAGMA integrity_check;' >/dev/null 2>&1 || exit 1

ENTRYPOINT ["malla-capture"]
