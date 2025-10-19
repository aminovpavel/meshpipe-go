# Meshpipe Storage Reference

This document describes how Meshpipe uses SQLite for packet persistence, what
schema is produced by the built-in migrations, and which configuration flags
control database behaviour.

## File layout

Meshpipe writes to a single SQLite database. The file path is taken from
`database_file` in the YAML config or the `MESHPIPE_DATABASE_FILE` environment
variable (default `meshtastic_history.db` in the working directory). The
writer ensures the parent directory exists when the service starts.

## Connection settings

When the writer opens the database it applies the following PRAGMA settings:

| PRAGMA | Purpose |
| --- | --- |
| `journal_mode=WAL` | Enables write-ahead logging for concurrent reads. |
| `synchronous=NORMAL` | Balances durability with throughput. |
| `busy_timeout=30000` | Waits up to 30 s for locks before failing. |
| `foreign_keys=ON` | Keeps child tables consistent with `packet_history`. |
| `temp_store=MEMORY` | Uses memory-backed temporary tables. |
| `wal_autocheckpoint=1000` | Flushes WAL pages automatically. |
| `journal_size_limit=67108864` | Caps WAL size at 64 MiB. |
| `cache_size=-8192` | Allocates an ~8 MiB page cache. |

### Maintenance

- Every `maintenance_interval_minutes` (default 360) Meshpipe runs
  `PRAGMA wal_checkpoint(TRUNCATE)` and `PRAGMA optimize`.
- On shutdown the finalizer performs `wal_checkpoint(TRUNCATE)`, `VACUUM`, and
  `ANALYZE` to keep file size and statistics in check.

These actions can be tuned with `MESHPIPE_MAINTENANCE_INTERVAL_MINUTES`.

## Tables

### `packet_history`

Primary log of every MQTT frame Meshpipe receives.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment primary key. |
| `timestamp` | REAL | Epoch seconds with microsecond precision from the MQTT envelope. |
| `topic` | TEXT | MQTT topic that delivered the packet. |
| `from_node_id` / `to_node_id` | INTEGER | Mesh node numeric identifiers. |
| `portnum` / `portnum_name` | INTEGER/TEXT | Meshtastic port number and readable label. |
| `gateway_id` | TEXT | Mesh gateway identifier when available. |
| `channel_id` / `channel_name` | TEXT | Channel identifier and human-friendly name. |
| `mesh_packet_id` | INTEGER | Packet sequence number. |
| `rssi` / `snr` | INTEGER/REAL | Radio telemetry extracted from the envelope. |
| `hop_limit` / `hop_start` | INTEGER | Hop values for routing. |
| `payload_length` | INTEGER | Size of raw payload in bytes. |
| `raw_payload` | BLOB | Raw protobuf payload (if stored). Controlled by `MESHPIPE_CAPTURE_STORE_RAW`. |
| `processed_successfully` | INTEGER | 1 when decoding succeeded. |
| `via_mqtt`, `want_ack`, `priority`, `delayed`, `channel_index`, `rx_time` | INTEGER | Flags mirrored from ServiceEnvelope/MeshPacket. |
| `pki_encrypted`, `next_hop`, `relay_node`, `tx_after` | INTEGER | PKI and routing hints. |
| `message_type` | TEXT | Derived message classification (`TEXT`, `POSITION`, etc). |
| `raw_service_envelope` | BLOB | Full envelope bytes (stored when `capture_store_raw=true`). |
| `parsing_error` | TEXT | Error message when decode fails. |
| `transport` | INTEGER | Transport enum reported by Meshtastic. |
| `qos` / `retained` | INTEGER | MQTT QoS/retained flags (defaults to 0). |

### `node_info`

Cached node metadata kept in sync with incoming NODEINFO packets.

| Column | Type | Notes |
| --- | --- | --- |
| `node_id` | INTEGER PK | Unique identifier for the node. |
| `user_id`, `hex_id` | TEXT | Mesh node call signs. `hex_id` is indexed. |
| `long_name`, `short_name` | TEXT | Human-readable names. |
| `hw_model`, `role` | INTEGER | Meshtastic enumerations. |
| `hw_model_name`, `role_name` | TEXT | Human-friendly names derived from the enumerations. |
| `is_licensed`, `is_favorite`, `is_ignored`, `is_key_verified` | INTEGER | Boolean flags (0/1). |
| `mac_address` | TEXT | Optional MAC reported by firmware. |
| `primary_channel` | TEXT | Latest primary channel (indexed). |
| `snr` | REAL | Last reported SNR. |
| `last_heard` | INTEGER | Epoch seconds of last radio contact. |
| `via_mqtt` | INTEGER | 1 when node info arrived via MQTT. |
| `channel` | INTEGER | Channel index the info was observed on. |
| `hops_away` | INTEGER | Hop distance if provided. |
| `first_seen`, `last_updated` | REAL | Timestamps managed by the writer cache. |
| `region`, `region_name` | TEXT | Reported LoRa region code and human-friendly name. |
| `firmware_version` | TEXT | Firmware version reported via MapReport. |
| `modem_preset`, `modem_preset_name` | TEXT | LoRa modem preset code and human-friendly name. |

Indexes:

- `idx_node_hex_id` on `hex_id`
- `idx_node_primary_channel` on `primary_channel`

### `text_messages`

Linked table for decoded `TEXT_MESSAGE_APP` payloads.

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK to `packet_history.id` (ON DELETE CASCADE). |
| `text` | TEXT | Message body. |
| Remaining columns | INTEGER | `want_response`, `dest`, `source`, `request_id`, `reply_id`, `emoji`, `bitfield`, `compressed`. |

### `positions`

Stores decoded position payloads.

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK to `packet_history.id`. |
| `latitude`, `longitude` | REAL | Coordinates. |
| `altitude` | INTEGER | Altitude in meters. |
| `time`, `timestamp` | INTEGER | Timestamp fields carried by payload. |
| `raw_payload` | BLOB | Binary payload for downstream consumers. |

### `telemetry`

Raw telemetry protobuf frames keyed by packet.

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK to `packet_history.id`. |
| `raw_payload` | BLOB | Telemetry payload for later decoding. |

### `range_test_results`

Хранит фреймы RangeTest (текстовые волны).

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK к `packet_history.id`. |
| `text` | TEXT | Строка, переданная RangeTest. |
| `raw_payload` | BLOB | Исходный payload.

### `store_forward_events`

Подробности StoreForward (router/client stats, history и т.п.).

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK к `packet_history.id`. |
| `request_response` | TEXT | Тип RR (`ROUTER_STATS`, `CLIENT_PING` и т.д.). |
| `variant` | TEXT | Какой вариант полезной нагрузки (`stats`, `history`, `heartbeat`, `text`, `none`). |
| `messages_total`, `messages_saved`, `messages_max` | INTEGER | Показатели из Statistics. |
| `uptime_seconds`, `requests_total`, `requests_history` | INTEGER | Router uptime и счётчики запросов. |
| `heartbeat_flag` | INTEGER | 1, если флаг heartbeat в Statistics. |
| `return_max`, `return_window` | INTEGER | Параметры выдачи истории. |
| `history_messages`, `history_window`, `history_last_request` | INTEGER | Поля из History. |
| `heartbeat_period`, `heartbeat_secondary` | INTEGER | Поля Heartbeat. |
| `text_payload` | BLOB | Payload текстового ответа (если есть). |
| `raw_payload` | BLOB | Полное сообщение StoreAndForward (protobuf). |

### `paxcounter_samples`

Срезы модуля Paxcounter.

| Column | Type | Notes |
| --- | --- | --- |
| `packet_id` | INTEGER PK | FK к `packet_history.id`. |
| `wifi`, `ble` | INTEGER | Количество MAC в видимости. |
| `uptime_seconds` | INTEGER | Аптайм узла (сек). |
| `raw_payload` | BLOB | Сырый protobuf Paxcount. |

### `link_history`

Пер-пакетная информация о приёме для графов и аналитики.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment primary key. |
| `packet_id` | INTEGER UNIQUE | FK к `packet_history.id`. |
| `gateway_id` | TEXT | Gateway, принявший пакет. |
| `from_node_id`, `to_node_id` | INTEGER | Отправитель/получатель. |
| `hop_index`, `hop_limit` | INTEGER | Значения hop. |
| `rssi`, `snr` | INTEGER/REAL | Радиометрия на gateway. |
| `channel_id`, `channel_name` | TEXT | Канал и его имя. |
| `received_at` | REAL | Epoch seconds с микросекундной точностью. |

### `gateway_node_stats`

Скользящие агрегаты по каждой паре gateway/node.

| Column | Type | Notes |
| --- | --- | --- |
| `gateway_id`, `node_id` | TEXT/INTEGER | Композитный PK. |
| `first_seen`, `last_seen` | REAL | Первое/последнее наблюдение. |
| `packets_total` | INTEGER | Количество пакетов от узла. |
| `last_rssi`, `last_snr` | INTEGER/REAL | Последняя радиометрия. |

### `neighbor_history`

Хранилище NEIGHBORINFO (и, позднее, TRACEROUTE) для построения графов.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | INTEGER PK | Auto-increment primary key. |
| `packet_id` | INTEGER | FK к `packet_history.id`. |
| `origin_node_id` | INTEGER | Узел-источник NeighborInfo. |
| `neighbor_node_id` | INTEGER | Сообщённый сосед. |
| `snr`, `last_rx_time` | REAL/INTEGER | Метрики из payload. |
| `broadcast_interval` | INTEGER | Интервал вещания соседа. |
| `gateway_id` | TEXT | Gateway, доставивший отчёт. |
| `channel_id` | TEXT | Канал. |
| `received_at` | REAL | Epoch seconds (μs). |

### Views

- `gateway_stats` — агрегаты по gateway (сумма пакетов, число уникальных нод, first/last seen, средние `rssi/snr`) поверх `gateway_node_stats`.
- `link_aggregate` — сводка по (gateway, channel, пара узлов) с количеством пакетов и радиометрией.
- `gateway_diversity` — агрегаты для витрин сравнения gateway (уникальные источники/получатели, средние hop и радиометрия).
- `longest_links` — список пар узлов с максимальными наблюдаемыми hop по gateway.

## Node cache behaviour

Meshpipe keeps an in-memory cache (`internal/storage/node_cache`) that mirrors
`node_info`. On start the cache loads current rows, and every NODEINFO packet
updates both the cache and table. The cache ensures `first_seen` is stable and
`last_updated` advances monotonically.

## Related tooling

- `cmd/meshpipe-replay` can rebuild a new database from captured
  `packet_history.raw_service_envelope` entries.
- `cmd/meshpipe-diff` compares two SQLite files and reports differences in
  `packet_history` and `node_info` rows. This is useful after schema changes or
  decoder updates.
