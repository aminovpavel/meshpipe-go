package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	"github.com/aminovpavel/mw-malla-capture/internal/observability"
)

// SQLiteConfig holds configuration values for the SQLite writer.
type SQLiteConfig struct {
	Path                string
	QueueSize           int
	MaintenanceInterval time.Duration
}

// Writer is the minimal interface required by the pipeline to persist packets.
type Writer interface {
	Store(ctx context.Context, pkt decode.Packet) error
}

// StartStopper represents writers that need explicit lifecycle management.
type StartStopper interface {
	Writer
	Start(ctx context.Context) error
	Stop() error
}

// SQLiteWriter persists packets into a SQLite database.
type SQLiteWriter struct {
	cfg   SQLiteConfig
	db    *sql.DB
	queue chan decode.Packet
	wg    sync.WaitGroup
	once  sync.Once

	logger  *slog.Logger
	metrics *observability.Metrics
	cache   *nodeCache

	maintenanceInterval time.Duration
	maintenanceStop     chan struct{}
}

// NewSQLiteWriter constructs a writer with the provided configuration.
func NewSQLiteWriter(cfg SQLiteConfig, opts ...Option) (*SQLiteWriter, error) {
	if cfg.Path == "" {
		return nil, errors.New("storage: database path must be provided")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 512
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 6 * time.Hour
	}

	w := &SQLiteWriter{
		cfg:                 cfg,
		queue:               make(chan decode.Packet, cfg.QueueSize),
		logger:              slog.Default(),
		cache:               newNodeCache(),
		maintenanceInterval: cfg.MaintenanceInterval,
		maintenanceStop:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}
	if w.logger == nil {
		w.logger = slog.Default()
	}

	return w, nil
}

// Option configures the writer.
type Option func(*SQLiteWriter)

// WithLogger injects a structured logger into the writer.
func WithLogger(logger *slog.Logger) Option {
	return func(w *SQLiteWriter) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// WithMetrics attaches metrics instrumentation.
func WithMetrics(metrics *observability.Metrics) Option {
	return func(w *SQLiteWriter) {
		if metrics != nil {
			w.metrics = metrics
		}
	}
}

// Start opens the database, runs migrations, and begins processing the queue.
func (w *SQLiteWriter) Start(ctx context.Context) error {
	// Ensure directory exists.
	abs, err := filepath.Abs(w.cfg.Path)
	if err != nil {
		return fmt.Errorf("storage: resolve path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("storage: ensure directory: %w", err)
	}

	db, err := sql.Open("sqlite", abs)
	if err != nil {
		return fmt.Errorf("storage: open sqlite: %w", err)
	}

	if err := configureConnection(db); err != nil {
		db.Close()
		return err
	}

	if err := migrate(db); err != nil {
		db.Close()
		return err
	}

	if w.cache == nil {
		w.cache = newNodeCache()
	}
	if err := w.cache.load(db); err != nil {
		db.Close()
		return fmt.Errorf("storage: load node cache: %w", err)
	}

	w.db = db
	w.startMaintenance(ctx)

	w.wg.Add(1)
	go w.loop(ctx)

	return nil
}

// Store pushes a packet into the queue for asynchronous persistence.
func (w *SQLiteWriter) Store(ctx context.Context, pkt decode.Packet) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.queue <- pkt:
		w.metrics.ObserveQueueDepth(len(w.queue))
		return nil
	default:
		w.metrics.ObserveQueueDepth(len(w.queue))
		return errors.New("storage: queue full")
	}
}

// Stop finalises the writer and closes the database connection.
func (w *SQLiteWriter) Stop() error {
	w.once.Do(func() {
		if w.maintenanceStop != nil {
			close(w.maintenanceStop)
		}
		close(w.queue)
		w.wg.Wait()
		if w.db != nil {
			w.runFinalMaintenance()
			_ = w.db.Close()
		}
		w.metrics.ObserveQueueDepth(0)
	})
	return nil
}

func (w *SQLiteWriter) startMaintenance(ctx context.Context) {
	if w.maintenanceInterval <= 0 || w.db == nil {
		return
	}

	ticker := time.NewTicker(w.maintenanceInterval)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.maintenanceStop:
				return
			case <-ticker.C:
				if err := w.runMaintenance(ctx); err != nil && !errors.Is(err, context.Canceled) {
					w.logger.Warn("sqlite maintenance failed", slog.Any("error", err))
				}
			}
		}
	}()
}

func (w *SQLiteWriter) runMaintenance(ctx context.Context) error {
	if w.db == nil {
		return nil
	}

	start := time.Now()
	if _, err := w.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return fmt.Errorf("maintenance: wal_checkpoint: %w", err)
	}
	if _, err := w.db.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return fmt.Errorf("maintenance: optimize: %w", err)
	}
	if w.logger != nil {
		w.logger.Info("sqlite maintenance completed",
			slog.Duration("duration", time.Since(start)))
	}
	return nil
}

func (w *SQLiteWriter) runFinalMaintenance() {
	if w.db == nil {
		return
	}

	if _, err := w.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		w.logger.Warn("final maintenance checkpoint failed", slog.Any("error", err))
	}
	if _, err := w.db.Exec("VACUUM"); err != nil {
		w.logger.Warn("final maintenance vacuum failed", slog.Any("error", err))
	}
	if _, err := w.db.Exec("ANALYZE"); err != nil {
		w.logger.Warn("final maintenance analyze failed", slog.Any("error", err))
	}
}

func (w *SQLiteWriter) loop(ctx context.Context) {
	defer w.wg.Done()

	stmt, err := w.db.Prepare(`INSERT INTO packet_history (
        timestamp,
        topic,
        from_node_id,
        to_node_id,
        portnum,
        portnum_name,
        gateway_id,
        channel_id,
        channel_name,
        mesh_packet_id,
        rssi,
        snr,
        hop_limit,
        hop_start,
        payload_length,
        raw_payload,
        processed_successfully,
        via_mqtt,
        want_ack,
        priority,
        delayed,
        channel_index,
        rx_time,
        pki_encrypted,
        next_hop,
        relay_node,
        tx_after,
        message_type,
        raw_service_envelope,
        parsing_error,
        transport,
        qos,
        retained
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		w.publishErr(fmt.Errorf("storage: prepare insert: %w", err))
		return
	}
	defer stmt.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-w.queue:
			if !ok {
				return
			}
			w.metrics.ObserveQueueDepth(len(w.queue))

			packetID, err := insertPacket(stmt, pkt)
			if err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
				continue
			}

			if w.cache != nil {
				if entry, created := w.cache.ensureGateway(pkt.GatewayID, pkt.ReceivedAt); created {
					if err := w.upsertNode(entry); err != nil {
						w.metrics.IncStoreErrors()
						w.publishErr(err)
					} else {
						w.metrics.IncNodeUpsert()
					}
				}

				if nodeEntry := w.cache.updateFromPacket(pkt); nodeEntry != nil {
					if err := w.upsertNode(nodeEntry); err != nil {
						w.metrics.IncStoreErrors()
						w.publishErr(err)
					} else {
						w.metrics.IncNodeUpsert()
					}
				}
			} else if pkt.Node != nil {
				fallback := &nodeEntry{
					NodeID:         pkt.From,
					UserID:         pkt.Node.UserID,
					HexID:          pkt.Node.UserID,
					LongName:       pkt.Node.LongName,
					ShortName:      pkt.Node.ShortName,
					HWModel:        pkt.Node.HWModel,
					Role:           pkt.Node.Role,
					IsLicensed:     pkt.Node.IsLicensed,
					MacAddress:     pkt.Node.MacAddress,
					PrimaryChannel: nonEmpty(pkt.Node.PrimaryChannel, pkt.ChannelID),
					Snr:            pkt.Node.Snr,
					LastHeard:      pkt.Node.LastHeard,
					ViaMQTT:        pkt.Node.ViaMQTT,
					Channel:        pkt.Node.Channel,
					IsFavorite:     pkt.Node.IsFavorite,
					IsIgnored:      pkt.Node.IsIgnored,
					IsKeyVerified:  pkt.Node.IsKeyVerified,
					FirstSeen:      pkt.ReceivedAt,
					LastUpdated:    pkt.ReceivedAt,
				}
				if pkt.Node.HopsAway != nil {
					val := *pkt.Node.HopsAway
					fallback.HopsAway = &val
				}
				if err := w.upsertNode(fallback); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncNodeUpsert()
				}
			}

			if pkt.Text != nil {
				if err := w.storeText(packetID, pkt.Text); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncTextStored()
				}
			}

			if pkt.Position != nil {
				if err := w.storePosition(packetID, pkt.Position); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncPositionStored()
				}
			}

			if pkt.Telemetry != nil {
				if err := w.storeTelemetry(packetID, pkt.Telemetry); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncTelemetryStored()
				}
			}
		}
	}
}

func insertPacket(stmt *sql.Stmt, pkt decode.Packet) (int64, error) {
	res, err := stmt.Exec(
		pkt.ReceivedAt.UnixMicro(),
		pkt.Topic,
		int64(pkt.From),
		int64(pkt.To),
		int64(pkt.PortNum),
		pkt.PortNumName,
		nullString(pkt.GatewayID),
		nullString(pkt.ChannelID),
		nullString(pkt.ChannelName),
		int64(pkt.MeshPacketID),
		int64(pkt.RxRssi),
		float64(pkt.RxSnr),
		int64(pkt.HopLimit),
		int64(pkt.HopStart),
		pkt.PayloadLength,
		nullBytes(pkt.Payload),
		boolToInt(pkt.ProcessedSuccessfully),
		boolToInt(pkt.ViaMQTT),
		boolToInt(pkt.WantAck),
		int64(pkt.Priority),
		int64(pkt.Delayed),
		int64(pkt.ChannelIndex),
		int64(pkt.RxTime),
		boolToInt(pkt.PKIEncrypted),
		int64(pkt.NextHop),
		int64(pkt.RelayNode),
		int64(pkt.TxAfter),
		nullString(pkt.MessageType),
		nullBytes(pkt.RawServiceEnvelope),
		nullString(pkt.ParsingError),
		int64(pkt.Transport),
		int64(pkt.QoS),
		boolToInt(pkt.Retained),
	)
	if err != nil {
		return 0, fmt.Errorf("storage: insert packet: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("storage: last insert id: %w", err)
	}
	return id, nil
}

func (w *SQLiteWriter) storeText(packetID int64, msg *decode.TextMessage) error {
	_, err := w.db.Exec(`INSERT INTO text_messages (
        packet_id,
        text,
        want_response,
        dest,
        source,
        request_id,
        reply_id,
        emoji,
        bitfield,
        compressed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		packetID,
		msg.Text,
		boolToInt(msg.WantResponse),
		int64(msg.Dest),
		int64(msg.Source),
		int64(msg.RequestID),
		int64(msg.ReplyID),
		int64(msg.Emoji),
		int64(msg.Bitfield),
		boolToInt(msg.Compressed),
	)
	if err != nil {
		return fmt.Errorf("storage: insert text message: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storePosition(packetID int64, pos *decode.PositionInfo) error {
	_, err := w.db.Exec(`INSERT INTO positions (
        packet_id,
        latitude,
        longitude,
        altitude,
        time,
        timestamp,
        raw_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		packetID,
		nullFloat64(pos.Latitude),
		nullFloat64(pos.Longitude),
		nullInt32(pos.Altitude),
		int64(pos.Time),
		int64(pos.Timestamp),
		nullBytes(pos.RawPayload),
	)
	if err != nil {
		return fmt.Errorf("storage: insert position: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeTelemetry(packetID int64, tele *decode.TelemetryInfo) error {
	_, err := w.db.Exec(`INSERT INTO telemetry (
        packet_id,
        raw_payload
    ) VALUES (?, ?)`,
		packetID,
		nullBytes(tele.RawPayload),
	)
	if err != nil {
		return fmt.Errorf("storage: insert telemetry: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) upsertNode(entry *nodeEntry) error {
	if entry == nil {
		return nil
	}

	if entry.HexID == "" {
		entry.HexID = entry.UserID
	}

	_, err := w.db.Exec(`INSERT INTO node_info (
	    node_id,
	    user_id,
	    hex_id,
	    long_name,
	    short_name,
	    hw_model,
	    role,
	    is_licensed,
	    mac_address,
	    primary_channel,
	    snr,
	    last_heard,
	    via_mqtt,
	    channel,
	    hops_away,
	    is_favorite,
	    is_ignored,
	    is_key_verified,
	    first_seen,
	    last_updated
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_id) DO UPDATE SET
	    user_id=excluded.user_id,
	    hex_id=excluded.hex_id,
	    long_name=excluded.long_name,
	    short_name=excluded.short_name,
	    hw_model=excluded.hw_model,
	    role=excluded.role,
	    is_licensed=excluded.is_licensed,
	    mac_address=excluded.mac_address,
	    primary_channel=excluded.primary_channel,
	    snr=excluded.snr,
	    last_heard=excluded.last_heard,
	    via_mqtt=excluded.via_mqtt,
	    channel=excluded.channel,
	    hops_away=excluded.hops_away,
	    is_favorite=excluded.is_favorite,
	    is_ignored=excluded.is_ignored,
	    is_key_verified=excluded.is_key_verified,
	    first_seen=MIN(node_info.first_seen, excluded.first_seen),
	    last_updated=excluded.last_updated`,
		int64(entry.NodeID),
		nullString(entry.UserID),
		nullString(entry.HexID),
		nullString(entry.LongName),
		nullString(entry.ShortName),
		int64(entry.HWModel),
		int64(entry.Role),
		boolToInt(entry.IsLicensed),
		nullString(entry.MacAddress),
		nullString(entry.PrimaryChannel),
		float64(entry.Snr),
		int64(entry.LastHeard),
		boolToInt(entry.ViaMQTT),
		int64(entry.Channel),
		nullUint32(entry.HopsAway),
		boolToInt(entry.IsFavorite),
		boolToInt(entry.IsIgnored),
		boolToInt(entry.IsKeyVerified),
		timeToSeconds(entry.FirstSeen),
		timeToSeconds(entry.LastUpdated),
	)
	if err != nil {
		return fmt.Errorf("storage: upsert node: %w", err)
	}
	return nil
}

func configureConnection(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=30000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA wal_autocheckpoint=1000",
		"PRAGMA journal_size_limit=67108864",
		"PRAGMA cache_size=-8192",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("storage: apply pragma %q: %w", pragma, err)
		}
	}

	return nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS packet_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        topic TEXT NOT NULL,
        from_node_id INTEGER,
        to_node_id INTEGER,
        portnum INTEGER,
        portnum_name TEXT,
        gateway_id TEXT,
        channel_id TEXT,
        channel_name TEXT,
        mesh_packet_id INTEGER,
        rssi INTEGER,
        snr REAL,
        hop_limit INTEGER,
        hop_start INTEGER,
        payload_length INTEGER,
        raw_payload BLOB,
        processed_successfully INTEGER,
        via_mqtt INTEGER,
        want_ack INTEGER,
        priority INTEGER,
        delayed INTEGER,
        channel_index INTEGER,
        rx_time INTEGER,
        pki_encrypted INTEGER,
        next_hop INTEGER,
        relay_node INTEGER,
        tx_after INTEGER,
        message_type TEXT,
        raw_service_envelope BLOB,
        parsing_error TEXT,
        transport INTEGER,
        qos INTEGER,
        retained INTEGER
    )`)
	if err != nil {
		return fmt.Errorf("storage: migrate packet_history: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS node_info (
	 	    node_id INTEGER PRIMARY KEY,
	    user_id TEXT,
	    hex_id TEXT,
	    long_name TEXT,
	    short_name TEXT,
	    hw_model INTEGER,
	    role INTEGER,
	    is_licensed INTEGER,
	    mac_address TEXT,
	    primary_channel TEXT,
	    snr REAL,
	    last_heard INTEGER,
	    via_mqtt INTEGER,
	    channel INTEGER,
	    hops_away INTEGER,
	    is_favorite INTEGER,
	    is_ignored INTEGER,
	    is_key_verified INTEGER,
	    first_seen REAL,
	    last_updated REAL
	 )`)
	if err != nil {
		return fmt.Errorf("storage: migrate node_info: %w", err)
	}

	if err := renameColumnIfExists(db, "node_info", "updated_at", "last_updated"); err != nil {
		return fmt.Errorf("storage: rename updated_at column: %w", err)
	}

	if err := addColumnIfMissing(db, "node_info", "hex_id", "TEXT"); err != nil {
		return fmt.Errorf("storage: add hex_id column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "primary_channel", "TEXT"); err != nil {
		return fmt.Errorf("storage: add primary_channel column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "first_seen", "REAL"); err != nil {
		return fmt.Errorf("storage: add first_seen column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "last_updated", "REAL"); err != nil {
		return fmt.Errorf("storage: add last_updated column: %w", err)
	}

	if err := copyColumnIfExists(db, "node_info", "updated_at", "last_updated"); err != nil {
		return fmt.Errorf("storage: copy updated_at to last_updated: %w", err)
	}

	if err := populateHexColumn(db); err != nil {
		return fmt.Errorf("storage: populate hex column: %w", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_node_hex_id ON node_info(hex_id)`); err != nil {
		return fmt.Errorf("storage: create hex_id index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_node_primary_channel ON node_info(primary_channel)`); err != nil {
		return fmt.Errorf("storage: create primary_channel index: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS text_messages (
	    packet_id INTEGER PRIMARY KEY,
	    text TEXT,
	    want_response INTEGER,
	    dest INTEGER,
	    source INTEGER,
	    request_id INTEGER,
	    reply_id INTEGER,
	    emoji INTEGER,
	    bitfield INTEGER,
	    compressed INTEGER,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate text_messages: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS positions (
	    packet_id INTEGER PRIMARY KEY,
	    latitude REAL,
	    longitude REAL,
	    altitude INTEGER,
	    time INTEGER,
	    timestamp INTEGER,
	    raw_payload BLOB,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate positions: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS telemetry (
	    packet_id INTEGER PRIMARY KEY,
	    raw_payload BLOB,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate telemetry: %w", err)
	}

	return nil
}

func addColumnIfMissing(db *sql.DB, table, column, columnType string) error {
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, columnType)
	if _, err := db.Exec(query); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "duplicate column name") {
			return nil
		}
		return err
	}
	return nil
}

func renameColumnIfExists(db *sql.DB, table, oldName, newName string) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName)
	if _, err := db.Exec(query); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such column") || strings.Contains(errMsg, "duplicate column name") || strings.Contains(errMsg, "syntax error") {
			return nil
		}
		return err
	}
	return nil
}

func copyColumnIfExists(db *sql.DB, table, from, to string) error {
	hasFrom, err := columnExists(db, table, from)
	if err != nil {
		return err
	}
	hasTo, err := columnExists(db, table, to)
	if err != nil {
		return err
	}
	if !hasFrom || !hasTo {
		return nil
	}
	query := fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, %s) WHERE %s IS NOT NULL", table, to, to, from, from)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func populateHexColumn(db *sql.DB) error {
	_, err := db.Exec(`UPDATE node_info SET hex_id = user_id WHERE (hex_id IS NULL OR hex_id = '') AND user_id IS NOT NULL`)
	return err
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		_ = cid
		_ = typeName
		_ = notNull
		_ = defaultVal
		_ = pk
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}

	return false, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullFloat64(v *float64) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

func nullInt32(v *int32) interface{} {
	if v == nil {
		return nil
	}
	return int64(*v)
}

func nullUint32(u *uint32) interface{} {
	if u == nil {
		return nil
	}
	return int64(*u)
}

func (w *SQLiteWriter) publishErr(err error) {
	if err == nil {
		return
	}
	w.logger.Error("storage error", slog.Any("error", err))
}
