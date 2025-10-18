package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
)

// SQLiteConfig holds configuration values for the SQLite writer.
type SQLiteConfig struct {
	Path      string
	QueueSize int
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
}

// NewSQLiteWriter constructs a writer with the provided configuration.
func NewSQLiteWriter(cfg SQLiteConfig) (*SQLiteWriter, error) {
	if cfg.Path == "" {
		return nil, errors.New("storage: database path must be provided")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 512
	}

	return &SQLiteWriter{
		cfg:   cfg,
		queue: make(chan decode.Packet, cfg.QueueSize),
	}, nil
}

// Start opens the database, runs migrations, and begins processing the queue.
func (w *SQLiteWriter) Start(ctx context.Context) error {
	// Ensure directory exists.
	abs, err := filepath.Abs(w.cfg.Path)
	if err != nil {
		return fmt.Errorf("storage: resolve path: %w", err)
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

	w.db = db

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
		return nil
	default:
		return errors.New("storage: queue full")
	}
}

// Stop finalises the writer and closes the database connection.
func (w *SQLiteWriter) Stop() error {
	w.once.Do(func() {
		close(w.queue)
		w.wg.Wait()
		if w.db != nil {
			_ = w.db.Close()
		}
	})
	return nil
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
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
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

			if err := insertPacket(stmt, pkt); err != nil {
				w.publishErr(err)
			}
		}
	}
}

func insertPacket(stmt *sql.Stmt, pkt decode.Packet) error {
	_, err := stmt.Exec(
		pkt.ReceivedAt.UnixMicro(),
		pkt.Topic,
		int64(pkt.From),
		int64(pkt.To),
		int64(pkt.PortNum),
		pkt.PortNumName,
		nullString(pkt.GatewayID),
		nullString(pkt.ChannelID),
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
		return fmt.Errorf("storage: insert packet: %w", err)
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
	return nil
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

func (w *SQLiteWriter) publishErr(err error) {
	if err == nil {
		return
	}
	log.Printf("storage: %v", err)
}
