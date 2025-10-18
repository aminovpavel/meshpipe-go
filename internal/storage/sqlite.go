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
        payload_length,
        raw_payload,
        qos,
        retained
    ) VALUES (?, ?, ?, ?, ?, ?)`)
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
		len(pkt.Payload),
		pkt.Payload,
		pkt.QoS,
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
        payload_length INTEGER NOT NULL,
        raw_payload BLOB NOT NULL,
        qos INTEGER NOT NULL,
        retained INTEGER NOT NULL
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

func (w *SQLiteWriter) publishErr(err error) {
	if err == nil {
		return
	}
	log.Printf("storage: %v", err)
}
