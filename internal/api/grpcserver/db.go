package grpcserver

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

func openReadOnlyDB(path string) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("grpcserver: database path must be provided")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("grpcserver: resolve db path: %w", err)
	}

	escaped := url.PathEscape(abs)
	dsn := fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-4096&_temp_store=memory", escaped)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("grpcserver: open sqlite: %w", err)
	}

	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	if _, err := db.Exec("PRAGMA query_only=ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("grpcserver: set query_only pragma: %w", err)
	}

	return db, nil
}
