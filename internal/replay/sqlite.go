package replay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
	"github.com/aminovpavel/mw-malla-capture/internal/storage"
	_ "modernc.org/sqlite"
)

// Options configures how packets are selected from the source database.
type Options struct {
	StartID          int64
	EndID            int64
	Limit            int
	MaxEnvelopeBytes int
}

// ReplaySQLite reads ServiceEnvelope blobs from packet_history in the provided
// SQLite database and replays them through the supplied decoder and writer.
// The writer must already be started; callers are responsible for stopping it.
func ReplaySQLite(ctx context.Context, sourcePath string, decoder decode.Decoder, writer storage.Writer, opts Options) (int, error) {
	if sourcePath == "" {
		return 0, errors.New("replay: source sqlite path must be provided")
	}
	if decoder == nil {
		return 0, errors.New("replay: decoder must not be nil")
	}
	if writer == nil {
		return 0, errors.New("replay: writer must not be nil")
	}

	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		return 0, fmt.Errorf("replay: open source sqlite: %w", err)
	}
	defer db.Close()

	baseQuery, err := buildPacketQuery(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("replay: build packet query: %w", err)
	}

	query, args := buildQuery(baseQuery, opts)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("replay: query packet_history: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			id        int64
			topic     string
			payload   []byte
			qos       sql.NullInt64
			retained  sql.NullInt64
			timestamp sql.NullInt64
		)

		if err := rows.Scan(&id, &topic, &payload, &qos, &retained, &timestamp); err != nil {
			return count, fmt.Errorf("replay: scan row: %w", err)
		}

		if len(payload) == 0 {
			continue
		}
		if opts.MaxEnvelopeBytes > 0 && len(payload) > opts.MaxEnvelopeBytes {
			continue
		}

		msg := mqtt.Message{
			Topic:    topic,
			Payload:  append([]byte(nil), payload...),
			QoS:      toByte(qos),
			Retained: retained.Valid && retained.Int64 != 0,
			Time:     fromMicro(timestamp),
		}

		packet, err := decoder.Decode(ctx, msg)
		if err != nil {
			return count, fmt.Errorf("replay: decode packet id %d: %w", id, err)
		}

		for {
			err := writer.Store(ctx, packet)
			if err == nil {
				break
			}
			if !isQueueFull(err) {
				return count, fmt.Errorf("replay: store packet id %d: %w", id, err)
			}
			select {
			case <-ctx.Done():
				return count, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}

		count++
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
	}

	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("replay: iterate rows: %w", err)
	}

	return count, nil
}

func buildQuery(base string, opts Options) (string, []any) {
	query := base

	args := make([]any, 0, 3)
	if opts.StartID > 0 {
		query += ` AND id >= ?`
		args = append(args, opts.StartID)
	}
	if opts.EndID > 0 {
		query += ` AND id <= ?`
		args = append(args, opts.EndID)
	}

	query += ` ORDER BY id`
	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}

	return query, args
}

func buildPacketQuery(ctx context.Context, db *sql.DB) (string, error) {
	hasQoS, err := tableHasColumn(ctx, db, "packet_history", "qos")
	if err != nil {
		return "", err
	}
	hasRetained, err := tableHasColumn(ctx, db, "packet_history", "retained")
	if err != nil {
		return "", err
	}

	qosExpr := "0"
	if hasQoS {
		qosExpr = "COALESCE(qos, 0)"
	}
	retainedExpr := "0"
	if hasRetained {
		retainedExpr = "COALESCE(retained, 0)"
	}

	return fmt.Sprintf(`SELECT id, topic, raw_service_envelope, %s AS qos, %s AS retained, COALESCE(CAST(timestamp AS INTEGER), 0) AS timestamp FROM packet_history WHERE raw_service_envelope IS NOT NULL`, qosExpr, retainedExpr), nil
}

func tableHasColumn(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.QueryContext(ctx, query)
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
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}

	return false, rows.Err()
}

func isQueueFull(err error) bool {
	return err != nil && strings.Contains(err.Error(), "queue full")
}

func toByte(v sql.NullInt64) byte {
	if !v.Valid {
		return 0
	}
	return byte(v.Int64)
}

func fromMicro(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMicro(v.Int64).UTC()
}
