package diff

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

// Options configures diff behaviour.
type Options struct {
	SampleLimit int
}

type Summary struct {
	PacketHistory TableDiff
	NodeInfo      TableDiff
}

type TableDiff struct {
	OnlyA       int
	OnlyB       int
	SampleOnlyA []string
	SampleOnlyB []string
}

func CompareSQLite(ctx context.Context, pathA, pathB string, opts Options) (Summary, error) {
	if pathA == "" || pathB == "" {
		return Summary{}, errors.New("diff: both database paths must be provided")
	}

	dbA, err := openDB(pathA)
	if err != nil {
		return Summary{}, err
	}
	defer dbA.Close()

	dbB, err := openDB(pathB)
	if err != nil {
		return Summary{}, err
	}
	defer dbB.Close()

	packetQuery, err := fingerprintQuery(ctx, dbA, dbB, "packet_history", packetRequiredColumns)
	if err != nil {
		return Summary{}, fmt.Errorf("diff packet_history: %w", err)
	}
	packetA, err := collectFingerprints(ctx, dbA, packetQuery)
	if err != nil {
		return Summary{}, fmt.Errorf("diff packet_history (A): %w", err)
	}
	packetB, err := collectFingerprints(ctx, dbB, packetQuery)
	if err != nil {
		return Summary{}, fmt.Errorf("diff packet_history (B): %w", err)
	}

	nodeQuery, err := fingerprintQuery(ctx, dbA, dbB, "node_info", nodeRequiredColumns)
	if err != nil {
		return Summary{}, fmt.Errorf("diff node_info: %w", err)
	}
	nodeA, err := collectFingerprints(ctx, dbA, nodeQuery)
	if err != nil {
		return Summary{}, fmt.Errorf("diff node_info (A): %w", err)
	}
	nodeB, err := collectFingerprints(ctx, dbB, nodeQuery)
	if err != nil {
		return Summary{}, fmt.Errorf("diff node_info (B): %w", err)
	}

	return Summary{
		PacketHistory: diffMaps(packetA, packetB, opts.SampleLimit),
		NodeInfo:      diffMaps(nodeA, nodeB, opts.SampleLimit),
	}, nil
}

func openDB(path string) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("diff: resolve path %s: %w", path, err)
	}
	db, err := sql.Open("sqlite", abs)
	if err != nil {
		return nil, fmt.Errorf("diff: open sqlite %s: %w", abs, err)
	}
	return db, nil
}

func collectFingerprints(ctx context.Context, db *sql.DB, query string) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		result[fp]++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func diffMaps(a, b map[string]int, sampleLimit int) TableDiff {
	var (
		onlyA   int
		onlyB   int
		sampleA []string
		sampleB []string
	)

	if sampleLimit < 0 {
		sampleLimit = 0
	}

	for key, countA := range a {
		countB := b[key]
		switch {
		case countA > countB:
			diff := countA - countB
			onlyA += diff
			for i := 0; i < diff && len(sampleA) < sampleLimit; i++ {
				sampleA = append(sampleA, key)
			}
			b[key] = 0
		case countB > countA:
			diff := countB - countA
			onlyB += diff
			for i := 0; i < diff && len(sampleB) < sampleLimit; i++ {
				sampleB = append(sampleB, key)
			}
			b[key] = diff
		default:
			b[key] = 0
		}
	}

	for key, countB := range b {
		if countB <= 0 {
			continue
		}
		onlyB += countB
		for i := 0; i < countB && len(sampleB) < sampleLimit; i++ {
			sampleB = append(sampleB, key)
		}
	}

	return TableDiff{
		OnlyA:       onlyA,
		OnlyB:       onlyB,
		SampleOnlyA: sampleA,
		SampleOnlyB: sampleB,
	}
}

var (
	packetRequiredColumns = []string{"topic", "raw_service_envelope"}
	nodeRequiredColumns   = []string{"node_id", "hex_id", "long_name", "short_name", "hw_model", "role", "primary_channel", "is_licensed", "mac_address", "first_seen", "last_updated"}
)

func fingerprintQuery(ctx context.Context, dbA, dbB *sql.DB, table string, required []string) (string, error) {
	schemaA, err := tableColumnTypes(ctx, dbA, table)
	if err != nil {
		return "", err
	}
	schemaB, err := tableColumnTypes(ctx, dbB, table)
	if err != nil {
		return "", err
	}

	cols := make([]columnInfo, 0, len(required))
	for _, name := range required {
		typ, okA := schemaA[name]
		_, okB := schemaB[name]
		if !okA || !okB {
			return "", fmt.Errorf("table %s: required column %s missing in one of the databases", table, name)
		}
		cols = append(cols, columnInfo{Name: name, Type: typ})
	}

	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })

	parts := make([]string, 0, len(cols)*2)
	for _, col := range cols {
		expr := columnExpression(col)
		parts = append(parts, fmt.Sprintf("'%s', %s", col.Name, expr))
	}

	return fmt.Sprintf("SELECT json_object(%s) FROM %s", strings.Join(parts, ", "), table), nil
}

type columnInfo struct {
	Name string
	Type string
}

func tableColumnTypes(ctx context.Context, db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
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
			return nil, err
		}
		result[name] = strings.ToUpper(typeName)
	}

	return result, rows.Err()
}

func columnExpression(col columnInfo) string {
	upper := strings.ToUpper(col.Type)
	switch {
	case strings.Contains(upper, "BLOB"):
		return fmt.Sprintf("COALESCE(hex(%s), '')", col.Name)
	case isNumericType(upper):
		return fmt.Sprintf("COALESCE(%s, 0)", col.Name)
	default:
		return fmt.Sprintf("COALESCE(%s, '')", col.Name)
	}
}

func isNumericType(t string) bool {
	return strings.Contains(t, "INT") ||
		strings.Contains(t, "REAL") ||
		strings.Contains(t, "NUM") ||
		strings.Contains(t, "DOUBLE") ||
		strings.Contains(t, "FLOAT") ||
		strings.Contains(t, "DEC") ||
		strings.Contains(t, "BOOL")
}
