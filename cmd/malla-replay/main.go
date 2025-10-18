package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aminovpavel/meshpipe-go/internal/config"
	"github.com/aminovpavel/meshpipe-go/internal/decode"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"github.com/aminovpavel/meshpipe-go/internal/replay"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
)

func main() {
	var (
		source     = flag.String("source", "", "Path to SQLite capture database with packet_history")
		output     = flag.String("output", "malla_replay_output.db", "Path to SQLite database to write")
		configPath = flag.String("config", "", "Path to config.yaml (defaults to config.yaml in cwd)")
		force      = flag.Bool("force", false, "Overwrite output database if it exists")
		startID    = flag.Int64("start-id", 0, "Replay starting from packet_history.id (inclusive)")
		endID      = flag.Int64("end-id", 0, "Replay up to packet_history.id (inclusive)")
		limit      = flag.Int("limit", 0, "Limit the number of packets to replay (0 = all)")
	)
	flag.Parse()

	if *source == "" {
		log.Fatal("malla-replay: --source is required")
	}

	if err := ensureOutput(*output, *force); err != nil {
		log.Fatalf("malla-replay: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.New(*configPath)
	if err != nil {
		log.Fatalf("malla-replay: load config: %v", err)
	}

	if cfg.DefaultChannelKey == "" {
		log.Println("malla-replay: warning: default channel key is empty; encrypted packets may fail to decode")
	}

	logger := observability.NewLogger(cfg.LogLevel)
	metrics := observability.NewMetrics(observability.WithNamespace("malla_replay"))

	decoder := decode.NewMeshtasticDecoder(decode.MeshtasticConfig{
		StoreRawEnvelope: cfg.CaptureStoreRaw,
		DefaultKeyBase64: cfg.DefaultChannelKey,
	})

	writer, err := storage.NewSQLiteWriter(
		storage.SQLiteConfig{Path: *output, QueueSize: 8192, MaintenanceInterval: 0},
		storage.WithLogger(logger.With("component", "storage")),
		storage.WithMetrics(metrics),
	)
	if err != nil {
		log.Fatalf("malla-replay: init storage writer: %v", err)
	}

	if err := writer.Start(ctx); err != nil {
		log.Fatalf("malla-replay: start storage writer: %v", err)
	}
	defer func() {
		if err := writer.Stop(); err != nil {
			log.Printf("malla-replay: warning: stop writer: %v", err)
		}
	}()

	count, err := replay.ReplaySQLite(ctx, *source, decoder, writer, replay.Options{
		StartID:          *startID,
		EndID:            *endID,
		Limit:            *limit,
		MaxEnvelopeBytes: cfg.MaxEnvelopeBytes,
	})
	if err != nil {
		log.Fatalf("malla-replay: %v", err)
	}

	logger.Info("replay completed",
		"source", *source,
		"output", *output,
		"packets", count,
	)
}

func ensureOutput(path string, force bool) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}

	if _, err := os.Stat(abs); err == nil {
		if !force {
			return fmt.Errorf("output file %s already exists (use --force to overwrite)", abs)
		}
		if err := os.Remove(abs); err != nil {
			return fmt.Errorf("remove existing output %s: %w", abs, err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("ensure output directory: %w", err)
	}

	return nil
}
