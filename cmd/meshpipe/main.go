package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/app"
	"github.com/aminovpavel/meshpipe-go/internal/config"
	"github.com/aminovpavel/meshpipe-go/internal/decode"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"github.com/aminovpavel/meshpipe-go/internal/pipeline"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.New("")
	if err != nil {
		panic(fmt.Errorf("load config: %w", err))
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	metrics := observability.NewMetrics()

	mqttCfg := app.BuildMQTTConfig(cfg)
	client, err := mqtt.NewClient(mqttCfg)
	if err != nil {
		logger.Error("failed to initialise MQTT client", slog.Any("error", err))
		return
	}

	decoder := decode.NewMeshtasticDecoder(decode.MeshtasticConfig{
		StoreRawEnvelope: cfg.CaptureStoreRaw,
		DefaultKeyBase64: cfg.DefaultChannelKey,
	})

	writer, err := storage.NewSQLiteWriter(
		storage.SQLiteConfig{
			Path:                cfg.DatabaseFile,
			MaintenanceInterval: time.Duration(cfg.MaintenanceInterval) * time.Minute,
		},
		storage.WithLogger(logger.With(slog.String("component", "storage"))),
		storage.WithMetrics(metrics),
	)
	if err != nil {
		logger.Error("failed to initialise storage writer", slog.Any("error", err))
		return
	}

	if err := writer.Start(ctx); err != nil {
		logger.Error("failed to start storage writer", slog.Any("error", err))
		return
	}
	defer func() {
		if err := writer.Stop(); err != nil {
			logger.Error("storage stop error", slog.Any("error", err))
		}
	}()

	pipe := pipeline.New(
		client,
		decoder,
		writer,
		pipeline.WithLogger(logger.With(slog.String("component", "pipeline"))),
		pipeline.WithMetrics(metrics),
		pipeline.WithMaxEnvelopeBytes(cfg.MaxEnvelopeBytes),
	)

	obsServer := observability.NewServer(observability.ServerConfig{
		Address: cfg.ObservabilityAddress,
		Logger:  logger.With(slog.String("component", "observability")),
		Metrics: metrics,
	})
	go obsServer.Run(ctx)

	go func() {
		for err := range pipe.Errors() {
			if err == nil || errors.Is(err, context.Canceled) {
				continue
			}
			logger.Error("pipeline error", slog.Any("error", err))
		}
	}()

	logger.Info("meshpipe starting",
		slog.String("broker_host", mqttCfg.BrokerHost),
		slog.Int("broker_port", mqttCfg.BrokerPort),
		slog.String("observability_address", cfg.ObservabilityAddress),
	)

	if err := pipe.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("pipeline stopped with error", slog.Any("error", err))
	}

	logger.Info("meshpipe stopped")
}
