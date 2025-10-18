package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/aminovpavel/mw-malla-capture/internal/app"
	"github.com/aminovpavel/mw-malla-capture/internal/config"
	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
	"github.com/aminovpavel/mw-malla-capture/internal/pipeline"
	"github.com/aminovpavel/mw-malla-capture/internal/storage"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.New("")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	mqttCfg := app.BuildMQTTConfig(cfg)
	client, err := mqtt.NewClient(mqttCfg)
	if err != nil {
		log.Fatalf("init mqtt client: %v", err)
	}

	decoder := decode.NewMeshtasticDecoder(decode.MeshtasticConfig{
		StoreRawEnvelope: cfg.CaptureStoreRaw,
	})

	writer, err := storage.NewSQLiteWriter(storage.SQLiteConfig{Path: cfg.DatabaseFile})
	if err != nil {
		log.Fatalf("init storage writer: %v", err)
	}

	if err := writer.Start(ctx); err != nil {
		log.Fatalf("start storage writer: %v", err)
	}
	defer func() {
		if err := writer.Stop(); err != nil {
			log.Printf("storage stop error: %v", err)
		}
	}()

	pipe := pipeline.New(client, decoder, writer)

	go func() {
		for err := range pipe.Errors() {
			if err == nil || errors.Is(err, context.Canceled) {
				continue
			}
			log.Printf("pipeline error: %v", err)
		}
	}()

	log.Printf("malla-capture starting (broker %s:%d)", mqttCfg.BrokerHost, mqttCfg.BrokerPort)

	if err := pipe.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("pipeline stopped with error: %v", err)
	}
}
