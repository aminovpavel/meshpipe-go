package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
)

func main() {
	cfg := mqtt.Config{
		BrokerHost:  getenv("MALLA_MQTT_BROKER_ADDRESS", "meshtastic.taubetele.com"),
		BrokerPort:  getenvInt("MALLA_MQTT_PORT", 1883),
		Username:    os.Getenv("MALLA_MQTT_USERNAME"),
		Password:    os.Getenv("MALLA_MQTT_PASSWORD"),
		TopicPrefix: getenv("MALLA_MQTT_TOPIC_PREFIX", "msh/msk"),
		TopicSuffix: getenv("MALLA_MQTT_TOPIC_SUFFIX", "+/+/+/#"),
		ClientID:    fmt.Sprintf("malla-smoke-%d", time.Now().UnixNano()),
		KeepAlive:   30 * time.Second,
	}

	client, err := mqtt.NewClient(cfg)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		log.Fatalf("start client: %v", err)
	}
	defer client.Stop()

	log.Printf("connected to %s:%d, awaiting messages...", cfg.BrokerHost, cfg.BrokerPort)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("context cancelled, exiting")
			return
		case msg, ok := <-client.Messages():
			if !ok {
				log.Printf("messages channel closed")
				return
			}
			log.Printf("MSG topic=%s retained=%t qos=%d size=%d", msg.Topic, msg.Retained, msg.QoS, len(msg.Payload))
		case err := <-client.Errors():
			log.Printf("ERR %v", err)
		case <-ticker.C:
			log.Printf("still connected, no messages in the last interval")
		}
	}
}

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		var parsed int
		if _, err := fmt.Sscanf(val, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}
