package app

import (
	"strings"

	"github.com/aminovpavel/mw-malla-capture/internal/config"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
)

// BuildMQTTConfig translates the application configuration into an MQTT client config.
func BuildMQTTConfig(cfg *config.App) mqtt.Config {
	if cfg == nil {
		return mqtt.Config{}
	}

	return mqtt.Config{
		BrokerHost:  strings.TrimSpace(cfg.MQTTBrokerAddress),
		BrokerPort:  cfg.MQTTPort,
		Username:    strings.TrimSpace(cfg.MQTTUsername),
		Password:    strings.TrimSpace(cfg.MQTTPassword),
		TopicPrefix: cfg.MQTTTopicPrefix,
		TopicSuffix: cfg.MQTTTopicSuffix,
	}
}
