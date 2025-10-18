package app_test

import (
	"testing"

	"github.com/aminovpavel/meshpipe-go/internal/app"
	"github.com/aminovpavel/meshpipe-go/internal/config"
)

func TestBuildMQTTConfig(t *testing.T) {
	cfg := &config.App{
		MQTTBrokerAddress: "meshtastic.taubetele.com ",
		MQTTPort:          1883,
		MQTTUsername:      " meshdev",
		MQTTPassword:      "large4cats ",
		MQTTTopicPrefix:   "msh/msk",
		MQTTTopicSuffix:   "+/+/+/#",
	}

	mqttCfg := app.BuildMQTTConfig(cfg)

	if mqttCfg.BrokerHost != "meshtastic.taubetele.com" {
		t.Fatalf("expected trimmed broker host, got %q", mqttCfg.BrokerHost)
	}
	if mqttCfg.Username != "meshdev" {
		t.Fatalf("expected trimmed username, got %q", mqttCfg.Username)
	}
	if mqttCfg.Password != "large4cats" {
		t.Fatalf("expected trimmed password, got %q", mqttCfg.Password)
	}
	if mqttCfg.TopicPrefix != "msh/msk" {
		t.Fatalf("expected prefix preserved, got %q", mqttCfg.TopicPrefix)
	}
	if mqttCfg.TopicSuffix != "+/+/+/#" {
		t.Fatalf("expected suffix preserved, got %q", mqttCfg.TopicSuffix)
	}
}
