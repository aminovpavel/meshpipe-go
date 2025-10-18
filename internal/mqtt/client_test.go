package mqtt_test

import (
	"testing"

	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
)

func TestSubscriptionTopic(t *testing.T) {
	tests := []struct {
		name   string
		cfg    mqtt.Config
		expect string
	}{
		{name: "prefix and suffix", cfg: mqtt.Config{TopicPrefix: "msh/msk", TopicSuffix: "+/+/+/#"}, expect: "msh/msk/+/+/+/#"},
		{name: "prefix only", cfg: mqtt.Config{TopicPrefix: "msh"}, expect: "msh"},
		{name: "suffix only", cfg: mqtt.Config{TopicSuffix: "+/#"}, expect: "+/#"},
		{name: "both empty", cfg: mqtt.Config{}, expect: "#"},
	}

	for _, tt := range tests {
		if topic := tt.cfg.SubscriptionTopic(); topic != tt.expect {
			t.Fatalf("%s: expected %q, got %q", tt.name, tt.expect, topic)
		}
	}
}

func TestNewClientValidation(t *testing.T) {
	_, err := mqtt.NewClient(mqtt.Config{})
	if err == nil {
		t.Fatalf("expected validation error for empty config")
	}

	cfg := mqtt.Config{BrokerHost: "meshtastic.taubetele.com", BrokerPort: 1883}
	client, err := mqtt.NewClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatalf("expected client instance")
	}
}
