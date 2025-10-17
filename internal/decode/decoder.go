package decode

import (
	"context"

	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
)

// Packet represents a decoded MQTT payload.
type Packet struct {
	Message mqtt.Message
}

// Decoder converts raw MQTT messages into packets ready for storage.
type Decoder interface {
	Decode(ctx context.Context, msg mqtt.Message) (Packet, error)
}

// PassthroughDecoder keeps the raw message without interpreting it.
type PassthroughDecoder struct{}

// Decode returns the input message wrapped in a Packet.
func (PassthroughDecoder) Decode(_ context.Context, msg mqtt.Message) (Packet, error) {
	return Packet{Message: msg}, nil
}
