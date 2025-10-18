package decode

import (
	"context"
	"time"

	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
)

// Packet represents a message that has been prepared for persistence.
type Packet struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retained   bool
	ReceivedAt time.Time
}

// Decoder converts raw MQTT messages into structured packets.
type Decoder interface {
	Decode(ctx context.Context, msg mqtt.Message) (Packet, error)
}

// PassthroughDecoder returns packets without transformation.
type PassthroughDecoder struct{}

// Decode implements Decoder by copying the raw MQTT message into Packet.
func (PassthroughDecoder) Decode(_ context.Context, msg mqtt.Message) (Packet, error) {
	return Packet{
		Topic:      msg.Topic,
		Payload:    append([]byte(nil), msg.Payload...),
		QoS:        msg.QoS,
		Retained:   msg.Retained,
		ReceivedAt: msg.Time,
	}, nil
}
