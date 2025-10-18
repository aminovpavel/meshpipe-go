package decode

import (
	"context"
	"time"

	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
)

// Packet represents a Meshtastic packet ready for persistence.
type Packet struct {
	Topic      string
	QoS        byte
	Retained   bool
	ReceivedAt time.Time

	MessageType string
	GatewayID   string
	ChannelID   string

	From         uint32
	To           uint32
	ChannelIndex uint32
	MeshPacketID uint32
	RxTime       uint32
	RxSnr        float32
	RxRssi       int32
	HopLimit     uint32
	HopStart     uint32
	ViaMQTT      bool
	WantAck      bool
	Priority     int32
	Delayed      int32
	PKIEncrypted bool
	NextHop      uint32
	RelayNode    uint32
	TxAfter      uint32
	Transport    int32
	PortNum      int32
	PortNumName  string

	Payload            []byte
	PayloadLength      int
	RawServiceEnvelope []byte

	ProcessedSuccessfully bool
	ParsingError          string
}

// Decoder defines behaviour required to transform MQTT messages into packets.
type Decoder interface {
	Decode(ctx context.Context, msg mqtt.Message) (Packet, error)
}
