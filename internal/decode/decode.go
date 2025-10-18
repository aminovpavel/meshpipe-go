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
	ChannelName string
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

	Node *NodeInfo
}

// Decoder defines behaviour required to transform MQTT messages into packets.
type Decoder interface {
	Decode(ctx context.Context, msg mqtt.Message) (Packet, error)
}

// NodeInfo captures metadata about a node derived from NODEINFO packets.
type NodeInfo struct {
	NodeID        uint32
	UserID        string
	LongName      string
	ShortName     string
	HWModel       int32
	Role          int32
	IsLicensed    bool
	MacAddress    string
	Snr           float32
	LastHeard     uint32
	ViaMQTT       bool
	Channel       uint32
	HopsAway      *uint32
	IsFavorite    bool
	IsIgnored     bool
	IsKeyVerified bool
	UpdatedAt     time.Time
}
