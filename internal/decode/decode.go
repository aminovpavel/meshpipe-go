package decode

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
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
	RequestID   uint32
	ReplyID     uint32

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
	DecodedPortPayload    map[string]proto.Message
	ExtraText             map[string]string
	Traceroutes           []*meshtasticpb.RouteDiscovery

	Node      *NodeInfo
	Text      *TextMessage
	Position  *PositionInfo
	Telemetry *TelemetryInfo
}

// Decoder defines behaviour required to transform MQTT messages into packets.
type Decoder interface {
	Decode(ctx context.Context, msg mqtt.Message) (Packet, error)
}

// NodeInfo captures metadata about a node derived from NODEINFO packets.
type NodeInfo struct {
	NodeID          uint32
	UserID          string
	PrimaryChannel  string
	LongName        string
	ShortName       string
	HWModel         int32
	HWModelName     string
	Role            int32
	RoleName        string
	IsLicensed      bool
	MacAddress      string
	Snr             float32
	LastHeard       uint32
	ViaMQTT         bool
	Channel         uint32
	HopsAway        *uint32
	IsFavorite      bool
	IsIgnored       bool
	IsKeyVerified   bool
	UpdatedAt       time.Time
	Region          string
	RegionName      string
	FirmwareVersion string
	ModemPreset     string
	ModemPresetName string
}

// TextMessage captures decoded text payloads.
type TextMessage struct {
	Text         string
	WantResponse bool
	Dest         uint32
	Source       uint32
	RequestID    uint32
	ReplyID      uint32
	Emoji        uint32
	Bitfield     uint32
	Compressed   bool
}

// PositionInfo holds decoded position data.
type PositionInfo struct {
	Proto      *meshtasticpb.Position
	Latitude   *float64
	Longitude  *float64
	Altitude   *int32
	Time       uint32
	Timestamp  uint32
	RawPayload []byte
}

// TelemetryInfo wraps telemetry metrics.
type TelemetryInfo struct {
	Proto      *meshtasticpb.Telemetry
	RawPayload []byte
}
