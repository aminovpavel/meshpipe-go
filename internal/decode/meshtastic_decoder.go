package decode

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"google.golang.org/protobuf/proto"
)

// MeshtasticConfig controls how packets are decoded.
type MeshtasticConfig struct {
	StoreRawEnvelope bool
	DefaultKeyBase64 string
}

// MeshtasticDecoder parses MQTT payloads into structured Meshtastic packets.
type MeshtasticDecoder struct {
	cfg MeshtasticConfig
}

// NewMeshtasticDecoder constructs a decoder with the provided configuration.
func NewMeshtasticDecoder(cfg MeshtasticConfig) MeshtasticDecoder {
	return MeshtasticDecoder{cfg: cfg}
}

// Decode converts the raw MQTT message into a Packet. It never returns an error –
// any parsing failure is recorded in the returned Packet. This allows downstream
// storage layers to persist the raw payload for later inspection.
func (d MeshtasticDecoder) Decode(_ context.Context, msg mqtt.Message) (Packet, error) {
	packet := Packet{
		Topic:       msg.Topic,
		QoS:         msg.QoS,
		Retained:    msg.Retained,
		ReceivedAt:  msg.Time,
		MessageType: extractMessageType(msg.Topic),
		ChannelName: extractChannelName(msg.Topic),
	}

	if d.cfg.StoreRawEnvelope && len(msg.Payload) > 0 {
		packet.RawServiceEnvelope = append([]byte(nil), msg.Payload...)
	}

	var env meshtasticpb.ServiceEnvelope
	if err := proto.Unmarshal(msg.Payload, &env); err != nil {
		packet.ParsingError = err.Error()
		packet.ProcessedSuccessfully = false
		return packet, nil
	}

	packet.ChannelID = env.GetChannelId()
	packet.GatewayID = env.GetGatewayId()

	mesh := env.GetPacket()
	if mesh == nil {
		packet.ParsingError = "missing mesh packet"
		packet.ProcessedSuccessfully = false
		return packet, nil
	}

	packet.MeshPacketID = mesh.GetId()
	packet.From = mesh.GetFrom()
	packet.To = mesh.GetTo()
	packet.ChannelIndex = mesh.GetChannel()
	packet.RxTime = mesh.GetRxTime()
	packet.RxSnr = mesh.GetRxSnr()
	packet.RxRssi = mesh.GetRxRssi()
	packet.HopLimit = mesh.GetHopLimit()
	packet.HopStart = mesh.GetHopStart()
	packet.ViaMQTT = mesh.GetViaMqtt()
	packet.WantAck = mesh.GetWantAck()
	packet.Priority = int32(mesh.GetPriority())
	//lint:ignore SA1019 legacy field retained for schema compatibility
	packet.Delayed = int32(mesh.GetDelayed())
	packet.PKIEncrypted = mesh.GetPkiEncrypted()
	packet.NextHop = mesh.GetNextHop()
	packet.RelayNode = mesh.GetRelayNode()
	packet.TxAfter = mesh.GetTxAfter()
	packet.Transport = int32(mesh.GetTransportMechanism())

	if data := mesh.GetDecoded(); data != nil {
		d.populateFromData(&packet, data, msg.Time)
	} else if enc := mesh.GetEncrypted(); enc != nil {
		packet.Payload = append([]byte(nil), enc...)

		plaintext, err := d.decryptMeshPacket(mesh, packet.ChannelName)
		if err != nil {
			packet.ParsingError = err.Error()
			packet.ProcessedSuccessfully = false
			return packet, nil
		}

		data := &meshtasticpb.Data{}
		if err := proto.Unmarshal(plaintext, data); err != nil {
			packet.ParsingError = err.Error()
			packet.ProcessedSuccessfully = false
			return packet, nil
		}

		d.populateFromData(&packet, data, msg.Time)
	}

	packet.PayloadLength = len(packet.Payload)
	packet.ProcessedSuccessfully = packet.ParsingError == ""

	return packet, nil
}

func (d MeshtasticDecoder) populateFromData(packet *Packet, data *meshtasticpb.Data, receivedAt time.Time) {
	packet.PortNum = int32(data.GetPortnum())
	packet.PortNumName = portNumName(data.GetPortnum())
	packet.Payload = append([]byte(nil), data.GetPayload()...)

	var (
		buildErr error
	)

	switch meshtasticpb.PortNum(data.GetPortnum()) {
	case meshtasticpb.PortNum_NODEINFO_APP:
		var node *NodeInfo
		node, buildErr = buildNodeInfo(data, receivedAt)
		packet.Node = node
		if node != nil && packet.ChannelID != "" {
			node.PrimaryChannel = packet.ChannelID
		}
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_ALERT_APP,
		meshtasticpb.PortNum_REPLY_APP,
		meshtasticpb.PortNum_DETECTION_SENSOR_APP:
		packet.Text = buildTextMessage(data, false)
	case meshtasticpb.PortNum_TEXT_MESSAGE_COMPRESSED_APP:
		packet.Text = buildTextMessage(data, true)
	case meshtasticpb.PortNum_POSITION_APP:
		packet.Position, buildErr = buildPositionInfo(data.GetPayload())
	case meshtasticpb.PortNum_TELEMETRY_APP:
		packet.Telemetry, buildErr = buildTelemetryInfo(data.GetPayload())
	default:
		// no additional decoding
	}

	if buildErr != nil {
		if packet.ParsingError == "" {
			packet.ParsingError = buildErr.Error()
		} else {
			packet.ParsingError += "; " + buildErr.Error()
		}
		packet.ProcessedSuccessfully = false
	}
}

func buildNodeInfo(data *meshtasticpb.Data, receivedAt time.Time) (*NodeInfo, error) {
	if meshtasticpb.PortNum(data.GetPortnum()) != meshtasticpb.PortNum_NODEINFO_APP {
		return nil, nil
	}

	nodeProto := &meshtasticpb.NodeInfo{}
	if err := proto.Unmarshal(data.GetPayload(), nodeProto); err != nil {
		return nil, err
	}

	var (
		userID     string
		longName   string
		shortName  string
		hwModel    int32
		role       int32
		isLicensed bool
		mac        []byte
	)

	if user := nodeProto.GetUser(); user != nil {
		userID = user.GetId()
		longName = user.GetLongName()
		shortName = user.GetShortName()
		hwModel = int32(user.GetHwModel())
		role = int32(user.GetRole())
		isLicensed = user.GetIsLicensed()
		//lint:ignore SA1019 legacy mac field retained for compatibility
		mac = user.GetMacaddr()
	}

	node := &NodeInfo{
		NodeID:        nodeProto.GetNum(),
		UserID:        userID,
		LongName:      longName,
		ShortName:     shortName,
		HWModel:       hwModel,
		Role:          role,
		IsLicensed:    isLicensed,
		MacAddress:    macToString(mac),
		Snr:           nodeProto.GetSnr(),
		LastHeard:     nodeProto.GetLastHeard(),
		ViaMQTT:       nodeProto.GetViaMqtt(),
		Channel:       nodeProto.GetChannel(),
		IsFavorite:    nodeProto.GetIsFavorite(),
		IsIgnored:     nodeProto.GetIsIgnored(),
		IsKeyVerified: nodeProto.GetIsKeyManuallyVerified(),
		UpdatedAt:     receivedAt,
	}

	if hops := nodeProto.GetHopsAway(); nodeProto.HopsAway != nil {
		v := hops
		node.HopsAway = &v
	}

	return node, nil
}

func buildTextMessage(data *meshtasticpb.Data, compressed bool) *TextMessage {
	return &TextMessage{
		Text:         string(data.GetPayload()),
		WantResponse: data.GetWantResponse(),
		Dest:         data.GetDest(),
		Source:       data.GetSource(),
		RequestID:    data.GetRequestId(),
		ReplyID:      data.GetReplyId(),
		Emoji:        data.GetEmoji(),
		Bitfield:     data.GetBitfield(),
		Compressed:   compressed,
	}
}

func buildPositionInfo(payload []byte) (*PositionInfo, error) {
	pos := &meshtasticpb.Position{}
	if err := proto.Unmarshal(payload, pos); err != nil {
		return nil, err
	}

	info := &PositionInfo{
		Proto:      pos,
		RawPayload: append([]byte(nil), payload...),
		Time:       pos.GetTime(),
		Timestamp:  pos.GetTimestamp(),
	}

	if pos.LatitudeI != nil {
		v := float64(*pos.LatitudeI) / 1e7
		info.Latitude = &v
	}
	if pos.LongitudeI != nil {
		v := float64(*pos.LongitudeI) / 1e7
		info.Longitude = &v
	}
	if pos.Altitude != nil {
		v := *pos.Altitude
		info.Altitude = &v
	}

	return info, nil
}

func buildTelemetryInfo(payload []byte) (*TelemetryInfo, error) {
	tele := &meshtasticpb.Telemetry{}
	if err := proto.Unmarshal(payload, tele); err != nil {
		return nil, err
	}
	return &TelemetryInfo{
		Proto:      tele,
		RawPayload: append([]byte(nil), payload...),
	}, nil
}

func (d MeshtasticDecoder) decryptMeshPacket(mesh *meshtasticpb.MeshPacket, channelName string) ([]byte, error) {
	trimmed := strings.TrimSpace(d.cfg.DefaultKeyBase64)
	if trimmed == "" {
		return nil, errors.New("decode: default key not configured")
	}

	keyBytes, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, errors.New("decode: invalid base64 default key")
	}
	if len(keyBytes) != 32 {
		return nil, errors.New("decode: default key must be 32 bytes after decoding")
	}

	if plaintext, err := decryptPayloadWithKey(mesh, keyBytes); err == nil {
		return plaintext, nil
	}

	if channelName != "" {
		channelKey := deriveChannelKey(keyBytes, channelName)
		if plaintext, err := decryptPayloadWithKey(mesh, channelKey); err == nil {
			return plaintext, nil
		}
	}

	return nil, errors.New("decode: unable to decrypt payload")
}

func decryptPayloadWithKey(mesh *meshtasticpb.MeshPacket, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aes.BlockSize)
	binary.LittleEndian.PutUint64(nonce[:8], uint64(mesh.GetId()))
	binary.LittleEndian.PutUint64(nonce[8:], uint64(mesh.GetFrom()))

	stream := cipher.NewCTR(block, nonce)
	plaintext := make([]byte, len(mesh.GetEncrypted()))
	stream.XORKeyStream(plaintext, mesh.GetEncrypted())
	return plaintext, nil
}

func deriveChannelKey(defaultKey []byte, channelName string) []byte {
	hasher := sha256.New()
	hasher.Write(defaultKey)
	hasher.Write([]byte(channelName))
	return hasher.Sum(nil)
}

func extractMessageType(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func extractChannelName(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 5 {
		return parts[4]
	}
	return ""
}

func portNumName(port meshtasticpb.PortNum) string {
	if name, ok := meshtasticpb.PortNum_name[int32(port)]; ok {
		return name
	}
	return ""
}

func macToString(mac []byte) string {
	if len(mac) == 0 {
		return ""
	}
	return strings.ToUpper(hex.EncodeToString(mac))
}
