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
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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

const unknownGatewayID = "UNKNOWN_GATEWAY"

var portDisplayNames = map[meshtasticpb.PortNum]string{
	meshtasticpb.PortNum_UNKNOWN_APP:                 "Unknown",
	meshtasticpb.PortNum_TEXT_MESSAGE_APP:            "Text message",
	meshtasticpb.PortNum_TEXT_MESSAGE_COMPRESSED_APP: "Text message (compressed)",
	meshtasticpb.PortNum_ALERT_APP:                   "Alert",
	meshtasticpb.PortNum_DETECTION_SENSOR_APP:        "Detection sensor",
	meshtasticpb.PortNum_REMOTE_HARDWARE_APP:         "Remote hardware",
	meshtasticpb.PortNum_POSITION_APP:                "Position",
	meshtasticpb.PortNum_NODEINFO_APP:                "Node info",
	meshtasticpb.PortNum_ROUTING_APP:                 "Routing",
	meshtasticpb.PortNum_ADMIN_APP:                   "Admin",
	meshtasticpb.PortNum_WAYPOINT_APP:                "Waypoint",
	meshtasticpb.PortNum_REPLY_APP:                   "Reply",
	meshtasticpb.PortNum_RANGE_TEST_APP:              "Range test",
	meshtasticpb.PortNum_TELEMETRY_APP:               "Telemetry",
	meshtasticpb.PortNum_STORE_FORWARD_APP:           "Store & forward",
	meshtasticpb.PortNum_PAXCOUNTER_APP:              "Paxcounter",
	meshtasticpb.PortNum_ZPS_APP:                     "ZPS",
	meshtasticpb.PortNum_POWERSTRESS_APP:             "Power stress",
	meshtasticpb.PortNum_NEIGHBORINFO_APP:            "Neighbor info",
	meshtasticpb.PortNum_MAP_REPORT_APP:              "Map report",
	meshtasticpb.PortNum_ATAK_PLUGIN:                 "ATAK plugin",
	meshtasticpb.PortNum_AUDIO_APP:                   "Audio",
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
		sanitizePacket(&packet)
		return packet, nil
	}

	packet.ChannelID = env.GetChannelId()
	packet.GatewayID = env.GetGatewayId()

	mesh := env.GetPacket()
	if mesh == nil {
		packet.ParsingError = "missing mesh packet"
		packet.ProcessedSuccessfully = false
		sanitizePacket(&packet)
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
			sanitizePacket(&packet)
			return packet, nil
		}

		data := &meshtasticpb.Data{}
		if err := proto.Unmarshal(plaintext, data); err != nil {
			packet.ParsingError = err.Error()
			packet.ProcessedSuccessfully = false
			sanitizePacket(&packet)
			return packet, nil
		}

		d.populateFromData(&packet, data, msg.Time)
	}

	packet.PayloadLength = len(packet.Payload)
	packet.ProcessedSuccessfully = packet.ParsingError == ""
	sanitizePacket(&packet)

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
		if node != nil {
			node.PrimaryChannel = firstNonEmpty(packet.ChannelID, node.PrimaryChannel)
		}
	case meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		meshtasticpb.PortNum_ALERT_APP,
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

	d.decodeAdditionalPort(packet, data, receivedAt)

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
		HWModelName:   hardwareModelName(meshtasticpb.HardwareModel(hwModel)),
		Role:          role,
		RoleName:      roleDisplayName(meshtasticpb.Config_DeviceConfig_Role(role)),
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

func applyMapReport(packet *Packet, report *meshtasticpb.MapReport, receivedAt time.Time) {
	if report == nil {
		return
	}

	node := packet.Node
	if node == nil {
		node = &NodeInfo{
			NodeID:    packet.From,
			UpdatedAt: receivedAt,
		}
		packet.Node = node
	}

	if node.NodeID == 0 {
		node.NodeID = packet.From
	}

	node.UpdatedAt = receivedAt
	node.PrimaryChannel = firstNonEmpty(packet.ChannelID, node.PrimaryChannel)

	if v := report.GetLongName(); v != "" {
		node.LongName = v
	}
	if v := report.GetShortName(); v != "" {
		node.ShortName = v
	}
	if fw := report.GetFirmwareVersion(); fw != "" {
		node.FirmwareVersion = fw
	}

	if hw := report.GetHwModel(); hw != meshtasticpb.HardwareModel_UNSET {
		node.HWModel = int32(hw)
		node.HWModelName = hardwareModelName(hw)
	}

	role := report.GetRole()
	node.Role = int32(role)
	node.RoleName = roleDisplayName(role)

	if region := report.GetRegion(); region != meshtasticpb.Config_LoRaConfig_UNSET {
		node.Region = regionCodeString(region)
		node.RegionName = regionDisplayName(region)
	}

	preset := report.GetModemPreset()
	node.ModemPreset = modemPresetCode(preset)
	node.ModemPresetName = modemPresetName(preset)
}

func (d MeshtasticDecoder) decodeAdditionalPort(packet *Packet, data *meshtasticpb.Data, receivedAt time.Time) {
	port := meshtasticpb.PortNum(data.GetPortnum())
	payload := data.GetPayload()

	switch port {
	case meshtasticpb.PortNum_REMOTE_HARDWARE_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.HardwareMessage{})
	case meshtasticpb.PortNum_ROUTING_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.Routing{})
	case meshtasticpb.PortNum_ADMIN_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.AdminMessage{})
	case meshtasticpb.PortNum_WAYPOINT_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.Waypoint{})
	case meshtasticpb.PortNum_KEY_VERIFICATION_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.KeyVerification{})
	case meshtasticpb.PortNum_PAXCOUNTER_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.Paxcount{})
	case meshtasticpb.PortNum_STORE_FORWARD_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.StoreAndForward{})
	case meshtasticpb.PortNum_TRACEROUTE_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.RouteDiscovery{})
		if message, ok := packet.DecodedPortPayload[meshtasticpb.PortNum_TRACEROUTE_APP.String()]; ok {
			if route, ok := message.(*meshtasticpb.RouteDiscovery); ok {
				packet.addTraceroute(route)
			}
		}
	case meshtasticpb.PortNum_NEIGHBORINFO_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.NeighborInfo{})
	case meshtasticpb.PortNum_MAP_REPORT_APP:
		report := &meshtasticpb.MapReport{}
		if err := proto.Unmarshal(payload, report); err != nil {
			appendParsingError(packet, fmt.Errorf("%s decode: %w", port.String(), err))
			return
		}
		packet.addDecodedPort(port, report)
		applyMapReport(packet, report, receivedAt)
	case meshtasticpb.PortNum_POWERSTRESS_APP:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.PowerStressMessage{})
	case meshtasticpb.PortNum_ATAK_PLUGIN:
		decodeProtoPayload(packet, port, payload, &meshtasticpb.TAKPacket{})
	case meshtasticpb.PortNum_REPLY_APP, meshtasticpb.PortNum_RANGE_TEST_APP:
		if len(payload) == 0 {
			return
		}
		if !utf8.Valid(payload) {
			appendParsingError(packet, fmt.Errorf("%s payload is not valid UTF-8", port.String()))
			return
		}
		packet.addExtraText(port, string(payload))
	case meshtasticpb.PortNum_AUDIO_APP:
		if len(payload) < 4 {
			appendParsingError(packet, fmt.Errorf("%s audio frame too short", port.String()))
			return
		}
		if payload[0] != 0xc0 || payload[1] != 0xde || payload[2] != 0xc2 {
			appendParsingError(packet, fmt.Errorf("%s invalid audio header", port.String()))
			return
		}
	case meshtasticpb.PortNum_ZPS_APP:
		if len(payload) > 0 && len(payload)%8 != 0 {
			appendParsingError(packet, fmt.Errorf("%s payload must be multiple of 8 bytes", port.String()))
		}
	default:
		// other ports either use plaintext already handled above or carry binary payloads.
	}
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
	switch len(keyBytes) {
	case 16, 24, 32:
	default:
		return nil, errors.New("decode: default key must be 16, 24, or 32 bytes after decoding")
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

func extractChannelID(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

func portNumName(port meshtasticpb.PortNum) string {
	if name, ok := portDisplayNames[port]; ok {
		return name
	}
	if raw, ok := meshtasticpb.PortNum_name[int32(port)]; ok {
		trimmed := strings.TrimPrefix(raw, "PortNum_")
		trimmed = strings.TrimSuffix(trimmed, "_APP")
		return humanizeWords(trimmed)
	}
	return "Unknown"
}

func macToString(mac []byte) string {
	if len(mac) == 0 {
		return ""
	}
	return strings.ToUpper(hex.EncodeToString(mac))
}

func sanitizePacket(packet *Packet) {
	if strings.TrimSpace(packet.GatewayID) == "" {
		packet.GatewayID = unknownGatewayID
	}

	if packet.ChannelID == "" {
		packet.ChannelID = extractChannelID(packet.Topic)
	}

	if packet.ChannelName == "" || strings.HasPrefix(packet.ChannelName, "!") {
		if fallback := fallbackChannelName(packet.MessageType); fallback != "" {
			packet.ChannelName = fallback
		}
	}

	if packet.PortNumName == "" {
		packet.PortNumName = fallbackPortNumName(packet.MessageType)
	}
}

func fallbackChannelName(messageType string) string {
	switch strings.ToLower(messageType) {
	case "":
		return ""
	case "stat":
		return "STAT"
	case "json":
		return "JSON"
	case "map":
		return "MAP"
	case "e":
		return ""
	default:
		return strings.ToUpper(messageType)
	}
}

func fallbackPortNumName(messageType string) string {
	switch strings.ToLower(messageType) {
	case "stat":
		return "Node stat"
	case "json":
		return "JSON"
	case "map":
		return "Map report"
	default:
		return "Unknown"
	}
}

func decodeProtoPayload(packet *Packet, port meshtasticpb.PortNum, payload []byte, msg proto.Message) {
	if len(payload) == 0 {
		return
	}
	if err := proto.Unmarshal(payload, msg); err != nil {
		appendParsingError(packet, fmt.Errorf("%s decode: %w", port.String(), err))
		return
	}
	packet.addDecodedPort(port, msg)
}

func appendParsingError(packet *Packet, err error) {
	if err == nil {
		return
	}
	if packet.ParsingError == "" {
		packet.ParsingError = err.Error()
	} else {
		packet.ParsingError += "; " + err.Error()
	}
}

func (packet *Packet) addDecodedPort(port meshtasticpb.PortNum, msg proto.Message) {
	if packet.DecodedPortPayload == nil {
		packet.DecodedPortPayload = make(map[string]proto.Message)
	}
	packet.DecodedPortPayload[port.String()] = msg
}

func (packet *Packet) addExtraText(port meshtasticpb.PortNum, text string) {
	if packet.ExtraText == nil {
		packet.ExtraText = make(map[string]string)
	}
	packet.ExtraText[port.String()] = text
}

func (packet *Packet) addTraceroute(route *meshtasticpb.RouteDiscovery) {
	if route == nil {
		return
	}
	packet.DecodedPortPayload[meshtasticpb.PortNum_TRACEROUTE_APP.String()] = route
	packet.Traceroutes = append(packet.Traceroutes, route)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func hardwareModelName(model meshtasticpb.HardwareModel) string {
	code := strings.TrimSpace(model.String())
	if code == "" || code == "UNSET" || code == "UNKNOWN" {
		return ""
	}
	return humanizeWords(code)
}

func roleDisplayName(role meshtasticpb.Config_DeviceConfig_Role) string {
	code := strings.TrimSpace(role.String())
	if code == "" {
		return ""
	}
	return humanizeWords(code)
}

func regionCodeString(region meshtasticpb.Config_LoRaConfig_RegionCode) string {
	code := strings.TrimSpace(region.String())
	if code == "" || code == "UNSET" {
		return ""
	}
	return code
}

func regionDisplayName(region meshtasticpb.Config_LoRaConfig_RegionCode) string {
	code := regionCodeString(region)
	if code == "" {
		return ""
	}
	return humanizeWords(code)
}

func modemPresetCode(preset meshtasticpb.Config_LoRaConfig_ModemPreset) string {
	code := strings.TrimSpace(preset.String())
	return code
}

func modemPresetName(preset meshtasticpb.Config_LoRaConfig_ModemPreset) string {
	code := modemPresetCode(preset)
	if code == "" {
		return ""
	}
	return humanizeWords(code)
}

func humanizeWords(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	replaced := strings.ReplaceAll(value, "_", " ")
	lowered := strings.ToLower(replaced)
	parts := strings.Fields(lowered)
	for i, part := range parts {
		if containsDigit(part) || len(part) <= 3 {
			parts[i] = strings.ToUpper(part)
		} else {
			runes := []rune(part)
			runes[0] = unicode.ToUpper(runes[0])
			parts[i] = string(runes)
		}
	}
	return strings.Join(parts, " ")
}

func containsDigit(s string) bool {
	for _, r := range s {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}
