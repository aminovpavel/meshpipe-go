package decode

import (
	"context"
	"strings"

	meshtasticpb "github.com/aminovpavel/mw-malla-capture/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
	"google.golang.org/protobuf/proto"
)

// MeshtasticConfig controls how packets are decoded.
type MeshtasticConfig struct {
	StoreRawEnvelope bool
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
	}

	if d.cfg.StoreRawEnvelope && len(msg.Payload) > 0 {
		packet.RawServiceEnvelope = append([]byte(nil), msg.Payload...)
	}

	var env meshtasticpb.ServiceEnvelope
	if err := proto.Unmarshal(msg.Payload, &env); err != nil {
		packet.ParsingError = err.Error()
		return packet, nil
	}

	packet.ChannelID = env.GetChannelId()
	packet.GatewayID = env.GetGatewayId()

	mesh := env.GetPacket()
	if mesh == nil {
		packet.ParsingError = "missing mesh packet"
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
		packet.PortNum = int32(data.GetPortnum())
		packet.PortNumName = portNumName(data.GetPortnum())
		packet.Payload = append([]byte(nil), data.GetPayload()...)
	} else if encrypted := mesh.GetEncrypted(); encrypted != nil {
		packet.Payload = append([]byte(nil), encrypted...)
	}

	packet.PayloadLength = len(packet.Payload)
	packet.ProcessedSuccessfully = packet.ParsingError == ""

	return packet, nil
}

func extractMessageType(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func portNumName(port meshtasticpb.PortNum) string {
	if name, ok := meshtasticpb.PortNum_name[int32(port)]; ok {
		return name
	}
	return ""
}
