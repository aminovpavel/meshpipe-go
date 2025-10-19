package decode

import (
	"testing"
	"time"

	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"google.golang.org/protobuf/proto"
)

func TestSanitizePacketStatFallback(t *testing.T) {
	pkt := Packet{
		Topic:       "msh/msk/2/stat/!57eb6b28",
		MessageType: "stat",
	}

	sanitizePacket(&pkt)

	if pkt.ChannelID != "2" {
		t.Fatalf("expected channel id fallback to \"2\", got %q", pkt.ChannelID)
	}
	if pkt.ChannelName != "STAT" {
		t.Fatalf("expected channel name fallback to STAT, got %q", pkt.ChannelName)
	}
	if pkt.PortNumName != "Node stat" {
		t.Fatalf("expected portnum fallback to Node stat, got %q", pkt.PortNumName)
	}
}

func TestSanitizePacketUnknownDefaults(t *testing.T) {
	pkt := Packet{
		MessageType: "e",
	}
	sanitizePacket(&pkt)

	if pkt.PortNumName != "Unknown" {
		t.Fatalf("expected Unknown fallback, got %q", pkt.PortNumName)
	}
	if pkt.ChannelName != "" {
		t.Fatalf("expected empty channel name fallback for message type 'e', got %q", pkt.ChannelName)
	}
}

func TestSanitizePacketGatewayFallback(t *testing.T) {
	pkt := Packet{}
	sanitizePacket(&pkt)

	if pkt.GatewayID != unknownGatewayID {
		t.Fatalf("expected gateway fallback to %s, got %q", unknownGatewayID, pkt.GatewayID)
	}
}

func TestDecodeAdditionalPorts(t *testing.T) {
	decoder := MeshtasticDecoder{}
	now := time.Now()

	tests := []struct {
		name          string
		port          meshtasticpb.PortNum
		payload       []byte
		expectDecoded bool
		expectText    string
		expectError   bool
	}{
		{
			name:          "remote hardware",
			port:          meshtasticpb.PortNum_REMOTE_HARDWARE_APP,
			payload:       mustMarshal(&meshtasticpb.HardwareMessage{Type: meshtasticpb.HardwareMessage_WRITE_GPIOS}),
			expectDecoded: true,
		},
		{
			name:          "admin",
			port:          meshtasticpb.PortNum_ADMIN_APP,
			payload:       mustMarshal(&meshtasticpb.AdminMessage{SessionPasskey: []byte{0x01}}),
			expectDecoded: true,
		},
		{
			name:          "map report",
			port:          meshtasticpb.PortNum_MAP_REPORT_APP,
			payload:       mustMarshal(&meshtasticpb.MapReport{LongName: "Test"}),
			expectDecoded: true,
		},
		{
			name:       "reply text",
			port:       meshtasticpb.PortNum_REPLY_APP,
			payload:    []byte("pong"),
			expectText: "pong",
		},
		{
			name:        "audio frame",
			port:        meshtasticpb.PortNum_AUDIO_APP,
			payload:     []byte{0xc0, 0xde, 0xc2, 0x01, 0x02},
			expectError: false,
		},
		{
			name:        "zps invalid length",
			port:        meshtasticpb.PortNum_ZPS_APP,
			payload:     []byte{0x01},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkt := Packet{}
			data := &meshtasticpb.Data{
				Portnum: tt.port,
				Payload: tt.payload,
			}

			decoder.populateFromData(&pkt, data, now)

			if tt.expectError {
				if pkt.ParsingError == "" {
					t.Fatalf("expected parsing error for %s", tt.name)
				}
				return
			}

			if pkt.ParsingError != "" {
				t.Fatalf("unexpected parsing error for %s: %s", tt.name, pkt.ParsingError)
			}

			if tt.expectText != "" {
				if pkt.ExtraText == nil {
					t.Fatalf("expected extra text map for %s", tt.name)
				}
				if got := pkt.ExtraText[tt.port.String()]; got != tt.expectText {
					t.Fatalf("expected extra text %q, got %q", tt.expectText, got)
				}
			}

			if tt.expectDecoded {
				if pkt.DecodedPortPayload == nil {
					t.Fatalf("expected decoded payload map for %s", tt.name)
				}
				if _, ok := pkt.DecodedPortPayload[tt.port.String()]; !ok {
					t.Fatalf("expected decoded payload entry for %s", tt.name)
				}
			}
		})
	}
}

func TestMapReportUpdatesNode(t *testing.T) {
	decoder := MeshtasticDecoder{}
	now := time.Now()
	report := &meshtasticpb.MapReport{
		LongName:        "Relay",
		ShortName:       "RL",
		Role:            meshtasticpb.Config_DeviceConfig_REPEATER,
		HwModel:         meshtasticpb.HardwareModel_TBEAM,
		FirmwareVersion: "2.2.0",
		Region:          meshtasticpb.Config_LoRaConfig_US,
		ModemPreset:     meshtasticpb.Config_LoRaConfig_LONG_FAST,
	}
	payload := mustMarshal(report)
	data := &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_MAP_REPORT_APP,
		Payload: payload,
	}

	pkt := Packet{From: 0x9876, ChannelID: "Primary"}
	decoder.populateFromData(&pkt, data, now)

	if pkt.Node == nil {
		t.Fatalf("expected node to be created from map report")
	}
	if pkt.Node.NodeID != 0x9876 {
		t.Fatalf("expected node id 0x9876, got %d", pkt.Node.NodeID)
	}
	if pkt.Node.LongName != "Relay" || pkt.Node.ShortName != "RL" {
		t.Fatalf("unexpected names: long=%s short=%s", pkt.Node.LongName, pkt.Node.ShortName)
	}
	if pkt.Node.RoleName != "Repeater" {
		t.Fatalf("expected role name Repeater, got %s", pkt.Node.RoleName)
	}
	if pkt.Node.HWModelName != "Tbeam" {
		t.Fatalf("expected hardware name Tbeam, got %s", pkt.Node.HWModelName)
	}
	if pkt.Node.Region != "US" || pkt.Node.RegionName != "US" {
		t.Fatalf("unexpected region info: code=%s name=%s", pkt.Node.Region, pkt.Node.RegionName)
	}
	if pkt.Node.FirmwareVersion != "2.2.0" {
		t.Fatalf("expected firmware 2.2.0, got %s", pkt.Node.FirmwareVersion)
	}
	if pkt.Node.ModemPresetName != "Long Fast" {
		t.Fatalf("expected modem preset Long Fast, got %s", pkt.Node.ModemPresetName)
	}
	if pkt.Node.PrimaryChannel != "Primary" {
		t.Fatalf("expected primary channel Primary, got %s", pkt.Node.PrimaryChannel)
	}
}

func mustMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return data
}
