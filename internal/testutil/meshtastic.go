package testutil

import (
	"testing"

	"google.golang.org/protobuf/proto"

	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
)

// BytesRepeating creates a slice filled with a repeated byte.
func BytesRepeating(b byte, count int) []byte {
	buf := make([]byte, count)
	for i := range buf {
		buf[i] = b
	}
	return buf
}

// BuildServiceEnvelope marshals a service envelope with common defaults used in tests.
func BuildServiceEnvelope(t testing.TB, data *meshtasticpb.Data) []byte {
	t.Helper()
	env := &meshtasticpb.ServiceEnvelope{
		ChannelId: "LongFast",
		GatewayId: "!gateway",
		Packet: &meshtasticpb.MeshPacket{
			Id:       123,
			From:     0x1234,
			To:       0xFFFF,
			Priority: meshtasticpb.MeshPacket_DEFAULT,
			PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
				Decoded: data,
			},
		},
	}
	payload, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal service envelope: %v", err)
	}
	return payload
}

// BuildNodeInfoData produces a NODEINFO payload for tests.
func BuildNodeInfoData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	node := &meshtasticpb.NodeInfo{
		Num: 0x1234,
		User: &meshtasticpb.User{
			Id:         "!12345678",
			LongName:   "Test Node",
			ShortName:  "TN",
			HwModel:    meshtasticpb.HardwareModel_HELTEC_V3,
			Role:       meshtasticpb.Config_DeviceConfig_CLIENT,
			IsLicensed: true,
			Macaddr:    []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF},
		},
		Snr:                   12.5,
		LastHeard:             42,
		ViaMqtt:               true,
		Channel:               7,
		HopsAway:              proto.Uint32(1),
		IsFavorite:            true,
		IsIgnored:             false,
		IsKeyManuallyVerified: true,
	}
	data, err := proto.Marshal(node)
	if err != nil {
		t.Fatalf("marshal nodeinfo: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_NODEINFO_APP,
		Payload: data,
	}
}

// BuildMapReportData produces a MAP_REPORT payload.
func BuildMapReportData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	report := &meshtasticpb.MapReport{
		LongName:        "Gateway Node",
		ShortName:       "GW",
		Role:            meshtasticpb.Config_DeviceConfig_ROUTER,
		HwModel:         meshtasticpb.HardwareModel_TBEAM,
		FirmwareVersion: "2.1.0",
		Region:          meshtasticpb.Config_LoRaConfig_EU_868,
		ModemPreset:     meshtasticpb.Config_LoRaConfig_SHORT_FAST,
	}
	payload, err := proto.Marshal(report)
	if err != nil {
		t.Fatalf("marshal map report: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_MAP_REPORT_APP,
		Payload: payload,
	}
}

// BuildNeighborInfoData returns a NEIGHBORINFO payload.
func BuildNeighborInfoData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	neighborInfo := &meshtasticpb.NeighborInfo{
		NodeId:                    0x1234,
		NodeBroadcastIntervalSecs: 60,
		Neighbors: []*meshtasticpb.Neighbor{
			{
				NodeId:                    0x5678,
				Snr:                       9.5,
				LastRxTime:                1111,
				NodeBroadcastIntervalSecs: 30,
			},
		},
	}
	payload, err := proto.Marshal(neighborInfo)
	if err != nil {
		t.Fatalf("marshal neighbor info: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_NEIGHBORINFO_APP,
		Payload: payload,
	}
}

// BuildTextMessageData returns a text message payload.
func BuildTextMessageData() *meshtasticpb.Data {
	return &meshtasticpb.Data{
		Portnum:      meshtasticpb.PortNum_TEXT_MESSAGE_APP,
		Payload:      []byte("hello world"),
		WantResponse: true,
		Dest:         111,
		Source:       222,
		RequestId:    333,
		ReplyId:      444,
		Emoji:        12,
		Bitfield:     proto.Uint32(34),
	}
}

// BuildPositionData creates a dummy POSITION payload.
func BuildPositionData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	position := &meshtasticpb.Position{
		LatitudeI:  proto.Int32(123456789),
		LongitudeI: proto.Int32(234567890),
		Altitude:   proto.Int32(543),
		Time:       111,
		Timestamp:  222,
	}
	payload, err := proto.Marshal(position)
	if err != nil {
		t.Fatalf("marshal position: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_POSITION_APP,
		Payload: payload,
	}
}

// BuildTelemetryData produces a TELEMETRY payload.
func BuildTelemetryData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	tele := &meshtasticpb.Telemetry{
		Time: 333,
		Variant: &meshtasticpb.Telemetry_EnvironmentMetrics{
			EnvironmentMetrics: &meshtasticpb.EnvironmentMetrics{
				Temperature: proto.Float32(21.5),
			},
		},
	}
	payload, err := proto.Marshal(tele)
	if err != nil {
		t.Fatalf("marshal telemetry: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_TELEMETRY_APP,
		Payload: payload,
	}
}

// BuildRangeTestData returns a RANGE_TEST payload.
func BuildRangeTestData() *meshtasticpb.Data {
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_RANGE_TEST_APP,
		Payload: []byte("RANGE_OK"),
	}
}

// BuildStoreForwardData returns a STORE_FORWARD payload with history variant.
func BuildStoreForwardData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	msg := &meshtasticpb.StoreAndForward{
		Rr: meshtasticpb.StoreAndForward_ROUTER_STATS,
		Variant: &meshtasticpb.StoreAndForward_Stats{
			Stats: &meshtasticpb.StoreAndForward_Statistics{
				MessagesTotal:   10,
				MessagesSaved:   8,
				MessagesMax:     50,
				UpTime:          777,
				Requests:        3,
				RequestsHistory: 2,
				Heartbeat:       true,
				ReturnMax:       12,
				ReturnWindow:    30,
			},
		},
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal storeforward: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_STORE_FORWARD_APP,
		Payload: payload,
	}
}

// BuildTracerouteData produces a TRACEROUTE payload with a sample path.
func BuildTracerouteData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	trace := &meshtasticpb.RouteDiscovery{
		Route:      []uint32{0x1234, 0x4321, 0x5678},
		SnrTowards: []int32{40, 36, 32},
		RouteBack:  []uint32{0x5678, 0x4321, 0x1234},
		SnrBack:    []int32{28, 24, 20},
	}
	payload, err := proto.Marshal(trace)
	if err != nil {
		t.Fatalf("marshal traceroute: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_TRACEROUTE_APP,
		Payload: payload,
	}
}

// BuildPaxcounterData returns a PAXCOUNTER payload.
func BuildPaxcounterData(t testing.TB) *meshtasticpb.Data {
	t.Helper()
	pax := &meshtasticpb.Paxcount{
		Wifi:   12,
		Ble:    5,
		Uptime: 3600,
	}
	payload, err := proto.Marshal(pax)
	if err != nil {
		t.Fatalf("marshal paxcounter: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_PAXCOUNTER_APP,
		Payload: payload,
	}
}
