package pipeline

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func TestPipelineStoresPacketVariants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := t.TempDir() + "/capture.db"
	writer, err := storage.NewSQLiteWriter(storage.SQLiteConfig{Path: dbPath, QueueSize: 32})
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.Start(ctx); err != nil {
		t.Fatalf("start writer: %v", err)
	}
	defer func() {
		if err := writer.Stop(); err != nil {
			t.Errorf("stop writer: %v", err)
		}
	}()

	defaultKey := base64.StdEncoding.EncodeToString(bytesRepeating(0x42, 32))
	decoder := decode.NewMeshtasticDecoder(decode.MeshtasticConfig{
		StoreRawEnvelope: true,
		DefaultKeyBase64: defaultKey,
	})

	client := newIntegrationStubClient()
	pipe := New(client, decoder, writer)

	errCh := make(chan error, 1)
	go func() {
		if err := pipe.Run(ctx); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	<-client.started

	makeTopic := func(msgType string) string {
		return fmt.Sprintf("msh/test/1/%s/LongFast/!12345678", msgType)
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("e"),
		Payload: buildServiceEnvelope(t, buildNodeInfoData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("map"),
		Payload: buildServiceEnvelope(t, buildMapReportData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("n"),
		Payload: buildServiceEnvelope(t, buildNeighborInfoData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("t"),
		Payload: buildServiceEnvelope(t, buildTextMessageData()),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("p"),
		Payload: buildServiceEnvelope(t, buildPositionData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("m"),
		Payload: buildServiceEnvelope(t, buildTelemetryData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("r"),
		Payload: buildServiceEnvelope(t, buildRangeTestData()),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("sf"),
		Payload: buildServiceEnvelope(t, buildStoreForwardData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("tr"),
		Payload: buildServiceEnvelope(t, buildTracerouteData(t)),
		Time:    time.Now(),
	}

	client.messages <- mqtt.Message{
		Topic:   makeTopic("pc"),
		Payload: buildServiceEnvelope(t, buildPaxcounterData(t)),
		Time:    time.Now(),
	}

	waitFor(t, func() error {
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			return err
		}
		defer db.Close()
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM packet_history`).Scan(&count); err != nil {
			return err
		}
		if count < 10 {
			return fmt.Errorf("expected packet_history >= 10, got %d", count)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM text_messages`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("text payload not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM positions`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("position payload not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("telemetry payload not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM node_info`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("node not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM range_test_results`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("range test not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM store_forward_events`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("store-forward not stored yet")
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM paxcounter_samples`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("paxcounter not stored yet")
		}
		return nil
	})

	cancel()
	<-errCh

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	row := db.QueryRow(`SELECT portnum_name, message_type, channel_name FROM packet_history LIMIT 1`)
	var portName, msgType, chName string
	if err := row.Scan(&portName, &msgType, &chName); err != nil {
		t.Fatalf("scan packet: %v", err)
	}
	if portName != "Node info" {
		t.Fatalf("expected Node info, got %s", portName)
	}
	if msgType != "e" {
		t.Fatalf("expected message type 'e', got %s", msgType)
	}
	if chName != "LongFast" {
		t.Fatalf("expected channel name LongFast, got %s", chName)
	}

	var ts float64
	if err := db.QueryRow(`SELECT timestamp FROM packet_history LIMIT 1`).Scan(&ts); err != nil {
		t.Fatalf("scan timestamp: %v", err)
	}
	if ts > 1e12 {
		t.Fatalf("expected timestamp in seconds, got %f", ts)
	}
	if _, frac := math.Modf(ts); frac == 0 {
		t.Fatalf("expected fractional second precision, got %f", ts)
	}

	nodeRow := db.QueryRow(`SELECT user_id, hex_id, long_name, primary_channel, first_seen, last_updated, role_name, hw_model_name, region, region_name, firmware_version, modem_preset_name FROM node_info WHERE node_id = ?`, 0x1234)
	var userID, hexID, longName, primary, roleName, hwModelName, region, regionName, firmwareVersion, modemPresetName string
	var firstSeen, lastUpdated float64
	if err := nodeRow.Scan(&userID, &hexID, &longName, &primary, &firstSeen, &lastUpdated, &roleName, &hwModelName, &region, &regionName, &firmwareVersion, &modemPresetName); err != nil {
		t.Fatalf("scan node_info: %v", err)
	}
	if userID != "!12345678" || hexID != "!12345678" || longName != "Gateway Node" {
		t.Fatalf("unexpected node info: user=%s hex=%s long=%s", userID, hexID, longName)
	}
	if primary != "LongFast" {
		t.Fatalf("expected primary_channel LongFast, got %s", primary)
	}
	if firstSeen <= 0 || lastUpdated <= 0 {
		t.Fatalf("expected timestamps to be populated, got first=%f last=%f", firstSeen, lastUpdated)
	}
	if lastUpdated < firstSeen {
		t.Fatalf("expected last_updated >= first_seen, got first=%f last=%f", firstSeen, lastUpdated)
	}
	if roleName != "Router" {
		t.Fatalf("expected role name Router, got %s", roleName)
	}
	if hwModelName != "Tbeam" {
		t.Fatalf("expected hardware name Tbeam, got %s", hwModelName)
	}
	if region != "EU_868" {
		t.Fatalf("expected region code EU_868, got %s", region)
	}
	if regionName != "EU 868" {
		t.Fatalf("expected region name EU 868, got %s", regionName)
	}
	if firmwareVersion != "2.1.0" {
		t.Fatalf("expected firmware version 2.1.0, got %s", firmwareVersion)
	}
	if modemPresetName != "Short Fast" {
		t.Fatalf("expected modem preset Short Fast, got %s", modemPresetName)
	}

	linkRow := db.QueryRow(`SELECT gateway_id, from_node_id, to_node_id, hop_index FROM link_history ORDER BY id DESC LIMIT 1`)
	var linkGateway string
	var linkFrom, linkTo, hopIndex int64
	if err := linkRow.Scan(&linkGateway, &linkFrom, &linkTo, &hopIndex); err != nil {
		t.Fatalf("scan link_history: %v", err)
	}
	if linkGateway != "!gateway" {
		t.Fatalf("expected link gateway !gateway, got %s", linkGateway)
	}
	if linkFrom != 0x1234 {
		t.Fatalf("expected link from 0x1234, got %d", linkFrom)
	}
	if hopIndex < 0 {
		t.Fatalf("expected non-negative hop index, got %d", hopIndex)
	}

	statsRow := db.QueryRow(`SELECT packets_total, last_seen FROM gateway_node_stats WHERE gateway_id = ? AND node_id = ?`, "!gateway", 0x1234)
	var packetsTotal int64
	var lastSeen float64
	if err := statsRow.Scan(&packetsTotal, &lastSeen); err != nil {
		t.Fatalf("scan gateway_node_stats: %v", err)
	}
	if packetsTotal < 6 {
		t.Fatalf("expected packets_total >= 6, got %d", packetsTotal)
	}
	if lastSeen <= 0 {
		t.Fatalf("expected last_seen > 0")
	}

	viewRow := db.QueryRow(`SELECT packets_total, distinct_nodes FROM gateway_stats WHERE gateway_id = ?`, "!gateway")
	var viewPackets, distinctNodes int64
	if err := viewRow.Scan(&viewPackets, &distinctNodes); err != nil {
		t.Fatalf("scan gateway_stats view: %v", err)
	}
	if viewPackets != packetsTotal {
		t.Fatalf("expected packets_total %d, got %d", packetsTotal, viewPackets)
	}
	if distinctNodes < 1 {
		t.Fatalf("expected at least 1 distinct node, got %d", distinctNodes)
	}

	neighborRow := db.QueryRow(`SELECT origin_node_id, neighbor_node_id FROM neighbor_history ORDER BY id DESC LIMIT 1`)
	var originID, neighborID int64
	if err := neighborRow.Scan(&originID, &neighborID); err != nil {
		t.Fatalf("scan neighbor_history: %v", err)
	}
	if originID != 0x1234 || neighborID != 0x5678 {
		t.Fatalf("unexpected neighbor entry origin=%d neighbor=%d", originID, neighborID)
	}

	hopCountRow := db.QueryRow(`SELECT COUNT(*) FROM traceroute_hops WHERE direction = 'towards'`)
	var hopCount int
	if err := hopCountRow.Scan(&hopCount); err != nil {
		t.Fatalf("scan traceroute_hops count: %v", err)
	}
	if hopCount != 3 {
		t.Fatalf("expected 3 traceroute hops, got %d", hopCount)
	}

	longestRow := db.QueryRow(`SELECT max_hops FROM traceroute_longest_paths LIMIT 1`)
	var longestHops int
	if err := longestRow.Scan(&longestHops); err != nil {
		t.Fatalf("scan traceroute_longest_paths: %v", err)
	}
	if longestHops != 3 {
		t.Fatalf("expected longest traceroute hops = 3, got %d", longestHops)
	}

	rangeRow := db.QueryRow(`SELECT text FROM range_test_results LIMIT 1`)
	var rangeText string
	if err := rangeRow.Scan(&rangeText); err != nil {
		t.Fatalf("scan range_test_results: %v", err)
	}
	if rangeText != "RANGE_OK" {
		t.Fatalf("unexpected range test text: %s", rangeText)
	}

	storeRow := db.QueryRow(`SELECT request_response, variant, messages_total, heartbeat_flag FROM store_forward_events LIMIT 1`)
	var rr, variant string
	var messagesTotal, heartbeatFlag int64
	if err := storeRow.Scan(&rr, &variant, &messagesTotal, &heartbeatFlag); err != nil {
		t.Fatalf("scan store_forward_events: %v", err)
	}
	if rr != meshtasticpb.StoreAndForward_ROUTER_STATS.String() {
		t.Fatalf("unexpected store forward rr: %s", rr)
	}
	if variant != "stats" {
		t.Fatalf("unexpected store forward variant: %s", variant)
	}
	if messagesTotal != 10 {
		t.Fatalf("unexpected store forward messages total: %d", messagesTotal)
	}
	if heartbeatFlag != 1 {
		t.Fatalf("expected heartbeat flag to be 1, got %d", heartbeatFlag)
	}

	paxRow := db.QueryRow(`SELECT wifi, ble, uptime_seconds FROM paxcounter_samples LIMIT 1`)
	var wifi, ble, uptime int64
	if err := paxRow.Scan(&wifi, &ble, &uptime); err != nil {
		t.Fatalf("scan paxcounter_samples: %v", err)
	}
	if wifi != 12 || ble != 5 || uptime != 3600 {
		t.Fatalf("unexpected paxcounter sample wifi=%d ble=%d uptime=%d", wifi, ble, uptime)
	}

	textRow := db.QueryRow(`SELECT text, want_response, compressed FROM text_messages LIMIT 1`)
	var textPayload string
	var wantResponse, compressed int
	if err := textRow.Scan(&textPayload, &wantResponse, &compressed); err != nil {
		t.Fatalf("scan text message: %v", err)
	}
	if textPayload != "hello world" {
		t.Fatalf("expected text payload 'hello world', got %s", textPayload)
	}
	if wantResponse != 1 || compressed != 0 {
		t.Fatalf("unexpected text flags want=%d compressed=%d", wantResponse, compressed)
	}

	posRow := db.QueryRow(`SELECT latitude, longitude, altitude, time, timestamp FROM positions LIMIT 1`)
	var lat, lon sql.NullFloat64
	var alt sql.NullInt64
	var posTime, posTimestamp int64
	if err := posRow.Scan(&lat, &lon, &alt, &posTime, &posTimestamp); err != nil {
		t.Fatalf("scan position: %v", err)
	}
	if !lat.Valid || !lon.Valid {
		t.Fatalf("expected latitude/longitude to be set")
	}
	if lat.Float64 != 12.3456789 || lon.Float64 != 23.456789 {
		t.Fatalf("unexpected coordinates lat=%f lon=%f", lat.Float64, lon.Float64)
	}
	if !alt.Valid || alt.Int64 != 543 {
		t.Fatalf("unexpected altitude: %+v", alt)
	}
	if posTime != 111 || posTimestamp != 222 {
		t.Fatalf("unexpected position timestamps time=%d timestamp=%d", posTime, posTimestamp)
	}

	telemetryRow := db.QueryRow(`SELECT LENGTH(raw_payload) FROM telemetry LIMIT 1`)
	var rawLen sql.NullInt64
	if err := telemetryRow.Scan(&rawLen); err != nil {
		t.Fatalf("scan telemetry: %v", err)
	}
	if !rawLen.Valid || rawLen.Int64 == 0 {
		t.Fatalf("expected telemetry raw payload to be stored")
	}
}

func waitFor(t *testing.T, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := fn()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func buildServiceEnvelope(t *testing.T, data *meshtasticpb.Data) []byte {
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
		t.Fatalf("marshal envelope: %v", err)
	}
	return payload
}

func buildNodeInfoData(t *testing.T) *meshtasticpb.Data {
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

func buildMapReportData(t *testing.T) *meshtasticpb.Data {
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

func buildNeighborInfoData(t *testing.T) *meshtasticpb.Data {
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

func buildTextMessageData() *meshtasticpb.Data {
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

func buildPositionData(t *testing.T) *meshtasticpb.Data {
	t.Helper()
	pos := &meshtasticpb.Position{
		LatitudeI:  proto.Int32(123456789),
		LongitudeI: proto.Int32(234567890),
		Altitude:   proto.Int32(543),
		Time:       111,
		Timestamp:  222,
	}
	payload, err := proto.Marshal(pos)
	if err != nil {
		t.Fatalf("marshal position: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_POSITION_APP,
		Payload: payload,
	}
}

func buildTelemetryData(t *testing.T) *meshtasticpb.Data {
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

func buildRangeTestData() *meshtasticpb.Data {
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_RANGE_TEST_APP,
		Payload: []byte("RANGE_OK"),
	}
}

func buildStoreForwardData(t *testing.T) *meshtasticpb.Data {
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

func buildPaxcounterData(t *testing.T) *meshtasticpb.Data {
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

func buildTracerouteData(t *testing.T) *meshtasticpb.Data {
	t.Helper()
	tr := &meshtasticpb.RouteDiscovery{
		Route:      []uint32{0x1234, 0x2000, 0x3000},
		SnrTowards: []int32{40, 36, 32},
		RouteBack:  []uint32{0x3000, 0x2000, 0x1234},
		SnrBack:    []int32{28, 24, 20},
	}
	payload, err := proto.Marshal(tr)
	if err != nil {
		t.Fatalf("marshal traceroute: %v", err)
	}
	return &meshtasticpb.Data{
		Portnum: meshtasticpb.PortNum_TRACEROUTE_APP,
		Payload: payload,
	}
}

func bytesRepeating(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

type integrationStubClient struct {
	messages chan mqtt.Message
	errs     chan error
	started  chan struct{}
	stopOnce sync.Once
}

func newIntegrationStubClient() *integrationStubClient {
	return &integrationStubClient{
		messages: make(chan mqtt.Message, 2),
		errs:     make(chan error, 1),
		started:  make(chan struct{}),
	}
}

func (s *integrationStubClient) Start(context.Context) error {
	close(s.started)
	return nil
}

func (s *integrationStubClient) Stop() {
	s.stopOnce.Do(func() {
		close(s.messages)
		close(s.errs)
	})
}

func (s *integrationStubClient) Messages() <-chan mqtt.Message { return s.messages }
func (s *integrationStubClient) Errors() <-chan error          { return s.errs }
