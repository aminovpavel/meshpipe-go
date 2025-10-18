package pipeline

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	meshtasticpb "github.com/aminovpavel/mw-malla-capture/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
	"github.com/aminovpavel/mw-malla-capture/internal/storage"
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
		if count < 4 {
			return fmt.Errorf("expected packet_history >= 4, got %d", count)
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
	if portName != "NODEINFO_APP" {
		t.Fatalf("expected NODEINFO_APP, got %s", portName)
	}
	if msgType != "e" {
		t.Fatalf("expected message type 'e', got %s", msgType)
	}
	if chName != "LongFast" {
		t.Fatalf("expected channel name LongFast, got %s", chName)
	}

	nodeRow := db.QueryRow(`SELECT user_id, hex_id, long_name, primary_channel, first_seen, last_updated FROM node_info WHERE node_id = ?`, 0x1234)
	var userID, hexID, longName, primary string
	var firstSeen, lastUpdated float64
	if err := nodeRow.Scan(&userID, &hexID, &longName, &primary, &firstSeen, &lastUpdated); err != nil {
		t.Fatalf("scan node_info: %v", err)
	}
	if userID != "!12345678" || hexID != "!12345678" || longName != "Test Node" {
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
