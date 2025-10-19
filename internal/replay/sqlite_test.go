package replay

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
	"google.golang.org/protobuf/proto"
)

func TestReplaySQLite(t *testing.T) {
	ctx := context.Background()

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source.db")
	targetPath := filepath.Join(tempDir, "target.db")

	logger := observability.NoOpLogger()
	metrics := observability.NewMetrics(observability.WithNamespace("test_replay"))

	sourceWriter, err := storage.NewSQLiteWriter(
		storage.SQLiteConfig{Path: sourcePath, QueueSize: 64},
		storage.WithLogger(logger),
		storage.WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("new source writer: %v", err)
	}
	if err := sourceWriter.Start(ctx); err != nil {
		t.Fatalf("start source writer: %v", err)
	}

	decoder := decode.NewMeshtasticDecoder(decode.MeshtasticConfig{
		StoreRawEnvelope: true,
		DefaultKeyBase64: "",
	})

	msg := mqtt.Message{
		Topic:    "msh/test/1/e/LongFast/!12345678",
		Payload:  buildServiceEnvelope(t, buildNodeInfoData(t)),
		Time:     time.Now().UTC(),
		QoS:      0,
		Retained: false,
	}

	packet, err := decoder.Decode(ctx, msg)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}

	if err := sourceWriter.Store(ctx, packet); err != nil {
		t.Fatalf("store packet: %v", err)
	}
	if err := sourceWriter.Stop(); err != nil {
		t.Fatalf("stop source writer: %v", err)
	}

	targetWriter, err := storage.NewSQLiteWriter(
		storage.SQLiteConfig{Path: targetPath, QueueSize: 64},
		storage.WithLogger(logger),
		storage.WithMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("new target writer: %v", err)
	}
	if err := targetWriter.Start(ctx); err != nil {
		t.Fatalf("start target writer: %v", err)
	}
	t.Cleanup(func() {
		if err := targetWriter.Stop(); err != nil {
			t.Errorf("stop target writer: %v", err)
		}
	})

	count, err := ReplaySQLite(ctx, sourcePath, decoder, targetWriter, Options{})
	if err != nil {
		t.Fatalf("replay sqlite: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected to replay 1 packet, got %d", count)
	}

	// Stop the writer before inspecting contents - Stop() waits for
	// background workers to drain the queue and close the connection.
	if err := targetWriter.Stop(); err != nil {
		t.Fatalf("stop target writer: %v", err)
	}

	db, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatalf("open target sqlite: %v", err)
	}
	defer db.Close()

	var (
		packetCount int
		nodeCount   int
	)
	if err := db.QueryRow(`SELECT COUNT(*) FROM packet_history`).Scan(&packetCount); err != nil {
		t.Fatalf("count packet_history: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM node_info`).Scan(&nodeCount); err != nil {
		t.Fatalf("count node_info: %v", err)
	}
	if packetCount != 1 {
		t.Fatalf("expected 1 packet in target db, got %d", packetCount)
	}
	if nodeCount != 1 {
		t.Fatalf("expected 1 node in target db, got %d", nodeCount)
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
