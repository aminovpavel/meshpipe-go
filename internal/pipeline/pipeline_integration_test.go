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

func TestPipelineStoresPacketAndNodeInfo(t *testing.T) {
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

	envelope := buildServiceEnvelope(t, buildNodeInfoPacket(t))
	client.messages <- mqtt.Message{
		Topic:   "msh/test/1/e/LongFast/!12345678",
		Payload: envelope,
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
		if count == 0 {
			return fmt.Errorf("packet not stored yet")
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

	nodeRow := db.QueryRow(`SELECT user_id, long_name FROM node_info WHERE node_id = ?`, 0x1234)
	var userID, longName string
	if err := nodeRow.Scan(&userID, &longName); err != nil {
		t.Fatalf("scan node_info: %v", err)
	}
	if userID != "!12345678" || longName != "Test Node" {
		t.Fatalf("unexpected node info: user=%s long=%s", userID, longName)
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

func buildServiceEnvelope(t *testing.T, data []byte) []byte {
	t.Helper()
	env := &meshtasticpb.ServiceEnvelope{
		ChannelId: "LongFast",
		GatewayId: "!gateway",
		Packet: &meshtasticpb.MeshPacket{
			Id:       123,
			From:     0x55AA,
			To:       0xFFFF,
			Priority: meshtasticpb.MeshPacket_DEFAULT,
			PayloadVariant: &meshtasticpb.MeshPacket_Decoded{
				Decoded: &meshtasticpb.Data{
					Portnum: meshtasticpb.PortNum_NODEINFO_APP,
					Payload: data,
				},
			},
		},
	}
	payload, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return payload
}

func buildNodeInfoPacket(t *testing.T) []byte {
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
	return data
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
