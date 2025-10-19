package grpcserver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"testing"
	"time"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/meshpipe/v1"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufConnSize = 1 << 20

func TestMeshpipeDataServiceEndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/meshpipe_test.db"
	db, _ := seedSampleDatabase(t, dbPath)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	service := newService(db, logger, 200)

	const token = "secret-token"
	lis := bufconn.Listen(bufConnSize)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(tokenUnaryInterceptor(token)),
		grpc.StreamInterceptor(tokenStreamInterceptor(token)),
	)
	meshpipev1.RegisterMeshpipeDataServer(grpcServer, service)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc serve: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(
		ctx,
		"bufconn",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := meshpipev1.NewMeshpipeDataClient(conn)

	authCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))

	t.Run("dashboard", func(t *testing.T) {
		dash, err := client.GetDashboardStats(authCtx, &meshpipev1.DashboardRequest{})
		if err != nil {
			t.Fatalf("GetDashboardStats: %v", err)
		}
		if dash.GetTotalNodes() != 2 {
			t.Fatalf("total nodes mismatch: want 2, got %d", dash.GetTotalNodes())
		}
		if dash.GetTotalPackets() != 4 {
			t.Fatalf("total packets mismatch: want 4, got %d", dash.GetTotalPackets())
		}
		if dash.GetRecentPackets() != 3 {
			t.Fatalf("recent packets mismatch: want 3, got %d", dash.GetRecentPackets())
		}
		if dash.GetActiveNodes_24H() != 2 {
			t.Fatalf("active nodes mismatch: want 2, got %d", dash.GetActiveNodes_24H())
		}
		if math.Abs(dash.GetSuccessRate()-75.0) > 0.001 {
			t.Fatalf("success rate mismatch: want 75%%, got %.2f", dash.GetSuccessRate())
		}
		if len(dash.GetPacketTypes()) != 4 {
			t.Fatalf("expected 4 packet type buckets, got %d", len(dash.GetPacketTypes()))
		}
	})

	t.Run("list packets with pagination", func(t *testing.T) {
		all, err := client.ListPackets(authCtx, &meshpipev1.ListPacketsRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 10},
		})
		if err != nil {
			t.Fatalf("ListPackets (all): %v", err)
		}
		if len(all.GetPackets()) != 4 {
			t.Fatalf("expected 4 packets in dataset, got %d", len(all.GetPackets()))
		}

		first, err := client.ListPackets(authCtx, &meshpipev1.ListPacketsRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 2},
		})
		if err != nil {
			t.Fatalf("ListPackets first page: %v", err)
		}
		if len(first.GetPackets()) != 2 {
			t.Fatalf("expected 2 packets on first page, got %d", len(first.GetPackets()))
		}
		if first.GetPackets()[0].GetId() != 4 || first.GetPackets()[1].GetId() != 3 {
			t.Fatalf("unexpected packet order on first page: ids %d, %d", first.GetPackets()[0].GetId(), first.GetPackets()[1].GetId())
		}
		if first.GetNextCursor() == "" {
			t.Fatalf("expected cursor for second page")
		}

		second, err := client.ListPackets(authCtx, &meshpipev1.ListPacketsRequest{
			Pagination: &meshpipev1.Pagination{
				PageSize: 2,
				Cursor:   first.GetNextCursor(),
			},
		})
		if err != nil {
			t.Fatalf("ListPackets second page: %v", err)
		}
		if len(second.GetPackets()) == 0 {
			t.Fatalf("expected at least one packet on second page, got 0")
		}
		if second.GetPackets()[0].GetId() != 2 {
			t.Fatalf("expected oldest packet id 2 on second page, got %d", second.GetPackets()[0].GetId())
		}
		if second.GetNextCursor() != "" {
			t.Fatalf("expected empty cursor after second page, got %q", second.GetNextCursor())
		}

		withPayload, err := client.ListPackets(authCtx, &meshpipev1.ListPacketsRequest{
			Pagination:     &meshpipev1.Pagination{PageSize: 1},
			IncludePayload: true,
		})
		if err != nil {
			t.Fatalf("ListPackets include payload: %v", err)
		}
		if len(withPayload.GetPackets()) != 1 {
			t.Fatalf("expected single packet, got %d", len(withPayload.GetPackets()))
		}
		if len(withPayload.GetPackets()[0].GetRawPayload()) == 0 {
			t.Fatalf("expected raw payload to be populated")
		}
	})

	t.Run("stream packets", func(t *testing.T) {
		stream, err := client.StreamPackets(authCtx, &meshpipev1.ListPacketsRequest{
			IncludePayload: false,
		})
		if err != nil {
			t.Fatalf("StreamPackets: %v", err)
		}
		var count int
		for {
			pkt, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				t.Fatalf("stream recv: %v", recvErr)
			}
			if count == 0 && pkt.GetId() != 4 {
				t.Fatalf("expected first streamed packet to be id 4, got %d", pkt.GetId())
			}
			count++
		}
		if count != 4 {
			t.Fatalf("expected 4 streamed packets, got %d", count)
		}
	})

	t.Run("list nodes and get node", func(t *testing.T) {
		nodes, err := client.ListNodes(authCtx, &meshpipev1.ListNodesRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 10},
		})
		if err != nil {
			t.Fatalf("ListNodes: %v", err)
		}
		if nodes.GetTotal() != 2 {
			t.Fatalf("expected total=2, got %d", nodes.GetTotal())
		}
		var found bool
		for _, node := range nodes.GetNodes() {
			switch node.GetNodeId() {
			case 0x1234:
				found = true
				if node.GetTotalPackets() != 3 {
					t.Fatalf("node 0x1234 total packets: want 3, got %d", node.GetTotalPackets())
				}
				if node.GetUniqueGateways() != 1 {
					t.Fatalf("node 0x1234 unique gateways: want 1, got %d", node.GetUniqueGateways())
				}
			}
		}
		if !found {
			t.Fatalf("failed to find node 0x1234 in response")
		}

		one, err := client.GetNode(authCtx, &meshpipev1.GetNodeRequest{NodeId: 0x1234})
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if one.GetNode().GetNodeId() != 0x1234 {
			t.Fatalf("expected node id 0x1234, got %d", one.GetNode().GetNodeId())
		}
		if one.GetNode().GetTotalPackets() != 3 {
			t.Fatalf("unexpected total packets for node: %d", one.GetNode().GetTotalPackets())
		}
	})

	t.Run("gateway stats and links", func(t *testing.T) {
		stats, err := client.GetGatewayStats(authCtx, &meshpipev1.GatewayFilter{})
		if err != nil {
			t.Fatalf("GetGatewayStats: %v", err)
		}
		if len(stats.GetStats()) != 1 {
			t.Fatalf("expected one gateway stat, got %d", len(stats.GetStats()))
		}
		if stats.GetStats()[0].GetPacketsTotal() != 4 {
			t.Fatalf("gateway packets total mismatch: want 4, got %d", stats.GetStats()[0].GetPacketsTotal())
		}

		links, err := client.ListLinks(authCtx, &meshpipev1.ListLinksRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 5},
		})
		if err != nil {
			t.Fatalf("ListLinks: %v", err)
		}
		if len(links.GetLinks()) != 1 {
			t.Fatalf("expected single link aggregate, got %d", len(links.GetLinks()))
		}
		link := links.GetLinks()[0]
		if link.GetPacketsTotal() != 1 {
			t.Fatalf("link packets total mismatch: got %d", link.GetPacketsTotal())
		}
		if link.GetMaxHopIndex() != 1 || link.GetMaxHopLimit() != 3 {
			t.Fatalf("unexpected hop stats: index=%d limit=%d", link.GetMaxHopIndex(), link.GetMaxHopLimit())
		}
	})

	t.Run("traceroute summaries", func(t *testing.T) {
		tr, err := client.ListTraceroutes(authCtx, &meshpipev1.ListTraceroutesRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 5},
		})
		if err != nil {
			t.Fatalf("ListTraceroutes: %v", err)
		}
		if len(tr.GetPaths()) != 1 {
			t.Fatalf("expected single traceroute path, got %d", len(tr.GetPaths()))
		}
		path := tr.GetPaths()[0]
		if path.GetMaxHops() != 3 {
			t.Fatalf("expected max hops = 3, got %d", path.GetMaxHops())
		}
		if path.GetObservations() != 1 {
			t.Fatalf("expected observations = 1, got %d", path.GetObservations())
		}
	})

	t.Run("module tables", func(t *testing.T) {
		rt, err := client.ListRangeTests(authCtx, &meshpipev1.ListRangeTestsRequest{})
		if err != nil {
			t.Fatalf("ListRangeTests: %v", err)
		}
		if len(rt.GetResults()) != 1 || len(rt.GetResults()[0].GetPayload()) == 0 {
			t.Fatalf("unexpected range test payload: %+v", rt.GetResults())
		}

		sf, err := client.ListStoreForward(authCtx, &meshpipev1.ListStoreForwardRequest{})
		if err != nil {
			t.Fatalf("ListStoreForward: %v", err)
		}
		if len(sf.GetEvents()) != 1 || sf.GetEvents()[0].GetVariant() != "router_stats" {
			t.Fatalf("unexpected store-forward variant: %+v", sf.GetEvents())
		}

		pc, err := client.ListPaxcounter(authCtx, &meshpipev1.ListPaxcounterRequest{})
		if err != nil {
			t.Fatalf("ListPaxcounter: %v", err)
		}
		if len(pc.GetSamples()) != 1 || pc.GetSamples()[0].GetWifi() != 11 || pc.GetSamples()[0].GetBle() != 6 {
			t.Fatalf("unexpected paxcounter sample: %+v", pc.GetSamples())
		}
	})

	t.Run("unauthorized requests rejected", func(t *testing.T) {
		_, err := client.GetDashboardStats(ctx, &meshpipev1.DashboardRequest{})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected permission denied without token, got %v", err)
		}
	})
}

func TestTokenInterceptors(t *testing.T) {
	const token = "super"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	if err := authorize(ctx, token); err != nil {
		t.Fatalf("authorize should succeed: %v", err)
	}

	err := authorize(context.Background(), token)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated for missing metadata, got %v", err)
	}

	md := metadata.New(map[string]string{"authorization": "Bearer wrong"})
	err = authorize(metadata.NewIncomingContext(context.Background(), md), token)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied for wrong token, got %v", err)
	}
}

type seededTimestamps struct {
	Recent float64
	Old    float64
	Range  float64
	Trace  float64
}

func seedSampleDatabase(t *testing.T, path string) (*sql.DB, seededTimestamps) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	writer, err := storage.NewSQLiteWriter(storage.SQLiteConfig{Path: path, QueueSize: 8})
	if err != nil {
		t.Fatalf("new sqlite writer: %v", err)
	}
	if err := writer.Start(ctx); err != nil {
		t.Fatalf("start writer: %v", err)
	}
	cancel()
	if err := writer.Stop(); err != nil {
		t.Fatalf("stop writer: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	now := time.Now().UTC()
	ts := seededTimestamps{
		Recent: toSeconds(now.Add(-10 * time.Minute)),
		Old:    toSeconds(now.Add(-2 * time.Hour)),
		Range:  toSeconds(now.Add(-5 * time.Minute)),
		Trace:  toSeconds(now.Add(-3 * time.Minute)),
	}

	exec := func(query string, args ...any) {
		if _, execErr := db.Exec(query, args...); execErr != nil {
			t.Fatalf("exec %q with %v: %v", query, args, execErr)
		}
	}

	exec(`INSERT INTO node_info (node_id, user_id, hex_id, long_name, short_name, hw_model, hw_model_name, role, role_name, first_seen, last_updated, firmware_version, region, region_name, primary_channel, modem_preset_name)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		0x1234, "!node1", "!node1", "Gateway One", "G1", 5, "Heltec", 3, "Router", ts.Old, ts.Range, "v2.1.0", "EU_868", "EU 868", "LongFast", "Short Fast")

	exec(`INSERT INTO node_info (node_id, user_id, hex_id, long_name, short_name, hw_model_name, role_name, first_seen, last_updated, primary_channel)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		0x5678, "!node2", "!node2", "Client Two", "C2", "Generic", "Client", ts.Old, ts.Recent, "LongFast")

	insertPacket := func(id int64, timestamp float64, from, to uint32, portNum int32, portName, msgType string, processed bool, rssi int32, snr float64) {
		payload := []byte{byte(id), 0xAB, 0xCD}
		exec(`INSERT INTO packet_history (id, timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id, channel_id, channel_name, mesh_packet_id, rssi, snr, hop_limit, hop_start, payload_length, raw_payload, processed_successfully, message_type)
		      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id,
			timestamp,
			fmt.Sprintf("msh/test/%d", id),
			from,
			to,
			portNum,
			portName,
			"gw-1",
			"LongFast",
			"LongFast",
			100+id,
			rssi,
			snr,
			2,
			3,
			len(payload),
			payload,
			boolToInt(processed),
			msgType,
		)
	}

	insertPacket(1, ts.Recent, 0x1234, 0xFFFF, 1, "Text message", "t", true, -70, 9.5)
	insertPacket(2, ts.Old, 0x5678, 0xFFFF, 4, "Telemetry", "m", false, -80, 7.0)
	insertPacket(3, ts.Range, 0x1234, 0x5678, 10, "Range test", "r", true, -65, 10.2)
	insertPacket(4, ts.Trace, 0x1234, 0x5678, 11, "Traceroute", "tr", true, -60, 8.8)

	exec(`INSERT INTO range_test_results (packet_id, text, raw_payload) VALUES (?, ?, ?)`,
		3, "RANGE_OK", []byte("RANGE"))

	exec(`INSERT INTO store_forward_events (packet_id, request_response, variant, messages_total, messages_saved, messages_max, uptime_seconds, requests_total, requests_history, heartbeat_flag, return_max, return_window, raw_payload)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1, "stats", "router_stats", 10, 8, 50, 777, 3, 2, 1, 12, 30, []byte{0x01})

	exec(`INSERT INTO paxcounter_samples (packet_id, wifi, ble, uptime_seconds, raw_payload)
	      VALUES (?, ?, ?, ?, ?)`,
		2, 11, 6, 3600, []byte{0x02})

	exec(`INSERT INTO link_history (packet_id, gateway_id, from_node_id, to_node_id, hop_index, hop_limit, rssi, snr, channel_id, channel_name, received_at)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		3, "gw-1", 0x1234, 0x5678, 1, 3, -68, 8.1, "LongFast", "LongFast", ts.Range)

	exec(`INSERT INTO gateway_node_stats (gateway_id, node_id, first_seen, last_seen, packets_total, last_rssi, last_snr)
	      VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"gw-1", 0x1234, ts.Old, ts.Range, 3, -65, 9.0)

	exec(`INSERT INTO gateway_node_stats (gateway_id, node_id, first_seen, last_seen, packets_total, last_rssi, last_snr)
	      VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"gw-1", 0x5678, ts.Old, ts.Old, 1, -80, 7.0)

	exec(`INSERT INTO neighbor_history (packet_id, origin_node_id, neighbor_node_id, snr, last_rx_time, broadcast_interval, gateway_id, channel_id, received_at)
	      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		2, 0x1234, 0x5678, 9.4, 1200, 60, "gw-1", "LongFast", ts.Old)

	insertHop := func(index int, nodeID uint32, snr float64) {
		exec(`INSERT INTO traceroute_hops (packet_id, gateway_id, request_id, origin_node_id, destination_node_id, direction, hop_index, hop_node_id, snr, received_at)
		      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			4, "gw-1", 555, 0x1234, 0x5678, "towards", index, nodeID, snr, ts.Trace)
	}

	insertHop(0, 0x1234, 9.3)
	insertHop(1, 0x2222, 8.4)
	insertHop(2, 0x5678, 7.9)

	return db, ts
}

func toSeconds(ts time.Time) float64 {
	return float64(ts.UnixNano()) / 1e9
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
