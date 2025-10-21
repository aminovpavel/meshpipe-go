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

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/v1"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufConnSize = 1 << 20

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		state := conn.GetState()
		if state == connectivity.Idle {
			conn.Connect()
		}
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("connection state %s without context error", state.String())
		}
	}
}

func TestMeshpipeDataServiceEndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/meshpipe_test.db"
	db, _ := seedSampleDatabase(t, dbPath)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	t.Setenv("MESHPIPE_VERSION", "v0.2.0-test")
	t.Setenv("MESHPIPE_GIT_SHA", "4314a53")
	t.Setenv("MESHPIPE_BUILD_DATE", "2025-10-19T09:20:52Z")
	service := newService(db, logger, 200, nil, 0, "")

	const token = "secret-token"
	lis := bufconn.Listen(bufConnSize)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(tokenUnaryInterceptor(token)),
		grpc.StreamInterceptor(tokenStreamInterceptor(token)),
	)
	meshpipev1.RegisterMeshpipeDataServer(grpcServer, service)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	const target = "passthrough:///bufconn"
	conn, err := grpc.NewClient(
		target,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := waitForReady(ctx, conn); err != nil {
		t.Fatalf("grpc conn not ready: %v", err)
	}

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

	t.Run("list packets aggregation", func(t *testing.T) {
		resp, err := client.ListPackets(authCtx, &meshpipev1.ListPacketsRequest{
			Pagination:  &meshpipev1.Pagination{PageSize: 10},
			Aggregation: &meshpipev1.MeshPacketAggregationOptions{Enabled: true},
		})
		if err != nil {
			t.Fatalf("ListPackets with aggregation: %v", err)
		}
		if len(resp.GetMeshPacketAggregates()) == 0 {
			t.Fatalf("expected mesh packet aggregates in response")
		}
		var found bool
		for _, agg := range resp.GetMeshPacketAggregates() {
			if agg.GetMeshPacketId() == 103 {
				found = true
				if agg.GetReceptionCount() != 1 {
					t.Fatalf("expected reception count 1, got %d", agg.GetReceptionCount())
				}
				if agg.GetGatewayCount() != 1 {
					t.Fatalf("expected gateway count 1, got %d", agg.GetGatewayCount())
				}
				if agg.GetMinRssi() != -68 || agg.GetMaxRssi() != -68 {
					t.Fatalf("unexpected RSSI bounds: min %d max %d", agg.GetMinRssi(), agg.GetMaxRssi())
				}
				if agg.GetMinHopCount() != 1 || agg.GetMaxHopCount() != 1 {
					t.Fatalf("unexpected hop bounds: min %d max %d", agg.GetMinHopCount(), agg.GetMaxHopCount())
				}
				if agg.GetFirstReceivedAt() == nil || agg.GetLastReceivedAt() == nil {
					t.Fatalf("expected first/last received timestamps")
				}
			}
		}
		if !found {
			t.Fatalf("expected aggregate for mesh_packet_id 103")
		}
	})

	t.Run("list node locations", func(t *testing.T) {
		locations, err := client.ListNodeLocations(authCtx, &meshpipev1.ListNodeLocationsRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 5},
		})
		if err != nil {
			t.Fatalf("ListNodeLocations: %v", err)
		}
		if len(locations.GetLocations()) != 1 {
			t.Fatalf("expected single node location, got %d", len(locations.GetLocations()))
		}
		loc := locations.GetLocations()[0]
		if loc.GetNodeId() != 0x1234 {
			t.Fatalf("expected node id 0x1234, got 0x%x", loc.GetNodeId())
		}
		if math.Abs(loc.GetLatitude()-37.7749) > 0.0001 || math.Abs(loc.GetLongitude()-(-122.4194)) > 0.0001 {
			t.Fatalf("unexpected coordinates: lat %.5f lon %.5f", loc.GetLatitude(), loc.GetLongitude())
		}
		if loc.GetSatsInView() != 8 {
			t.Fatalf("expected sats_in_view 8, got %d", loc.GetSatsInView())
		}
		if loc.GetPrecisionMeters() <= 0 {
			t.Fatalf("expected positive precision meters, got %.2f", loc.GetPrecisionMeters())
		}
		if loc.GetDisplayName() == "" {
			t.Fatalf("expected display name to be populated")
		}
		if loc.GetAgeHours() < 0 {
			t.Fatalf("expected non-negative age hours, got %.2f", loc.GetAgeHours())
		}
	})

	t.Run("chat window", func(t *testing.T) {
		resp, err := client.GetChatWindow(authCtx, &meshpipev1.GetChatWindowRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 5},
		})
		if err != nil {
			t.Fatalf("GetChatWindow: %v", err)
		}
		if len(resp.GetMessages()) != 1 {
			t.Fatalf("expected single chat message, got %d", len(resp.GetMessages()))
		}
		msg := resp.GetMessages()[0]
		if msg.GetPacketId() == 0 || msg.GetText() == "" {
			t.Fatalf("unexpected chat message: %+v", msg)
		}
		if resp.GetCounters().GetMessages_24H() == 0 {
			t.Fatalf("expected 24h chat counter to be non-zero")
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

	t.Run("node analytics", func(t *testing.T) {
		resp, err := client.GetNodeAnalytics(authCtx, &meshpipev1.GetNodeAnalyticsRequest{NodeId: 0x1234})
		if err != nil {
			t.Fatalf("GetNodeAnalytics: %v", err)
		}
		if resp.GetAnalytics().GetPackets_24H() == 0 {
			t.Fatalf("expected packets_24h to be populated")
		}
		if len(resp.GetAnalytics().GetGateways()) == 0 {
			t.Fatalf("expected gateway metrics in analytics")
		}
		if len(resp.GetAnalytics().GetNeighbors()) == 0 {
			t.Fatalf("expected neighbor metrics in analytics")
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

	t.Run("gateway overview", func(t *testing.T) {
		overview, err := client.GetGatewayOverview(authCtx, &meshpipev1.GetGatewayOverviewRequest{})
		if err != nil {
			t.Fatalf("GetGatewayOverview: %v", err)
		}
		if len(overview.GetGateways()) == 0 {
			t.Fatalf("expected at least one gateway overview entry")
		}
		if overview.GetGateways()[0].GetPacketCount() == 0 {
			t.Fatalf("expected packet count in gateway overview")
		}
	})

	t.Run("analytics summary", func(t *testing.T) {
		summary, err := client.GetAnalyticsSummary(authCtx, &meshpipev1.GetAnalyticsSummaryRequest{})
		if err != nil {
			t.Fatalf("GetAnalyticsSummary: %v", err)
		}
		if summary.GetPacketSuccess().GetTotalPackets() == 0 {
			t.Fatalf("expected packet success stats to be populated")
		}
		if len(summary.GetHourlyPackets()) != 24 {
			t.Fatalf("expected 24 temporal buckets, got %d", len(summary.GetHourlyPackets()))
		}
		if len(summary.GetGatewayDistribution()) == 0 {
			t.Fatalf("expected gateway distribution entries")
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

	t.Run("traceroute hops", func(t *testing.T) {
		hops, err := client.ListTracerouteHops(authCtx, &meshpipev1.ListTracerouteHopsRequest{
			Pagination: &meshpipev1.Pagination{PageSize: 10},
		})
		if err != nil {
			t.Fatalf("ListTracerouteHops: %v", err)
		}
		if len(hops.GetHops()) != 3 {
			t.Fatalf("expected 3 traceroute hops, got %d", len(hops.GetHops()))
		}
	})

	t.Run("traceroute graph", func(t *testing.T) {
		graph, err := client.GetTracerouteGraph(authCtx, &meshpipev1.TracerouteGraphRequest{})
		if err != nil {
			t.Fatalf("GetTracerouteGraph: %v", err)
		}
		if len(graph.GetNodes()) == 0 || len(graph.GetEdges()) == 0 {
			t.Fatalf("expected traceroute graph to include nodes and edges")
		}
	})

	t.Run("traceroute packets rpc", func(t *testing.T) {
		initial, err := client.ListTraceroutePackets(authCtx, &meshpipev1.ListTraceroutePacketsRequest{
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("ListTraceroutePackets initial: %v", err)
		}
		if initial.GetTotalCount() != 1 {
			t.Fatalf("expected single traceroute packet in seed, got %d", initial.GetTotalCount())
		}
		if len(initial.GetPackets()) != 1 {
			t.Fatalf("expected packet slice of length 1, got %d", len(initial.GetPackets()))
		}
		if initial.GetPackets()[0].GetGatewayId() != "gw-1" {
			t.Fatalf("unexpected gateway id for seed traceroute: %q", initial.GetPackets()[0].GetGatewayId())
		}

		filtered, err := client.ListTraceroutePackets(authCtx, &meshpipev1.ListTraceroutePacketsRequest{
			Filter: &meshpipev1.TraceroutePacketFilter{
				FromNodeId:                0x1234,
				ProcessedSuccessfullyOnly: true,
			},
			Limit: 5,
		})
		if err != nil {
			t.Fatalf("ListTraceroutePackets filtered: %v", err)
		}
		if filtered.GetTotalCount() != 1 || len(filtered.GetPackets()) != 1 {
			t.Fatalf("filtered traceroute mismatch: %+v", filtered)
		}

		extraID := int64(5)
		extraTS := toSeconds(time.Now().UTC())
		extraPayload := []byte{0x05, 0xBE, 0xEF, 0x99}

		if _, err := db.Exec(`INSERT INTO packet_history (
            id, timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id, channel_id, channel_name,
            mesh_packet_id, rssi, snr, hop_limit, hop_start, payload_length, raw_payload, processed_successfully, message_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			extraID,
			extraTS,
			fmt.Sprintf("msh/test/%d", extraID),
			0x1234,
			0x5678,
			70,
			"TRACEROUTE_APP",
			"gw-2",
			"LongFast",
			"LongFast",
			100+extraID,
			-55,
			9.1,
			1,
			1,
			len(extraPayload),
			extraPayload,
			boolToInt(true),
			"tr",
		); err != nil {
			t.Fatalf("insert extra traceroute packet: %v", err)
		}

		if _, err := db.Exec(`INSERT INTO traceroute_hops (
            packet_id, gateway_id, request_id, origin_node_id, destination_node_id, direction, hop_index, hop_node_id, snr, received_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			extraID, "gw-2", 777, 0x1234, 0x5678, "towards", 0, 0x1234, 9.0, extraTS); err != nil {
			t.Fatalf("insert traceroute hop 0: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO traceroute_hops (
            packet_id, gateway_id, request_id, origin_node_id, destination_node_id, direction, hop_index, hop_node_id, snr, received_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			extraID, "gw-2", 777, 0x1234, 0x5678, "towards", 1, 0x5678, 8.6, extraTS); err != nil {
			t.Fatalf("insert traceroute hop 1: %v", err)
		}

		pageOne, err := client.ListTraceroutePackets(authCtx, &meshpipev1.ListTraceroutePacketsRequest{
			Limit:    1,
			OrderBy:  "timestamp",
			OrderDir: "desc",
		})
		if err != nil {
			t.Fatalf("ListTraceroutePackets page one: %v", err)
		}
		if pageOne.GetTotalCount() != 2 || len(pageOne.GetPackets()) != 1 {
			t.Fatalf("unexpected pagination totals after insert: %+v", pageOne)
		}
		if pageOne.GetPackets()[0].GetId() != uint64(extraID) {
			t.Fatalf("expected newest traceroute id %d, got %d", extraID, pageOne.GetPackets()[0].GetId())
		}
		if !pageOne.GetHasMore() {
			t.Fatalf("expected has_more on first page with limit 1")
		}

		pageTwo, err := client.ListTraceroutePackets(authCtx, &meshpipev1.ListTraceroutePacketsRequest{
			Limit:    1,
			Offset:   1,
			OrderBy:  "timestamp",
			OrderDir: "desc",
		})
		if err != nil {
			t.Fatalf("ListTraceroutePackets page two: %v", err)
		}
		// Note: hasMore should be false for the last page.
		if pageTwo.GetTotalCount() != 2 || len(pageTwo.GetPackets()) != 1 {
			t.Fatalf("unexpected second page stats: %+v", pageTwo)
		}
		if pageTwo.GetPackets()[0].GetId() != 4 {
			t.Fatalf("expected original traceroute id 4 on second page, got %d", pageTwo.GetPackets()[0].GetId())
		}
		if pageTwo.GetHasMore() {
			t.Fatalf("did not expect has_more on final page")
		}

		details, err := client.GetTracerouteDetails(authCtx, &meshpipev1.GetTracerouteDetailsRequest{PacketId: uint64(extraID)})
		if err != nil {
			t.Fatalf("GetTracerouteDetails extra: %v", err)
		}
		if details.GetPacket().GetHopStart() != 1 || details.GetPacket().GetHopLimit() != 1 {
			t.Fatalf("expected hop start/limit 1 for extra packet, got %d/%d", details.GetPacket().GetHopStart(), details.GetPacket().GetHopLimit())
		}
		if len(details.GetHops()) != 2 {
			t.Fatalf("expected two hops in details, got %d", len(details.GetHops()))
		}

		direct, err := client.ListNodeDirectReceptions(authCtx, &meshpipev1.ListNodeDirectReceptionsRequest{
			NodeId:    0x1234,
			Direction: "sent",
			Limit:     5,
		})
		if err != nil {
			t.Fatalf("ListNodeDirectReceptions: %v", err)
		}
		if len(direct.GetReceptions()) == 0 {
			t.Fatalf("expected at least one direct reception after inserting traceroute packet")
		}
		if direct.GetReceptions()[0].GetPacketId() != uint64(extraID) {
			t.Fatalf("unexpected packet id in direct reception: %+v", direct.GetReceptions()[0])
		}

		names, err := client.ListNodeNames(authCtx, &meshpipev1.ListNodeNamesRequest{
			NodeIds: []uint32{0x1234, 0x5678},
		})
		if err != nil {
			t.Fatalf("ListNodeNames: %v", err)
		}
		if len(names.GetEntries()) != 2 {
			t.Fatalf("expected two node name entries, got %d", len(names.GetEntries()))
		}

		channels, err := client.ListPrimaryChannels(authCtx, &meshpipev1.ListPrimaryChannelsRequest{})
		if err != nil {
			t.Fatalf("ListPrimaryChannels: %v", err)
		}
		var foundLongFast bool
		for _, ch := range channels.GetChannels() {
			if ch == "LongFast" {
				foundLongFast = true
				break
			}
		}
		if !foundLongFast {
			t.Fatalf("expected LongFast in primary channels: %v", channels.GetChannels())
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

	t.Run("healthz", func(t *testing.T) {
		health, err := client.Healthz(authCtx, &meshpipev1.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("Healthz: %v", err)
		}
		if !health.GetReady() {
			t.Fatalf("expected health ready")
		}
	})

	t.Run("version", func(t *testing.T) {
		version, err := client.GetVersion(authCtx, &meshpipev1.GetVersionRequest{})
		if err != nil {
			t.Fatalf("GetVersion: %v", err)
		}
		if version.GetVersion() != "v0.2.0-test" || version.GetGitSha() != "4314a53" {
			t.Fatalf("unexpected version info: %+v", version)
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
	insertPacket(4, ts.Trace, 0x1234, 0x5678, 70, "TRACEROUTE_APP", "tr", true, -60, 8.8)

	exec(`INSERT INTO text_messages (packet_id, text, want_response, dest, source, request_id, reply_id, emoji, bitfield, compressed)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1, "Hello from node 0x1234", 0, 0, 0, 0, 0, 0, 0, 0)

	exec(`INSERT INTO positions (
        packet_id, latitude, longitude, altitude, time, timestamp, raw_payload,
        precision_bits, gps_accuracy, pdop, hdop, vdop, sats_in_view, fix_quality, fix_type, ground_speed, ground_track, next_update, seq_number
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1,
		37.7749, -122.4194,
		15,
		uint32(ts.Range),
		uint32(ts.Range),
		[]byte{0x10},
		10,
		3000,
		150,
		80,
		70,
		8,
		2,
		3,
		35,
		1200,
		60,
		42,
	)

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
