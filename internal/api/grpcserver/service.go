package grpcserver

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/meshpipe/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type service struct {
	meshpipev1.UnimplementedMeshpipeDataServer

	db          *sql.DB
	logger      *slog.Logger
	maxPageSize int
}

func newService(db *sql.DB, logger *slog.Logger, maxPageSize int) *service {
	return &service{
		db:          db,
		logger:      logger.With(slog.String("component", "grpc-service")),
		maxPageSize: maxPageSize,
	}
}

func (s *service) GetDashboardStats(ctx context.Context, req *meshpipev1.DashboardRequest) (*meshpipev1.DashboardResponse, error) {
	const nodesQuery = `SELECT COUNT(*) FROM node_info`

	var totalNodes uint64
	if err := s.db.QueryRowContext(ctx, nodesQuery).Scan(&totalNodes); err != nil {
		return nil, status.Errorf(codes.Internal, "query total nodes: %v", err)
	}

	now := time.Now().UTC()
	oneHourAgo := float64(now.Add(-1 * time.Hour).Unix())
	oneDayAgo := float64(now.Add(-24 * time.Hour).Unix())

	gatewayFilter := ""
	var gatewayArgs []any
	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		gatewayFilter = " AND gateway_id = ?"
		gatewayArgs = append(gatewayArgs, gw)
	}

	statsArgs := []any{oneHourAgo, oneDayAgo}
	statsArgs = append(statsArgs, gatewayArgs...)

	const statsQuery = `
	    SELECT
	        COUNT(*) AS total_packets,
	        COUNT(DISTINCT CASE WHEN from_node_id IS NOT NULL THEN from_node_id END) AS active_nodes_24h,
	        COUNT(CASE WHEN timestamp > ? THEN 1 END) AS recent_packets,
	        AVG(CASE WHEN rssi IS NOT NULL AND rssi != 0 THEN rssi END) AS avg_rssi,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	        SUM(CASE WHEN processed_successfully = 1 THEN 1 ELSE 0 END) AS successful_packets
	    FROM packet_history
	    WHERE timestamp > ?` + "%s"

	var (
		totalPackets        sql.NullInt64
		activeNodes24h      sql.NullInt64
		recentPackets       sql.NullInt64
		avgRSSI             sql.NullFloat64
		avgSNR              sql.NullFloat64
		successfulPackets   sql.NullInt64
		totalPacketsAllTime sql.NullInt64
	)

	statsQueryFinal := fmt.Sprintf(statsQuery, gatewayFilter)
	if err := s.db.QueryRowContext(ctx, statsQueryFinal, statsArgs...).Scan(
		&totalPackets,
		&activeNodes24h,
		&recentPackets,
		&avgRSSI,
		&avgSNR,
		&successfulPackets,
	); err != nil {
		return nil, status.Errorf(codes.Internal, "query dashboard stats: %v", err)
	}

	totalAllTimeQuery := `SELECT COUNT(*) FROM packet_history WHERE 1=1` + gatewayFilter
	if err := s.db.QueryRowContext(ctx, totalAllTimeQuery, gatewayArgs...).Scan(&totalPacketsAllTime); err != nil {
		return nil, status.Errorf(codes.Internal, "query total packets: %v", err)
	}

	packetTypeArgs := []any{oneDayAgo}
	packetTypeArgs = append(packetTypeArgs, gatewayArgs...)
	packetTypesQuery := `
	    SELECT portnum_name, COUNT(*) AS count
	    FROM packet_history
	    WHERE portnum_name IS NOT NULL AND timestamp > ?` + gatewayFilter + `
	    GROUP BY portnum_name
	    ORDER BY count DESC
	    LIMIT 50`

	rows, err := s.db.QueryContext(ctx, packetTypesQuery, packetTypeArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query packet types: %v", err)
	}
	defer rows.Close()

	var packetTypes []*meshpipev1.PacketTypeCount
	for rows.Next() {
		var (
			name  sql.NullString
			count sql.NullInt64
		)
		if err := rows.Scan(&name, &count); err != nil {
			return nil, status.Errorf(codes.Internal, "scan packet type: %v", err)
		}
		packetTypes = append(packetTypes, &meshpipev1.PacketTypeCount{
			PortnumName: name.String,
			Count:       uint64(max64(count.Int64, 0)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate packet types: %v", err)
	}

	total := uint64FromNull(totalPacketsAllTime)
	successful := uint64FromNull(successfulPackets)
	successRate := 0.0
	if total > 0 {
		successRate = float64(successful) * 100.0 / float64(total)
	}

	return &meshpipev1.DashboardResponse{
		TotalNodes:      totalNodes,
		TotalPackets:    total,
		ActiveNodes_24H: uint64FromNull(activeNodes24h),
		RecentPackets:   uint64FromNull(recentPackets),
		AvgRssi:         avgRSSI.Float64,
		AvgSnr:          avgSNR.Float64,
		SuccessRate:     successRate,
		PacketTypes:     packetTypes,
	}, nil
}

func (s *service) ListPackets(ctx context.Context, req *meshpipev1.ListPacketsRequest) (*meshpipev1.ListPacketsResponse, error) {
	packets, cursor, err := s.queryPackets(ctx, req)
	if err != nil {
		return nil, err
	}
	return &meshpipev1.ListPacketsResponse{
		Packets:    packets,
		NextCursor: cursor,
	}, nil
}

func (s *service) StreamPackets(req *meshpipev1.ListPacketsRequest, stream meshpipev1.MeshpipeData_StreamPacketsServer) error {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)
	includePayload := req.GetIncludePayload()

	where := []string{"1=1"}
	args := make([]any, 0, 12)

	if filter != nil {
		if ts := filter.GetStartTime(); ts != nil {
			where = append(where, "timestamp >= ?")
			args = append(args, timestampToFloat(ts))
		}
		if ts := filter.GetEndTime(); ts != nil {
			where = append(where, "timestamp <= ?")
			args = append(args, timestampToFloat(ts))
		}
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "gateway_id = ?")
			args = append(args, gw)
		}
		if from := filter.GetFromNodeId(); from != 0 {
			where = append(where, "from_node_id = ?")
			args = append(args, from)
		}
		if to := filter.GetToNodeId(); to != 0 {
			where = append(where, "to_node_id = ?")
			args = append(args, to)
		}
		if chans := strings.TrimSpace(filter.GetChannelId()); chans != "" {
			where = append(where, "channel_id = ?")
			args = append(args, chans)
		}
		if portNames := filter.GetPortnumNames(); len(portNames) > 0 {
			placeholders := make([]string, 0, len(portNames))
			for _, pn := range portNames {
				if pn = strings.TrimSpace(pn); pn != "" {
					placeholders = append(placeholders, "?")
					args = append(args, pn)
				}
			}
			if len(placeholders) > 0 {
				where = append(where, fmt.Sprintf("portnum_name IN (%s)", strings.Join(placeholders, ",")))
			}
		}
		if hop := filter.GetHopCount(); hop != 0 {
			where = append(where, "(hop_start IS NOT NULL AND hop_limit IS NOT NULL AND (hop_start - hop_limit) = ?)")
			args = append(args, hop)
		}
		if search := strings.TrimSpace(filter.GetSearch()); search != "" {
			pattern := "%" + search + "%"
			where = append(where, `(gateway_id LIKE ? OR channel_id LIKE ? OR portnum_name LIKE ? OR CAST(from_node_id AS TEXT) LIKE ? OR CAST(to_node_id AS TEXT) LIKE ?)`)
			args = append(args, pattern, pattern, pattern, pattern, pattern)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(timestamp < ? OR (timestamp = ? AND id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	columns := []string{
		"id",
		"timestamp",
		"from_node_id",
		"to_node_id",
		"portnum_name",
		"gateway_id",
		"channel_id",
		"hop_start",
		"hop_limit",
		"rssi",
		"snr",
		"mesh_packet_id",
		"processed_successfully",
		"payload_length",
	}
	if includePayload {
		columns = append(columns, "raw_payload")
	}

	query := fmt.Sprintf(`SELECT %s FROM packet_history WHERE %s ORDER BY timestamp DESC, id DESC LIMIT ?`,
		strings.Join(columns, ", "),
		strings.Join(where, " AND "),
	)

	args = append(args, limit)

	rows, err := s.db.QueryContext(stream.Context(), query, args...)
	if err != nil {
		return status.Errorf(codes.Internal, "query packets stream: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id         int64
			timestamp  sql.NullFloat64
			fromNode   sql.NullInt64
			toNode     sql.NullInt64
			portName   sql.NullString
			gatewayID  sql.NullString
			channelID  sql.NullString
			hopStart   sql.NullInt64
			hopLimit   sql.NullInt64
			rssi       sql.NullInt64
			snr        sql.NullFloat64
			meshID     sql.NullInt64
			proc       sql.NullInt64
			payloadLen sql.NullInt64
			rawPayload []byte
		)

		dest := []any{
			&id,
			&timestamp,
			&fromNode,
			&toNode,
			&portName,
			&gatewayID,
			&channelID,
			&hopStart,
			&hopLimit,
			&rssi,
			&snr,
			&meshID,
			&proc,
			&payloadLen,
		}
		if includePayload {
			dest = append(dest, &rawPayload)
		}

		if err := rows.Scan(dest...); err != nil {
			return status.Errorf(codes.Internal, "scan packet row: %v", err)
		}

		pkt := &meshpipev1.Packet{
			Id:                    uint64(id),
			Timestamp:             toTimestamp(timestamp),
			FromNodeId:            uint32FromNull(fromNode),
			ToNodeId:              uint32FromNull(toNode),
			PortnumName:           portName.String,
			GatewayId:             gatewayID.String,
			ChannelId:             channelID.String,
			HopStart:              int32(hopStart.Int64),
			HopLimit:              int32(hopLimit.Int64),
			Rssi:                  int32(rssi.Int64),
			Snr:                   snr.Float64,
			MeshPacketId:          uint32FromNull(meshID),
			ProcessedSuccessfully: boolFromInt(proc),
			PayloadLength:         uint32FromNull(payloadLen),
		}

		if hopStart.Valid && hopLimit.Valid {
			pkt.HopCount = int32(hopStart.Int64 - hopLimit.Int64)
		}
		if includePayload && rawPayload != nil {
			pkt.RawPayload = append([]byte{}, rawPayload...)
		}

		if err := stream.Send(pkt); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "iterate packet stream: %v", err)
	}
	return nil
}

func (s *service) ListNodes(ctx context.Context, req *meshpipev1.ListNodesRequest) (*meshpipev1.ListNodesResponse, error) {
	nodes, cursor, total, err := s.queryNodes(ctx, req)
	if err != nil {
		return nil, err
	}
	return &meshpipev1.ListNodesResponse{
		Nodes:      nodes,
		NextCursor: cursor,
		Total:      total,
	}, nil
}

func (s *service) GetNode(ctx context.Context, req *meshpipev1.GetNodeRequest) (*meshpipev1.GetNodeResponse, error) {
	if req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	query := `
	    WITH node_stats AS (
	        SELECT
	            from_node_id AS node_id,
	            COUNT(*) AS total_packets,
	            COUNT(DISTINCT to_node_id) AS unique_destinations,
	            COUNT(DISTINCT gateway_id) AS unique_gateways,
	            MIN(timestamp) AS first_seen,
	            MAX(timestamp) AS last_seen,
	            AVG(CASE WHEN rssi IS NOT NULL AND rssi != 0 THEN rssi END) AS avg_rssi,
	            AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	            AVG(CASE WHEN hop_start IS NOT NULL AND hop_limit IS NOT NULL THEN hop_start - hop_limit END) AS avg_hops
	        FROM packet_history
	        WHERE from_node_id IS NOT NULL
	        GROUP BY from_node_id
	    )
	    SELECT
	        ni.node_id,
	        ni.hex_id,
	        ni.long_name,
        ni.short_name,
        ni.hw_model,
        ni.hw_model_name,
        ni.role,
        ni.role_name,
        ni.region,
        ni.region_name,
        ni.modem_preset,
        ni.modem_preset_name,
        ns.total_packets,
        ns.unique_destinations,
        ns.unique_gateways,
        ns.first_seen,
        ns.last_seen,
        ns.avg_rssi,
        ns.avg_snr,
        ns.avg_hops
	    FROM node_info ni
	    LEFT JOIN node_stats ns ON ns.node_id = ni.node_id
	    WHERE ni.node_id = ?`

	row := s.db.QueryRowContext(ctx, query, req.GetNodeId())
	node, err := scanNode(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query node: %v", err)
	}
	return &meshpipev1.GetNodeResponse{Node: node}, nil
}

func (s *service) GetGatewayStats(ctx context.Context, req *meshpipev1.GatewayFilter) (*meshpipev1.GatewayStatsResponse, error) {
	query := `
	    SELECT gateway_id, packets_total, distinct_nodes, first_seen, last_seen, avg_rssi, avg_snr
	    FROM gateway_stats`
	var args []any
	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		query += " WHERE gateway_id = ?"
		args = append(args, gw)
	}
	query += " ORDER BY last_seen DESC NULLS LAST"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query gateway stats: %v", err)
	}
	defer rows.Close()

	var stats []*meshpipev1.GatewayStat
	for rows.Next() {
		var (
			gatewayID     sql.NullString
			totalPackets  sql.NullInt64
			distinctNodes sql.NullInt64
			firstSeen     sql.NullFloat64
			lastSeen      sql.NullFloat64
			avgRSSI       sql.NullFloat64
			avgSNR        sql.NullFloat64
		)
		if err := rows.Scan(&gatewayID, &totalPackets, &distinctNodes, &firstSeen, &lastSeen, &avgRSSI, &avgSNR); err != nil {
			return nil, status.Errorf(codes.Internal, "scan gateway stats: %v", err)
		}
		stats = append(stats, &meshpipev1.GatewayStat{
			GatewayId:     gatewayID.String,
			PacketsTotal:  uint64(max64(totalPackets.Int64, 0)),
			DistinctNodes: uint32(max64(distinctNodes.Int64, 0)),
			FirstSeen:     toTimestamp(firstSeen),
			LastSeen:      toTimestamp(lastSeen),
			AvgRssi:       avgRSSI.Float64,
			AvgSnr:        avgSNR.Float64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate gateway stats: %v", err)
	}
	return &meshpipev1.GatewayStatsResponse{Stats: stats}, nil
}

func (s *service) ListLinks(ctx context.Context, req *meshpipev1.ListLinksRequest) (*meshpipev1.ListLinksResponse, error) {
	response, err := s.queryLinks(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *service) ListTraceroutes(ctx context.Context, req *meshpipev1.ListTraceroutesRequest) (*meshpipev1.ListTraceroutesResponse, error) {
	response, err := s.queryTraceroutes(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *service) ListRangeTests(ctx context.Context, req *meshpipev1.ListRangeTestsRequest) (*meshpipev1.ListRangeTestsResponse, error) {
	response, err := s.queryRangeTests(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *service) ListStoreForward(ctx context.Context, req *meshpipev1.ListStoreForwardRequest) (*meshpipev1.ListStoreForwardResponse, error) {
	response, err := s.queryStoreForward(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *service) ListPaxcounter(ctx context.Context, req *meshpipev1.ListPaxcounterRequest) (*meshpipev1.ListPaxcounterResponse, error) {
	response, err := s.queryPaxcounter(ctx, req)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Helper methods ------------------------------------------------------------

type packetCursor struct {
	Timestamp float64 `json:"ts"`
	ID        int64   `json:"id"`
}

func encodeCursor(ts float64, id int64) string {
	if ts <= 0 && id <= 0 {
		return ""
	}
	cur := packetCursor{Timestamp: ts, ID: id}
	payload, _ := json.Marshal(cur)
	return base64.StdEncoding.EncodeToString(payload)
}

func decodeCursor(raw string) (float64, int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, 0, nil
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return 0, 0, fmt.Errorf("decode cursor: %w", err)
	}
	var cur packetCursor
	if err := json.Unmarshal(data, &cur); err != nil {
		return 0, 0, fmt.Errorf("decode cursor json: %w", err)
	}
	return cur.Timestamp, cur.ID, nil
}

func limitPageSize(requested uint32, max int) int {
	if requested == 0 {
		if max < 100 {
			return max
		}
		return 100
	}
	if requested > uint32(max) {
		return max
	}
	return int(requested)
}

func toTimestamp(value sql.NullFloat64) *timestamppb.Timestamp {
	if !value.Valid {
		return nil
	}
	return floatToTimestamp(value.Float64)
}

func floatToTimestamp(value float64) *timestamppb.Timestamp {
	if value <= 0 {
		return nil
	}
	sec := int64(value)
	nsec := int64((value - float64(sec)) * 1e9)
	if nsec < 0 {
		nsec = 0
	}
	return timestamppb.New(time.Unix(sec, nsec).UTC())
}

func int64ToUint32(v sql.NullInt64) uint32 {
	if !v.Valid {
		return 0
	}
	if v.Int64 < 0 {
		return 0
	}
	if v.Int64 > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v.Int64)
}

func max64(value int64, fallback int64) int64 {
	if value < fallback {
		return fallback
	}
	return value
}

func uint64FromNull(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	return uint64(value.Int64)
}

func uint32FromNull(value sql.NullInt64) uint32 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	if value.Int64 > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value.Int64)
}

func boolFromInt(value sql.NullInt64) bool {
	return value.Valid && value.Int64 != 0
}
