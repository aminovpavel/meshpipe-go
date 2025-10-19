package grpcserver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/meshpipe/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *service) queryPackets(ctx context.Context, req *meshpipev1.ListPacketsRequest) ([]*meshpipev1.Packet, string, error) {
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
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
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

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "query packets: %v", err)
	}
	defer rows.Close()

	type cursorInfo struct {
		ts float64
		id int64
	}

	var (
		results []*meshpipev1.Packet
		cursors []cursorInfo
	)

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
			return nil, "", status.Errorf(codes.Internal, "scan packet row: %v", err)
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

		results = append(results, pkt)
		cursors = append(cursors, cursorInfo{
			ts: timestamp.Float64,
			id: id,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", status.Errorf(codes.Internal, "iterate packets: %v", err)
	}

	nextCursor := ""
	if len(results) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.ts, last.id)
		results = results[:limit]
	}

	return results, nextCursor, nil
}

func (s *service) queryNodes(ctx context.Context, req *meshpipev1.ListNodesRequest) ([]*meshpipev1.Node, string, uint64, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	filterClauses := make([]string, 0, 6)
	filterArgs := make([]any, 0, 6)

	if filter != nil {
		if search := strings.TrimSpace(filter.GetSearch()); search != "" {
			pattern := "%" + search + "%"
			filterClauses = append(filterClauses, "(ni.long_name LIKE ? OR ni.short_name LIKE ? OR ni.hex_id LIKE ?)")
			filterArgs = append(filterArgs, pattern, pattern, pattern)
		}
		if role := strings.TrimSpace(filter.GetRole()); role != "" {
			filterClauses = append(filterClauses, "ni.role_name = ?")
			filterArgs = append(filterArgs, role)
		}
		if hw := strings.TrimSpace(filter.GetHardwareModel()); hw != "" {
			filterClauses = append(filterClauses, "ni.hw_model_name = ?")
			filterArgs = append(filterArgs, hw)
		}
		if ch := strings.TrimSpace(filter.GetPrimaryChannel()); ch != "" {
			filterClauses = append(filterClauses, "ni.primary_channel = ?")
			filterArgs = append(filterArgs, ch)
		}
		if filter.GetNamedOnly() {
			filterClauses = append(filterClauses, "(ni.long_name IS NOT NULL AND ni.long_name != '')")
		}
	}

	where := append([]string{}, filterClauses...)
	args := append([]any{}, filterArgs...)

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", 0, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(COALESCE(ns.last_seen, 0) < ? OR (COALESCE(ns.last_seen, 0) = ? AND ni.node_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	query := fmt.Sprintf(`
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
	    %s
	    ORDER BY COALESCE(ns.last_seen, 0) DESC, ni.node_id DESC
	    LIMIT ?`, whereClause)

	argsWithLimit := append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, argsWithLimit...)
	if err != nil {
		return nil, "", 0, status.Errorf(codes.Internal, "query nodes: %v", err)
	}
	defer rows.Close()

	var (
		nodes   []*meshpipev1.Node
		cursors []packetCursor
	)

	for rows.Next() {
		node, cursorInfo, err := scanNodeWithCursor(rows)
		if err != nil {
			return nil, "", 0, err
		}
		nodes = append(nodes, node)
		cursors = append(cursors, cursorInfo)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, status.Errorf(codes.Internal, "iterate nodes: %v", err)
	}

	totalQuery := "SELECT COUNT(*) FROM node_info ni"
	if len(filterClauses) > 0 {
		totalQuery += " WHERE " + strings.Join(filterClauses, " AND ")
	}
	var total sql.NullInt64
	if err := s.db.QueryRowContext(ctx, totalQuery, filterArgs...).Scan(&total); err != nil {
		return nil, "", 0, status.Errorf(codes.Internal, "count nodes: %v", err)
	}

	nextCursor := ""
	if len(nodes) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		nodes = nodes[:limit]
	}

	return nodes, nextCursor, uint64FromNull(total), nil
}

func (s *service) queryLinks(ctx context.Context, req *meshpipev1.ListLinksRequest) (*meshpipev1.ListLinksResponse, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 6)

	if filter != nil {
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "gateway_id = ?")
			args = append(args, gw)
		}
		if ch := strings.TrimSpace(filter.GetChannelId()); ch != "" {
			where = append(where, "channel_id = ?")
			args = append(args, ch)
		}
		if from := filter.GetFromNodeId(); from != 0 {
			where = append(where, "from_node_id = ?")
			args = append(args, from)
		}
		if to := filter.GetToNodeId(); to != 0 {
			where = append(where, "to_node_id = ?")
			args = append(args, to)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(last_seen < ? OR (last_seen = ? AND last_packet_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        gateway_id,
	        channel_id,
	        from_node_id,
	        to_node_id,
	        packets_total,
	        first_seen,
	        last_seen,
	        avg_rssi,
	        avg_snr,
	        max_hop_index,
	        max_hop_limit,
	        last_packet_id
	    FROM link_aggregate
	    WHERE %s
	    ORDER BY last_seen DESC, last_packet_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query links: %v", err)
	}
	defer rows.Close()

	var (
		links   []*meshpipev1.LinkAggregate
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			gatewayID  sql.NullString
			channelID  sql.NullString
			fromNode   sql.NullInt64
			toNode     sql.NullInt64
			total      sql.NullInt64
			firstSeen  sql.NullFloat64
			lastSeen   sql.NullFloat64
			avgRSSI    sql.NullFloat64
			avgSNR     sql.NullFloat64
			maxHopIdx  sql.NullInt64
			maxHopLim  sql.NullInt64
			lastPacket sql.NullInt64
		)
		if err := rows.Scan(&gatewayID, &channelID, &fromNode, &toNode, &total, &firstSeen, &lastSeen, &avgRSSI, &avgSNR, &maxHopIdx, &maxHopLim, &lastPacket); err != nil {
			return nil, status.Errorf(codes.Internal, "scan link: %v", err)
		}
		links = append(links, &meshpipev1.LinkAggregate{
			GatewayId:    gatewayID.String,
			ChannelId:    channelID.String,
			FromNodeId:   uint32FromNull(fromNode),
			ToNodeId:     uint32FromNull(toNode),
			PacketsTotal: uint64FromNull(total),
			FirstSeen:    toTimestamp(firstSeen),
			LastSeen:     toTimestamp(lastSeen),
			AvgRssi:      avgRSSI.Float64,
			AvgSnr:       avgSNR.Float64,
			MaxHopIndex:  uint32FromNull(maxHopIdx),
			MaxHopLimit:  uint32FromNull(maxHopLim),
		})
		cursors = append(cursors, packetCursor{
			Timestamp: lastSeen.Float64,
			ID:        lastPacket.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate links: %v", err)
	}

	nextCursor := ""
	if len(links) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		links = links[:limit]
	}

	return &meshpipev1.ListLinksResponse{
		Links:      links,
		NextCursor: nextCursor,
	}, nil
}

func (s *service) queryTraceroutes(ctx context.Context, req *meshpipev1.ListTraceroutesRequest) (*meshpipev1.ListTraceroutesResponse, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 6)

	if filter != nil {
		if origin := filter.GetOriginNodeId(); origin != 0 {
			where = append(where, "origin_node_id = ?")
			args = append(args, origin)
		}
		if dest := filter.GetDestinationNodeId(); dest != 0 {
			where = append(where, "destination_node_id = ?")
			args = append(args, dest)
		}
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "gateway_id = ?")
			args = append(args, gw)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(last_seen < ? OR (last_seen = ? AND origin_node_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        origin_node_id,
	        destination_node_id,
	        gateway_id,
	        max_hops,
	        observations,
	        first_seen,
	        last_seen
	    FROM traceroute_longest_paths
	    WHERE %s
	    ORDER BY last_seen DESC, origin_node_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroutes: %v", err)
	}
	defer rows.Close()

	var (
		paths   []*meshpipev1.TraceroutePath
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			origin       sql.NullInt64
			dest         sql.NullInt64
			gateway      sql.NullString
			maxHops      sql.NullInt64
			observations sql.NullInt64
			firstSeen    sql.NullFloat64
			lastSeen     sql.NullFloat64
		)
		if err := rows.Scan(&origin, &dest, &gateway, &maxHops, &observations, &firstSeen, &lastSeen); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute: %v", err)
		}
		paths = append(paths, &meshpipev1.TraceroutePath{
			OriginNodeId:      uint32FromNull(origin),
			DestinationNodeId: uint32FromNull(dest),
			GatewayId:         gateway.String,
			MaxHops:           uint32FromNull(maxHops),
			Observations:      uint64FromNull(observations),
			FirstSeen:         toTimestamp(firstSeen),
			LastSeen:          toTimestamp(lastSeen),
		})
		cursors = append(cursors, packetCursor{
			Timestamp: lastSeen.Float64,
			ID:        origin.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroutes: %v", err)
	}

	nextCursor := ""
	if len(paths) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		paths = paths[:limit]
	}

	return &meshpipev1.ListTraceroutesResponse{
		Paths:      paths,
		NextCursor: nextCursor,
	}, nil
}

func (s *service) queryRangeTests(ctx context.Context, req *meshpipev1.ListRangeTestsRequest) (*meshpipev1.ListRangeTestsResponse, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 6)

	if filter != nil {
		if from := filter.GetFromNodeId(); from != 0 {
			where = append(where, "p.from_node_id = ?")
			args = append(args, from)
		}
		if to := filter.GetToNodeId(); to != 0 {
			where = append(where, "p.to_node_id = ?")
			args = append(args, to)
		}
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "p.gateway_id = ?")
			args = append(args, gw)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(p.timestamp < ? OR (p.timestamp = ? AND p.id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        r.packet_id,
	        p.timestamp,
	        p.from_node_id,
	        p.to_node_id,
	        p.gateway_id,
	        p.rssi,
	        p.snr,
	        p.hop_limit,
	        p.hop_start,
	        r.raw_payload
	    FROM range_test_results r
	    JOIN packet_history p ON p.id = r.packet_id
	    WHERE %s
	    ORDER BY p.timestamp DESC, r.packet_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query range tests: %v", err)
	}
	defer rows.Close()

	var (
		results []*meshpipev1.RangeTestResult
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			fromID   sql.NullInt64
			toID     sql.NullInt64
			gateway  sql.NullString
			rssi     sql.NullInt64
			snr      sql.NullFloat64
			hopLimit sql.NullInt64
			hopStart sql.NullInt64
			payload  []byte
		)
		if err := rows.Scan(&packetID, &ts, &fromID, &toID, &gateway, &rssi, &snr, &hopLimit, &hopStart, &payload); err != nil {
			return nil, status.Errorf(codes.Internal, "scan range test: %v", err)
		}
		result := &meshpipev1.RangeTestResult{
			PacketId:   uint64FromNull(packetID),
			FromNodeId: uint32FromNull(fromID),
			ToNodeId:   uint32FromNull(toID),
			GatewayId:  gateway.String,
			ReceivedAt: toTimestamp(ts),
			Rssi:       int32(rssi.Int64),
			Snr:        snr.Float64,
			HopLimit:   uint32FromNull(hopLimit),
			HopStart:   uint32FromNull(hopStart),
			Payload:    append([]byte(nil), payload...),
		}
		results = append(results, result)
		cursors = append(cursors, packetCursor{
			Timestamp: ts.Float64,
			ID:        packetID.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate range tests: %v", err)
	}

	nextCursor := ""
	if len(results) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		results = results[:limit]
	}

	return &meshpipev1.ListRangeTestsResponse{
		Results:    results,
		NextCursor: nextCursor,
	}, nil
}

func (s *service) queryStoreForward(ctx context.Context, req *meshpipev1.ListStoreForwardRequest) (*meshpipev1.ListStoreForwardResponse, error) {
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 4)

	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		where = append(where, "p.gateway_id = ?")
		args = append(args, gw)
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(p.timestamp < ? OR (p.timestamp = ? AND s.packet_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        s.packet_id,
	        p.timestamp,
	        p.gateway_id,
	        s.variant,
	        s.raw_payload
	    FROM store_forward_events s
	    JOIN packet_history p ON p.id = s.packet_id
	    WHERE %s
	    ORDER BY p.timestamp DESC, s.packet_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query store forward: %v", err)
	}
	defer rows.Close()

	var (
		events  []*meshpipev1.StoreForwardEvent
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			gateway  sql.NullString
			variant  sql.NullString
			payload  []byte
		)
		if err := rows.Scan(&packetID, &ts, &gateway, &variant, &payload); err != nil {
			return nil, status.Errorf(codes.Internal, "scan store forward: %v", err)
		}
		events = append(events, &meshpipev1.StoreForwardEvent{
			PacketId:   uint64FromNull(packetID),
			ReceivedAt: toTimestamp(ts),
			GatewayId:  gateway.String,
			Variant:    variant.String,
			Payload:    append([]byte(nil), payload...),
		})
		cursors = append(cursors, packetCursor{
			Timestamp: ts.Float64,
			ID:        packetID.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate store forward: %v", err)
	}

	nextCursor := ""
	if len(events) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		events = events[:limit]
	}

	return &meshpipev1.ListStoreForwardResponse{
		Events:     events,
		NextCursor: nextCursor,
	}, nil
}

func (s *service) queryPaxcounter(ctx context.Context, req *meshpipev1.ListPaxcounterRequest) (*meshpipev1.ListPaxcounterResponse, error) {
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 4)

	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		where = append(where, "p.gateway_id = ?")
		args = append(args, gw)
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(p.timestamp < ? OR (p.timestamp = ? AND x.packet_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        x.packet_id,
	        p.timestamp,
	        p.gateway_id,
	        x.wifi,
	        x.ble,
	        x.uptime_seconds,
	        x.raw_payload
	    FROM paxcounter_samples x
	    JOIN packet_history p ON p.id = x.packet_id
	    WHERE %s
	    ORDER BY p.timestamp DESC, x.packet_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query paxcounter: %v", err)
	}
	defer rows.Close()

	var (
		samples []*meshpipev1.PaxcounterSample
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			gateway  sql.NullString
			wifi     sql.NullInt64
			ble      sql.NullInt64
			uptime   sql.NullInt64
			payload  []byte
		)
		if err := rows.Scan(&packetID, &ts, &gateway, &wifi, &ble, &uptime, &payload); err != nil {
			return nil, status.Errorf(codes.Internal, "scan paxcounter: %v", err)
		}
		samples = append(samples, &meshpipev1.PaxcounterSample{
			PacketId:      uint64FromNull(packetID),
			ReceivedAt:    toTimestamp(ts),
			GatewayId:     gateway.String,
			Wifi:          uint32FromNull(wifi),
			Ble:           uint32FromNull(ble),
			UptimeSeconds: uint32FromNull(uptime),
		})
		cursors = append(cursors, packetCursor{
			Timestamp: ts.Float64,
			ID:        packetID.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate paxcounter: %v", err)
	}

	nextCursor := ""
	if len(samples) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		samples = samples[:limit]
	}

	return &meshpipev1.ListPaxcounterResponse{
		Samples:    samples,
		NextCursor: nextCursor,
	}, nil
}

type nodeScanner interface {
	Scan(dest ...any) error
}

func scanNode(scanner nodeScanner) (*meshpipev1.Node, error) {
	node, cursor, err := scanNodeWithCursor(scanner)
	if err != nil {
		return nil, err
	}
	_ = cursor // unused for direct scan
	return node, nil
}

func scanNodeWithCursor(scanner nodeScanner) (*meshpipev1.Node, packetCursor, error) {
	var (
		nodeID          sql.NullInt64
		hexID           sql.NullString
		longName        sql.NullString
		shortName       sql.NullString
		hwModel         sql.NullInt64
		hwModelName     sql.NullString
		role            sql.NullInt64
		roleName        sql.NullString
		region          sql.NullString
		regionName      sql.NullString
		modemPreset     sql.NullString
		modemPresetName sql.NullString
		totalPackets    sql.NullInt64
		uniqueDest      sql.NullInt64
		uniqueGateways  sql.NullInt64
		firstSeen       sql.NullFloat64
		lastSeen        sql.NullFloat64
		avgRSSI         sql.NullFloat64
		avgSNR          sql.NullFloat64
		avgHops         sql.NullFloat64
	)

	if err := scanner.Scan(
		&nodeID,
		&hexID,
		&longName,
		&shortName,
		&hwModel,
		&hwModelName,
		&role,
		&roleName,
		&region,
		&regionName,
		&modemPreset,
		&modemPresetName,
		&totalPackets,
		&uniqueDest,
		&uniqueGateways,
		&firstSeen,
		&lastSeen,
		&avgRSSI,
		&avgSNR,
		&avgHops,
	); err != nil {
		return nil, packetCursor{}, status.Errorf(codes.Internal, "scan node: %v", err)
	}

	display := chooseDisplayName(longName.String, shortName.String, hexID.String, nodeID.Int64)

	node := &meshpipev1.Node{
		NodeId:             uint32FromNull(nodeID),
		HexId:              hexID.String,
		DisplayName:        display,
		LongName:           longName.String,
		ShortName:          shortName.String,
		HardwareModel:      fmt.Sprintf("%d", hwModel.Int64),
		HardwareModelName:  hwModelName.String,
		Role:               fmt.Sprintf("%d", role.Int64),
		RoleName:           roleName.String,
		Region:             region.String,
		RegionName:         regionName.String,
		ModemPreset:        modemPreset.String,
		ModemPresetName:    modemPresetName.String,
		TotalPackets:       uint64FromNull(totalPackets),
		UniqueDestinations: uint32FromNull(uniqueDest),
		UniqueGateways:     uint32FromNull(uniqueGateways),
		FirstSeen:          toTimestamp(firstSeen),
		LastSeen:           toTimestamp(lastSeen),
		AvgRssi:            avgRSSI.Float64,
		AvgSnr:             avgSNR.Float64,
		AvgHops:            avgHops.Float64,
	}

	return node, packetCursor{
		Timestamp: lastSeen.Float64,
		ID:        nodeID.Int64,
	}, nil
}

func chooseDisplayName(longName, shortName, hex string, nodeID int64) string {
	candidates := []string{strings.TrimSpace(longName), strings.TrimSpace(shortName), strings.TrimSpace(hex)}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	if nodeID > 0 {
		return fmt.Sprintf("!%08x", nodeID)
	}
	return "Unknown"
}

func timestampToFloat(ts *timestamppb.Timestamp) float64 {
	if ts == nil {
		return 0
	}
	return float64(ts.AsTime().UnixNano()) / 1e9
}
