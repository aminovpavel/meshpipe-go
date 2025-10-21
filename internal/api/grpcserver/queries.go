package grpcserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "%", "\\%")
	value = strings.ReplaceAll(value, "_", "\\_")
	return value
}

func (s *service) queryPackets(ctx context.Context, req *meshpipev1.ListPacketsRequest) ([]*meshpipev1.Packet, string, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)
	includePayload := req.GetIncludePayload()

	where := []string{"1=1"}
	args := make([]any, 0, 12)

	if filter != nil {
		filterClauses, filterArgs := buildPacketFilterClauses(filter, "")
		where = append(where, filterClauses...)
		args = append(args, filterArgs...)
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

const chatAudienceExpr = "(CASE WHEN dest IS NULL OR dest = 0 OR dest = 65535 THEN 'broadcast' ELSE 'direct' END)"

func (s *service) queryChatWindow(ctx context.Context, req *meshpipev1.GetChatWindowRequest) (*meshpipev1.GetChatWindowResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	windowHours := req.GetWindowHours()
	if windowHours == 0 {
		windowHours = 1
	}
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	var windowEnd float64
	if before := req.GetBefore(); before != nil {
		windowEnd = timestampToFloat(before)
	}
	if windowEnd <= 0 {
		now := time.Now().UTC()
		windowEnd = float64(now.UnixNano()) / 1e9
	}

	windowStart := req.GetWindowStart()
	windowStartFloat := float64(windowEnd - float64(windowHours)*3600)
	if windowStart != nil {
		candidate := timestampToFloat(windowStart)
		if candidate > 0 && candidate < windowEnd {
			windowStartFloat = candidate
		}
	}
	if windowStartFloat < 0 {
		windowStartFloat = 0
	}

	filterClauses, filterArgs := buildChatFilterClauses(req.GetFilter())

	where := []string{
		"timestamp >= ?",
		"timestamp <= ?",
	}
	args := []any{windowStartFloat, windowEnd}
	where = append(where, filterClauses...)
	args = append(args, filterArgs...)

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(timestamp < ? OR (timestamp = ? AND packet_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	selectColumns := []string{
		"packet_id",
		"timestamp",
		"from_node_id",
		"to_node_id",
		"channel_id",
		"gateway_id",
		"processed_successfully",
		"text",
		"want_response",
		"dest",
		"source",
		"request_id",
		"reply_id",
		"compressed",
		chatAudienceExpr + " AS audience",
	}

	query := fmt.Sprintf(`SELECT %s FROM chat_messages WHERE %s ORDER BY timestamp DESC, packet_id DESC LIMIT ?`,
		strings.Join(selectColumns, ", "),
		strings.Join(where, " AND "))

	argsWithLimit := append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, argsWithLimit...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query chat window: %v", err)
	}
	defer rows.Close()

	type chatRow struct {
		packetID   sql.NullInt64
		timestamp  sql.NullFloat64
		from       sql.NullInt64
		to         sql.NullInt64
		channel    sql.NullString
		gatewayID  sql.NullString
		processed  sql.NullInt64
		text       sql.NullString
		want       sql.NullInt64
		dest       sql.NullInt64
		source     sql.NullInt64
		reqID      sql.NullInt64
		replyID    sql.NullInt64
		compressed sql.NullInt64
		audience   sql.NullString
	}

	var (
		messages []*meshpipev1.ChatMessage
		cursors  []packetCursor
		rowsData []chatRow
	)

	for rows.Next() {
		var r chatRow
		if err := rows.Scan(
			&r.packetID,
			&r.timestamp,
			&r.from,
			&r.to,
			&r.channel,
			&r.gatewayID,
			&r.processed,
			&r.text,
			&r.want,
			&r.dest,
			&r.source,
			&r.reqID,
			&r.replyID,
			&r.compressed,
			&r.audience,
		); err != nil {
			return nil, status.Errorf(codes.Internal, "scan chat message: %v", err)
		}
		rowsData = append(rowsData, r)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate chat messages: %v", err)
	}

	packetIDs := make([]uint64, 0, len(rowsData))
	for _, r := range rowsData {
		packetIDs = append(packetIDs, uint64FromNull(r.packetID))
	}

	gatewayMap, err := s.loadChatGateways(ctx, packetIDs)
	if err != nil {
		return nil, err
	}

	for _, r := range rowsData {
		msg := &meshpipev1.ChatMessage{
			PacketId:              uint64FromNull(r.packetID),
			Timestamp:             toTimestamp(r.timestamp),
			FromNodeId:            uint32FromNull(r.from),
			ToNodeId:              uint32FromNull(r.to),
			ChannelId:             r.channel.String,
			Audience:              r.audience.String,
			Text:                  r.text.String,
			ProcessedSuccessfully: boolFromInt(r.processed),
		}
		msg.Gateways = gatewayMap[msg.GetPacketId()]
		messages = append(messages, msg)
		cursors = append(cursors, packetCursor{Timestamp: r.timestamp.Float64, ID: r.packetID.Int64})
	}

	nextCursor := ""
	if len(messages) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		messages = messages[:limit]
	}

	now := time.Now().UTC()
	oneHour := float64(now.Add(-1*time.Hour).UnixNano()) / 1e9
	sixHour := float64(now.Add(-6*time.Hour).UnixNano()) / 1e9
	day := float64(now.Add(-24*time.Hour).UnixNano()) / 1e9

	counterWhere := append([]string{}, filterClauses...)
	if len(counterWhere) == 0 {
		counterWhere = append(counterWhere, "1=1")
	}
	counterQuery := fmt.Sprintf(`SELECT
	    SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS count_1h,
	    SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS count_6h,
	    SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS count_24h
	FROM chat_messages
	WHERE %s`, strings.Join(counterWhere, " AND "))
	counterArgs := []any{oneHour, sixHour, day}
	counterArgs = append(counterArgs, filterArgs...)

	var c1, c6, c24 sql.NullInt64
	if err := s.db.QueryRowContext(ctx, counterQuery, counterArgs...).Scan(&c1, &c6, &c24); err != nil {
		return nil, status.Errorf(codes.Internal, "count chat messages: %v", err)
	}

	return &meshpipev1.GetChatWindowResponse{
		Messages: messages,
		Counters: &meshpipev1.ChatWindowCounters{
			Messages_1H:  uint64FromNull(c1),
			Messages_6H:  uint64FromNull(c6),
			Messages_24H: uint64FromNull(c24),
		},
		WindowStart: floatToTimestamp(windowStartFloat),
		WindowHours: windowHours,
		NextCursor:  nextCursor,
	}, nil
}

func (s *service) queryMeshPacketAggregates(ctx context.Context, ids []uint32, filter *meshpipev1.PacketFilter) ([]*meshpipev1.MeshPacketAggregate, error) {
	unique := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		unique[id] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, nil
	}

	placeholders := make([]string, 0, len(unique))
	args := make([]any, 0, len(unique)+6)
	for id := range unique {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}

	where := []string{
		fmt.Sprintf("ph.mesh_packet_id IN (%s)", strings.Join(placeholders, ",")),
	}

	if filter != nil {
		filterClauses, filterArgs := buildPacketFilterClauses(filter, "ph")
		where = append(where, filterClauses...)
		args = append(args, filterArgs...)

		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "lh.gateway_id = ?")
			args = append(args, gw)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        ph.mesh_packet_id,
	        COUNT(lh.id) AS reception_count,
	        COUNT(DISTINCT lh.gateway_id) AS gateway_count,
	        MIN(lh.rssi) AS min_rssi,
	        MAX(lh.rssi) AS max_rssi,
	        MIN(lh.snr) AS min_snr,
	        MAX(lh.snr) AS max_snr,
	        MIN(lh.received_at) AS first_received_at,
	        MAX(lh.received_at) AS last_received_at,
	        MIN(CASE WHEN ph.hop_start IS NOT NULL AND ph.hop_limit IS NOT NULL THEN ph.hop_start - ph.hop_limit END) AS min_hop_count,
	        MAX(CASE WHEN ph.hop_start IS NOT NULL AND ph.hop_limit IS NOT NULL THEN ph.hop_start - ph.hop_limit END) AS max_hop_count
	    FROM packet_history ph
	    JOIN link_history lh ON lh.packet_id = ph.id
	    WHERE %s
	    GROUP BY ph.mesh_packet_id`, strings.Join(where, " AND "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query mesh packet aggregates: %v", err)
	}
	defer rows.Close()

	var aggregates []*meshpipev1.MeshPacketAggregate
	for rows.Next() {
		var (
			meshID         sql.NullInt64
			receptionCount sql.NullInt64
			gatewayCount   sql.NullInt64
			minRSSI        sql.NullInt64
			maxRSSI        sql.NullInt64
			minSNR         sql.NullFloat64
			maxSNR         sql.NullFloat64
			firstSeen      sql.NullFloat64
			lastSeen       sql.NullFloat64
			minHop         sql.NullInt64
			maxHop         sql.NullInt64
		)
		if err := rows.Scan(
			&meshID,
			&receptionCount,
			&gatewayCount,
			&minRSSI,
			&maxRSSI,
			&minSNR,
			&maxSNR,
			&firstSeen,
			&lastSeen,
			&minHop,
			&maxHop,
		); err != nil {
			return nil, status.Errorf(codes.Internal, "scan mesh packet aggregate: %v", err)
		}

		if !meshID.Valid {
			continue
		}

		aggregates = append(aggregates, &meshpipev1.MeshPacketAggregate{
			MeshPacketId:    uint32(meshID.Int64),
			ReceptionCount:  uint64FromNull(receptionCount),
			GatewayCount:    uint32FromNull(gatewayCount),
			MinRssi:         int32(minRSSI.Int64),
			MaxRssi:         int32(maxRSSI.Int64),
			MinSnr:          minSNR.Float64,
			MaxSnr:          maxSNR.Float64,
			MinHopCount:     int32(minHop.Int64),
			MaxHopCount:     int32(maxHop.Int64),
			FirstReceivedAt: toTimestamp(firstSeen),
			LastReceivedAt:  toTimestamp(lastSeen),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate mesh packet aggregates: %v", err)
	}
	return aggregates, nil
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
	            SUM(CASE WHEN timestamp >= unixepoch('now','-24 hours') THEN 1 ELSE 0 END) AS packets_24h,
	            COUNT(DISTINCT CASE WHEN timestamp >= unixepoch('now','-24 hours') THEN gateway_id END) AS gateways_24h,
	            MIN(timestamp) AS first_seen,
	            MAX(timestamp) AS last_seen,
	            MAX(timestamp) AS last_packet_time,
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
	        ns.packets_24h,
	        ns.last_packet_time,
	        ns.gateways_24h,
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

func (s *service) queryTraceroutePackets(ctx context.Context, req *meshpipev1.ListTraceroutePacketsRequest) (*meshpipev1.ListTraceroutePacketsResponse, error) {
	if req == nil {
		req = &meshpipev1.ListTraceroutePacketsRequest{}
	}

	if req.GetGroupPackets() {
		return nil, status.Errorf(codes.Unimplemented, "group_packets handling not yet implemented")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	if limit > s.maxPageSize {
		limit = s.maxPageSize
	}
	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}

	where := []string{"portnum_name = 'TRACEROUTE_APP'"}
	args := make([]any, 0, 10)

	if filter := req.GetFilter(); filter != nil {
		if ts := filter.GetStartTime(); ts != nil {
			where = append(where, "timestamp >= ?")
			args = append(args, float64(ts.AsTime().UnixNano())/1e9)
		}
		if ts := filter.GetEndTime(); ts != nil {
			where = append(where, "timestamp <= ?")
			args = append(args, float64(ts.AsTime().UnixNano())/1e9)
		}
		if v := filter.GetFromNodeId(); v != 0 {
			where = append(where, "from_node_id = ?")
			args = append(args, v)
		}
		if v := filter.GetToNodeId(); v != 0 {
			where = append(where, "to_node_id = ?")
			args = append(args, v)
		}
		if v := strings.TrimSpace(filter.GetGatewayId()); v != "" {
			where = append(where, "gateway_id = ?")
			args = append(args, v)
		}
		if v := strings.TrimSpace(filter.GetPrimaryChannel()); v != "" {
			where = append(where, "channel_id = ?")
			args = append(args, v)
		}
		if filter.GetProcessedSuccessfullyOnly() {
			where = append(where, "processed_successfully = 1")
		}
		if search := strings.TrimSpace(filter.GetSearch()); search != "" {
			escaped := escapeLike(search)
			like := "%" + escaped + "%"
			where = append(where, "(gateway_id LIKE ? ESCAPE '\\' OR CAST(from_node_id AS TEXT) LIKE ? ESCAPE '\\' OR CAST(to_node_id AS TEXT) LIKE ? ESCAPE '\\')")
			args = append(args, like, like, like)
		}
	}

	whereClause := strings.Join(where, " AND ")

	orderBy := "timestamp"
	switch strings.ToLower(strings.TrimSpace(req.GetOrderBy())) {
	case "timestamp", "id", "rssi", "snr":
		orderBy = req.GetOrderBy()
	case "hop_count":
		orderBy = "(hop_start - hop_limit)"
	}

	orderDir := "DESC"
	if strings.EqualFold(req.GetOrderDir(), "asc") {
		orderDir = "ASC"
	}

	totalQuery := fmt.Sprintf("SELECT COUNT(*) FROM packet_history WHERE %s", whereClause)
	var totalCount sql.NullInt64
	if err := s.db.QueryRowContext(ctx, totalQuery, args...).Scan(&totalCount); err != nil {
		return nil, status.Errorf(codes.Internal, "count traceroute packets: %v", err)
	}

	queryArgs := append([]any(nil), args...)
	queryArgs = append(queryArgs, limit, offset)

	query := fmt.Sprintf(`
        SELECT
            id,
            timestamp,
            from_node_id,
            to_node_id,
            gateway_id,
            channel_id,
            hop_start,
            hop_limit,
            rssi,
            snr,
            mesh_packet_id,
            processed_successfully,
            payload_length,
            raw_payload
        FROM packet_history
        WHERE %s
        ORDER BY %s %s
        LIMIT ? OFFSET ?`, whereClause, orderBy, orderDir)

	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute packets: %v", err)
	}
	defer rows.Close()

	packets := make([]*meshpipev1.TraceroutePacket, 0, limit)
	for rows.Next() {
		var (
			id         sql.NullInt64
			ts         sql.NullFloat64
			fromID     sql.NullInt64
			toID       sql.NullInt64
			gateway    sql.NullString
			channel    sql.NullString
			hopStart   sql.NullInt64
			hopLimit   sql.NullInt64
			rssi       sql.NullInt64
			snr        sql.NullFloat64
			meshID     sql.NullInt64
			success    sql.NullInt64
			payloadLen sql.NullInt64
			rawPayload []byte
		)

		if err := rows.Scan(&id, &ts, &fromID, &toID, &gateway, &channel, &hopStart, &hopLimit, &rssi, &snr, &meshID, &success, &payloadLen, &rawPayload); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute packet: %v", err)
		}

		hopCount := int32(0)
		if hopStart.Valid && hopLimit.Valid {
			hopCount = int32(hopStart.Int64 - hopLimit.Int64)
		}

		var (
			rssiVal int32
			snrVal  float64
			hasRssi = rssi.Valid
			hasSnr  = snr.Valid
		)

		if hasRssi {
			rssiVal = int32(rssi.Int64)
		}
		if hasSnr {
			snrVal = snr.Float64
		}

		gatewayCount := uint32(0)
		if gateway.String != "" {
			gatewayCount = 1
		}

		pkt := &meshpipev1.TraceroutePacket{
			Id:                    uint64FromNull(id),
			Timestamp:             toTimestamp(ts),
			FromNodeId:            uint32FromNull(fromID),
			ToNodeId:              uint32FromNull(toID),
			GatewayId:             gateway.String,
			ChannelId:             channel.String,
			HopStart:              int32FromNull(hopStart),
			HopLimit:              int32FromNull(hopLimit),
			HopCount:              hopCount,
			Rssi:                  rssiVal,
			Snr:                   snrVal,
			MeshPacketId:          uint32FromNull(meshID),
			ProcessedSuccessfully: success.Valid && success.Int64 != 0,
			PayloadLength:         uint32(max64(payloadLen.Int64, 0)),
			ReceptionCount:        1,
			GatewayCount:          gatewayCount,
			RawPayload:            append([]byte(nil), rawPayload...),
			IsGrouped:             false,
		}

		if hasRssi {
			pkt.MinRssi = rssiVal
			pkt.MaxRssi = rssiVal
		}
		if hasSnr {
			pkt.MinSnr = snrVal
			pkt.MaxSnr = snrVal
		}

		packets = append(packets, pkt)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute packets: %v", err)
	}

	hasMore := offset+len(packets) < int(totalCount.Int64)

	return &meshpipev1.ListTraceroutePacketsResponse{
		Packets:    packets,
		TotalCount: uint64(max64(totalCount.Int64, 0)),
		Limit:      uint32(limit),
		Offset:     uint32(offset),
		IsGrouped:  false,
		HasMore:    hasMore,
	}, nil
}

func (s *service) queryTracerouteDetails(ctx context.Context, req *meshpipev1.GetTracerouteDetailsRequest) (*meshpipev1.GetTracerouteDetailsResponse, error) {
	if req == nil || req.GetPacketId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "packet_id is required")
	}

	const packetQuery = `
        SELECT
            id,
            timestamp,
            from_node_id,
            to_node_id,
            gateway_id,
            channel_id,
            hop_start,
            hop_limit,
            rssi,
            snr,
            mesh_packet_id,
            processed_successfully,
            payload_length,
            raw_payload
        FROM packet_history
        WHERE id = ? AND portnum_name = 'TRACEROUTE_APP'
    `

	var (
		id         sql.NullInt64
		ts         sql.NullFloat64
		fromID     sql.NullInt64
		toID       sql.NullInt64
		gateway    sql.NullString
		channel    sql.NullString
		hopStart   sql.NullInt64
		hopLimit   sql.NullInt64
		rssi       sql.NullInt64
		snr        sql.NullFloat64
		meshID     sql.NullInt64
		success    sql.NullInt64
		payloadLen sql.NullInt64
		rawPayload []byte
	)

	if err := s.db.QueryRowContext(ctx, packetQuery, req.GetPacketId()).Scan(&id, &ts, &fromID, &toID, &gateway, &channel, &hopStart, &hopLimit, &rssi, &snr, &meshID, &success, &payloadLen, &rawPayload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "packet not found")
		}
		return nil, status.Errorf(codes.Internal, "get traceroute details: %v", err)
	}

	hopCount := int32(0)
	if hopStart.Valid && hopLimit.Valid {
		hopCount = int32(hopStart.Int64 - hopLimit.Int64)
	}

	var (
		rssiVal int32
		snrVal  float64
		hasRssi = rssi.Valid
		hasSnr  = snr.Valid
	)

	if hasRssi {
		rssiVal = int32(rssi.Int64)
	}
	if hasSnr {
		snrVal = snr.Float64
	}

	gatewayCount := uint32(0)
	if gateway.String != "" {
		gatewayCount = 1
	}

	packet := &meshpipev1.TraceroutePacket{
		Id:                    uint64FromNull(id),
		Timestamp:             toTimestamp(ts),
		FromNodeId:            uint32FromNull(fromID),
		ToNodeId:              uint32FromNull(toID),
		GatewayId:             gateway.String,
		ChannelId:             channel.String,
		HopStart:              int32FromNull(hopStart),
		HopLimit:              int32FromNull(hopLimit),
		HopCount:              hopCount,
		Rssi:                  rssiVal,
		Snr:                   snrVal,
		MeshPacketId:          uint32FromNull(meshID),
		ProcessedSuccessfully: success.Valid && success.Int64 != 0,
		PayloadLength:         uint32(max64(payloadLen.Int64, 0)),
		ReceptionCount:        1,
		GatewayCount:          gatewayCount,
		RawPayload:            append([]byte(nil), rawPayload...),
		IsGrouped:             false,
	}

	if hasRssi {
		packet.MinRssi = rssiVal
		packet.MaxRssi = rssiVal
	}
	if hasSnr {
		packet.MinSnr = snrVal
		packet.MaxSnr = snrVal
	}

	const hopsQuery = `
        SELECT id, packet_id, origin_node_id, destination_node_id, gateway_id, direction, hop_index, hop_node_id, snr, received_at
        FROM traceroute_hops
        WHERE packet_id = ?
        ORDER BY hop_index ASC, id ASC
    `

	hopRows, err := s.db.QueryContext(ctx, hopsQuery, req.GetPacketId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute hops: %v", err)
	}
	defer hopRows.Close()

	var hops []*meshpipev1.TracerouteHop
	for hopRows.Next() {
		var (
			hopID     sql.NullInt64
			packetID  sql.NullInt64
			origin    sql.NullInt64
			dest      sql.NullInt64
			gateway   sql.NullString
			direction sql.NullString
			hopIndex  sql.NullInt64
			hopNode   sql.NullInt64
			snr       sql.NullFloat64
			received  sql.NullFloat64
		)
		if err := hopRows.Scan(&hopID, &packetID, &origin, &dest, &gateway, &direction, &hopIndex, &hopNode, &snr, &received); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute hop: %v", err)
		}
		hops = append(hops, &meshpipev1.TracerouteHop{
			Id:                uint64FromNull(hopID),
			PacketId:          uint64FromNull(packetID),
			OriginNodeId:      uint32FromNull(origin),
			DestinationNodeId: uint32FromNull(dest),
			GatewayId:         gateway.String,
			Direction:         direction.String,
			HopIndex:          uint32FromNull(hopIndex),
			HopNodeId:         uint32FromNull(hopNode),
			Snr:               snr.Float64,
			ReceivedAt:        toTimestamp(received),
		})
	}
	if err := hopRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute hops: %v", err)
	}

	return &meshpipev1.GetTracerouteDetailsResponse{Packet: packet, Hops: hops}, nil
}

func (s *service) queryNodeDirectReceptions(ctx context.Context, req *meshpipev1.ListNodeDirectReceptionsRequest) (*meshpipev1.ListNodeDirectReceptionsResponse, error) {
	if req == nil || req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 500
	}
	if limit > s.maxPageSize {
		limit = s.maxPageSize
	}

	direction := strings.ToLower(strings.TrimSpace(req.GetDirection()))
	if direction == "" {
		direction = "received"
	}

	var (
		where string
		args  []any
	)

	switch direction {
	case "sent":
		where = "from_node_id = ? AND hop_start IS NOT NULL AND hop_limit IS NOT NULL AND (hop_start - hop_limit) = 0"
		args = []any{req.GetNodeId()}
	default:
		where = "gateway_id = ? AND hop_start IS NOT NULL AND hop_limit IS NOT NULL AND (hop_start - hop_limit) = 0"
		args = []any{fmt.Sprintf("!%08x", req.GetNodeId())}
	}

	query := fmt.Sprintf(`
        SELECT id, timestamp, from_node_id, to_node_id, gateway_id, snr, rssi
        FROM packet_history
        WHERE %s
        ORDER BY timestamp DESC
        LIMIT ?
    `, where)

	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query direct receptions: %v", err)
	}
	defer rows.Close()

	receptions := make([]*meshpipev1.NodeDirectReception, 0, limit)
	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			fromID   sql.NullInt64
			toID     sql.NullInt64
			gateway  sql.NullString
			snr      sql.NullFloat64
			rssi     sql.NullInt64
		)
		if err := rows.Scan(&packetID, &ts, &fromID, &toID, &gateway, &snr, &rssi); err != nil {
			return nil, status.Errorf(codes.Internal, "scan direct reception: %v", err)
		}
		receptions = append(receptions, &meshpipev1.NodeDirectReception{
			PacketId:          uint64FromNull(packetID),
			OriginNodeId:      uint32FromNull(fromID),
			DestinationNodeId: uint32FromNull(toID),
			GatewayId:         gateway.String,
			Snr:               snr.Float64,
			Rssi:              int32(rssi.Int64),
			Timestamp:         toTimestamp(ts),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate direct receptions: %v", err)
	}

	return &meshpipev1.ListNodeDirectReceptionsResponse{Receptions: receptions}, nil
}

func (s *service) queryNodeNames(ctx context.Context, req *meshpipev1.ListNodeNamesRequest) (*meshpipev1.ListNodeNamesResponse, error) {
	ids := req.GetNodeIds()
	if len(ids) == 0 {
		return &meshpipev1.ListNodeNamesResponse{}, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
        SELECT node_id, long_name, short_name
        FROM node_info
        WHERE node_id IN (%s)
    `, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query node names: %v", err)
	}
	defer rows.Close()

	entries := make([]*meshpipev1.NodeNameEntry, 0, len(ids))
	for rows.Next() {
		var (
			nodeID    sql.NullInt64
			longName  sql.NullString
			shortName sql.NullString
		)
		if err := rows.Scan(&nodeID, &longName, &shortName); err != nil {
			return nil, status.Errorf(codes.Internal, "scan node name: %v", err)
		}
		entries = append(entries, &meshpipev1.NodeNameEntry{
			NodeId:      uint32FromNull(nodeID),
			DisplayName: longName.String,
			ShortName:   shortName.String,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate node names: %v", err)
	}

	return &meshpipev1.ListNodeNamesResponse{Entries: entries}, nil
}

func (s *service) queryPrimaryChannels(ctx context.Context, _ *meshpipev1.ListPrimaryChannelsRequest) (*meshpipev1.ListPrimaryChannelsResponse, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT DISTINCT primary_channel
        FROM node_info
        WHERE primary_channel IS NOT NULL AND TRIM(primary_channel) != ''
        ORDER BY primary_channel
    `)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query primary channels: %v", err)
	}
	defer rows.Close()

	var channels []string
	for rows.Next() {
		var ch sql.NullString
		if err := rows.Scan(&ch); err != nil {
			return nil, status.Errorf(codes.Internal, "scan primary channel: %v", err)
		}
		if ch.Valid {
			channels = append(channels, ch.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate primary channels: %v", err)
	}

	return &meshpipev1.ListPrimaryChannelsResponse{Channels: channels}, nil
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

func (s *service) queryNodeLocations(ctx context.Context, req *meshpipev1.ListNodeLocationsRequest) ([]*meshpipev1.NodeLocation, string, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	where := []string{"1=1"}
	args := make([]any, 0, 8)

	if filter != nil {
		if nodeIDs := filter.GetNodeIds(); len(nodeIDs) > 0 {
			placeholders := make([]string, 0, len(nodeIDs))
			for _, id := range nodeIDs {
				if id == 0 {
					continue
				}
				placeholders = append(placeholders, "?")
				args = append(args, id)
			}
			if len(placeholders) > 0 {
				where = append(where, fmt.Sprintf("node_id IN (%s)", strings.Join(placeholders, ",")))
			}
		}
		if ts := filter.GetStartTime(); ts != nil {
			where = append(where, "timestamp >= ?")
			args = append(args, timestampToFloat(ts))
		}
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			where = append(where, "gateway_id = ?")
			args = append(args, gw)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			where = append(where, "(timestamp < ? OR (timestamp = ? AND node_id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        node_id,
	        gateway_id,
	        channel_id,
	        timestamp,
	        latitude,
	        longitude,
	        altitude,
	        precision_bits,
	        gps_accuracy,
	        pdop,
	        hdop,
	        vdop,
	        sats_in_view,
	        fix_quality,
	        fix_type,
	        long_name,
	        short_name,
	        primary_channel
	    FROM latest_node_locations
	    WHERE %s
	    ORDER BY timestamp DESC, node_id DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "query node locations: %v", err)
	}
	defer rows.Close()

	var (
		locations []*meshpipev1.NodeLocation
		cursors   []packetCursor
		now       = time.Now().UTC()
	)

	for rows.Next() {
		var (
			nodeID         sql.NullInt64
			gatewayID      sql.NullString
			channelID      sql.NullString
			timestamp      sql.NullFloat64
			latitude       sql.NullFloat64
			longitude      sql.NullFloat64
			altitude       sql.NullInt64
			precisionBits  sql.NullInt64
			gpsAccuracy    sql.NullInt64
			pdop           sql.NullInt64
			hdop           sql.NullInt64
			vdop           sql.NullInt64
			satsInView     sql.NullInt64
			fixQuality     sql.NullInt64
			fixType        sql.NullInt64
			longName       sql.NullString
			shortName      sql.NullString
			primaryChannel sql.NullString
		)
		if err := rows.Scan(
			&nodeID,
			&gatewayID,
			&channelID,
			&timestamp,
			&latitude,
			&longitude,
			&altitude,
			&precisionBits,
			&gpsAccuracy,
			&pdop,
			&hdop,
			&vdop,
			&satsInView,
			&fixQuality,
			&fixType,
			&longName,
			&shortName,
			&primaryChannel,
		); err != nil {
			return nil, "", status.Errorf(codes.Internal, "scan node location: %v", err)
		}

		tsProto := toTimestamp(timestamp)
		var ageHours float64
		if tsProto != nil {
			ageHours = now.Sub(tsProto.AsTime()).Hours()
			if ageHours < 0 {
				ageHours = 0
			}
		}

		display := chooseDisplayName(longName.String, shortName.String, "", nodeID.Int64)
		precisionMeters := calculatePrecisionMeters(gpsAccuracy, pdop)
		if precisionMeters == 0 && precisionBits.Valid {
			precisionMeters = estimatePrecisionFromBits(precisionBits.Int64)
		}

		location := &meshpipev1.NodeLocation{
			NodeId:          uint32FromNull(nodeID),
			GatewayId:       gatewayID.String,
			Latitude:        latitude.Float64,
			Longitude:       longitude.Float64,
			Altitude:        int32(altitude.Int64),
			Timestamp:       tsProto,
			AgeHours:        ageHours,
			DisplayName:     display,
			PrimaryChannel:  firstNonEmpty(primaryChannel.String, channelID.String),
			SatsInView:      uint32FromNull(satsInView),
			PrecisionMeters: precisionMeters,
		}

		locations = append(locations, location)
		cursors = append(cursors, packetCursor{
			Timestamp: timestamp.Float64,
			ID:        nodeID.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", status.Errorf(codes.Internal, "iterate node locations: %v", err)
	}

	nextCursor := ""
	if len(locations) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		locations = locations[:limit]
	}

	return locations, nextCursor, nil
}

func (s *service) queryNodeAnalytics(ctx context.Context, nodeID uint32) (*meshpipev1.NodeAnalytics, error) {
	if nodeID == 0 {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	now := time.Now().UTC()
	since24h := float64(now.Add(-24*time.Hour).UnixNano()) / 1e9
	since7d := float64(now.Add(-7*24*time.Hour).UnixNano()) / 1e9

	const statsQuery = `
	    SELECT
	        SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS packets_24h,
	        SUM(CASE WHEN timestamp >= ? THEN 1 ELSE 0 END) AS packets_7d,
	        MAX(timestamp) AS last_packet_time
	    FROM packet_history
	    WHERE from_node_id = ?`

	var (
		packets24h sql.NullInt64
		packets7d  sql.NullInt64
		lastPacket sql.NullFloat64
	)
	if err := s.db.QueryRowContext(ctx, statsQuery, since24h, since7d, nodeID).Scan(&packets24h, &packets7d, &lastPacket); err != nil {
		return nil, status.Errorf(codes.Internal, "query node analytics stats: %v", err)
	}

	const gatewayQuery = `
	    SELECT gateway_id, packets_total, last_seen, last_rssi, last_snr
	    FROM gateway_node_stats
	    WHERE node_id = ?
	    ORDER BY COALESCE(last_seen, 0) DESC
	    LIMIT 50`

	gatewayRows, err := s.db.QueryContext(ctx, gatewayQuery, nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query node gateway metrics: %v", err)
	}
	defer gatewayRows.Close()

	var gateways []*meshpipev1.NodeGatewayMetric
	for gatewayRows.Next() {
		var (
			gatewayID sql.NullString
			total     sql.NullInt64
			lastSeen  sql.NullFloat64
			lastRSSI  sql.NullInt64
			lastSNR   sql.NullFloat64
		)
		if scanErr := gatewayRows.Scan(&gatewayID, &total, &lastSeen, &lastRSSI, &lastSNR); scanErr != nil {
			return nil, status.Errorf(codes.Internal, "scan node gateway metric: %v", scanErr)
		}
		metric := &meshpipev1.NodeGatewayMetric{
			GatewayId: gatewayID.String,
			Packets:   uint64FromNull(total),
			AvgRssi:   float64(lastRSSI.Int64),
			AvgSnr:    lastSNR.Float64,
			LastSeen:  toTimestamp(lastSeen),
		}
		if lastRSSI.Valid {
			metric.AvgRssi = float64(lastRSSI.Int64)
		}
		gateways = append(gateways, metric)
	}
	if err := gatewayRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate node gateway metrics: %v", err)
	}

	const neighborQuery = `
	    SELECT neighbor_node_id, AVG(snr) AS avg_snr, COUNT(*) AS observations, MAX(received_at) AS last_seen
	    FROM neighbor_history
	    WHERE origin_node_id = ?
	    GROUP BY neighbor_node_id
	    ORDER BY COALESCE(last_seen, 0) DESC
	    LIMIT 50`

	neighborRows, err := s.db.QueryContext(ctx, neighborQuery, nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query neighbor analytics: %v", err)
	}
	defer neighborRows.Close()

	var neighbors []*meshpipev1.NodeNeighbor
	for neighborRows.Next() {
		var (
			neighborID sql.NullInt64
			avgSNR     sql.NullFloat64
			observed   sql.NullInt64
			lastSeen   sql.NullFloat64
		)
		if scanErr := neighborRows.Scan(&neighborID, &avgSNR, &observed, &lastSeen); scanErr != nil {
			return nil, status.Errorf(codes.Internal, "scan neighbor analytics: %v", scanErr)
		}
		neighbors = append(neighbors, &meshpipev1.NodeNeighbor{
			NeighborNodeId: uint32FromNull(neighborID),
			AvgSnr:         avgSNR.Float64,
			Observations:   uint64FromNull(observed),
			LastSeen:       toTimestamp(lastSeen),
		})
	}
	if err := neighborRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate neighbor analytics: %v", err)
	}

	const roleQuery = `
	    SELECT COALESCE(NULLIF(ni.role_name, ''), 'Unknown') AS role_name, COUNT(DISTINCT ph.to_node_id) AS role_count
	    FROM packet_history ph
	    LEFT JOIN node_info ni ON ni.node_id = ph.to_node_id
	    WHERE ph.from_node_id = ? AND ph.to_node_id IS NOT NULL
	    GROUP BY role_name
	    ORDER BY role_count DESC
	    LIMIT 10`

	roleRows, err := s.db.QueryContext(ctx, roleQuery, nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query node role summary: %v", err)
	}
	defer roleRows.Close()

	var roles []*meshpipev1.NodeRoleSummary
	for roleRows.Next() {
		var (
			roleName sql.NullString
			count    sql.NullInt64
		)
		if scanErr := roleRows.Scan(&roleName, &count); scanErr != nil {
			return nil, status.Errorf(codes.Internal, "scan node role summary: %v", scanErr)
		}
		roles = append(roles, &meshpipev1.NodeRoleSummary{
			RoleName: roleName.String,
			Count:    uint64FromNull(count),
		})
	}
	if err := roleRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate node role summary: %v", err)
	}

	return &meshpipev1.NodeAnalytics{
		Packets_24H:    uint64FromNull(packets24h),
		Packets_7D:     uint64FromNull(packets7d),
		LastPacketTime: toTimestamp(lastPacket),
		Gateways:       gateways,
		Neighbors:      neighbors,
		Roles:          roles,
	}, nil
}

func (s *service) queryGatewayOverview(ctx context.Context, req *meshpipev1.GetGatewayOverviewRequest) ([]*meshpipev1.GatewayOverview, error) {
	limit := int(req.GetLimit())
	if limit <= 0 || limit > s.maxPageSize {
		limit = 20
	}

	now := time.Now().UTC()
	end := timestampToFloat(req.GetEndTime())
	if end <= 0 {
		end = float64(now.UnixNano()) / 1e9
	}
	start := timestampToFloat(req.GetStartTime())
	if start <= 0 || start >= end {
		start = end - 24*3600
	}

	clauses := []string{"timestamp BETWEEN ? AND ?"}
	args := []any{start, end}

	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		clauses = append(clauses, "gateway_id = ?")
		args = append(args, gw)
	}

	whereClause := strings.Join(clauses, " AND ")
	query := fmt.Sprintf(`
	    SELECT
	        gateway_id,
	        COUNT(*) AS packet_count,
	        COUNT(DISTINCT from_node_id) AS unique_sources,
	        AVG(CASE WHEN rssi IS NOT NULL AND rssi != 0 THEN rssi END) AS avg_rssi,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	        MAX(timestamp) AS last_seen
	    FROM packet_history
	    WHERE gateway_id IS NOT NULL AND %s
	    GROUP BY gateway_id
	    ORDER BY packet_count DESC
	    LIMIT ?`, whereClause)

	argsWithLimit := append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, argsWithLimit...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query gateway overview: %v", err)
	}
	defer rows.Close()

	type gatewayRow struct {
		id           sql.NullString
		packetCount  sql.NullInt64
		uniqueSource sql.NullInt64
		avgRSSI      sql.NullFloat64
		avgSNR       sql.NullFloat64
		lastSeen     sql.NullFloat64
	}

	gwRows := make([]gatewayRow, 0, limit)
	gatewayIDs := make([]string, 0, limit)

	for rows.Next() {
		var r gatewayRow
		if err := rows.Scan(&r.id, &r.packetCount, &r.uniqueSource, &r.avgRSSI, &r.avgSNR, &r.lastSeen); err != nil {
			return nil, status.Errorf(codes.Internal, "scan gateway overview: %v", err)
		}
		gwRows = append(gwRows, r)
		if r.id.String != "" {
			gatewayIDs = append(gatewayIDs, r.id.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate gateway overview: %v", err)
	}

	nodesWithMulti := make(map[string]uint64, len(gatewayIDs))
	if len(gatewayIDs) > 0 {
		placeholders := make([]string, 0, len(gatewayIDs))
		argsMulti := make([]any, 0, len(gatewayIDs)+4)
		argsMulti = append(argsMulti, start, end)
		for _, id := range gatewayIDs {
			placeholders = append(placeholders, "?")
			argsMulti = append(argsMulti, id)
		}
		argsMulti = append(argsMulti, start, end)

		queryMulti := fmt.Sprintf(`
		    WITH multi_gateway_nodes AS (
		        SELECT from_node_id
		        FROM packet_history
		        WHERE timestamp BETWEEN ? AND ?
		        GROUP BY from_node_id
		        HAVING COUNT(DISTINCT gateway_id) > 1
		    )
		    SELECT ph.gateway_id, COUNT(DISTINCT ph.from_node_id) AS cnt
		    FROM packet_history ph
		    JOIN multi_gateway_nodes mn ON mn.from_node_id = ph.from_node_id
		    WHERE ph.gateway_id IN (%s) AND ph.timestamp BETWEEN ? AND ?
		    GROUP BY ph.gateway_id`, strings.Join(placeholders, ","))

		multiRows, err := s.db.QueryContext(ctx, queryMulti, argsMulti...)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "query gateway multi-node stats: %v", err)
		}
		defer multiRows.Close()
		for multiRows.Next() {
			var (
				id  sql.NullString
				cnt sql.NullInt64
			)
			if err := multiRows.Scan(&id, &cnt); err != nil {
				return nil, status.Errorf(codes.Internal, "scan gateway multi-node stats: %v", err)
			}
			if id.String != "" {
				nodesWithMulti[id.String] = uint64FromNull(cnt)
			}
		}
		if err := multiRows.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "iterate gateway multi-node stats: %v", err)
		}
	}

	overviews := make([]*meshpipev1.GatewayOverview, 0, len(gwRows))
	for _, r := range gwRows {
		packetCount := uint64FromNull(r.packetCount)
		uniqueSources := uint64FromNull(r.uniqueSource)
		score := 0.0
		if packetCount > 0 {
			score = math.Min(100, (float64(uniqueSources)/float64(packetCount))*100.0)
		}
		overviews = append(overviews, &meshpipev1.GatewayOverview{
			GatewayId:                 r.id.String,
			PacketCount:               packetCount,
			UniqueSources:             uint32FromNull(r.uniqueSource),
			AvgRssi:                   r.avgRSSI.Float64,
			AvgSnr:                    r.avgSNR.Float64,
			LastSeen:                  toTimestamp(r.lastSeen),
			DiversityScore:            score,
			NodesWithMultipleGateways: uint32(nodesWithMulti[r.id.String]),
		})
	}
	return overviews, nil
}

func (s *service) queryAnalyticsSummary(ctx context.Context, req *meshpipev1.GetAnalyticsSummaryRequest) (*meshpipev1.GetAnalyticsSummaryResponse, error) {
	now := time.Now().UTC()
	since24h := float64(now.Add(-24*time.Hour).UnixNano()) / 1e9
	since7d := float64(now.Add(-7*24*time.Hour).UnixNano()) / 1e9

	baseClauses, baseArgs := buildAnalyticsWhere(req, since24h)
	whereClause := strings.Join(baseClauses, " AND ")

	statsQuery := fmt.Sprintf(`
	    SELECT
	        COUNT(*) AS total_packets,
	        SUM(CASE WHEN processed_successfully = 1 THEN 1 ELSE 0 END) AS successful_packets,
	        AVG(CASE WHEN payload_length IS NOT NULL AND payload_length > 0 THEN payload_length END) AS avg_payload_bytes
	    FROM packet_history
	    WHERE %s`, whereClause)

	var (
		totalPackets      sql.NullInt64
		successfulPackets sql.NullInt64
		avgPayload        sql.NullFloat64
	)
	if err := s.db.QueryRowContext(ctx, statsQuery, baseArgs...).Scan(&totalPackets, &successfulPackets, &avgPayload); err != nil {
		return nil, status.Errorf(codes.Internal, "query analytics packet stats: %v", err)
	}

	activityQuery := fmt.Sprintf(`
	    WITH node_activity AS (
	        SELECT from_node_id AS node_id, COUNT(*) AS packet_count
	        FROM packet_history
	        WHERE from_node_id IS NOT NULL AND %s
	        GROUP BY from_node_id
	    )
	    SELECT
	        SUM(CASE WHEN packet_count > 100 THEN 1 ELSE 0 END) AS very_active,
	        SUM(CASE WHEN packet_count > 10 AND packet_count <= 100 THEN 1 ELSE 0 END) AS moderately_active,
	        SUM(CASE WHEN packet_count >= 1 AND packet_count <= 10 THEN 1 ELSE 0 END) AS lightly_active
	    FROM node_activity`, whereClause)

	var (
		veryActive sql.NullInt64
		moderately sql.NullInt64
		lightly    sql.NullInt64
	)
	if err := s.db.QueryRowContext(ctx, activityQuery, baseArgs...).Scan(&veryActive, &moderately, &lightly); err != nil {
		return nil, status.Errorf(codes.Internal, "query node activity: %v", err)
	}

	signalQuery := fmt.Sprintf(`
	    SELECT
	        AVG(CASE WHEN rssi IS NOT NULL AND rssi != 0 THEN rssi END) AS avg_rssi,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr
	    FROM packet_history
	    WHERE %s`, whereClause)

	var (
		avgRSSI sql.NullFloat64
		avgSNR  sql.NullFloat64
	)
	if err := s.db.QueryRowContext(ctx, signalQuery, baseArgs...).Scan(&avgRSSI, &avgSNR); err != nil {
		return nil, status.Errorf(codes.Internal, "query signal quality: %v", err)
	}

	tempQuery := fmt.Sprintf(`
	    SELECT timestamp, processed_successfully
	    FROM packet_history
	    WHERE %s`, whereClause)

	tempRows, err := s.db.QueryContext(ctx, tempQuery, baseArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query temporal buckets: %v", err)
	}
	defer tempRows.Close()

	hourCounts := make(map[int64]uint64, 24)
	hourSuccess := make(map[int64]uint64, 24)
	for tempRows.Next() {
		var (
			ts      sql.NullFloat64
			success sql.NullInt64
		)
		if err := tempRows.Scan(&ts, &success); err != nil {
			return nil, status.Errorf(codes.Internal, "scan temporal bucket row: %v", err)
		}
		if !ts.Valid {
			continue
		}
		sec := int64(math.Floor(ts.Float64))
		hourStart := (sec / 3600) * 3600
		hourCounts[hourStart]++
		if success.Valid && success.Int64 != 0 {
			hourSuccess[hourStart]++
		}
	}
	if err := tempRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate temporal buckets: %v", err)
	}

	startHour := now.Truncate(time.Hour).Add(-24 * time.Hour)
	temporalBuckets := make([]*meshpipev1.TemporalBucket, 0, 24)
	for i := 0; i < 24; i++ {
		bucketTime := startHour.Add(time.Duration(i) * time.Hour)
		sec := bucketTime.Unix()
		count := hourCounts[sec]
		temporalBuckets = append(temporalBuckets, &meshpipev1.TemporalBucket{
			BucketStart: timestamppb.New(bucketTime),
			Packets:     count,
		})
	}

	topClauses, topArgs := buildAnalyticsWhere(req, since7d)
	topClause := strings.Join(topClauses, " AND ")
	topQuery := fmt.Sprintf(`
	    SELECT
	        ph.from_node_id,
	        COUNT(*) AS packets,
	        MAX(ph.timestamp) AS last_seen,
	        AVG(CASE WHEN ph.rssi IS NOT NULL AND ph.rssi != 0 THEN ph.rssi END) AS avg_rssi,
	        AVG(CASE WHEN ph.snr IS NOT NULL THEN ph.snr END) AS avg_snr,
	        ni.long_name,
	        ni.short_name,
	        ni.hw_model_name
	    FROM packet_history ph
	    LEFT JOIN node_info ni ON ni.node_id = ph.from_node_id
	    WHERE ph.from_node_id IS NOT NULL AND %s
	    GROUP BY ph.from_node_id
	    ORDER BY packets DESC
	    LIMIT 10`, topClause)

	topRows, err := s.db.QueryContext(ctx, topQuery, topArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query top nodes: %v", err)
	}
	defer topRows.Close()

	var topNodes []*meshpipev1.TopNode
	for topRows.Next() {
		var (
			nodeID     sql.NullInt64
			packetCnt  sql.NullInt64
			lastSeen   sql.NullFloat64
			nodeAvgRSS sql.NullFloat64
			nodeAvgSNR sql.NullFloat64
			longName   sql.NullString
			shortName  sql.NullString
			hwModel    sql.NullString
		)
		if err := topRows.Scan(&nodeID, &packetCnt, &lastSeen, &nodeAvgRSS, &nodeAvgSNR, &longName, &shortName, &hwModel); err != nil {
			return nil, status.Errorf(codes.Internal, "scan top node row: %v", err)
		}
		display := chooseDisplayName(longName.String, shortName.String, "", nodeID.Int64)
		topNodes = append(topNodes, &meshpipev1.TopNode{
			NodeId:      uint32FromNull(nodeID),
			DisplayName: display,
			Packets:     uint64FromNull(packetCnt),
		})
	}
	if err := topRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate top node rows: %v", err)
	}

	packetTypeQuery := fmt.Sprintf(`
	    SELECT portnum_name, COUNT(*) AS cnt
	    FROM packet_history
	    WHERE portnum_name IS NOT NULL AND %s
	    GROUP BY portnum_name
	    ORDER BY cnt DESC
	    LIMIT 15`, whereClause)

	packetTypeRows, err := s.db.QueryContext(ctx, packetTypeQuery, baseArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query packet type distribution: %v", err)
	}
	defer packetTypeRows.Close()

	var packetTypeEntries []*meshpipev1.DistributionEntry
	for packetTypeRows.Next() {
		var (
			name sql.NullString
			cnt  sql.NullInt64
		)
		if err := packetTypeRows.Scan(&name, &cnt); err != nil {
			return nil, status.Errorf(codes.Internal, "scan packet type distribution: %v", err)
		}
		packetTypeEntries = append(packetTypeEntries, &meshpipev1.DistributionEntry{
			Key:   name.String,
			Count: uint64FromNull(cnt),
		})
	}
	if err := packetTypeRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate packet type distribution: %v", err)
	}

	gatewayDistQuery := fmt.Sprintf(`
	    SELECT gateway_id, COUNT(*) AS cnt
	    FROM packet_history
	    WHERE gateway_id IS NOT NULL AND %s
	    GROUP BY gateway_id
	    ORDER BY cnt DESC
	    LIMIT 15`, whereClause)

	gatewayRows, err := s.db.QueryContext(ctx, gatewayDistQuery, baseArgs...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query gateway distribution: %v", err)
	}
	defer gatewayRows.Close()

	var gatewayEntries []*meshpipev1.DistributionEntry
	for gatewayRows.Next() {
		var (
			id  sql.NullString
			cnt sql.NullInt64
		)
		if err := gatewayRows.Scan(&id, &cnt); err != nil {
			return nil, status.Errorf(codes.Internal, "scan gateway distribution: %v", err)
		}
		gatewayEntries = append(gatewayEntries, &meshpipev1.DistributionEntry{
			Key:   id.String,
			Count: uint64FromNull(cnt),
		})
	}
	if err := gatewayRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate gateway distribution: %v", err)
	}

	return &meshpipev1.GetAnalyticsSummaryResponse{
		PacketSuccess: &meshpipev1.PacketSuccessStats{
			TotalPackets:        uint64FromNull(totalPackets),
			SuccessfulPackets:   uint64FromNull(successfulPackets),
			AveragePayloadBytes: avgPayload.Float64,
		},
		NodeActivity: []*meshpipev1.NodeActivityBucket{
			{Label: "very_active", NodeCount: uint64FromNull(veryActive)},
			{Label: "moderately_active", NodeCount: uint64FromNull(moderately)},
			{Label: "lightly_active", NodeCount: uint64FromNull(lightly)},
		},
		SignalQuality: &meshpipev1.SignalQualitySummary{
			AvgRssi: avgRSSI.Float64,
			AvgSnr:  avgSNR.Float64,
		},
		HourlyPackets:          temporalBuckets,
		TopNodes:               topNodes,
		PacketTypeDistribution: packetTypeEntries,
		GatewayDistribution:    gatewayEntries,
	}, nil
}

func (s *service) queryTracerouteHops(ctx context.Context, req *meshpipev1.ListTracerouteHopsRequest) (*meshpipev1.ListTracerouteHopsResponse, error) {
	filter := req.GetFilter()
	page := req.GetPagination()
	limit := limitPageSize(page.GetPageSize(), s.maxPageSize)

	clauses := []string{"1=1"}
	args := make([]any, 0, 8)

	if filter != nil {
		if id := filter.GetPacketId(); id != 0 {
			clauses = append(clauses, "packet_id = ?")
			args = append(args, id)
		}
		if origin := filter.GetOriginNodeId(); origin != 0 {
			clauses = append(clauses, "origin_node_id = ?")
			args = append(args, origin)
		}
		if dest := filter.GetDestinationNodeId(); dest != 0 {
			clauses = append(clauses, "destination_node_id = ?")
			args = append(args, dest)
		}
		if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
			clauses = append(clauses, "gateway_id = ?")
			args = append(args, gw)
		}
		if reqID := filter.GetRequestId(); reqID != 0 {
			clauses = append(clauses, "request_id = ?")
			args = append(args, reqID)
		}
		if dir := strings.TrimSpace(filter.GetDirection()); dir != "" {
			clauses = append(clauses, "direction = ?")
			args = append(args, dir)
		}
	}

	if cursor := strings.TrimSpace(page.GetCursor()); cursor != "" {
		ts, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		if ts > 0 || id > 0 {
			clauses = append(clauses, "(received_at < ? OR (received_at = ? AND id < ?))")
			args = append(args, ts, ts, id)
		}
	}

	query := fmt.Sprintf(`
	    SELECT
	        id,
	        packet_id,
	        origin_node_id,
	        destination_node_id,
	        gateway_id,
	        direction,
	        hop_index,
	        hop_node_id,
	        snr,
	        received_at
	    FROM traceroute_hops
	    WHERE %s
	    ORDER BY received_at DESC, id DESC
	    LIMIT ?`, strings.Join(clauses, " AND "))

	argsWithLimit := append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, argsWithLimit...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute hops: %v", err)
	}
	defer rows.Close()

	var (
		hops    []*meshpipev1.TracerouteHop
		cursors []packetCursor
	)

	for rows.Next() {
		var (
			id         sql.NullInt64
			packetID   sql.NullInt64
			origin     sql.NullInt64
			dest       sql.NullInt64
			gateway    sql.NullString
			direction  sql.NullString
			hopIndex   sql.NullInt64
			hopNode    sql.NullInt64
			snr        sql.NullFloat64
			receivedAt sql.NullFloat64
		)
		if err := rows.Scan(&id, &packetID, &origin, &dest, &gateway, &direction, &hopIndex, &hopNode, &snr, &receivedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute hop: %v", err)
		}
		hops = append(hops, &meshpipev1.TracerouteHop{
			Id:                uint64FromNull(id),
			PacketId:          uint64FromNull(packetID),
			OriginNodeId:      uint32FromNull(origin),
			DestinationNodeId: uint32FromNull(dest),
			GatewayId:         gateway.String,
			Direction:         direction.String,
			HopIndex:          uint32FromNull(hopIndex),
			HopNodeId:         uint32FromNull(hopNode),
			Snr:               snr.Float64,
			ReceivedAt:        toTimestamp(receivedAt),
		})
		cursors = append(cursors, packetCursor{
			Timestamp: receivedAt.Float64,
			ID:        id.Int64,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute hops: %v", err)
	}

	nextCursor := ""
	if len(hops) > limit {
		last := cursors[len(cursors)-1]
		nextCursor = encodeCursor(last.Timestamp, last.ID)
		hops = hops[:limit]
	}

	return &meshpipev1.ListTracerouteHopsResponse{
		Hops:       hops,
		NextCursor: nextCursor,
	}, nil
}

func (s *service) queryTracerouteGraph(ctx context.Context, req *meshpipev1.TracerouteGraphRequest) (*meshpipev1.TracerouteGraphResponse, error) {
	now := time.Now().UTC()
	start := now.Add(-24 * time.Hour)
	if req.GetStartTime() != nil {
		start = req.GetStartTime().AsTime()
	}
	end := now
	if req.GetEndTime() != nil {
		end = req.GetEndTime().AsTime()
	}
	if end.Before(start) {
		end = start
	}

	clauses := []string{
		"direction = 'towards'",
		"received_at >= ?",
		"received_at <= ?",
	}
	args := []any{
		float64(start.UnixNano()) / 1e9,
		float64(end.UnixNano()) / 1e9,
	}
	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		clauses = append(clauses, "gateway_id = ?")
		args = append(args, gw)
	}

	whereClause := strings.Join(clauses, " AND ")
	nodeQuery := fmt.Sprintf(`
	    SELECT hop_node_id, COUNT(DISTINCT packet_id) AS packets
	    FROM traceroute_hops
	    WHERE %s
	    GROUP BY hop_node_id`, whereClause)

	nodeRows, err := s.db.QueryContext(ctx, nodeQuery, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute graph nodes: %v", err)
	}
	defer nodeRows.Close()

	var nodes []*meshpipev1.TracerouteGraphNode
	for nodeRows.Next() {
		var (
			nodeID sql.NullInt64
			count  sql.NullInt64
		)
		if err := nodeRows.Scan(&nodeID, &count); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute graph node: %v", err)
		}
		nodes = append(nodes, &meshpipev1.TracerouteGraphNode{
			NodeId:      uint32FromNull(nodeID),
			PacketCount: uint64FromNull(count),
		})
	}
	if err := nodeRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute graph nodes: %v", err)
	}

	edgeQuery := fmt.Sprintf(`
	    SELECT
	        origin_node_id,
	        destination_node_id,
	        AVG(snr) AS avg_snr,
        COUNT(DISTINCT packet_id) AS packets,
        MAX(hop_index + 1) AS max_hops
	    FROM traceroute_hops
	    WHERE %s
	    GROUP BY origin_node_id, destination_node_id`, whereClause)

	edgeRows, err := s.db.QueryContext(ctx, edgeQuery, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute graph edges: %v", err)
	}
	defer edgeRows.Close()

	var edges []*meshpipev1.TracerouteGraphEdge
	for edgeRows.Next() {
		var (
			origin  sql.NullInt64
			dest    sql.NullInt64
			avgSNR  sql.NullFloat64
			count   sql.NullInt64
			maxHops sql.NullInt64
		)
		if err := edgeRows.Scan(&origin, &dest, &avgSNR, &count, &maxHops); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute graph edge: %v", err)
		}
		edges = append(edges, &meshpipev1.TracerouteGraphEdge{
			FromNodeId:  uint32FromNull(origin),
			ToNodeId:    uint32FromNull(dest),
			AvgSnr:      avgSNR.Float64,
			PacketCount: uint64FromNull(count),
			MaxHops:     uint32FromNull(maxHops),
		})
	}
	if err := edgeRows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute graph edges: %v", err)
	}

	return &meshpipev1.TracerouteGraphResponse{
		Nodes: nodes,
		Edges: edges,
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
		packets24h      sql.NullInt64
		lastPacket      sql.NullFloat64
		gateways24h     sql.NullInt64
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
		&packets24h,
		&lastPacket,
		&gateways24h,
		&avgRSSI,
		&avgSNR,
		&avgHops,
	); err != nil {
		return nil, packetCursor{}, status.Errorf(codes.Internal, "scan node: %v", err)
	}

	display := chooseDisplayName(longName.String, shortName.String, hexID.String, nodeID.Int64)

	node := &meshpipev1.Node{
		NodeId:                 uint32FromNull(nodeID),
		HexId:                  hexID.String,
		DisplayName:            display,
		LongName:               longName.String,
		ShortName:              shortName.String,
		HardwareModel:          fmt.Sprintf("%d", hwModel.Int64),
		HardwareModelName:      hwModelName.String,
		Role:                   fmt.Sprintf("%d", role.Int64),
		RoleName:               roleName.String,
		Region:                 region.String,
		RegionName:             regionName.String,
		ModemPreset:            modemPreset.String,
		ModemPresetName:        modemPresetName.String,
		TotalPackets:           uint64FromNull(totalPackets),
		UniqueDestinations:     uint32FromNull(uniqueDest),
		UniqueGateways:         uint32FromNull(uniqueGateways),
		FirstSeen:              toTimestamp(firstSeen),
		LastSeen:               toTimestamp(lastSeen),
		Packets_24H:            uint64FromNull(packets24h),
		LastPacketTime:         toTimestamp(lastPacket),
		GatewayPacketCount_24H: uint64FromNull(gateways24h),
		AvgRssi:                avgRSSI.Float64,
		AvgSnr:                 avgSNR.Float64,
		AvgHops:                avgHops.Float64,
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

func buildPacketFilterClauses(filter *meshpipev1.PacketFilter, alias string) ([]string, []any) {
	if filter == nil {
		return nil, nil
	}
	col := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}

	clauses := make([]string, 0, 8)
	args := make([]any, 0, 8)

	if ts := filter.GetStartTime(); ts != nil {
		clauses = append(clauses, fmt.Sprintf("%s >= ?", col("timestamp")))
		args = append(args, timestampToFloat(ts))
	}
	if ts := filter.GetEndTime(); ts != nil {
		clauses = append(clauses, fmt.Sprintf("%s <= ?", col("timestamp")))
		args = append(args, timestampToFloat(ts))
	}
	if gw := strings.TrimSpace(filter.GetGatewayId()); gw != "" {
		clauses = append(clauses, fmt.Sprintf("%s = ?", col("gateway_id")))
		args = append(args, gw)
	}
	if from := filter.GetFromNodeId(); from != 0 {
		clauses = append(clauses, fmt.Sprintf("%s = ?", col("from_node_id")))
		args = append(args, from)
	}
	if to := filter.GetToNodeId(); to != 0 {
		clauses = append(clauses, fmt.Sprintf("%s = ?", col("to_node_id")))
		args = append(args, to)
	}
	if chans := strings.TrimSpace(filter.GetChannelId()); chans != "" {
		clauses = append(clauses, fmt.Sprintf("%s = ?", col("channel_id")))
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
			clauses = append(clauses, fmt.Sprintf("%s IN (%s)", col("portnum_name"), strings.Join(placeholders, ",")))
		}
	}
	if hop := filter.GetHopCount(); hop != 0 {
		clauses = append(clauses, fmt.Sprintf("(%s IS NOT NULL AND %s IS NOT NULL AND (%s - %s) = ?)",
			col("hop_start"), col("hop_limit"), col("hop_start"), col("hop_limit")))
		args = append(args, hop)
	}
	if search := strings.TrimSpace(filter.GetSearch()); search != "" {
		pattern := "%" + search + "%"
		searchClauses := []string{
			fmt.Sprintf("%s LIKE ?", col("gateway_id")),
			fmt.Sprintf("%s LIKE ?", col("channel_id")),
			fmt.Sprintf("%s LIKE ?", col("portnum_name")),
			fmt.Sprintf("CAST(%s AS TEXT) LIKE ?", col("from_node_id")),
			fmt.Sprintf("CAST(%s AS TEXT) LIKE ?", col("to_node_id")),
		}
		clauses = append(clauses, fmt.Sprintf("(%s)", strings.Join(searchClauses, " OR ")))
		for range searchClauses {
			args = append(args, pattern)
		}
	}

	return clauses, args
}

func calculatePrecisionMeters(gpsAccuracy, pdop sql.NullInt64) float64 {
	if !gpsAccuracy.Valid {
		return 0
	}
	baseMeters := float64(gpsAccuracy.Int64) / 1000.0
	if pdop.Valid && pdop.Int64 > 0 {
		return baseMeters * (float64(pdop.Int64) / 100.0)
	}
	return baseMeters
}

func estimatePrecisionFromBits(bits int64) float64 {
	if bits <= 0 {
		return 0
	}
	// Rough approximation: more precision bits -> smaller error radius.
	scale := math.Pow(2, float64(bits))
	if scale <= 0 {
		return 0
	}
	return 20.0 / scale
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func timestampToFloat(ts *timestamppb.Timestamp) float64 {
	if ts == nil {
		return 0
	}
	return float64(ts.AsTime().UnixNano()) / 1e9
}

func buildAnalyticsWhere(req *meshpipev1.GetAnalyticsSummaryRequest, since float64) ([]string, []any) {
	clauses := []string{"timestamp >= ?"}
	args := []any{since}
	if req == nil {
		return clauses, args
	}
	if gw := strings.TrimSpace(req.GetGatewayId()); gw != "" {
		clauses = append(clauses, "gateway_id = ?")
		args = append(args, gw)
	}
	if from := req.GetFromNodeId(); from != 0 {
		clauses = append(clauses, "from_node_id = ?")
		args = append(args, from)
	}
	if hop := req.GetHopCount(); hop != 0 {
		clauses = append(clauses, "(hop_start IS NOT NULL AND hop_limit IS NOT NULL AND (hop_start - hop_limit) = ?)")
		args = append(args, hop)
	}
	return clauses, args
}

func buildChatFilterClauses(filter *meshpipev1.ChatWindowFilter) ([]string, []any) {
	if filter == nil {
		return nil, nil
	}
	clauses := make([]string, 0, 6)
	args := make([]any, 0, 8)

	if channel := strings.TrimSpace(filter.GetChannelId()); channel != "" {
		clauses = append(clauses, "channel_id = ?")
		args = append(args, channel)
	}
	if audience := strings.TrimSpace(filter.GetAudience()); audience != "" {
		clauses = append(clauses, chatAudienceExpr+" = ?")
		args = append(args, audience)
	}
	if sender := filter.GetSenderNodeId(); sender != 0 {
		clauses = append(clauses, "from_node_id = ?")
		args = append(args, sender)
	}
	if node := filter.GetNodeId(); node != 0 {
		clauses = append(clauses, "(from_node_id = ? OR to_node_id = ?)")
		args = append(args, node, node)
	}
	if search := strings.TrimSpace(filter.GetSearch()); search != "" {
		pattern := "%" + search + "%"
		clauses = append(clauses, "(text LIKE ? OR gateway_id LIKE ?)")
		args = append(args, pattern, pattern)
	}

	return clauses, args
}

func (s *service) loadChatGateways(ctx context.Context, packetIDs []uint64) (map[uint64][]*meshpipev1.ChatMessageGateway, error) {
	result := make(map[uint64][]*meshpipev1.ChatMessageGateway, len(packetIDs))
	if len(packetIDs) == 0 {
		return result, nil
	}

	unique := make([]uint64, 0, len(packetIDs))
	set := make(map[uint64]struct{}, len(packetIDs))
	for _, id := range packetIDs {
		if id == 0 {
			continue
		}
		if _, ok := set[id]; ok {
			continue
		}
		set[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(unique))
	args := make([]any, 0, len(unique))
	for _, id := range unique {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}

	query := fmt.Sprintf(`
	    SELECT
	        packet_id,
	        gateway_id,
	        MIN(min_rssi),
	        MAX(max_rssi),
	        MIN(min_snr),
	        MAX(max_snr)
	    FROM chat_message_gateways
	    WHERE packet_id IN (%s)
	    GROUP BY packet_id, gateway_id`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query chat gateways: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			packetID sql.NullInt64
			gateway  sql.NullString
			minRSSI  sql.NullInt64
			maxRSSI  sql.NullInt64
			minSNR   sql.NullFloat64
			maxSNR   sql.NullFloat64
		)
		if err := rows.Scan(&packetID, &gateway, &minRSSI, &maxRSSI, &minSNR, &maxSNR); err != nil {
			return nil, status.Errorf(codes.Internal, "scan chat gateway: %v", err)
		}
		if !packetID.Valid {
			continue
		}
		rssi := int32(0)
		switch {
		case minRSSI.Valid && maxRSSI.Valid:
			rssi = int32(math.Round((float64(minRSSI.Int64) + float64(maxRSSI.Int64)) / 2.0))
		case minRSSI.Valid:
			rssi = int32(minRSSI.Int64)
		case maxRSSI.Valid:
			rssi = int32(maxRSSI.Int64)
		}

		snr := 0.0
		switch {
		case minSNR.Valid && maxSNR.Valid:
			snr = (minSNR.Float64 + maxSNR.Float64) / 2.0
		case minSNR.Valid:
			snr = minSNR.Float64
		case maxSNR.Valid:
			snr = maxSNR.Float64
		}

		msg := &meshpipev1.ChatMessageGateway{
			GatewayId: gateway.String,
			Rssi:      rssi,
			Snr:       snr,
		}
		id := uint64FromNull(packetID)
		result[id] = append(result[id], msg)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate chat gateways: %v", err)
	}

	return result, nil
}
