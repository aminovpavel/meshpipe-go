package grpcserver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeLocationMeta struct {
	latitude    float64
	longitude   float64
	lastSeen    float64
	displayName string
	role        string
	region      string
}

type topologyNodeStats struct {
	neighbors   map[uint32]struct{}
	snrSum      float64
	snrSamples  uint64
	rssiSum     float64
	rssiSamples uint64
	packetCount uint64
	hopCount    uint64
	lastSeen    float64
}

type linkAccumulator struct {
	source           uint32
	target           uint32
	packetCount      uint64
	tracerouteCount  uint64
	snrSum           float64
	snrSamples       uint64
	lastSeen         float64
	lastPacketID     uint64
	directions       map[string]struct{}
	avgRssi          float64
	avgRssiSamples   uint64
	bidirectional    bool
	gatewayReference string
}

func (s *service) queryNetworkTopology(ctx context.Context, req *meshpipev1.NetworkTopologyRequest) (*meshpipev1.NetworkTopologyResponse, error) {
	now := time.Now().UTC()
	end := timestampToFloat(req.GetEndTime())
	if end <= 0 {
		end = float64(now.UnixNano()) / 1e9
	}
	start := timestampToFloat(req.GetStartTime())
	maxHours := req.GetMaxHours()
	if maxHours == 0 {
		maxHours = 24
	}
	if start <= 0 || start >= end {
		start = end - float64(maxHours)*3600
	}
	if start < 0 {
		start = 0
	}

	minSnr := req.GetMinSnr()
	if minSnr == 0 {
		minSnr = -200
	}
	includePacketLinks := req.GetIncludePacketLinks()
	gatewayFilter := strings.TrimSpace(req.GetGatewayId())

	locations, err := s.fetchLatestNodeLocations(ctx)
	if err != nil {
		return nil, err
	}

	packetLimit := int(req.GetPacketLimit())
	if packetLimit <= 0 || packetLimit > s.maxPageSize {
		packetLimit = s.maxPageSize
	}

	links, nodeStats, stats, err := s.collectTopologyLinks(ctx, start, end, minSnr, includePacketLinks, gatewayFilter, packetLimit, locations)
	if err != nil {
		return nil, err
	}

	if err := s.enrichMissingNodeMetadata(ctx, locations, nodeStats); err != nil {
		return nil, err
	}

	nodes := buildNetworkNodes(locations, nodeStats)

	sort.Slice(nodes, func(i, j int) bool {
		li := timestampToFloat(nodes[i].GetLastSeen())
		lj := timestampToFloat(nodes[j].GetLastSeen())
		if li == lj {
			return nodes[i].GetNodeId() < nodes[j].GetNodeId()
		}
		return li > lj
	})

	sort.Slice(links, func(i, j int) bool {
		if links[i].GetPacketCount() == links[j].GetPacketCount() {
			return links[i].GetStrength() > links[j].GetStrength()
		}
		return links[i].GetPacketCount() > links[j].GetPacketCount()
	})

	return &meshpipev1.NetworkTopologyResponse{
		Nodes: nodes,
		Links: links,
		Stats: stats,
	}, nil
}

func (s *service) fetchLatestNodeLocations(ctx context.Context) (map[uint32]nodeLocationMeta, error) {
	query := `
	    SELECT
	        l.node_id,
	        l.latitude,
	        l.longitude,
	        l.timestamp,
	        l.long_name,
	        l.short_name,
	        ni.role_name,
	        ni.region_name
	    FROM latest_node_locations l
	    LEFT JOIN node_info ni ON ni.node_id = l.node_id`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query latest node locations: %v", err)
	}
	defer rows.Close()

	locations := make(map[uint32]nodeLocationMeta)
	for rows.Next() {
		var (
			nodeID     sql.NullInt64
			lat        sql.NullFloat64
			lon        sql.NullFloat64
			ts         sql.NullFloat64
			longName   sql.NullString
			shortName  sql.NullString
			roleName   sql.NullString
			regionName sql.NullString
		)
		if err := rows.Scan(&nodeID, &lat, &lon, &ts, &longName, &shortName, &roleName, &regionName); err != nil {
			return nil, status.Errorf(codes.Internal, "scan node location: %v", err)
		}
		if !nodeID.Valid {
			continue
		}
		id := uint32(nodeID.Int64)
		locations[id] = nodeLocationMeta{
			latitude:    lat.Float64,
			longitude:   lon.Float64,
			lastSeen:    ts.Float64,
			displayName: firstNonEmpty(longName.String, shortName.String, fmt.Sprintf("!%08X", id)),
			role:        roleName.String,
			region:      regionName.String,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate node locations: %v", err)
	}
	return locations, nil
}

func (s *service) enrichMissingNodeMetadata(ctx context.Context, locations map[uint32]nodeLocationMeta, stats map[uint32]*topologyNodeStats) error {
	missing := make([]uint32, 0)
	for nodeID := range stats {
		if _, ok := locations[nodeID]; !ok {
			missing = append(missing, nodeID)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(missing))
	args := make([]any, 0, len(missing))
	for _, id := range missing {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}

	query := fmt.Sprintf(`
	    SELECT node_id, long_name, short_name, role_name, region_name
	    FROM node_info
	    WHERE node_id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return status.Errorf(codes.Internal, "query missing node metadata: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			nodeID     sql.NullInt64
			longName   sql.NullString
			shortName  sql.NullString
			roleName   sql.NullString
			regionName sql.NullString
		)
		if err := rows.Scan(&nodeID, &longName, &shortName, &roleName, &regionName); err != nil {
			return status.Errorf(codes.Internal, "scan node metadata: %v", err)
		}
		if !nodeID.Valid {
			continue
		}
		id := uint32(nodeID.Int64)
		locations[id] = nodeLocationMeta{
			latitude:    0,
			longitude:   0,
			lastSeen:    0,
			displayName: firstNonEmpty(longName.String, shortName.String, fmt.Sprintf("!%08X", id)),
			role:        roleName.String,
			region:      regionName.String,
		}
	}
	if err := rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "iterate node metadata: %v", err)
	}
	return nil
}

func (s *service) collectTopologyLinks(
	ctx context.Context,
	start float64,
	end float64,
	minSnr float64,
	includePacketLinks bool,
	gatewayFilter string,
	limit int,
	locations map[uint32]nodeLocationMeta,
) ([]*meshpipev1.NetworkLink, map[uint32]*topologyNodeStats, *meshpipev1.NetworkTopologyStats, error) {
	where := []string{
		"from_node_id IS NOT NULL",
		"to_node_id IS NOT NULL",
		"received_at BETWEEN ? AND ?",
	}
	args := []any{start, end}

	if gatewayFilter != "" {
		where = append(where, "gateway_id = ?")
		args = append(args, gatewayFilter)
	}
	if !includePacketLinks {
		where = append(where, "COALESCE(hop_index, 0) > 0")
	}

	query := fmt.Sprintf(`
	    SELECT
	        from_node_id,
	        to_node_id,
	        gateway_id,
	        COUNT(*) AS packet_count,
	        SUM(CASE WHEN COALESCE(hop_index, 0) > 0 THEN 1 ELSE 0 END) AS traceroute_count,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	        AVG(CASE WHEN rssi IS NOT NULL AND rssi != 0 THEN rssi END) AS avg_rssi,
	        MAX(received_at) AS last_seen,
	        MAX(packet_id) AS last_packet_id
	    FROM link_history
	    WHERE %s
	    GROUP BY from_node_id, to_node_id, gateway_id
	    ORDER BY packet_count DESC
	    LIMIT ?`, strings.Join(where, " AND "))

	argsWithLimit := append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, argsWithLimit...)
	if err != nil {
		return nil, nil, nil, status.Errorf(codes.Internal, "query topology links: %v", err)
	}
	defer rows.Close()

	linkMap := make(map[string]*linkAccumulator)
	nodeStats := make(map[uint32]*topologyNodeStats)
	stats := &meshpipev1.NetworkTopologyStats{}

	for rows.Next() {
		var (
			fromNode   sql.NullInt64
			toNode     sql.NullInt64
			gatewayID  sql.NullString
			packetCnt  sql.NullInt64
			traceroute sql.NullInt64
			avgSNR     sql.NullFloat64
			avgRSSI    sql.NullFloat64
			lastSeen   sql.NullFloat64
			lastPacket sql.NullInt64
		)
		if err := rows.Scan(&fromNode, &toNode, &gatewayID, &packetCnt, &traceroute, &avgSNR, &avgRSSI, &lastSeen, &lastPacket); err != nil {
			return nil, nil, nil, status.Errorf(codes.Internal, "scan topology link: %v", err)
		}

		if !fromNode.Valid || !toNode.Valid {
			continue
		}
		packetCount := uint64FromNull(packetCnt)
		if packetCount == 0 {
			continue
		}
		stats.PacketsAnalyzed += packetCount

		tracerouteCount := uint64FromNull(traceroute)
		stats.TotalRfHops += tracerouteCount
		if tracerouteCount > 0 {
			stats.PacketsWithRfHops += tracerouteCount
		}

		avgSnrValue := avgSNR.Float64
		if avgSNR.Valid && avgSnrValue < minSnr {
			stats.LinksFilteredBySnr++
			continue
		}
		if avgSNR.Valid && avgSnrValue == 0 {
			stats.LinksFilteredDueToSnrZero++
		}

		fromID := uint32FromNull(sql.NullInt64{Int64: fromNode.Int64, Valid: true})
		toID := uint32FromNull(sql.NullInt64{Int64: toNode.Int64, Valid: true})

		keySource := fromID
		keyTarget := toID
		if keySource > keyTarget {
			keySource, keyTarget = keyTarget, keySource
		}
		key := fmt.Sprintf("%d-%d", keySource, keyTarget)

		acc, exists := linkMap[key]
		if !exists {
			acc = &linkAccumulator{
				source:           keySource,
				target:           keyTarget,
				directions:       make(map[string]struct{}),
				gatewayReference: gatewayID.String,
			}
			linkMap[key] = acc
		}

		acc.packetCount += packetCount
		acc.tracerouteCount += tracerouteCount
		if avgSNR.Valid {
			acc.snrSum += avgSnrValue * float64(packetCount)
			acc.snrSamples += packetCount
		}
		if avgRSSI.Valid {
			acc.avgRssi += avgRSSI.Float64 * float64(packetCount)
			acc.avgRssiSamples += packetCount
		}
		if last := lastSeen.Float64; last > acc.lastSeen {
			acc.lastSeen = last
			acc.lastPacketID = uint64FromNull(lastPacket)
		}
		dirKey := fmt.Sprintf("%d-%d", fromID, toID)
		acc.directions[dirKey] = struct{}{}
		if len(acc.directions) > 1 {
			acc.bidirectional = true
		}

		updateNodeStats(nodeStats, fromID, toID, avgSnrValue, avgRSSI.Float64, packetCount, tracerouteCount, lastSeen.Float64)
		updateNodeStats(nodeStats, toID, fromID, avgSnrValue, avgRSSI.Float64, packetCount, tracerouteCount, lastSeen.Float64)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, status.Errorf(codes.Internal, "iterate topology links: %v", err)
	}

	links := make([]*meshpipev1.NetworkLink, 0, len(linkMap))
	for _, acc := range linkMap {
		avgSnr := 0.0
		if acc.snrSamples > 0 {
			avgSnr = acc.snrSum / float64(acc.snrSamples)
		}

		srcMeta, srcOk := locations[acc.source]
		dstMeta, dstOk := locations[acc.target]
		distance := 0.0
		if srcOk && dstOk {
			distance = haversineDistance(srcMeta.latitude, srcMeta.longitude, dstMeta.latitude, dstMeta.longitude)
		}

		strength := computeLinkStrength(avgSnr, acc.packetCount)

		link := &meshpipev1.NetworkLink{
			SourceNodeId:    acc.source,
			TargetNodeId:    acc.target,
			AvgSnr:          avgSnr,
			PacketCount:     acc.packetCount,
			TracerouteCount: acc.tracerouteCount,
			LastSeen:        floatToTimestamp(acc.lastSeen),
			DistanceKm:      distance,
			Strength:        strength,
			LastPacketId:    acc.lastPacketID,
			Bidirectional:   acc.bidirectional,
		}
		links = append(links, link)
	}

	return links, nodeStats, stats, nil
}

func updateNodeStats(stats map[uint32]*topologyNodeStats, nodeID, neighborID uint32, snrValue, rssiValue float64, packetCount, tracerouteCount uint64, lastSeen float64) {
	ns, ok := stats[nodeID]
	if !ok {
		ns = &topologyNodeStats{
			neighbors: make(map[uint32]struct{}),
		}
		stats[nodeID] = ns
	}
	if neighborID != 0 {
		ns.neighbors[neighborID] = struct{}{}
	}
	ns.packetCount += packetCount
	ns.hopCount += tracerouteCount
	if !math.IsNaN(snrValue) && snrValue != 0 {
		ns.snrSum += snrValue * float64(packetCount)
		ns.snrSamples += packetCount
	}
	if !math.IsNaN(rssiValue) && rssiValue != 0 {
		ns.rssiSum += rssiValue * float64(packetCount)
		ns.rssiSamples += packetCount
	}
	if lastSeen > ns.lastSeen {
		ns.lastSeen = lastSeen
	}
}

func buildNetworkNodes(locations map[uint32]nodeLocationMeta, stats map[uint32]*topologyNodeStats) []*meshpipev1.NetworkNode {
	nodeIDs := make([]uint32, 0, len(locations)+len(stats))
	seen := make(map[uint32]struct{})
	for id := range locations {
		nodeIDs = append(nodeIDs, id)
		seen[id] = struct{}{}
	}
	for id := range stats {
		if _, ok := seen[id]; !ok {
			nodeIDs = append(nodeIDs, id)
		}
	}

	nodes := make([]*meshpipev1.NetworkNode, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		meta := locations[id]
		stat := stats[id]

		avgSnr := 0.0
		if stat != nil && stat.snrSamples > 0 {
			avgSnr = stat.snrSum / float64(stat.snrSamples)
		}
		avgRssi := 0.0
		if stat != nil && stat.rssiSamples > 0 {
			avgRssi = stat.rssiSum / float64(stat.rssiSamples)
		}
		neighborCount := uint32(0)
		lastSeen := meta.lastSeen
		if stat != nil {
			neighborCount = uint32(len(stat.neighbors))
			if stat.lastSeen > lastSeen {
				lastSeen = stat.lastSeen
			}
		}

		nodes = append(nodes, &meshpipev1.NetworkNode{
			NodeId:        id,
			DisplayName:   meta.displayName,
			Latitude:      meta.latitude,
			Longitude:     meta.longitude,
			AvgSnr:        avgSnr,
			AvgRssi:       avgRssi,
			NeighborCount: neighborCount,
			LastSeen:      floatToTimestamp(lastSeen),
			Role:          meta.role,
			Region:        meta.region,
		})
	}
	return nodes
}

func computeLinkStrength(avgSnr float64, packetCount uint64) float64 {
	if packetCount == 0 {
		return 1.0
	}
	base := (avgSnr + 20.0) / 5.0
	if base < 0 {
		base = 0
	}
	strength := base + math.Log10(float64(packetCount))
	if strength < 1 {
		strength = 1
	}
	if strength > 10 {
		strength = 10
	}
	return strength
}

func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	if lat1 == 0 && lon1 == 0 {
		return 0
	}
	if lat2 == 0 && lon2 == 0 {
		return 0
	}

	const earthRadius = 6371.0
	radLat1 := lat1 * math.Pi / 180
	radLat2 := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(radLat1)*math.Cos(radLat2)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadius * c
}

func (s *service) queryLongestLinks(ctx context.Context, req *meshpipev1.LongestLinksRequest) (*meshpipev1.LongestLinksResponse, error) {
	now := time.Now().UTC()
	end := float64(now.UnixNano()) / 1e9
	lookback := req.GetLookbackHours()
	if lookback == 0 {
		lookback = 168
	}
	start := end - float64(lookback)*3600
	if start < 0 {
		start = 0
	}

	minDistance := req.GetMinDistanceKm()
	if minDistance <= 0 {
		minDistance = 1.0
	}
	minSnr := req.GetMinSnr()
	if minSnr == 0 {
		minSnr = -200
	}
	maxResults := int(req.GetMaxResults())
	if maxResults <= 0 {
		maxResults = 25
	}

	locations, err := s.fetchLatestNodeLocations(ctx)
	if err != nil {
		return nil, err
	}

	direct, packetsConsidered, err := s.collectDirectLongestLinks(ctx, start, minSnr, minDistance, maxResults*4, locations)
	if err != nil {
		return nil, err
	}

	indirect, hopsProcessed, err := s.collectIndirectLongestPaths(ctx, start, minSnr, maxResults*4, locations)
	if err != nil {
		return nil, err
	}

	if len(direct) > maxResults {
		direct = direct[:maxResults]
	}
	if len(indirect) > maxResults {
		indirect = indirect[:maxResults]
	}

	sort.Slice(direct, func(i, j int) bool {
		return direct[i].DistanceKm > direct[j].DistanceKm
	})
	sort.Slice(indirect, func(i, j int) bool {
		return indirect[i].TotalDistanceKm > indirect[j].TotalDistanceKm
	})

	return &meshpipev1.LongestLinksResponse{
		DirectLinks:   direct,
		IndirectPaths: indirect,
		Stats: &meshpipev1.LongestLinksStats{
			PacketsConsidered: packetsConsidered,
			HopsProcessed:     hopsProcessed,
		},
	}, nil
}

func (s *service) collectDirectLongestLinks(
	ctx context.Context,
	start float64,
	minSnr float64,
	minDistance float64,
	limit int,
	locations map[uint32]nodeLocationMeta,
) ([]*meshpipev1.DirectLongestLink, uint64, error) {
	query := `
	    SELECT
	        from_node_id,
	        to_node_id,
	        COUNT(*) AS packet_count,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	        MAX(received_at) AS last_seen
	    FROM link_history
	    WHERE COALESCE(hop_index, 0) = 0
	      AND from_node_id IS NOT NULL
	      AND to_node_id IS NOT NULL
	      AND received_at >= ?
	    GROUP BY from_node_id, to_node_id
	    ORDER BY packet_count DESC
	    LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, start, limit)
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "query direct longest links: %v", err)
	}
	defer rows.Close()

	type directCandidate struct {
		fromNode    uint32
		toNode      uint32
		packetCount uint64
		avgSnr      float64
		lastSeen    float64
		distance    float64
	}

	candidates := make([]directCandidate, 0, limit)
	var packetsConsidered uint64

	for rows.Next() {
		var (
			fromNode  sql.NullInt64
			toNode    sql.NullInt64
			packetCnt sql.NullInt64
			avgSnr    sql.NullFloat64
			lastSeen  sql.NullFloat64
		)
		if err := rows.Scan(&fromNode, &toNode, &packetCnt, &avgSnr, &lastSeen); err != nil {
			return nil, 0, status.Errorf(codes.Internal, "scan direct longest link: %v", err)
		}
		if !fromNode.Valid || !toNode.Valid {
			continue
		}
		packetCount := uint64FromNull(packetCnt)
		if packetCount == 0 {
			continue
		}

		if avgSnr.Valid && avgSnr.Float64 < minSnr {
			continue
		}

		fromID := uint32(fromNode.Int64)
		toID := uint32(toNode.Int64)
		srcMeta, srcOk := locations[fromID]
		dstMeta, dstOk := locations[toID]
		if !srcOk || !dstOk {
			continue
		}
		distance := haversineDistance(srcMeta.latitude, srcMeta.longitude, dstMeta.latitude, dstMeta.longitude)
		if distance < minDistance {
			continue
		}
		candidates = append(candidates, directCandidate{
			fromNode:    fromID,
			toNode:      toID,
			packetCount: packetCount,
			avgSnr:      avgSnr.Float64,
			lastSeen:    lastSeen.Float64,
			distance:    distance,
		})
		packetsConsidered += packetCount
	}
	if err := rows.Err(); err != nil {
		return nil, 0, status.Errorf(codes.Internal, "iterate direct longest links: %v", err)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance == candidates[j].distance {
			return candidates[i].packetCount > candidates[j].packetCount
		}
		return candidates[i].distance > candidates[j].distance
	})

	results := make([]*meshpipev1.DirectLongestLink, 0, len(candidates))
	for _, cand := range candidates {
		recentPackets, err := s.fetchRecentPacketsForLink(ctx, cand.fromNode, cand.toNode, start)
		if err != nil {
			return nil, 0, err
		}
		results = append(results, &meshpipev1.DirectLongestLink{
			FromNodeId:      cand.fromNode,
			ToNodeId:        cand.toNode,
			DistanceKm:      cand.distance,
			AvgSnr:          cand.avgSnr,
			LastSeen:        floatToTimestamp(cand.lastSeen),
			TracerouteCount: 0,
			RecentPackets:   recentPackets,
		})
	}

	return results, packetsConsidered, nil
}

func (s *service) fetchRecentPacketsForLink(ctx context.Context, fromNode, toNode uint32, start float64) ([]*meshpipev1.LongestLinkPacket, error) {
	query := `
	    SELECT ph.id, ph.timestamp, lh.snr, lh.rssi
	    FROM link_history lh
	    JOIN packet_history ph ON ph.id = lh.packet_id
	    WHERE lh.from_node_id = ? AND lh.to_node_id = ? AND COALESCE(lh.hop_index, 0) = 0 AND lh.received_at >= ?
	    ORDER BY lh.received_at DESC
	    LIMIT 5`

	rows, err := s.db.QueryContext(ctx, query, fromNode, toNode, start)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query recent packets for link: %v", err)
	}
	defer rows.Close()

	var packets []*meshpipev1.LongestLinkPacket
	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			snr      sql.NullFloat64
			rssi     sql.NullInt64
		)
		if err := rows.Scan(&packetID, &ts, &snr, &rssi); err != nil {
			return nil, status.Errorf(codes.Internal, "scan recent packet: %v", err)
		}
		if !packetID.Valid {
			continue
		}
		packets = append(packets, &meshpipev1.LongestLinkPacket{
			PacketId:  uint64(packetID.Int64),
			Timestamp: floatToTimestamp(ts.Float64),
			Snr:       snr.Float64,
			Rssi:      int32(rssi.Int64),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate recent packets: %v", err)
	}
	return packets, nil
}

func (s *service) collectIndirectLongestPaths(
	ctx context.Context,
	start float64,
	minSnr float64,
	limit int,
	locations map[uint32]nodeLocationMeta,
) ([]*meshpipev1.IndirectLongestPath, uint64, error) {
	query := `
	    SELECT
	        origin_node_id,
	        destination_node_id,
	        COUNT(DISTINCT packet_id) AS packet_count,
	        MAX(hop_index + 1) AS max_hops,
	        AVG(CASE WHEN snr IS NOT NULL THEN snr END) AS avg_snr,
	        MAX(received_at) AS last_seen
	    FROM traceroute_hops
	    WHERE direction = 'towards' AND received_at >= ?
	    GROUP BY origin_node_id, destination_node_id
	    ORDER BY max_hops DESC, last_seen DESC
	    LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, start, limit)
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "query indirect longest paths: %v", err)
	}
	defer rows.Close()

	type pathCandidate struct {
		startNode   uint32
		endNode     uint32
		packetCount uint64
		maxHops     uint64
		avgSnr      float64
		lastSeen    float64
		distance    float64
	}

	candidates := make([]pathCandidate, 0, limit)
	var totalHops uint64

	for rows.Next() {
		var (
			startNode sql.NullInt64
			endNode   sql.NullInt64
			packetCnt sql.NullInt64
			maxHops   sql.NullInt64
			avgSnr    sql.NullFloat64
			lastSeen  sql.NullFloat64
		)
		if err := rows.Scan(&startNode, &endNode, &packetCnt, &maxHops, &avgSnr, &lastSeen); err != nil {
			return nil, 0, status.Errorf(codes.Internal, "scan indirect path: %v", err)
		}
		if !startNode.Valid || !endNode.Valid {
			continue
		}
		if maxHops.Int64 <= 1 {
			continue
		}
		if avgSnr.Valid && avgSnr.Float64 < minSnr {
			continue
		}

		startID := uint32(startNode.Int64)
		endID := uint32(endNode.Int64)
		startMeta, okStart := locations[startID]
		endMeta, okEnd := locations[endID]
		if !okStart || !okEnd {
			continue
		}

		distance := haversineDistance(startMeta.latitude, startMeta.longitude, endMeta.latitude, endMeta.longitude)
		if distance <= 0 {
			continue
		}

		packetCount := uint64FromNull(packetCnt)
		candidates = append(candidates, pathCandidate{
			startNode:   startID,
			endNode:     endID,
			packetCount: packetCount,
			maxHops:     uint64FromNull(maxHops),
			avgSnr:      avgSnr.Float64,
			lastSeen:    lastSeen.Float64,
			distance:    distance,
		})
		totalHops += uint64FromNull(maxHops)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, status.Errorf(codes.Internal, "iterate indirect paths: %v", err)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance == candidates[j].distance {
			return candidates[i].packetCount > candidates[j].packetCount
		}
		return candidates[i].distance > candidates[j].distance
	})

	results := make([]*meshpipev1.IndirectLongestPath, 0, len(candidates))
	for _, cand := range candidates {
		recentIDs, err := s.fetchTraceroutePacketIDs(ctx, cand.startNode, cand.endNode, start)
		if err != nil {
			return nil, 0, err
		}
		results = append(results, &meshpipev1.IndirectLongestPath{
			StartNodeId:         cand.startNode,
			EndNodeId:           cand.endNode,
			HopCount:            uint32(cand.maxHops),
			TotalDistanceKm:     cand.distance,
			AvgSnr:              cand.avgSnr,
			LastSeen:            floatToTimestamp(cand.lastSeen),
			TraceroutePacketIds: recentIDs,
		})
	}

	return results, totalHops, nil
}

func (s *service) fetchTraceroutePacketIDs(ctx context.Context, startNode, endNode uint32, start float64) ([]uint64, error) {
	query := `
	    SELECT DISTINCT packet_id
	    FROM traceroute_hops
	    WHERE direction = 'towards'
	      AND origin_node_id = ?
	      AND destination_node_id = ?
	      AND received_at >= ?
	    ORDER BY received_at DESC
	    LIMIT 5`

	rows, err := s.db.QueryContext(ctx, query, startNode, endNode, start)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query traceroute packet ids: %v", err)
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id sql.NullInt64
		if err := rows.Scan(&id); err != nil {
			return nil, status.Errorf(codes.Internal, "scan traceroute packet id: %v", err)
		}
		if id.Valid {
			ids = append(ids, uint64(id.Int64))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate traceroute packet ids: %v", err)
	}
	return ids, nil
}

func (s *service) queryGatewayComparison(ctx context.Context, req *meshpipev1.GatewayComparisonRequest) (*meshpipev1.GatewayComparisonResponse, error) {
	if strings.TrimSpace(req.GetGatewayIdA()) == "" || strings.TrimSpace(req.GetGatewayIdB()) == "" {
		return nil, status.Error(codes.InvalidArgument, "both gateway ids are required")
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

	minSnr := req.GetMinSnr()
	if minSnr == 0 {
		minSnr = -200
	}

	query := `
	    SELECT
	        ph.id,
	        ph.timestamp,
	        ph.from_node_id,
	        ph.portnum_name,
	        ga.rssi AS rssi_a,
	        ga.snr AS snr_a,
	        gb.rssi AS rssi_b,
	        gb.snr AS snr_b
	    FROM packet_history ph
	    JOIN link_history ga ON ga.packet_id = ph.id AND ga.gateway_id = ?
	    JOIN link_history gb ON gb.packet_id = ph.id AND gb.gateway_id = ?
	    WHERE ph.timestamp BETWEEN ? AND ?
	    ORDER BY ph.timestamp DESC
	    LIMIT 1000`

	rows, err := s.db.QueryContext(ctx, query, strings.TrimSpace(req.GetGatewayIdA()), strings.TrimSpace(req.GetGatewayIdB()), start, end)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query gateway comparison packets: %v", err)
	}
	defer rows.Close()

	var (
		packets     []*meshpipev1.GatewayComparisonPacket
		snrDiffSum  float64
		rssiDiffSum float64
		snrSamples  float64
		rssiSamples float64
		firstSeen   float64
		lastSeen    float64
	)

	for rows.Next() {
		var (
			packetID sql.NullInt64
			ts       sql.NullFloat64
			fromNode sql.NullInt64
			portName sql.NullString
			rssiA    sql.NullInt64
			snrA     sql.NullFloat64
			rssiB    sql.NullInt64
			snrB     sql.NullFloat64
		)
		if err := rows.Scan(&packetID, &ts, &fromNode, &portName, &rssiA, &snrA, &rssiB, &snrB); err != nil {
			return nil, status.Errorf(codes.Internal, "scan gateway comparison packet: %v", err)
		}
		if !packetID.Valid || !ts.Valid {
			continue
		}
		if (snrA.Valid && snrA.Float64 < minSnr) || (snrB.Valid && snrB.Float64 < minSnr) {
			continue
		}

		var snrDiff float64
		if snrA.Valid && snrB.Valid {
			snrDiff = snrA.Float64 - snrB.Float64
			snrDiffSum += snrDiff
			snrSamples++
		}
		var rssiDiff float64
		if rssiA.Valid && rssiB.Valid {
			rssiDiff = float64(rssiA.Int64 - rssiB.Int64)
			rssiDiffSum += rssiDiff
			rssiSamples++
		}

		if firstSeen == 0 || ts.Float64 < firstSeen {
			firstSeen = ts.Float64
		}
		if ts.Float64 > lastSeen {
			lastSeen = ts.Float64
		}

		var snrAValue float64
		if snrA.Valid {
			snrAValue = snrA.Float64
		}
		var snrBValue float64
		if snrB.Valid {
			snrBValue = snrB.Float64
		}
		var rssiAValue int32
		if rssiA.Valid {
			rssiAValue = int32(rssiA.Int64)
		}
		var rssiBValue int32
		if rssiB.Valid {
			rssiBValue = int32(rssiB.Int64)
		}

		packets = append(packets, &meshpipev1.GatewayComparisonPacket{
			PacketId:     uint64(packetID.Int64),
			Timestamp:    floatToTimestamp(ts.Float64),
			FromNodeId:   uint32FromNull(fromNode),
			GatewayARssi: float64(rssiAValue),
			GatewayBRssi: float64(rssiBValue),
			GatewayASnr:  snrAValue,
			GatewayBSnr:  snrBValue,
			RssiDiff:     rssiDiff,
			SnrDiff:      snrDiff,
			PortnumName:  portName.String,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate gateway comparison packets: %v", err)
	}

	var avgSnrDiff, avgRssiDiff float64
	if snrSamples > 0 {
		avgSnrDiff = snrDiffSum / snrSamples
	}
	if rssiSamples > 0 {
		avgRssiDiff = rssiDiffSum / rssiSamples
	}

	overviewReq := &meshpipev1.GetGatewayOverviewRequest{
		GatewayId: req.GetGatewayIdA(),
		StartTime: floatToTimestamp(start),
		EndTime:   floatToTimestamp(end),
		Limit:     1,
	}
	gatewayA, err := s.queryGatewayOverview(ctx, overviewReq)
	if err != nil {
		return nil, err
	}
	overviewReq.GatewayId = req.GetGatewayIdB()
	gatewayB, err := s.queryGatewayOverview(ctx, overviewReq)
	if err != nil {
		return nil, err
	}

	resp := &meshpipev1.GatewayComparisonResponse{
		Packets:     packets,
		AvgSnrDiff:  avgSnrDiff,
		AvgRssiDiff: avgRssiDiff,
		FirstSeen:   floatToTimestamp(firstSeen),
		LastSeen:    floatToTimestamp(lastSeen),
	}
	if len(gatewayA) > 0 {
		resp.GatewayA = gatewayA[0]
	}
	if len(gatewayB) > 0 {
		resp.GatewayB = gatewayB[0]
	}
	return resp, nil
}

func (s *service) queryGatewayCandidates(ctx context.Context, req *meshpipev1.GatewayCandidatesRequest) (*meshpipev1.GatewayCandidatesResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 || limit > s.maxPageSize {
		limit = 20
	}

	queryText := strings.TrimSpace(req.GetQuery())
	popularOnly := req.GetPopularOnly()

	var rows *sql.Rows
	var err error
	if queryText == "" || popularOnly {
		rows, err = s.db.QueryContext(ctx, `
		    SELECT gateway_id, COUNT(*) AS packet_count
		    FROM link_history
		    WHERE gateway_id IS NOT NULL
		    GROUP BY gateway_id
		    ORDER BY packet_count DESC
		    LIMIT ?`, limit)
	} else {
		pattern := "%" + escapeLike(queryText) + "%"
		rows, err = s.db.QueryContext(ctx, `
		    SELECT gateway_id, COUNT(*) AS packet_count
		    FROM link_history
		    WHERE gateway_id LIKE ? ESCAPE '\'
		    GROUP BY gateway_id
		    ORDER BY packet_count DESC
		    LIMIT ?`, pattern, limit)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query gateway candidates: %v", err)
	}
	defer rows.Close()

	type candidateRow struct {
		id          sql.NullString
		packetCount sql.NullInt64
	}

	candidates := make([]candidateRow, 0, limit)
	for rows.Next() {
		var row candidateRow
		if err := rows.Scan(&row.id, &row.packetCount); err != nil {
			return nil, status.Errorf(codes.Internal, "scan gateway candidate: %v", err)
		}
		if row.id.String != "" {
			candidates = append(candidates, row)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate gateway candidates: %v", err)
	}

	result := &meshpipev1.GatewayCandidatesResponse{
		IsPopular:  popularOnly || queryText == "",
		TotalCount: uint64(len(candidates)),
	}

	if len(candidates) == 0 {
		return result, nil
	}

	nodeIDs := make([]uint32, 0, len(candidates))
	for _, cand := range candidates {
		if nodeID, ok := gatewayIDToNodeID(cand.id.String); ok {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	nameMap, err := s.fetchNodeNames(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}

	for _, cand := range candidates {
		displayName := cand.id.String
		var nodeID uint32
		if id, ok := gatewayIDToNodeID(cand.id.String); ok {
			nodeID = id
			if name, exists := nameMap[id]; exists && name != "" {
				displayName = fmt.Sprintf("%s (%s)", name, cand.id.String)
			}
		}
		result.Gateways = append(result.Gateways, &meshpipev1.GatewayCandidate{
			GatewayId:   cand.id.String,
			DisplayName: displayName,
			PacketCount: uint64FromNull(cand.packetCount),
			NodeId:      nodeID,
		})
	}

	return result, nil
}

func gatewayIDToNodeID(gateway string) (uint32, bool) {
	if len(gateway) < 2 || gateway[0] != '!' {
		return 0, false
	}
	value, err := strconv.ParseUint(gateway[1:], 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(value), true
}

func (s *service) fetchNodeNames(ctx context.Context, nodeIDs []uint32) (map[uint32]string, error) {
	if len(nodeIDs) == 0 {
		return map[uint32]string{}, nil
	}
	placeholders := make([]string, 0, len(nodeIDs))
	args := make([]any, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	query := fmt.Sprintf(`
	    SELECT node_id, long_name, short_name
	    FROM node_info
	    WHERE node_id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query node names: %v", err)
	}
	defer rows.Close()

	names := make(map[uint32]string, len(nodeIDs))
	for rows.Next() {
		var (
			nodeID    sql.NullInt64
			longName  sql.NullString
			shortName sql.NullString
		)
		if err := rows.Scan(&nodeID, &longName, &shortName); err != nil {
			return nil, status.Errorf(codes.Internal, "scan node name: %v", err)
		}
		if nodeID.Valid {
			id := uint32(nodeID.Int64)
			names[id] = firstNonEmpty(longName.String, shortName.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate node names: %v", err)
	}
	return names, nil
}
