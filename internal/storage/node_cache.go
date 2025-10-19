package storage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
)

type nodeEntry struct {
	NodeID          uint32
	UserID          string
	HexID           string
	LongName        string
	ShortName       string
	HWModel         int32
	HWModelName     string
	Role            int32
	RoleName        string
	IsLicensed      bool
	MacAddress      string
	PrimaryChannel  string
	Snr             float32
	LastHeard       uint32
	ViaMQTT         bool
	Channel         uint32
	HopsAway        *uint32
	IsFavorite      bool
	IsIgnored       bool
	IsKeyVerified   bool
	FirstSeen       time.Time
	LastUpdated     time.Time
	Region          string
	RegionName      string
	FirmwareVersion string
	ModemPreset     string
	ModemPresetName string
}

type nodeCache struct {
	mu    sync.RWMutex
	nodes map[uint32]*nodeEntry
}

func newNodeCache() *nodeCache {
	return &nodeCache{
		nodes: make(map[uint32]*nodeEntry),
	}
}

func (c *nodeCache) load(db *sql.DB) error {
	rows, err := db.Query(`
        SELECT
            node_id,
            COALESCE(hex_id, ''),
            COALESCE(user_id, ''),
            COALESCE(long_name, ''),
            COALESCE(short_name, ''),
            COALESCE(hw_model, 0),
            COALESCE(hw_model_name, ''),
            COALESCE(role, 0),
            COALESCE(role_name, ''),
            COALESCE(is_licensed, 0),
            COALESCE(mac_address, ''),
            COALESCE(primary_channel, ''),
            COALESCE(snr, 0),
            COALESCE(last_heard, 0),
            COALESCE(via_mqtt, 0),
            COALESCE(channel, 0),
            hops_away,
            COALESCE(is_favorite, 0),
            COALESCE(is_ignored, 0),
            COALESCE(is_key_verified, 0),
            COALESCE(first_seen, 0),
            COALESCE(last_updated, 0),
            COALESCE(region, ''),
            COALESCE(region_name, ''),
            COALESCE(firmware_version, ''),
            COALESCE(modem_preset, ''),
            COALESCE(modem_preset_name, '')
        FROM node_info
    `)
	if err != nil {
		return fmt.Errorf("node cache load query: %w", err)
	}
	defer rows.Close()

	c.mu.Lock()
	defer c.mu.Unlock()

	for rows.Next() {
		var (
			nodeID          int64
			hexID           string
			userID          string
			longName        string
			shortName       string
			hwModel         int64
			hwModelName     string
			role            int64
			roleName        string
			isLicensed      int64
			macAddress      string
			primary         string
			snr             float64
			lastHeard       int64
			viaMQTT         int64
			channel         int64
			hopsAwayNull    sql.NullInt64
			isFavorite      int64
			isIgnored       int64
			isKeyVerified   int64
			firstSeen       float64
			lastUpdated     float64
			region          string
			regionName      string
			firmware        string
			modemPreset     string
			modemPresetName string
		)

		if err := rows.Scan(
			&nodeID,
			&hexID,
			&userID,
			&longName,
			&shortName,
			&hwModel,
			&hwModelName,
			&role,
			&roleName,
			&isLicensed,
			&macAddress,
			&primary,
			&snr,
			&lastHeard,
			&viaMQTT,
			&channel,
			&hopsAwayNull,
			&isFavorite,
			&isIgnored,
			&isKeyVerified,
			&firstSeen,
			&lastUpdated,
			&region,
			&regionName,
			&firmware,
			&modemPreset,
			&modemPresetName,
		); err != nil {
			return fmt.Errorf("node cache scan: %w", err)
		}

		entry := &nodeEntry{
			NodeID:          uint32(nodeID),
			UserID:          userID,
			HexID:           fallbackString(hexID, userID),
			LongName:        longName,
			ShortName:       shortName,
			HWModel:         int32(hwModel),
			HWModelName:     hwModelName,
			Role:            int32(role),
			RoleName:        roleName,
			IsLicensed:      isLicensed != 0,
			MacAddress:      macAddress,
			PrimaryChannel:  primary,
			Snr:             float32(snr),
			LastHeard:       uint32(lastHeard),
			ViaMQTT:         viaMQTT != 0,
			Channel:         uint32(channel),
			IsFavorite:      isFavorite != 0,
			IsIgnored:       isIgnored != 0,
			IsKeyVerified:   isKeyVerified != 0,
			FirstSeen:       secondsToTime(firstSeen),
			LastUpdated:     secondsToTime(lastUpdated),
			Region:          region,
			RegionName:      regionName,
			FirmwareVersion: firmware,
			ModemPreset:     modemPreset,
			ModemPresetName: modemPresetName,
		}
		if hopsAwayNull.Valid {
			val := uint32(hopsAwayNull.Int64)
			entry.HopsAway = &val
		}

		c.nodes[entry.NodeID] = entry
	}

	return rows.Err()
}

func (c *nodeCache) updateFromPacket(pkt decode.Packet) *nodeEntry {
	if pkt.Node == nil || pkt.From == 0 {
		return nil
	}

	node := pkt.Node
	update := nodeUpdate{
		NodeID:          pkt.From,
		UserID:          node.UserID,
		HexID:           node.UserID,
		PrimaryChannel:  nonEmpty(node.PrimaryChannel, pkt.ChannelID),
		LongName:        node.LongName,
		ShortName:       node.ShortName,
		HWModel:         node.HWModel,
		HWModelName:     node.HWModelName,
		Role:            node.Role,
		RoleName:        node.RoleName,
		IsLicensed:      node.IsLicensed,
		MacAddress:      node.MacAddress,
		Snr:             node.Snr,
		LastHeard:       node.LastHeard,
		ViaMQTT:         node.ViaMQTT,
		Channel:         node.Channel,
		HopsAway:        node.HopsAway,
		IsFavorite:      node.IsFavorite,
		IsIgnored:       node.IsIgnored,
		IsKeyVerified:   node.IsKeyVerified,
		Region:          node.Region,
		RegionName:      node.RegionName,
		FirmwareVersion: node.FirmwareVersion,
		ModemPreset:     node.ModemPreset,
		ModemPresetName: node.ModemPresetName,
		UpdatedAt:       pkt.ReceivedAt,
	}

	return c.merge(update)
}

func (c *nodeCache) ensureGateway(hexID string, updatedAt time.Time) (*nodeEntry, bool) {
	nodeID, ok := hexIDToNumeric(hexID)
	if !ok {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.nodes[nodeID]; exists {
		return entry, false
	}

	entry := &nodeEntry{
		NodeID:      nodeID,
		UserID:      hexID,
		HexID:       hexID,
		FirstSeen:   updatedAt,
		LastUpdated: updatedAt,
	}
	c.nodes[nodeID] = entry
	return entry, true
}

func (c *nodeCache) merge(update nodeUpdate) *nodeEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.nodes[update.NodeID]
	if !exists {
		entry = &nodeEntry{
			NodeID:      update.NodeID,
			FirstSeen:   update.UpdatedAt,
			LastUpdated: update.UpdatedAt,
		}
		c.nodes[update.NodeID] = entry
	}

	if entry.FirstSeen.IsZero() {
		entry.FirstSeen = update.UpdatedAt
	}
	if update.UpdatedAt.After(entry.LastUpdated) || entry.LastUpdated.IsZero() {
		entry.LastUpdated = update.UpdatedAt
	}

	if update.UserID != "" {
		entry.UserID = update.UserID
	}
	if update.HexID != "" {
		entry.HexID = update.HexID
	} else if entry.HexID == "" {
		entry.HexID = entry.UserID
	}
	if update.LongName != "" {
		entry.LongName = update.LongName
	}
	if update.ShortName != "" {
		entry.ShortName = update.ShortName
	}
	if update.HWModel != 0 || entry.HWModel == 0 {
		entry.HWModel = update.HWModel
	}
	if update.HWModelName != "" {
		entry.HWModelName = update.HWModelName
	}
	if update.Role != 0 || entry.Role == 0 {
		entry.Role = update.Role
	}
	if update.RoleName != "" {
		entry.RoleName = update.RoleName
	}
	entry.IsLicensed = update.IsLicensed
	if update.MacAddress != "" {
		entry.MacAddress = update.MacAddress
	}
	if update.PrimaryChannel != "" {
		entry.PrimaryChannel = update.PrimaryChannel
	}
	entry.Snr = update.Snr
	if update.LastHeard != 0 {
		entry.LastHeard = update.LastHeard
	}
	entry.ViaMQTT = update.ViaMQTT
	if update.Channel != 0 {
		entry.Channel = update.Channel
	}
	if update.HopsAway != nil {
		val := *update.HopsAway
		entry.HopsAway = &val
	}
	entry.IsFavorite = update.IsFavorite
	entry.IsIgnored = update.IsIgnored
	entry.IsKeyVerified = update.IsKeyVerified
	if update.Region != "" {
		entry.Region = update.Region
	}
	if update.RegionName != "" {
		entry.RegionName = update.RegionName
	}
	if update.FirmwareVersion != "" {
		entry.FirmwareVersion = update.FirmwareVersion
	}
	if update.ModemPreset != "" {
		entry.ModemPreset = update.ModemPreset
	}
	if update.ModemPresetName != "" {
		entry.ModemPresetName = update.ModemPresetName
	}

	if entry.HexID == "" {
		entry.HexID = entry.UserID
	}

	return entry
}

type nodeUpdate struct {
	NodeID          uint32
	UserID          string
	HexID           string
	PrimaryChannel  string
	LongName        string
	ShortName       string
	HWModel         int32
	HWModelName     string
	Role            int32
	RoleName        string
	IsLicensed      bool
	MacAddress      string
	Snr             float32
	LastHeard       uint32
	ViaMQTT         bool
	Channel         uint32
	HopsAway        *uint32
	IsFavorite      bool
	IsIgnored       bool
	IsKeyVerified   bool
	Region          string
	RegionName      string
	FirmwareVersion string
	ModemPreset     string
	ModemPresetName string
	UpdatedAt       time.Time
}

func fallbackString(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func nonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func hexIDToNumeric(hexID string) (uint32, bool) {
	trimmed := strings.TrimSpace(hexID)
	if trimmed == "" {
		return 0, false
	}
	trimmed = strings.TrimPrefix(trimmed, "!")
	value, err := strconv.ParseUint(trimmed, 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(value), true
}
