package storage

import (
	"testing"
	"time"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
)

func TestNodeCacheUpdatePreservesFirstSeen(t *testing.T) {
	cache := newNodeCache()

	now := time.Unix(1_700_000_000, 123_000_000)
	pkt := decode.Packet{
		From:                  0x1234,
		ChannelID:             "Primary",
		ReceivedAt:            now,
		ProcessedSuccessfully: true,
		Node: &decode.NodeInfo{
			UserID:     "!12345678",
			LongName:   "First Node",
			ShortName:  "First",
			HWModel:    7,
			Role:       3,
			IsLicensed: true,
			Snr:        12.5,
			LastHeard:  42,
			ViaMQTT:    true,
			Channel:    9,
		},
	}

	entry := cache.updateFromPacket(pkt)
	if entry == nil {
		t.Fatalf("expected entry to be created")
	}
	if !entry.FirstSeen.Equal(now) {
		t.Fatalf("expected first seen %v, got %v", now, entry.FirstSeen)
	}
	if !entry.LastUpdated.Equal(now) {
		t.Fatalf("expected last updated %v, got %v", now, entry.LastUpdated)
	}
	if entry.PrimaryChannel != "Primary" {
		t.Fatalf("expected primary channel Primary, got %s", entry.PrimaryChannel)
	}

	later := now.Add(15 * time.Second)
	second := decode.Packet{
		From:       0x1234,
		ChannelID:  "",
		ReceivedAt: later,
		Node: &decode.NodeInfo{
			UserID:         "!12345678",
			LongName:       "",
			ShortName:      "Second",
			HWModel:        0,
			Role:           0,
			IsLicensed:     false,
			PrimaryChannel: "",
		},
	}

	updated := cache.updateFromPacket(second)
	if updated == nil {
		t.Fatalf("expected entry to exist on second update")
	}

	if !updated.FirstSeen.Equal(now) {
		t.Fatalf("expected first seen to remain %v, got %v", now, updated.FirstSeen)
	}
	if !updated.LastUpdated.Equal(later) {
		t.Fatalf("expected last updated %v, got %v", later, updated.LastUpdated)
	}
	if updated.LongName != "First Node" {
		t.Fatalf("expected long name to remain 'First Node', got %q", updated.LongName)
	}
	if updated.ShortName != "Second" {
		t.Fatalf("expected short name to update to 'Second', got %q", updated.ShortName)
	}
	if updated.PrimaryChannel != "Primary" {
		t.Fatalf("expected primary channel to stay 'Primary', got %s", updated.PrimaryChannel)
	}
}
