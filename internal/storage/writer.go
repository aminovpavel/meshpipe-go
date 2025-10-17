package storage

import (
	"context"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
)

// Writer persists decoded packets to the backing store.
type Writer interface {
	Store(ctx context.Context, pkt decode.Packet) error
}

// NopWriter drops packets (useful for tests and early bootstrap).
type NopWriter struct{}

// Store implements Writer by doing nothing.
func (NopWriter) Store(_ context.Context, _ decode.Packet) error {
	return nil
}
