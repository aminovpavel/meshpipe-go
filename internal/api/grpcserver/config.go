package grpcserver

// Config holds runtime configuration for the gRPC server.
type Config struct {
	Enabled      bool
	Address      string
	AuthToken    string
	DatabasePath string
	MaxPageSize  int
}
