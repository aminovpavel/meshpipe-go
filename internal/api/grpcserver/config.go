package grpcserver

// Config holds runtime configuration for the gRPC server.
type Config struct {
	Enabled      bool
	Address      string
	AuthToken    string
	DatabasePath string
	MaxPageSize  int
	Cache        CacheConfig
}

// CacheConfig holds cache-related options for the gRPC server.
type CacheConfig struct {
	Enabled                 bool
	KeyPrefix               string
	DefaultTTLSeconds       int
	RedisAddress            string
	RedisUsername           string
	RedisPassword           string
	RedisDB                 int
	RedisTLSEnabled         bool
	RedisInsecureSkipVerify bool
}
