package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

var envPrefixes = []string{"MESHPIPE_", "MALLA_"}

// App contains the full application configuration.
type App struct {
	Name                 string `yaml:"name"`
	DatabaseFile         string `yaml:"database_file"`
	MQTTBrokerAddress    string `yaml:"mqtt_broker_address"`
	MQTTPort             int    `yaml:"mqtt_port"`
	MQTTUsername         string `yaml:"mqtt_username"`
	MQTTPassword         string `yaml:"mqtt_password"`
	MQTTTopicPrefix      string `yaml:"mqtt_topic_prefix"`
	MQTTTopicSuffix      string `yaml:"mqtt_topic_suffix"`
	DefaultChannelKey    string `yaml:"default_channel_key"`
	LogLevel             string `yaml:"log_level"`
	CaptureStoreRaw      bool   `yaml:"capture_store_raw"`
	ObservabilityAddress string `yaml:"observability_address"`
	MaintenanceInterval  int    `yaml:"maintenance_interval"`
	MaxEnvelopeBytes     int    `yaml:"max_envelope_bytes"`
	GRPCEnabled          bool   `yaml:"grpc_enabled"`
	GRPCListenAddress    string `yaml:"grpc_listen_address"`
	GRPCAuthToken        string `yaml:"grpc_auth_token"`
	GRPCMaxPageSize      int    `yaml:"grpc_max_page_size"`
	WALAutocheckpoint    int    `yaml:"wal_autocheckpoint"`
	JournalSizeLimit     int    `yaml:"journal_size_limit"`
	SQLiteCacheKiB       int    `yaml:"sqlite_cache_kib"`
	SQLiteDisableMmap    bool   `yaml:"sqlite_disable_mmap"`
}

// New reads the configuration from file (if provided) and environment overrides.
func New(path string) (*App, error) {
	cfg := defaultConfig()

	if err := cfg.applyFile(path); err != nil {
		return nil, err
	}

	if err := cfg.applyEnv(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func defaultConfig() *App {
	return &App{
		Name:                 "Meshpipe",
		DatabaseFile:         "meshtastic_history.db",
		MQTTBrokerAddress:    "127.0.0.1",
		MQTTPort:             1883,
		MQTTTopicPrefix:      "msh",
		MQTTTopicSuffix:      "/+/+/+/#",
		DefaultChannelKey:    "",
		LogLevel:             "INFO",
		CaptureStoreRaw:      true,
		WALAutocheckpoint:    1000,
		JournalSizeLimit:     64 * 1024 * 1024,
		SQLiteCacheKiB:       8192,
		SQLiteDisableMmap:    true,
		ObservabilityAddress: ":2112",
		MaintenanceInterval:  360,
		MaxEnvelopeBytes:     256 * 1024,
		GRPCEnabled:          false,
		GRPCListenAddress:    ":7443",
		GRPCAuthToken:        "",
		GRPCMaxPageSize:      500,
	}
}
