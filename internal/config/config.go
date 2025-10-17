package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const envPrefix = "MALLA_"

// App holds user-facing configuration derived from YAML + environment overrides.
type App struct {
	Name              string `yaml:"name"`
	DatabaseFile      string `yaml:"database_file"`
	MQTTBrokerAddress string `yaml:"mqtt_broker_address"`
	MQTTPort          int    `yaml:"mqtt_port"`
	MQTTUsername      string `yaml:"mqtt_username"`
	MQTTPassword      string `yaml:"mqtt_password"`
	MQTTTopicPrefix   string `yaml:"mqtt_topic_prefix"`
	MQTTTopicSuffix   string `yaml:"mqtt_topic_suffix"`
	DefaultChannelKey string `yaml:"default_channel_key"`
	LogLevel          string `yaml:"log_level"`
	CaptureStoreRaw   bool   `yaml:"capture_store_raw"`
	WALAutocheckpoint int    `yaml:"wal_autocheckpoint_pages"`
	JournalSizeLimit  int    `yaml:"journal_size_limit_bytes"`
	SQLiteCacheKiB    int    `yaml:"sqlite_cache_kib"`
	SQLiteDisableMmap bool   `yaml:"sqlite_disable_mmap"`
	ConfigPath        string `yaml:"-"`
}

func New(defaultPath string) (*App, error) {
	configPath := resolveConfigPath(defaultPath)

	cfg := defaultConfig()
	cfg.ConfigPath = configPath

	if err := loadFromFile(cfg, configPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("load yaml config: %w", err)
		}
	}

	if err := overrideFromEnv(cfg); err != nil {
		return nil, fmt.Errorf("apply env overrides: %w", err)
	}

	return cfg, nil
}

func resolveConfigPath(defaultPath string) string {
	if override := os.Getenv("MALLA_CONFIG_FILE"); override != "" {
		return override
	}
	if defaultPath == "" {
		return "config.yaml"
	}
	return defaultPath
}

func loadFromFile(cfg *App, path string) error {
	if path == "" {
		return nil
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return err
	}
	cfg.ConfigPath = path
	return nil
}

func overrideFromEnv(cfg *App) error {
	for _, key := range os.Environ() {
		if !strings.HasPrefix(key, envPrefix) {
			continue
		}

		parts := strings.SplitN(key, "=", 2)
		if len(parts) != 2 {
			continue
		}

		name := strings.TrimPrefix(parts[0], envPrefix)
		value := parts[1]

		switch strings.ToLower(name) {
		case "name":
			cfg.Name = value
		case "database_file":
			cfg.DatabaseFile = value
		case "mqtt_broker_address":
			cfg.MQTTBrokerAddress = value
		case "mqtt_port":
			fmt.Sscanf(value, "%d", &cfg.MQTTPort)
		case "mqtt_username":
			cfg.MQTTUsername = value
		case "mqtt_password":
			cfg.MQTTPassword = value
		case "mqtt_topic_prefix":
			cfg.MQTTTopicPrefix = value
		case "mqtt_topic_suffix":
			cfg.MQTTTopicSuffix = value
		case "default_channel_key":
			cfg.DefaultChannelKey = value
		case "log_level":
			cfg.LogLevel = strings.ToUpper(value)
		case "capture_store_raw":
			cfg.CaptureStoreRaw = parseBool(value, cfg.CaptureStoreRaw)
		case "wal_autocheckpoint_pages":
			fmt.Sscanf(value, "%d", &cfg.WALAutocheckpoint)
		case "journal_size_limit_bytes":
			fmt.Sscanf(value, "%d", &cfg.JournalSizeLimit)
		case "sqlite_cache_kib":
			fmt.Sscanf(value, "%d", &cfg.SQLiteCacheKiB)
		case "sqlite_disable_mmap":
			cfg.SQLiteDisableMmap = parseBool(value, cfg.SQLiteDisableMmap)
		}
	}
	return nil
}

func parseBool(value string, fallback bool) bool {
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func defaultConfig() *App {
	return &App{
		Name:              "Malla",
		DatabaseFile:      "meshtastic_history.db",
		MQTTBrokerAddress: "127.0.0.1",
		MQTTPort:          1883,
		MQTTTopicPrefix:   "msh",
		MQTTTopicSuffix:   "/+/+/+/#",
		DefaultChannelKey: "1PG7OiApB1nwvP+rz05pAQ==",
		LogLevel:          "INFO",
		CaptureStoreRaw:   true,
		WALAutocheckpoint: 1000,
		JournalSizeLimit:  64 * 1024 * 1024,
		SQLiteCacheKiB:    8192,
		SQLiteDisableMmap: true,
	}
}
