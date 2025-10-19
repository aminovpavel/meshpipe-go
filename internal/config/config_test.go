package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aminovpavel/meshpipe-go/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	t.Setenv("MESHPIPE_CONFIG_FILE", filepath.Join(t.TempDir(), "nonexistent.yaml"))

	cfg, err := config.New("")
	if err != nil {
		t.Fatalf("config.New returned error: %v", err)
	}

	if cfg.Name != "Meshpipe" {
		t.Fatalf("expected default name 'Meshpipe', got %q", cfg.Name)
	}

	if cfg.MQTTPort != 1883 {
		t.Fatalf("expected default MQTT port 1883, got %d", cfg.MQTTPort)
	}

	if !cfg.CaptureStoreRaw {
		t.Fatalf("expected CaptureStoreRaw default true")
	}

	if cfg.GRPCEnabled {
		t.Fatalf("expected GRPCEnabled default false")
	}

	if cfg.GRPCListenAddress != ":7443" {
		t.Fatalf("expected default gRPC listen address :7443, got %q", cfg.GRPCListenAddress)
	}

	if cfg.GRPCMaxPageSize != 500 {
		t.Fatalf("expected default gRPC max page size 500, got %d", cfg.GRPCMaxPageSize)
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	yamlContent := `
name: Custom
mqtt_port: 1999
capture_store_raw: false
`

	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("write config yaml: %v", err)
	}

	cfg, err := config.New(yamlPath)
	if err != nil {
		t.Fatalf("config.New returned error: %v", err)
	}

	if cfg.Name != "Custom" {
		t.Fatalf("expected name Custom, got %q", cfg.Name)
	}

	if cfg.MQTTPort != 1999 {
		t.Fatalf("expected mqtt_port 1999, got %d", cfg.MQTTPort)
	}

	if cfg.CaptureStoreRaw {
		t.Fatalf("expected CaptureStoreRaw false from YAML override")
	}

	if cfg.ConfigPath != yamlPath {
		t.Fatalf("expected ConfigPath %q, got %q", yamlPath, cfg.ConfigPath)
	}
}

func TestEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(yamlPath, []byte("name: FromFile\n"), 0o600); err != nil {
		t.Fatalf("write config yaml: %v", err)
	}

	t.Setenv("MESHPIPE_NAME", "EnvName")
	t.Setenv("MESHPIPE_MQTT_PORT", "2001")
	t.Setenv("MESHPIPE_CAPTURE_STORE_RAW", "0")
	t.Setenv("MESHPIPE_GRPC_ENABLED", "1")
	t.Setenv("MESHPIPE_GRPC_LISTEN_ADDRESS", "127.0.0.1:7443")
	t.Setenv("MESHPIPE_GRPC_AUTH_TOKEN", "secret")
	t.Setenv("MESHPIPE_GRPC_MAX_PAGE_SIZE", "999")

	cfg, err := config.New(yamlPath)
	if err != nil {
		t.Fatalf("config.New returned error: %v", err)
	}

	if cfg.Name != "EnvName" {
		t.Fatalf("expected name EnvName from env, got %q", cfg.Name)
	}

	if cfg.MQTTPort != 2001 {
		t.Fatalf("expected mqtt_port 2001 from env, got %d", cfg.MQTTPort)
	}

	if cfg.CaptureStoreRaw {
		t.Fatalf("expected CaptureStoreRaw false from env override")
	}

	if !cfg.GRPCEnabled {
		t.Fatalf("expected gRPC enabled from env override")
	}

	if cfg.GRPCListenAddress != "127.0.0.1:7443" {
		t.Fatalf("unexpected gRPC listen address %q", cfg.GRPCListenAddress)
	}

	if cfg.GRPCAuthToken != "secret" {
		t.Fatalf("unexpected gRPC auth token %q", cfg.GRPCAuthToken)
	}

	if cfg.GRPCMaxPageSize != 999 {
		t.Fatalf("unexpected gRPC max page size %d", cfg.GRPCMaxPageSize)
	}
}

func TestEnvOverridesLegacyPrefix(t *testing.T) {
	yamlPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(yamlPath, []byte("name: FromFile\n"), 0o600); err != nil {
		t.Fatalf("write config yaml: %v", err)
	}

	t.Setenv("MALLA_NAME", "LegacyName")
	t.Setenv("MALLA_MQTT_PORT", "2002")

	cfg, err := config.New(yamlPath)
	if err != nil {
		t.Fatalf("config.New returned error: %v", err)
	}

	if cfg.Name != "LegacyName" {
		t.Fatalf("expected legacy name override, got %q", cfg.Name)
	}

	if cfg.MQTTPort != 2002 {
		t.Fatalf("expected legacy mqtt_port override, got %d", cfg.MQTTPort)
	}
}
