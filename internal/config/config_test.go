package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aminovpavel/mw-malla-capture/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	t.Setenv("MALLA_CONFIG_FILE", filepath.Join(t.TempDir(), "nonexistent.yaml"))

	cfg, err := config.New("")
	if err != nil {
		t.Fatalf("config.New returned error: %v", err)
	}

	if cfg.Name != "Malla" {
		t.Fatalf("expected default name 'Malla', got %q", cfg.Name)
	}

	if cfg.MQTTPort != 1883 {
		t.Fatalf("expected default MQTT port 1883, got %d", cfg.MQTTPort)
	}

	if !cfg.CaptureStoreRaw {
		t.Fatalf("expected CaptureStoreRaw default true")
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

	t.Setenv("MALLA_NAME", "EnvName")
	t.Setenv("MALLA_MQTT_PORT", "2001")
	t.Setenv("MALLA_CAPTURE_STORE_RAW", "0")

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
}
