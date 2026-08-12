package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)


func TestParseToml(t *testing.T) {
	content := `
config_mode = "standalone"

[server]
bind = "0.0.0.0"
port = 6379
maxclients = 128
databases = 16

[aof]
appendonly = true
appendfsync = "everysec"
`
	Properties = &ServerProperties{}
	parseTomlContent(content)

	if Properties.Bind != "0.0.0.0" {
		t.Errorf("TOML parse bind failed: got %s", Properties.Bind)
	}
	if Properties.Port != 6379 {
		t.Errorf("TOML parse port failed: got %d", Properties.Port)
	}
	if !Properties.AppendOnly {
		t.Error("TOML parse appendonly failed")
	}
	if Properties.MaxClients != 128 {
		t.Errorf("TOML parse maxclients failed: got %d", Properties.MaxClients)
	}
	if Properties.Databases != 16 {
		t.Errorf("TOML parse databases failed: got %d", Properties.Databases)
	}
	if Properties.ConfigMode != StandaloneMode {
		t.Errorf("TOML parse config_mode failed: got %s", Properties.ConfigMode)
	}
}

func TestDetectConfigFile(t *testing.T) {
	// Test that CONFIG env var takes priority
	t.Setenv("CONFIG", "/custom/path/redis.conf")
	path, found := DetectConfigFile()
	if !found || path != "/custom/path/redis.conf" {
		t.Errorf("DetectConfigFile should return CONFIG env: got %s", path)
	}
}

func TestSharedConfigEntries(t *testing.T) {
	Properties.Bind = "10.0.0.1"
	Properties.AppendOnly = true
	Properties.RequirePass = "s3cret"
	entries := SharedConfigEntries()
	if entries["appendonly"] != "true" {
		t.Errorf("appendonly missing from shared entries: %v", entries)
	}
	if entries["requirepass"] != "s3cret" {
		t.Errorf("requirepass missing from shared entries: %v", entries)
	}
	if _, ok := entries["bind"]; ok {
		t.Error("node-local param bind leaked into shared entries")
	}
	if _, ok := entries["cluster-id"]; ok {
		t.Error("node-local param cluster-id leaked into shared entries")
	}
}

func TestSetParameter(t *testing.T) {
	if err := SetParameter("appendonly", "yes"); err != nil || !Properties.AppendOnly {
		t.Errorf("SetParameter(appendonly, yes) failed: %v", err)
	}
	if err := SetParameter("slowlog-max-len", "42"); err != nil || Properties.SlowLogMaxLen != 42 {
		t.Errorf("SetParameter(slowlog-max-len, 42) failed: %v", err)
	}
	if err := SetParameter("bind", "10.0.0.2"); err == nil {
		t.Error("SetParameter on node-local param must fail")
	}
	if err := SetParameter("no-such-param", "x"); err == nil {
		t.Error("SetParameter on unknown param must fail")
	}
}

func TestRewriteConfigFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "godis.toml")
	if err := os.WriteFile(tmp, []byte("[aof]\nappendonly = false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	configFilePath = tmp
	SetParameter("appendonly", "yes")
	if err := RewriteConfigFile(); err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(tmp)
	if !strings.Contains(string(content), "appendonly = \"true\"") {
		t.Errorf("rewrite did not persist new value:\n%s", content)
	}
}
