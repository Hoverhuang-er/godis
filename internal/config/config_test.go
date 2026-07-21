package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"appendonly yes\n" +
		"peers a,b"
	p := parse(strings.NewReader(src))
	if p == nil {
		t.Error("cannot get result")
		return
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if !p.AppendOnly {
		t.Error("bool parse failed")
	}
}

func TestParseToml(t *testing.T) {
	content := `
config_mode = "standalone"

[server]
bind = "0.0.0.0"
port = 6399
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
	if Properties.Port != 6399 {
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
