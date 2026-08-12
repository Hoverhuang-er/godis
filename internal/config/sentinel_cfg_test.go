package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSentinelConfigParse guards the viper/mapstructure tags: UnmarshalKey
// ignores toml tags, so every SentinelMonitorConfig field needs both.
func TestSentinelConfigParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "godis.toml")
	content := `
[sentinel]
enable = true
port = 26380

[[sentinel.monitors]]
name = "mymaster"
host = "127.0.0.1"
port = 6379
quorum = 2
down_after_ms = 5000
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	SetupConfig(path)
	if !Properties.SentinelEnabled {
		t.Error("SentinelEnabled = false, want true")
	}
	if Properties.SentinelPort != 26380 {
		t.Errorf("SentinelPort = %d, want 26380", Properties.SentinelPort)
	}
	if len(Properties.SentinelMonitors) != 1 {
		t.Fatalf("SentinelMonitors = %d, want 1", len(Properties.SentinelMonitors))
	}
	m := Properties.SentinelMonitors[0]
	if m.Name != "mymaster" || m.Host != "127.0.0.1" || m.Port != 6379 || m.Quorum != 2 || m.DownAfterMs != 5000 {
		t.Errorf("monitor = %+v", m)
	}
}
