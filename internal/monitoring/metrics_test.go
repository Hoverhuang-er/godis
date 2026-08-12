package monitoring

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClusterMetricsEmission(t *testing.T) {
	m := New(func() []DBStat { return nil })
	m.SetClusterStatsFn(func() ClusterStats {
		return ClusterStats{
			Enabled:         true,
			Mode:            "master-slave",
			Role:            "master",
			MasterID:        "127.0.0.1:6379",
			MasterAddr:      "127.0.0.1:6379",
			NodeCount:       2,
			SyncLastSuccess: time.Now().Unix() - 5,
			SyncLagSeconds:  5,
			WalSeqApplied:   42,
			WalSeqPublished: 45,
			LastUploadMs:    12.5,
			LastDownloadMs:  3.25,
		}
	})
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"godis_cluster_enabled 1",
		`godis_cluster_mode{mode="master-slave"} 1`,
		`godis_cluster_role{role="master"} 1`,
		`godis_cluster_master_info{master_id="127.0.0.1:6379",master_addr="127.0.0.1:6379"} 1`,
		"godis_cluster_node_count 2",
		"godis_cluster_wal_seq_applied 42",
		"godis_cluster_wal_seq_published 45",
		"godis_cluster_sync_lag_seconds 5",
		"godis_cluster_wal_upload_ms 12.5",
		"godis_cluster_wal_download_ms 3.25",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing metric line %q in:\n%s", want, body)
		}
	}
}

func TestClusterMetricsDisabledByDefault(t *testing.T) {
	m := New(func() []DBStat { return nil })
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(rec.Body.String(), "godis_cluster_") {
		t.Errorf("cluster metrics emitted without SetClusterStatsFn:\n%s", rec.Body.String())
	}
}

func TestKvrocksMetricsEmission(t *testing.T) {
	m := New(func() []DBStat {
		return []DBStat{{Index: 0, Keys: 10, Expires: 2}}
	})
	m.SetReplStatsFn(func() ReplStats {
		return ReplStats{
			Role:            "master",
			ConnectedSlaves: 1,
			ReplOffset:      100,
			BacklogLen:      50,
			SlaveLags:       []SlaveLag{{IP: "127.0.0.1", Port: 6380, LagSeconds: 1.5}},
		}
	})
	m.IncrPartialResyncDenied()
	m.IncrFullResyncs()
	m.IncrPartialResync()
	m.RecordCommandDuration("get", 2*time.Millisecond)

	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"godis_uptime_in_seconds ",
		"godis_cpu_sys_seconds_total ",
		"godis_cpu_user_seconds_total ",
		"godis_used_disk_size ",
		"godis_disk_capacity_bytes ",
		"godis_used_db_size ",
		"godis_connected_clients ",
		"godis_monitor_clients 0",
		"godis_loading 0",
		"godis_connected_slaves 1",
		`godis_instance_info{role="master"} 1`,
		`godis_connected_slave_lag_seconds{slave_ip="127.0.0.1",slave_port="6380"} 1.5`,
		`godis_commands_total{cmd="get"} 1`,
		`godis_commands_duration_seconds_total{cmd="get"} 0.002`,
		"godis_memory_used_rss_bytes ",
		`godis_estimate_keys{column_family="metadata"} 10`,
		"godis_db_keys{db=\"0\"} 10",
		"godis_db_keys_expiring{db=\"0\"} 2",
		"godis_sequence ",
		"godis_replica_resyncs_full 1",
		"godis_replica_partial_resync_accepted 1",
		"godis_replica_partial_resync_denied 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing kvrocks metric line %q in:\n%s", want, body)
		}
	}
}
