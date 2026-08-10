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
