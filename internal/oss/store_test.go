package oss

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestCheckVersionGap(t *testing.T) {
	if err := CheckVersionGap("1.3.2", "2.5.0", 1); err != nil {
		t.Errorf("gap 1 should pass: %v", err)
	}
	if err := CheckVersionGap("4.0.0", "1.9.1", 3); err != nil {
		t.Errorf("gap 3 should pass: %v", err)
	}
	if err := CheckVersionGap("1.0.0", "1.4.0", 0); err != nil {
		t.Errorf("equal major should always pass: %v", err)
	}
	if err := CheckVersionGap("5.0.0", "1.2.3", 3); err == nil {
		t.Error("gap 4 should fail at max 3")
	}
	if err := CheckVersionGap("2.0.0", "6.0.0", 3); err == nil {
		t.Error("reverse gap 4 should fail at max 3")
	}
	if err := CheckVersionGap("2.0.0", "6.0.0", -1); err != nil {
		t.Errorf("negative maxGap disables the check: %v", err)
	}
	if err := CheckVersionGap("abc", "6.0.0", 3); err != nil {
		t.Errorf("unparseable local version should skip the check: %v", err)
	}
}

func TestMajorVersion(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"1.3.2", 1, false},
		{"v2.0.0", 2, false},
		{"10.4", 10, false},
		{"abc", 0, true},
		{"", 0, true},
	}
	for _, c := range cases {
		got, ok := majorVersion(c.in)
		if c.wantErr {
			if ok {
				t.Errorf("majorVersion(%q): expected failure", c.in)
			}
			continue
		}
		if !ok {
			t.Errorf("majorVersion(%q): unexpected failure", c.in)
			continue
		}
		if got != c.want {
			t.Errorf("majorVersion(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseWalKey(t *testing.T) {
	meta, ok := parseWalKey("godis/clusterA/wal/127.0.0.1:6379/seg-000000000042-1234-1700000000.gz")
	if !ok {
		t.Fatal("valid wal key should parse")
	}
	if meta.Seq != 42 || meta.Offset != 1234 {
		t.Errorf("seq/offset mismatch: %+v", meta)
	}
	if !meta.TS.Equal(time.Unix(1700000000, 0).UTC()) {
		t.Errorf("ts mismatch: %v", meta.TS)
	}
	if _, ok := parseWalKey("godis/clusterA/wal/127.0.0.1:6379/latest.json"); ok {
		t.Error("non-segment key should not parse")
	}
	if _, ok := parseWalKey("godis/clusterA/state/127.0.0.1:6379.json"); ok {
		t.Error("state marker key should not parse")
	}
	if _, ok := parseWalKey("seg-zzz-1-2.gz"); ok {
		t.Error("non-numeric seq should not parse")
	}
}

func TestParseWalKeyLegacyNoTs(t *testing.T) {
	meta, ok := parseWalKey("wal/127.0.0.1:6379/seg-000000000001-100.gz")
	if !ok {
		t.Fatal("legacy key without ts should parse")
	}
	if meta.Seq != 1 || meta.Offset != 100 {
		t.Errorf("seq/offset mismatch: %+v", meta)
	}
	if !meta.TS.IsZero() {
		t.Errorf("missing ts should be zero: %v", meta.TS)
	}
}

func TestStateMarkerRoundtrip(t *testing.T) {
	marker := StateMarker{
		NodeID:     "127.0.0.1:6379",
		AppliedSeq: 7,
		AppliedOff: 4096,
		TS:         time.Now().UTC(),
		Version:    "1.3.2",
		IsMaster:   true,
	}
	bin, err := json.Marshal(&marker)
	if err != nil {
		t.Fatal(err)
	}
	var got StateMarker
	if err := json.Unmarshal(bin, &got); err != nil {
		t.Fatal(err)
	}
	if got.NodeID != marker.NodeID || got.AppliedSeq != marker.AppliedSeq ||
		got.AppliedOff != marker.AppliedOff || got.IsMaster != marker.IsMaster {
		t.Errorf("roundtrip mismatch: %+v vs %+v", got, marker)
	}
	if !got.TS.Equal(marker.TS) {
		t.Errorf("ts mismatch: %v vs %v", got.TS, marker.TS)
	}
}

func TestGzipRoundtrip(t *testing.T) {
	payload := bytes.Repeat([]byte("godis-wal-payload"), 100)
	compressed, err := gzipCompress(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(payload) {
		t.Errorf("compression failed to shrink payload: %d >= %d", len(compressed), len(payload))
	}
	restored, err := gzipDecompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, payload) {
		t.Error("decompressed payload differs from original")
	}
}
