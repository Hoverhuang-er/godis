package oss

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const (
	metaSeq     = "godis-seq"
	metaOffset  = "godis-offset"
	metaTS      = "godis-ts"
	metaVersion = "godis-version"
)

// WalSegmentMeta describes one append-only WAL segment object of a node.
// Seq is the per-node monotonic sequence (order of application); Offset is
// the godis AOF/replication offset captured at publish time (informational);
// TS is the wall-clock publish time used for cross-node "newest data"
// comparison in disaster recovery.
type WalSegmentMeta struct {
	Seq     uint64
	Offset  int64
	TS      time.Time
	Version string
	Key     string
}

// UploadWalSegment gzip-compresses data and appends it as a new WAL segment
// object of nodeAddr. The object key embeds seq/offset/ts and the same values
// are carried as user metadata so disaster recovery can read the newest
// version without downloading payloads. Never deletes older segments.
func (s *Store) UploadWalSegment(ctx context.Context, nodeAddr string, seq uint64, offset int64, version string, data []byte) (string, error) {
	if len(data) == 0 {
		return "", errors.New("empty wal segment")
	}
	compressed, err := gzipCompress(data)
	if err != nil {
		return "", fmt.Errorf("compress wal segment: %w", err)
	}
	ts := time.Now().UTC()
	key := fmt.Sprintf("%s/seg-%012d-%d-%d.gz", s.WalDir(nodeAddr), seq, offset, ts.Unix())
	_, err = s.client.PutObject(ctx, s.cfg.Bucket, key, bytes.NewReader(compressed), int64(len(compressed)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
			UserMetadata: map[string]string{
				metaSeq:     strconv.FormatUint(seq, 10),
				metaOffset:  strconv.FormatInt(offset, 10),
				metaTS:      strconv.FormatInt(ts.UnixMilli(), 10),
				metaVersion: version,
			},
		})
	if err != nil {
		return "", fmt.Errorf("upload wal segment %s: %w", key, err)
	}
	return key, nil
}

// ListWalSegments returns the WAL segment metadata of nodeAddr ordered by
// sequence. Metadata is parsed from object keys (no extra stat calls).
func (s *Store) ListWalSegments(ctx context.Context, nodeAddr string) ([]WalSegmentMeta, error) {
	var metas []WalSegmentMeta
	for obj := range s.client.ListObjects(ctx, s.cfg.Bucket,
		minio.ListObjectsOptions{Prefix: s.WalDir(nodeAddr) + "/", Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		meta, ok := parseWalKey(obj.Key)
		if !ok {
			continue
		}
		metas = append(metas, meta)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Seq < metas[j].Seq })
	return metas, nil
}

// parseWalKey extracts segment metadata from an object key of the form
// .../seg-{seq:012d}-{offset}-{ts}.gz. Wall-clock ts falls back to the
// zero value when the key has no ts component (legacy).
func parseWalKey(key string) (WalSegmentMeta, bool) {
	base := key[strings.LastIndex(key, "/")+1:]
	if !strings.HasPrefix(base, "seg-") {
		return WalSegmentMeta{}, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(base, "seg-"), ".gz")
	parts := strings.Split(rest, "-")
	if len(parts) < 2 {
		return WalSegmentMeta{}, false
	}
	seq, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return WalSegmentMeta{}, false
	}
	offset, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return WalSegmentMeta{}, false
	}
	var ts time.Time
	if len(parts) > 2 {
		if sec, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			ts = time.Unix(sec, 0).UTC()
		}
	}
	return WalSegmentMeta{Seq: seq, Offset: offset, TS: ts, Key: key}, true
}

// DownloadWalSegment fetches and decompresses the segment with the given
// sequence of nodeAddr.
func (s *Store) DownloadWalSegment(ctx context.Context, nodeAddr string, seq uint64) ([]byte, error) {
	key, err := s.walKeyForSeq(ctx, nodeAddr, seq)
	if err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get wal segment %s: %w", key, err)
	}
	defer obj.Close()
	compressed, err := readAll(obj)
	if err != nil {
		return nil, fmt.Errorf("read wal segment %s: %w", key, err)
	}
	data, err := gzipDecompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress wal segment %s: %w", key, err)
	}
	return data, nil
}

// walKeyForSeq finds the object key of the segment with the given sequence.
// It lists keys only (no payloads); segments are never deleted so the seq is
// unique per node.
func (s *Store) walKeyForSeq(ctx context.Context, nodeAddr string, seq uint64) (string, error) {
	for obj := range s.client.ListObjects(ctx, s.cfg.Bucket,
		minio.ListObjectsOptions{Prefix: s.WalDir(nodeAddr) + "/", Recursive: true}) {
		if obj.Err != nil {
			return "", obj.Err
		}
		if meta, ok := parseWalKey(obj.Key); ok && meta.Seq == seq {
			return obj.Key, nil
		}
	}
	return "", fmt.Errorf("wal segment seq %d of %s not found", seq, nodeAddr)
}

// StateMarker is the small per-node object (overwritten via PUT, never
// deleted) describing how far a node has applied its WAL. It is the metadata
// source for <3-node disaster recovery: the node with the newest TS and
// highest Seq is considered the most up-to-date.
type StateMarker struct {
	NodeID      string    `json:"node_id"`
	AppliedSeq  uint64    `json:"applied_seq"`
	AppliedOff  int64     `json:"applied_offset"`
	TS          time.Time `json:"ts"`
	Version     string    `json:"version"`
	IsMaster    bool      `json:"is_master"`
	LastSyncErr string    `json:"last_sync_err,omitempty"`
}

// UploadStateMarker overwrites the state marker of nodeAddr.
func (s *Store) UploadStateMarker(ctx context.Context, nodeAddr string, marker StateMarker) error {
	marker.NodeID = nodeAddr
	if marker.TS.IsZero() {
		marker.TS = time.Now().UTC()
	}
	bin, err := json.Marshal(&marker)
	if err != nil {
		return fmt.Errorf("marshal state marker: %w", err)
	}
	key := fmt.Sprintf("%s/%s.json", s.StateDir(), nodeAddr)
	_, err = s.client.PutObject(ctx, s.cfg.Bucket, key, bytes.NewReader(bin), int64(len(bin)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("upload state marker %s: %w", key, err)
	}
	return nil
}

// ListStateMarkers returns the state markers of all nodes, newest TS first.
func (s *Store) ListStateMarkers(ctx context.Context) ([]StateMarker, error) {
	var markers []StateMarker
	for obj := range s.client.ListObjects(ctx, s.cfg.Bucket,
		minio.ListObjectsOptions{Prefix: s.StateDir() + "/", Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		o, err := s.client.GetObject(ctx, s.cfg.Bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		bin, err := readAll(o)
		o.Close()
		if err != nil {
			continue
		}
		var marker StateMarker
		if err := json.Unmarshal(bin, &marker); err != nil {
			continue
		}
		markers = append(markers, marker)
	}
	sort.Slice(markers, func(i, j int) bool { return markers[i].TS.After(markers[j].TS) })
	return markers, nil
}

// UploadBaseSnapshot stores an optional bootstrap snapshot of nodeAddr taken
// at appliedSeq. Replicas may start from the newest base snapshot instead of
// replaying the full WAL from seq 0. Like all writes here it is a PUT — old
// base snapshots are left in place, never deleted.
func (s *Store) UploadBaseSnapshot(ctx context.Context, nodeAddr string, appliedSeq uint64, version string, data []byte) error {
	if len(data) == 0 {
		return errors.New("empty base snapshot")
	}
	compressed, err := gzipCompress(data)
	if err != nil {
		return fmt.Errorf("compress base snapshot: %w", err)
	}
	key := fmt.Sprintf("%s/base-%012d.gz", s.BaseDir(nodeAddr), appliedSeq)
	_, err = s.client.PutObject(ctx, s.cfg.Bucket, key, bytes.NewReader(compressed), int64(len(compressed)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
			UserMetadata: map[string]string{
				metaSeq:     strconv.FormatUint(appliedSeq, 10),
				metaVersion: version,
			},
		})
	if err != nil {
		return fmt.Errorf("upload base snapshot %s: %w", key, err)
	}
	return nil
}

// LatestBaseSnapshot returns the base snapshot of nodeAddr with the highest
// applied sequence, or ok=false when none exists.
func (s *Store) LatestBaseSnapshot(ctx context.Context, nodeAddr string) (appliedSeq uint64, data []byte, ok bool, err error) {
	var best uint64 = 0
	for obj := range s.client.ListObjects(ctx, s.cfg.Bucket,
		minio.ListObjectsOptions{Prefix: s.BaseDir(nodeAddr) + "/", Recursive: true}) {
		if obj.Err != nil {
			return 0, nil, false, obj.Err
		}
		base := obj.Key[strings.LastIndex(obj.Key, "/")+1:]
		rest := strings.TrimSuffix(strings.TrimPrefix(base, "base-"), ".gz")
		seq, perr := strconv.ParseUint(rest, 10, 64)
		if perr != nil || seq <= best {
			continue
		}
		best = seq
	}
	if best == 0 {
		return 0, nil, false, nil
	}
	key := fmt.Sprintf("%s/base-%012d.gz", s.BaseDir(nodeAddr), best)
	o, gerr := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if gerr != nil {
		return 0, nil, false, gerr
	}
	defer o.Close()
	compressed, rerr := readAll(o)
	if rerr != nil {
		return 0, nil, false, rerr
	}
	data, derr := gzipDecompress(compressed)
	if derr != nil {
		return 0, nil, false, derr
	}
	return best, data, true, nil
}

// CheckVersionGap returns an error when the major version difference between
// local and remote godis versions exceeds maxMajorGap.
func CheckVersionGap(local, remote string, maxMajorGap int) error {
	if maxMajorGap <= 0 {
		return nil
	}
	lm, ok := majorVersion(local)
	if !ok {
		return nil
	}
	rm, ok := majorVersion(remote)
	if !ok {
		return nil
	}
	diff := lm - rm
	if diff < 0 {
		diff = -diff
	}
	if int(diff) > maxMajorGap {
		return fmt.Errorf("version gap too large: local %s vs remote %s (max %d major versions)",
			local, remote, maxMajorGap)
	}
	return nil
}

// majorVersion parses the leading numeric component of a semantic version.
func majorVersion(v string) (int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	dot := strings.IndexByte(v, '.')
	if dot > 0 {
		v = v[:dot]
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// LatestStateMarker fetches the state marker of nodeAddr, or ok=false when it
// does not exist. It is a single GET of a fixed key — the cheapest liveness
// probe available for a node in the OSS cluster.
func (s *Store) LatestStateMarker(ctx context.Context, nodeAddr string) (StateMarker, bool, error) {
	key := fmt.Sprintf("%s/%s.json", s.StateDir(), nodeAddr)
	exists, err := s.objectExists(ctx, key)
	if err != nil || !exists {
		return StateMarker{}, false, err
	}
	o, err := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return StateMarker{}, false, err
	}
	defer o.Close()
	bin, err := readAll(o)
	if err != nil {
		return StateMarker{}, false, err
	}
	var marker StateMarker
	if err := json.Unmarshal(bin, &marker); err != nil {
		return StateMarker{}, false, err
	}
	return marker, true, nil
}
