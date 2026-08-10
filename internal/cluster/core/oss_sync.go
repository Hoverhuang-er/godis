package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Hoverhuang-er/godis/internal/aof"
	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/oss"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	rdb "github.com/hdt3213/rdb/parser"
)

const (
	// walSegmentBytes is the target payload size of one WAL segment object.
	// Larger batches amortize the per-object PUT cost on S3.
	walSegmentBytes = 1 << 20 // 1 MiB
	// walFlushInterval bounds publish latency even under low traffic.
	walFlushInterval = 2 * time.Second
	// walPullBatch caps how many segments a replica fetches per cron tick.
	walPullBatch = 64
)

// setupOssStore builds the OSS store from config when OSS-synced cluster
// replication is enabled. A nil store falls back to TCP replication.
func setupOssStore() *oss.Store {
	props := config.Properties
	if !props.ClusterSyncViaOss || !props.OssEnabled {
		return nil
	}
	store, err := oss.NewStore(oss.Config{
		Endpoint:        props.OssEndpoint,
		Bucket:          props.OssBucket,
		AccessKeyID:     props.OssAccessKeyID,
		SecretAccessKey: props.OssSecretAccessKey,
		Region:          props.OssRegion,
		Prefix:          props.OssPrefix,
		ClusterID:       props.ClusterID,
		UseSSL:          props.OssUseSSL,
		ForcePathStyle:  props.OssForcePathStyle,
	})
	if err != nil {
		logger.Errorf("oss store init failed, fall back to tcp replication: %v", err)
		return nil
	}
	logger.Infof("oss replication enabled, root=%s", store.Root())
	return store
}

// walListener is registered on the AOF persister of the master. It buffers
// command batches and appends them to OSS as gzip-compressed WAL segments,
// then refreshes the node's state marker.
type walListener struct {
	cluster  *Cluster
	mu       sync.Mutex
	buffer   bytes.Buffer
	seq      uint64
	offset   int64
	lastPush time.Time
	closing  bool
}

func newWalListener(cluster *Cluster) *walListener {
	return &walListener{cluster: cluster, lastPush: time.Now()}
}

// Callback implements aof.Listener. It runs synchronously inside the AOF
// write path, so it must never block on I/O: bytes are buffered and the
// upload happens either here when the segment is full, or in the background
// flusher (see walFlusher).
func (l *walListener) Callback(cmdLines []database.CmdLine) {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return
	}
	for _, cmdLine := range cmdLines {
		bin := protocol.MakeMultiBulkReply(cmdLine).ToBytes()
		l.buffer.Write(bin)
		l.offset += int64(len(bin))
	}
	full := l.buffer.Len() >= walSegmentBytes
	l.mu.Unlock()
	if full {
		l.flush()
	}
}

// flush uploads the buffered commands as the next WAL segment.
func (l *walListener) flush() {
	l.mu.Lock()
	if l.buffer.Len() == 0 {
		l.mu.Unlock()
		return
	}
	data := make([]byte, l.buffer.Len())
	copy(data, l.buffer.Bytes())
	seq := l.seq + 1
	offset := l.offset
	l.buffer.Reset()
	l.lastPush = time.Now()
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	start := time.Now()
	key, err := l.cluster.ossStore.UploadWalSegment(ctx, l.cluster.SelfID(), seq, offset, dbimpl.Version(), data)
	if err != nil {
		logger.Errorf("upload wal segment seq=%d failed: %v", seq, err)
		return
	}
	l.cluster.lastUploadMs.Store(time.Since(start).Milliseconds())
	l.cluster.lastSyncUnix.Store(time.Now().Unix())
	logger.Debugf("wal segment uploaded seq=%d key=%s", seq, key)
	l.mu.Lock()
	if seq > l.seq {
		l.seq = seq
	}
	l.mu.Unlock()
	l.cluster.refreshStateMarker()
}

// walFlusher periodically flushes partially-filled segments so that low
// traffic does not delay replication indefinitely.
func (l *walListener) walFlusher() {
	ticker := time.NewTicker(walFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.flush()
		case <-l.cluster.closeChan:
			return
		}
	}
}

// close stops the listener and flushes any buffered commands one last time.
func (l *walListener) close() {
	l.mu.Lock()
	l.closing = true
	l.mu.Unlock()
	l.flush()
}

// startWalPublishing registers the WAL listener on the AOF persister when
// this node is the master. Requires appendonly to be enabled.
func (cluster *Cluster) startWalPublishing() error {
	if cluster.ossStore == nil {
		return nil
	}
	provider, ok := cluster.db.(interface {
		AddAofListener(aof.Listener)
		RemoveAofListener(aof.Listener)
	})
	if !ok {
		return errors.New("db does not support aof listeners; enable appendonly for oss wal replication")
	}
	if cluster.walListener != nil {
		return nil
	}
	listener := newWalListener(cluster)
	cluster.walListener = listener
	provider.AddAofListener(listener)
	go listener.walFlusher()
	logger.Infof("wal publishing started on %s", cluster.SelfID())
	return nil
}

// stopWalPublishing detaches and flushes the WAL listener.
func (cluster *Cluster) stopWalPublishing() {
	if cluster.walListener == nil {
		return
	}
	listener := cluster.walListener
	cluster.walListener = nil
	if provider, ok := cluster.db.(interface{ RemoveAofListener(aof.Listener) }); ok {
		provider.RemoveAofListener(listener)
	}
	listener.close()
}

// refreshStateMarker overwrites this node's state marker in OSS. It is cheap
// (one PUT of a fixed key) and is the metadata source for <3-node disaster
// recovery.
func (cluster *Cluster) refreshStateMarker() {
	if cluster.ossStore == nil {
		return
	}
	marker := oss.StateMarker{
		AppliedSeq: cluster.lastAppliedSeq(),
		AppliedOff: cluster.lastAppliedOffset(),
		Version:    dbimpl.Version(),
		IsMaster:   cluster.raftNode != nil && cluster.raftNode.FSM.GetMaster(cluster.SelfID()) == cluster.SelfID(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cluster.ossStore.UploadStateMarker(ctx, cluster.SelfID(), marker); err != nil {
		logger.Errorf("upload state marker failed: %v", err)
	}
}

// walProgress is the locally persisted replication cursor of a replica.
type walProgress struct {
	AppliedSeq uint64 `json:"applied_seq"`
	Offset     int64  `json:"offset"`
}

func (cluster *Cluster) progressFile() string {
	return filepath.Join(cluster.config.Dir, "wal-progress.json")
}

// lastAppliedSeq is the highest WAL segment seq applied locally. On the
// master it equals the published seq; on replicas it is the replay cursor.
func (cluster *Cluster) lastAppliedSeq() uint64 {
	if cluster.walListener != nil {
		cluster.walListener.mu.Lock()
		seq := cluster.walListener.seq
		cluster.walListener.mu.Unlock()
		return seq
	}
	cluster.progressMu.Lock()
	defer cluster.progressMu.Unlock()
	return cluster.progress.AppliedSeq
}

// lastAppliedOffset returns the byte offset of the last applied command.
func (cluster *Cluster) lastAppliedOffset() int64 {
	if cluster.walListener != nil {
		cluster.walListener.mu.Lock()
		off := cluster.walListener.offset
		cluster.walListener.mu.Unlock()
		return off
	}
	cluster.progressMu.Lock()
	defer cluster.progressMu.Unlock()
	return cluster.progress.Offset
}

// loadProgress reads the local WAL cursor from disk, if any.
func (cluster *Cluster) loadProgress() {
	cluster.progressMu.Lock()
	defer cluster.progressMu.Unlock()
	bin, err := os.ReadFile(cluster.progressFile())
	if err != nil {
		return
	}
	if err := json.Unmarshal(bin, &cluster.progress); err != nil {
		logger.Warnf("parse wal progress failed: %v", err)
	}
}

// persistProgress writes the local WAL cursor to disk (local overwrite).
func (cluster *Cluster) persistProgress() {
	cluster.progressMu.Lock()
	defer cluster.progressMu.Unlock()
	bin, err := json.Marshal(&cluster.progress)
	if err != nil {
		return
	}
	if err := os.MkdirAll(cluster.config.Dir, 0o755); err == nil {
		_ = os.WriteFile(cluster.progressFile(), bin, 0o644)
	}
}

// ossInitialSync bootstraps a replica from OSS: it restores the newest base
// snapshot (if any), replays WAL segments published after it, then marks this
// node as a replica of the master.
func (cluster *Cluster) ossInitialSync(master string) error {
	if cluster.ossStore == nil {
		return errors.New("oss store not initialized")
	}
	cluster.loadProgress()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	baseSeq, baseData, ok, err := cluster.ossStore.LatestBaseSnapshot(ctx, master)
	if err != nil {
		return fmt.Errorf("list base snapshot of %s: %w", master, err)
	}
	if ok {
		decoder := rdb.NewDecoder(bytes.NewReader(baseData))
		if err := cluster.db.LoadRDB(decoder); err != nil {
			return fmt.Errorf("load base snapshot of %s: %w", master, err)
		}
		logger.Infof("oss initial sync restored base snapshot of %s at seq=%d", master, baseSeq)
		if cluster.progress.AppliedSeq < baseSeq {
			cluster.progress.AppliedSeq = baseSeq
		}
		cluster.persistProgress()
	}

	host, port, err := net.SplitHostPort(master)
	if err != nil {
		return err
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	marker, ok := cluster.db.(interface{ MarkReplica(string, int) })
	if !ok {
		return errors.New("db does not support replica state")
	}
	marker.MarkReplica(host, portNum)

	if err := cluster.ossPull(master); err != nil {
		return err
	}
	logger.Infof("oss initial sync from master %s completed", master)
	return nil
}

// ossPull fetches WAL segments of masterAddr published after the local cursor
// and replays them, then refreshes the state marker.
func (cluster *Cluster) ossPull(master string) error {
	if cluster.ossStore == nil {
		return errors.New("oss store not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	metas, err := cluster.ossStore.ListWalSegments(ctx, master)
	if err != nil {
		return fmt.Errorf("list wal segments of %s: %w", master, err)
	}
	pulled := 0
	for _, meta := range metas {
		if meta.Seq <= cluster.lastAppliedSeq() {
			continue
		}
		if pulled >= walPullBatch {
			break
		}
		dlStart := time.Now()
		data, err := cluster.ossStore.DownloadWalSegment(ctx, master, meta.Seq)
		if err != nil {
			return fmt.Errorf("download wal segment seq=%d of %s: %w", meta.Seq, master, err)
		}
		cluster.lastDownloadMs.Store(time.Since(dlStart).Milliseconds())
		if err := cluster.replaySegment(data); err != nil {
			return fmt.Errorf("replay wal segment seq=%d of %s: %w", meta.Seq, master, err)
		}
		cluster.progressMu.Lock()
		cluster.progress.AppliedSeq = meta.Seq
		cluster.progress.Offset = meta.Offset
		cluster.progressMu.Unlock()
		pulled++
	}
	if pulled > 0 {
		cluster.persistProgress()
		cluster.refreshStateMarker()
	}
	cluster.lastSyncUnix.Store(time.Now().Unix())
	return nil
}

// replaySegment parses the RESP command stream of one WAL segment and
// executes it locally, mirroring the AOF load path.
func (cluster *Cluster) replaySegment(data []byte) error {
	replies, err := parser.ParseBytes(data)
	if err != nil {
		return err
	}
	fakeConn := connection.NewFakeConn()
	for _, reply := range replies {
		multi, ok := reply.(*protocol.MultiBulkReply)
		if !ok {
			continue
		}
		ret := cluster.db.Exec(fakeConn, multi.Args)
		if protocol.IsErrorReply(ret) {
			logger.Warnf("replay command error: %s", string(ret.ToBytes()))
		}
	}
	return nil
}

// ossCron runs periodically. The master refreshes its state marker; replicas
// pull new WAL segments from their master.
func (cluster *Cluster) ossCron() {
	if cluster.ossStore == nil {
		return
	}
	master := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
	if master == "" || master == cluster.SelfID() {
		cluster.refreshStateMarker()
		return
	}
	if err := cluster.ossPull(master); err != nil {
		logger.Errorf("oss pull from %s failed: %v", master, err)
	}
}

// ossMasterStale reports whether the master has stopped publishing: its state
// marker (updated by every WAL flush) is older than the failover timeout.
func (cluster *Cluster) ossMasterStale(masterAddr string, failoverTimeout time.Duration) bool {
	if cluster.ossStore == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	marker, ok, err := cluster.ossStore.LatestStateMarker(ctx, masterAddr)
	if err != nil {
		logger.Errorf("check state marker liveness of %s: %v", masterAddr, err)
		return false
	}
	if !ok {
		return false
	}
	return time.Since(marker.TS) > failoverTimeout
}

// ossPublishBaseSnapshot generates a local snapshot of the data and stores it
// as a base snapshot object at the current applied seq, so new replicas can
// bootstrap without replaying the whole WAL.
func (cluster *Cluster) ossPublishBaseSnapshot() error {
	if cluster.ossStore == nil {
		return errors.New("oss store not initialized")
	}
	gen, ok := cluster.db.(interface{ GenerateRDB(string) error })
	if !ok {
		return errors.New("db does not support snapshot generation")
	}
	filename := filepath.Join(config.GetTmpDir(), "godis-base-snapshot.rdb")
	if err := gen.GenerateRDB(filename); err != nil {
		return fmt.Errorf("generate base snapshot: %w", err)
	}
	bin, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read base snapshot: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	seq := cluster.lastAppliedSeq()
	if err := cluster.ossStore.UploadBaseSnapshot(ctx, cluster.SelfID(), seq, dbimpl.Version(), bin); err != nil {
		return fmt.Errorf("upload base snapshot: %w", err)
	}
	logger.Infof("base snapshot uploaded at seq=%d", seq)
	return nil
}
