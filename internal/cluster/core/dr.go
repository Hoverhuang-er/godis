package core

import (
	"context"
	"sort"
	"time"

	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/oss"
)

// drCheckInterval bounds how often the <3-node disaster-recovery check runs.
// It deliberately follows the health-probe cadence to stay cheap on S3.
const drCheckInterval = 30 * time.Second

// startDrRecovery launches the background disaster-recovery goroutine. It
// only acts in master-slave mode (<3 voting members, where raft cannot elect
// a leader) and only with OSS replication enabled: recovery metadata lives in
// the S3 state markers, and no data is ever deleted from OSS, so the newest
// segments remain pullable even after the node that wrote them is gone.
func (cluster *Cluster) startDrRecovery() {
	if cluster.ossStore == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(drCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if cluster.isMasterSlaveMode() {
					cluster.drCheck()
				}
			case <-cluster.closeChan:
				return
			}
		}
	}()
}

// drCheck compares all nodes' state markers, restores the newest data
// (highest applied WAL seq) and promotes this node when the master is lost.
func (cluster *Cluster) drCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	markers, err := cluster.ossStore.ListStateMarkers(ctx)
	if err != nil {
		logger.Errorf("dr: list state markers failed: %v", err)
		return
	}
	if len(markers) == 0 {
		return
	}
	// newest data = highest applied seq; tie-break by marker ts
	sort.Slice(markers, func(i, j int) bool {
		if markers[i].AppliedSeq != markers[j].AppliedSeq {
			return markers[i].AppliedSeq > markers[j].AppliedSeq
		}
		return markers[i].TS.After(markers[j].TS)
	})
	newest := markers[0]

	curMaster := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
	timeout := cluster.failoverTimeout()
	masterAlive := curMaster != "" && !cluster.ossMasterStale(curMaster, timeout)

	switch {
	case curMaster == "":
		// raft metadata lost entirely (e.g. all state wiped): the newest
		// node must be restored; everyone else follows it.
		cluster.drRestoreOrFollow(newest, "")
	case newest.NodeID == curMaster:
		// the newest data already lives on the current master: just catch
		// up to it if we are behind.
		cluster.drCatchUp(curMaster, newest.AppliedSeq)
	case !masterAlive:
		// master is gone but another node holds newer data than our view:
		// restore that data, then promote ourselves.
		logger.Warnf("dr: master %s lost, newest data on %s seq=%d",
			curMaster, newest.NodeID, newest.AppliedSeq)
		if err := cluster.drRestoreFrom(newest.NodeID, newest.AppliedSeq); err != nil {
			logger.Errorf("dr: restore from %s failed: %v", newest.NodeID, err)
			return
		}
		cluster.promoteSelf(curMaster)
	default:
		// master alive but a node claims newer data: only switch when the
		// claim is really ahead of our local cursor and of the master.
		curMasterMarker, ok := markerOf(markers, curMaster)
		if ok && newest.AppliedSeq > curMasterMarker.AppliedSeq {
			if err := cluster.drRestoreFrom(newest.NodeID, newest.AppliedSeq); err != nil {
				logger.Errorf("dr: restore newer data from %s failed: %v", newest.NodeID, err)
				return
			}
			cluster.promoteSelf(curMaster)
		}
	}
}

// drRestoreOrFollow handles the case of a fully lost raft view: if the
// newest marker belongs to us we promote ourselves, otherwise we follow the
// newest node.
func (cluster *Cluster) drRestoreOrFollow(newest oss.StateMarker, oldMaster string) {
	if newest.NodeID == cluster.SelfID() {
		cluster.promoteSelf(oldMaster)
		return
	}
	if err := cluster.drRestoreFrom(newest.NodeID, newest.AppliedSeq); err != nil {
		logger.Errorf("dr: restore from %s failed: %v", newest.NodeID, err)
		return
	}
	if err := cluster.SlaveOf(newest.NodeID); err != nil {
		logger.Errorf("dr: follow %s failed: %v", newest.NodeID, err)
	}
}

// drCatchUp pulls the remaining WAL segments of masterAddr up to targetSeq.
func (cluster *Cluster) drCatchUp(masterAddr string, targetSeq uint64) {
	if cluster.lastAppliedSeq() >= targetSeq {
		return
	}
	if err := cluster.ossPull(masterAddr); err != nil {
		logger.Errorf("dr: catch up to %s failed: %v", masterAddr, err)
	}
}

// drRestoreFrom replays the WAL segments of nodeAddr (published before it
// disappeared) up to targetSeq into this node. OSS never deletes segments,
// so this works even when nodeAddr is offline.
func (cluster *Cluster) drRestoreFrom(nodeAddr string, targetSeq uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	metas, err := cluster.ossStore.ListWalSegments(ctx, nodeAddr)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		if meta.Seq > targetSeq {
			break
		}
		if meta.Seq <= cluster.lastAppliedSeq() {
			continue
		}
		data, err := cluster.ossStore.DownloadWalSegment(ctx, nodeAddr, meta.Seq)
		if err != nil {
			return err
		}
		if err := cluster.replaySegment(data); err != nil {
			return err
		}
		cluster.progressMu.Lock()
		cluster.progress.AppliedSeq = meta.Seq
		cluster.progress.Offset = meta.Offset
		cluster.progressMu.Unlock()
	}
	cluster.persistProgress()
	cluster.refreshStateMarker()
	return nil
}

func markerOf(markers []oss.StateMarker, nodeID string) (oss.StateMarker, bool) {
	for _, m := range markers {
		if m.NodeID == nodeID {
			return m, true
		}
	}
	return oss.StateMarker{}, false
}
