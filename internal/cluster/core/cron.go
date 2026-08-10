package core

import (
	"sync/atomic"
	"time"

	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
	"github.com/Hoverhuang-er/godis/internal/config"
)

func (cluster *Cluster) clusterCron() {
	if cluster.config.noCron {
		return
	}
	// The probe interval is deliberately loose (default 15s) to tolerate
	// high-latency links between nodes.
	heartbeatInterval := config.Properties.HeartbeatInterval()
	snapshotInterval := time.Duration(config.Properties.ClusterSnapshotInterval) * time.Second
	if config.Properties.ClusterSnapshotInterval <= 0 {
		snapshotInterval = 60 * time.Second
	}

	ticker := time.NewTicker(time.Second)
	ossTicker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	defer ossTicker.Stop()

	var elapsed time.Duration
	var running int32
	for {
		select {
		case <-ticker.C:
			elapsed += time.Second
			if elapsed < heartbeatInterval {
				continue
			}
			elapsed = 0
			if cluster.raftNode.State() == raft.Leader {
				if atomic.CompareAndSwapInt32(&running, 0, 1) {
					go func() {
						cluster.doFailoverCheck()
						cluster.doRebalance()
						atomic.StoreInt32(&running, 0)
					}()
				}
				continue
			}
			cluster.sendHearbeat()
			if cluster.isMasterSlaveMode() {
				go cluster.doReplicaFailoverCheck()
			}
		case <-ossTicker.C:
			cluster.ossCron()
		case <-cluster.closeChan:
			return
		}
	}
}
