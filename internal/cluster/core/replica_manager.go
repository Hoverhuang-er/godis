package core

import (
	"sync"
	"time"

	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

/*
1. The master and slave are both added to the raft group, and failover does not involve changes of raft members. 
2. Timer job `doFailoverCheck` finds timeout masters, then calls `triggerFailover`
3. Raft leader sends `slaveof no one` to new master
4. Raft proposes `EventFinishFailover` to change route. 
Other slaves and old master will get this message from raft, and become slave of new master.(see cluster.registerOnFailover)
*/

const heartbeatCommand = "cluster.heartbeat"

func init() {
	RegisterCmd(heartbeatCommand, execHeartbeat)
}

const (
	statusNormal  = iota
	statusFailing // failover in progress
)

type replicaManager struct {
	mu               sync.RWMutex
	masterHeartbeats map[string]time.Time // id -> lastHeartbeatTime
	// masterProbeFailures counts consecutive failed liveness probes per
	// master, used by replicas in master-slave mode (<3 voters).
	masterProbeFailures map[string]int
}

func newReplicaManager() *replicaManager {
	return &replicaManager{
		masterHeartbeats:     make(map[string]time.Time),
		masterProbeFailures:  make(map[string]int),
	}
}

// execHeartbeat receives heartbeat from follower as raft leader
// cmdLine: cluster.heartbeat nodeId
func execHeartbeat(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(heartbeatCommand)
	}
	id := string(cmdLine[1])
	cluster.replicaManager.mu.Lock()
	cluster.replicaManager.masterHeartbeats[id] = time.Now()
	cluster.replicaManager.mu.Unlock()

	return protocol.MakeOkReply()
}

func (cluster *Cluster) sendHearbeat() {
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		logger.Error(err)
		return
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	reply := leaderConn.Send(utils.ToCmdLine(heartbeatCommand, cluster.SelfID()))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		logger.Error(err)
	}
}

// failoverTimeout returns how long a master may stay silent before being
// considered dead: probe interval * max consecutive failures (default 15s
// x 10 = 150s), configurable via cluster.heartbeat_interval and
// cluster.failover_max_failures.
func (cluster *Cluster) failoverTimeout() time.Duration {
	return config.Properties.FailoverTimeout()
}

func (cluster *Cluster) doFailoverCheck() {
	// A raft group with fewer than 2 voters cannot elect a leader to commit
	// a failover decision; failing over in that state would just churn the
	// FSM. Replicas in master-slave mode (<3 voters but >=2 nodes) use
	// doReplicaFailoverCheck instead, so skip raft-driven failover here.
	if cluster.raftNode.VoterCount() < 2 {
		return
	}
	var timeoutMasters []*raft.MasterSlave
	ddl := time.Now().Add(-cluster.failoverTimeout())
	cluster.replicaManager.mu.RLock()
	for masterId, lastTime := range cluster.replicaManager.masterHeartbeats {
		if lastTime.Second() == 0 {
			// do not set new joined node as timeout
			cluster.replicaManager.masterHeartbeats[masterId] = time.Now()
		}
		if lastTime.Before(ddl) {
			slaves := cluster.raftNode.GetSlaves(masterId)
			if slaves != nil && len(slaves.Slaves) > 0 {
				timeoutMasters = append(timeoutMasters, slaves)
			}

		}
	}
	cluster.replicaManager.mu.RUnlock()

	// trigger failover
	for _, failed := range timeoutMasters {
		cluster.triggerFailover(failed)
	}
}

func (cluster *Cluster) triggerFailover(failed *raft.MasterSlave) error {
	if failed == nil || failed.MasterId == "" || len(failed.Slaves) == 0 {
		return nil
	}
	newMaster := failed.Slaves[0]
	id := utils.RandString(20)
	logger.Infof("start failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterId, newMaster)
	// send slave of to new master
	conn, err := cluster.connections.BorrowPeerClient(newMaster)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(conn)

	reply := conn.Send(utils.ToCmdLine("slaveof", "no", "one"))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		return err
	}

	// new master is ready to receive commands, change route
	_, err = cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventFinishFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterId: failed.MasterId,
			NewMasterId: newMaster,
		},
	})
	if err != nil {
		return err
	}
	logger.Infof("proposed finish failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterId, newMaster)
	// other slaves will listen to raft to change master
	return nil
}

func (cluster *Cluster) registerOnFailover() {
	cluster.raftNode.SetOnFailover(func(newMaster string) {
		if newMaster == cluster.SelfID() {
			// this node was promoted: start publishing its WAL to OSS
			if err := cluster.startWalPublishing(); err != nil {
				logger.Errorf("start wal publishing after failover failed: %v", err)
			}
			return
		}
		if newMaster != "" {
			// this node is demoted: stop publishing, become a replica
			cluster.stopWalPublishing()
			if err := cluster.SlaveOf(newMaster); err != nil {
				logger.Errorf("slave of %s failed: %v", newMaster, err)
			}
		}
	})
}

// isMasterSlaveMode reports whether the cluster runs in master-slave mode,
// i.e. it has fewer than 3 voting members. In this mode the raft quorum
// cannot guarantee failover (a 2-node raft group cannot elect a leader when
// one node dies), so replicas decide failover themselves by probing their
// master; with 3+ voters the raft leader drives failover instead.
func (cluster *Cluster) isMasterSlaveMode() bool {
	return cluster.raftNode != nil && cluster.raftNode.VoterCount() < 3
}

// probeMaster returns true when the master at masterAddr acknowledged the
// liveness probe. When OSS replication is enabled the master is considered
// alive as long as it keeps publishing snapshots.
func (cluster *Cluster) probeMaster(masterAddr string) bool {
	if cluster.ossStore != nil {
		return !cluster.ossMasterStale(masterAddr, cluster.failoverTimeout())
	}
	conn, err := cluster.connections.BorrowPeerClient(masterAddr)
	if err != nil {
		return false
	}
	defer cluster.connections.ReturnPeerClient(conn)
	reply := conn.Send(utils.ToCmdLine(heartbeatCommand, cluster.SelfID()))
	return protocol.Try2ErrorReply(reply) == nil
}

// doReplicaFailoverCheck runs on replicas in master-slave mode (<3 voters).
// A replica that loses its master promotes itself to master after
// failoverMaxFailures consecutive failed probes.
func (cluster *Cluster) doReplicaFailoverCheck() {
	master := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
	if master == "" {
		return
	}
	ok := cluster.probeMaster(master)
	cluster.replicaManager.mu.Lock()
	if ok {
		delete(cluster.replicaManager.masterProbeFailures, master)
		cluster.replicaManager.mu.Unlock()
		return
	}
	cluster.replicaManager.masterProbeFailures[master]++
	failures := cluster.replicaManager.masterProbeFailures[master]
	cluster.replicaManager.mu.Unlock()

	maxFailures := config.Properties.ClusterFailoverMaxFailures
	if maxFailures <= 0 {
		maxFailures = 10
	}
	if failures < maxFailures {
		logger.Warnf("master %s probe failed %d/%d", master, failures, maxFailures)
		return
	}
	logger.Errorf("master %s lost after %d failed probes, promote self to master", master, failures)
	cluster.promoteSelf(master)
}

// promoteSelf promotes this replica to master. Data is immediately served
// locally; the raft metadata update is best-effort and retried when raft
// recovers (in a 2-node group the dead master may have been the raft leader,
// in which case no quorum exists until it returns).
func (cluster *Cluster) promoteSelf(oldMaster string) {
	c := connection.NewFakeConn()
	reply := cluster.db.Exec(c, utils.ToCmdLine("slaveof", "no", "one"))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		logger.Errorf("promote self failed: %v", err)
		return
	}
	// a promoted node becomes the master: start publishing its WAL to OSS
	if err := cluster.startWalPublishing(); err != nil {
		logger.Errorf("start wal publishing after self promotion failed: %v", err)
	}
	id := utils.RandString(20)
	_, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventFinishFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterId: oldMaster,
			NewMasterId: cluster.SelfID(),
		},
	})
	if err != nil {
		logger.Errorf("propose failover for self promotion failed (will retry): %v", err)
		go cluster.retryPromotePropose(oldMaster, id)
	}
}

func (cluster *Cluster) retryPromotePropose(oldMaster, id string) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err := cluster.raftNode.Propose(&raft.LogEntry{
				Event: raft.EventFinishFailover,
				FailoverTask: &raft.FailoverTask{
					ID:          id,
					OldMasterId: oldMaster,
					NewMasterId: cluster.SelfID(),
				},
			})
			if err == nil {
				logger.Infof("failover metadata updated after retry")
				return
			}
		case <-cluster.closeChan:
			return
		}
	}
}
