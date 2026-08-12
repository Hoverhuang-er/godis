package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/monitoring"
	"github.com/Hoverhuang-er/godis/internal/oss"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	rdbcore "github.com/hdt3213/rdb/core"
)

type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config
	ossStore    *oss.Store

	// walListener publishes this node's AOF batches to OSS when it is the
	// master; progress is the persisted replay cursor when it is a replica.
	walListener *walListener
	progress    walProgress
	progressMu  sync.Mutex

	// lastSyncUnix is the unix ts of the last successful WAL push (master)
	// or pull (replica); lastUploadMs/lastDownloadMs are the durations of
	// those operations. They feed the cluster metrics on /metrics.
	lastSyncUnix   atomic.Int64
	lastUploadMs   atomic.Int64
	lastDownloadMs atomic.Int64

	slotsManager    *slotsManager
	rebalanceManger *rebalanceManager
	transactions    *TransactionManager
	replicaManager  *replicaManager

	closeChan chan struct{}

	// allow inject route implementation
	getSlotImpl  func(key string) uint32
	pickNodeImpl func(slotID uint32) string
	id_          string // for tests only

	// slow log record
	slogLogger *dbimpl.SlowLogger
}

type Config struct {
	raft.RaftConfig
	StartAsSeed bool
	JoinAddress string
	// JoinHTTPAddress optionally replaces the TCP join channel with the
	// seed's HTTP API (host:port of the 63790 server). When non-empty the
	// join handshake is POSTed to http://JoinHTTPAddress/api/cluster/join.
	JoinHTTPAddress string
	// JoinHTTPToken is sent as the X-HEADER-AUTHTOKEN when joining over HTTP.
	JoinHTTPToken string
	Master        string
	// DB is the database engine the cluster wraps. When nil a fresh
	// standalone server is created; when set (runtime switch from
	// standalone mode) the running instance is reused so no data is lost.
	DB             database.DBEngine
	connectionStub ConnectionFactory // for test
	noCron         bool              // for test
}

func (c *Cluster) SelfID() string {
	if c.raftNode == nil {
		return c.id_
	}
	return c.raftNode.Cfg.ID()
}

// BootstrapConfNamespace writes the wrapped server's shared settings into
// the conf namespace (used when the cluster created its own standalone
// server).
func (c *Cluster) BootstrapConfNamespace() {
	if srv, ok := c.db.(*dbimpl.Server); ok {
		dbimpl.BootstrapConfNamespace(srv)
	}
}

// ClusterStats returns the current cluster state for the /metrics endpoint.
// Standalone nodes report Enabled=false and the empty mode.
func (c *Cluster) ClusterStats() monitoring.ClusterStats {
	stats := monitoring.ClusterStats{Enabled: c.ossStore != nil}
	if c.raftNode == nil {
		return stats
	}
	self := c.SelfID()
	masterID := c.raftNode.FSM.GetMaster(self)
	if masterID == "" {
		stats.Role = "master"
		stats.MasterID = self
	} else {
		stats.Role = "slave"
		stats.MasterID = masterID
		stats.MasterAddr = masterID
	}
	if c.isMasterSlaveMode() {
		stats.Mode = "master-slave"
	} else {
		stats.Mode = "raft"
	}
	c.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		stats.NodeCount = len(fsm.MasterSlaves)
	})
	stats.WalSeqApplied = c.lastAppliedSeq()
	if c.walListener != nil {
		c.walListener.mu.Lock()
		stats.WalSeqPublished = c.walListener.seq
		c.walListener.mu.Unlock()
	}
	stats.SyncLastSuccess = c.lastSyncUnix.Load()
	if stats.SyncLastSuccess > 0 {
		stats.SyncLagSeconds = time.Since(time.Unix(stats.SyncLastSuccess, 0)).Seconds()
	}
	stats.LastUploadMs = float64(c.lastUploadMs.Load())
	stats.LastDownloadMs = float64(c.lastDownloadMs.Load())
	return stats
}

func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionStub != nil {
		connections = cfg.connectionStub
	} else {
		connections = newDefaultClientFactory()
	}
	db := cfg.DB
	if db == nil {
		db = dbimpl.NewStandaloneServer()
	}
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		raftNode:        raftNode,
		db:              db,
		connections:     connections,
		config:          cfg,
		ossStore:        setupOssStore(),
		rebalanceManger: newRebalanceManager(),
		slotsManager:    newSlotsManager(),
		transactions:    newTransactionManager(),
		replicaManager:  newReplicaManager(),
		closeChan:       make(chan struct{}),
	}
	cluster.pickNodeImpl = func(slotID uint32) string {
		return defaultPickNodeImpl(cluster, slotID)
	}
	cluster.getSlotImpl = func(key string) uint32 {
		return defaultGetSlotImpl(cluster, key)
	}
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	cluster.registerOnFailover()

	// setup
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		if cfg.StartAsSeed {
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// join cluster
			if cfg.JoinHTTPAddress != "" {
				if err := httpJoinCluster(cfg); err != nil {
					return nil, err
				}
			} else {
				// The seed may still be promoting (a runtime-promoted seed
				// bootstraps raft for up to ~2x the heartbeat interval), so
				// retry the handshake instead of failing on the first
				// attempt's client timeout.
				deadline := time.Now().Add(2*cfg.ElectionTimeout + 5*time.Second)
				if cfg.ElectionTimeout == 0 {
					deadline = time.Now().Add(10 * time.Second)
				}
				var result redis.Reply
				for {
					conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
					if err == nil {
						joinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
						if config.Properties.ClusterID != "" {
							// new format with cluster-id handshake; legacy seeds without
							// a cluster id still accept the 2-arg form
							joinCmdLine = utils.ToCmdLine(joinClusterCommand, config.Properties.ClusterID,
								cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
						}
						if cfg.Master != "" {
							joinCmdLine = append(joinCmdLine, []byte(cfg.Master))
						}
						logger.Infof("send join cluster request to %s", cfg.JoinAddress)
						result = conn.Send(joinCmdLine)
						connections.ReturnPeerClient(conn)
						err := protocol.Try2ErrorReply(result)
						if err == nil || strings.Contains(err.Error(), "already in cluster") {
							// an accepted join that the client timed out on
							// may surface as "already in cluster" on retry;
							// membership is the goal, so treat it as success
							break
						}
					}
					if time.Now().After(deadline) {
						if err != nil {
							return nil, err
						}
						return nil, fmt.Errorf("join cluster %s failed: %s", cfg.JoinAddress, result.ToBytes())
					}
					time.Sleep(time.Second)
				}
			}
		}
	} else {
		masterAddr := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
		if masterAddr != "" {
			err := cluster.SlaveOf(masterAddr)
			if err != nil {
				panic(err)
			}
		}
	}

	// The seed node becomes the master: start publishing its WAL to OSS.
	if cluster.ossStore != nil && (cfg.StartAsSeed ||
		cluster.raftNode.FSM.GetMaster(cluster.SelfID()) == cluster.SelfID()) {
		cluster.loadProgress()
		if err := cluster.startWalPublishing(); err != nil {
			logger.Errorf("start wal publishing failed: %v", err)
		}
	}

	cluster.startDrRecovery()

	// expose cluster state on /metrics when the wrapped db owns the
	// Prometheus metrics server
	if setter, ok := cluster.db.(interface {
		SetClusterStatsFn(func() monitoring.ClusterStats)
	}); ok {
		setter.SetClusterStatsFn(cluster.ClusterStats)
	}

	// record slow log
	cluster.slogLogger = dbimpl.NewSlowLogger(config.Properties.SlowLogMaxLen, config.Properties.SlowLogSlowerThan)

	go cluster.clusterCron()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) Close() {
	close(cluster.closeChan)
	cluster.stopWalPublishing()
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		panic(err)
	}
}

// LoadRDB real implementation of loading rdb file
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

// httpJoinCluster sends the cluster-join handshake over the seed's HTTP API
// (POST http://JoinHTTPAddress/api/cluster/join) instead of the classic TCP
// join channel on the seed's redis port. The seed's web server forwards the
// request to its local godis node which performs the actual raft join, so the
// rest of the join flow (raft AddVoter, OSS sync) is identical to TCP joins.
func httpJoinCluster(cfg *Config) error {
	payload := struct {
		ClusterID string `json:"cluster_id"`
		RedisAddr string `json:"redis_addr"`
		RaftAddr  string `json:"raft_addr"`
		Master    string `json:"master,omitempty"`
	}{
		ClusterID: config.Properties.ClusterID,
		RedisAddr: cfg.RedisAdvertiseAddr,
		RaftAddr:  cfg.RaftAdvertiseAddr,
		Master:    cfg.Master,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := "http://" + cfg.JoinHTTPAddress + "/api/cluster/join"
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.JoinHTTPToken != "" {
		req.Header.Set("X-HEADER-AUTHTOKEN", cfg.JoinHTTPToken)
	}
	logger.Infof("send join cluster request to %s via http api", endpoint)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join cluster request to %s failed: http status %d", endpoint, resp.StatusCode)
	}
	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("join cluster request to %s failed: %w", endpoint, err)
	}
	if !result.Success {
		return fmt.Errorf("join cluster request to %s failed: %s", endpoint, result.Error)
	}
	return nil
}
