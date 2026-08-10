// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"os"
	"path"
	"sync/atomic"

	_ "github.com/Hoverhuang-er/godis/internal/cluster/commands" // register commands
	"github.com/Hoverhuang-er/godis/internal/cluster/core"
	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
	"github.com/Hoverhuang-er/godis/internal/config"
	idatabase "github.com/Hoverhuang-er/godis/internal/interface/database"
	"log/slog"
)

type Cluster = core.Cluster

// current is the most recently created cluster node, used by the HTTP API
// (/api/cluster/status) and reload hooks to report live cluster state.
var current atomic.Pointer[Cluster]

// Current returns the live cluster node, or nil when running standalone.
func Current() *Cluster {
	return current.Load()
}

// MakeCluster creates and starts a node of cluster.
// db may be nil (fresh standalone server is created) or an already-running
// standalone server, in which case the cluster reuses it — used for the
// runtime standalone-to-cluster switch so no data is lost.
func MakeCluster(db idatabase.DBEngine) *Cluster {
	raftPath := path.Join(config.Properties.Dir, "raft")
	err := os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	cluster, err := core.NewCluster(&core.Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: config.Properties.AnnounceAddress(),
			RaftListenAddr:     config.Properties.RaftListenAddr,
			RaftAdvertiseAddr:  config.Properties.RaftAnnounceAddress(),
			Dir:                raftPath,
			// High-latency friendly: raft heartbeat/election follow the
			// configured cluster health-probe interval (default 15s).
			HeartbeatTimeout: config.Properties.HeartbeatInterval(),
			ElectionTimeout:  config.Properties.HeartbeatInterval(),
		},
		DB:              db,
		StartAsSeed:     config.Properties.ClusterAsSeed,
		JoinAddress:     config.Properties.ClusterSeed,
		JoinHTTPAddress: config.Properties.ClusterJoinHTTP,
		JoinHTTPToken:   config.Properties.ClusterJoinHTTPToken,
		Master:          config.Properties.MasterInCluster,
	})
	if err != nil {
		slog.Error(err.Error())
		panic(err)
	}
	current.Store(cluster)
	return cluster
}
