// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"os"
	"path"

	_ "github.com/Hoverhuang-er/godis/internal/cluster/commands" // register commands
	"github.com/Hoverhuang-er/godis/internal/cluster/core"
	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
	"github.com/Hoverhuang-er/godis/internal/config"
	"log/slog"
)

type Cluster = core.Cluster

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {
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
		},
		StartAsSeed: config.Properties.ClusterAsSeed,
		JoinAddress: config.Properties.ClusterSeed,
		Master:      config.Properties.MasterInCluster,
	})
	if err != nil {
		slog.Error(err.Error())
		panic(err)
	}
	return cluster
}
