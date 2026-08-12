package database

import (
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
)

// clusterJoinHandler is registered by the cluster package to auto-promote a
// standalone node when it receives a cluster.join command (the first join
// of a new cluster). Kept as a callback to avoid an import cycle.
var clusterJoinHandler func(c redis.Connection, cmdLine [][]byte) redis.Reply

// RegisterClusterJoinHandler registers the cluster.join handler used when
// this node runs standalone.
func RegisterClusterJoinHandler(fn func(c redis.Connection, cmdLine [][]byte) redis.Reply) {
	clusterJoinHandler = fn
}
