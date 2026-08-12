package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// DBSwapper is implemented by servers that can atomically replace their
// active database engine (standalone server or cluster) without closing
// existing client connections.
type DBSwapper interface {
	CurrentDB() database.DB
	SwapDB(db database.DB)
}

// hotSwitched guards against double-switching when the config reload
// triggered by our own cluster_id write-back fires.
var hotSwitched bool

// errAlreadyInCluster is returned when a node is promoted twice.
var errAlreadyInCluster = errors.New("already in cluster")

// promoteMu serializes concurrent promotion attempts (e.g. two joiners
// racing a standalone seed).
var promoteMu sync.Mutex

// RegisterHotSwitch enables runtime standalone-to-cluster switching. It
// registers a config-reload hook: when cluster mode is turned on while the
// node runs standalone, the existing server (with all its data) is wrapped in
// a cluster and swapped in atomically. The generated cluster id is written
// back to the config source so later nodes can join with it.
func RegisterHotSwitch(swapper DBSwapper) {
	config.RegisterReloadHook(func() {
		hotSwitch(swapper)
	})
}

// promoteToSeed switches a running standalone server into cluster mode.
// asSeed=true makes this node the cluster seed (generates cluster_id and
// bootstraps raft); asSeed=false joins the configured seed instead.
func promoteToSeed(swapper DBSwapper, asSeed bool) error {
	promoteMu.Lock()
	defer promoteMu.Unlock()
	if hotSwitched {
		return errAlreadyInCluster
	}
	config.Properties.ClusterEnable = true
	config.Properties.ClusterAsSeed = asSeed
	// generate and persist the cluster id before building the cluster so
	// join handshakes and the OSS object layout both use it
	if config.Properties.ClusterID == "" {
		config.Properties.ClusterID = utils.RandHexString(16)
		config.WriteBackClusterID(config.Properties.ClusterID)
	}
	current := swapper.CurrentDB()
	if _, alreadyCluster := current.(*Cluster); alreadyCluster {
		return errAlreadyInCluster
	}
	server, ok := current.(*dbimpl.Server)
	if !ok {
		return fmt.Errorf("cannot promote: current db is %T, expected standalone server", current)
	}
	logger.Infof("hot-switching standalone node %s to cluster mode, cluster_id=%s",
		config.Properties.AnnounceAddress(), config.Properties.ClusterID)
	cl := MakeCluster(server)
	swapper.SwapDB(cl)
	hotSwitched = true
	logger.Infof("hot-switch complete: node is now a cluster node")
	return nil
}

// hotSwitch is the config-reload-triggered variant of promoteToSeed.
func hotSwitch(swapper DBSwapper) {
	if hotSwitched {
		return
	}
	if !config.Properties.ClusterEnable {
		return
	}
	if err := promoteToSeed(swapper, config.Properties.ClusterAsSeed); err != nil && !errors.Is(err, errAlreadyInCluster) {
		logger.Errorf("hot-switch failed: %v", err)
	}
}

// RegisterJoinIntercept wires the standalone cluster.join intercept: the
// first join request to this node promotes it to seed and hands the command
// to the new cluster. Later joins (node already promoted) are forwarded
// straight to the cluster's join handler.
func RegisterJoinIntercept(swapper DBSwapper) {
	dbimpl.RegisterClusterJoinHandler(func(c redis.Connection, cmdLine [][]byte) redis.Reply {
		err := promoteToSeed(swapper, true)
		if err != nil && !errors.Is(err, errAlreadyInCluster) {
			return protocol.MakeErrReply(err.Error())
		}
		cl := Current()
		if cl == nil {
			return protocol.MakeErrReply("join failed: node not promoted")
		}
		return cl.Exec(c, cmdLine)
	})
}
