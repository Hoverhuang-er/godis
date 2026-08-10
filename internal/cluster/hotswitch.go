package cluster

import (
	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
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

func hotSwitch(swapper DBSwapper) {
	if hotSwitched {
		return
	}
	if !config.Properties.ClusterEnable {
		return
	}
	current := swapper.CurrentDB()
	if _, alreadyCluster := current.(*Cluster); alreadyCluster {
		return
	}
	server, ok := current.(*dbimpl.Server)
	if !ok {
		return
	}
	// generate and persist the cluster id before building the cluster so
	// join handshakes and the OSS object layout both use it
	if config.Properties.ClusterID == "" {
		config.Properties.ClusterID = utils.RandHexString(16)
		config.WriteBackClusterID(config.Properties.ClusterID)
	}
	logger.Infof("hot-switching standalone node %s to cluster mode, cluster_id=%s",
		config.Properties.AnnounceAddress(), config.Properties.ClusterID)
	cl := MakeCluster(server)
	swapper.SwapDB(cl)
	hotSwitched = true
	logger.Infof("hot-switch complete: node is now a cluster node")
}
