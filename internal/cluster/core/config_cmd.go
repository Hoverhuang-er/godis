package core

import (
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
)

// confSlotID is the fixed slot that hosts the conf namespace. It is owned
// by the seed and never migrated (see makeRebalancePlan).
const confSlotID uint32 = 0

func init() {
	RegisterCmd("config", execConfigCluster)
}

// execConfigCluster serves CONFIG on the conf slot owner: reads and writes
// are relayed to PickNode(confSlotID) unless this node owns the slot.
func execConfigCluster(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	peer := cluster.PickNode(confSlotID)
	if peer == cluster.SelfID() {
		return cluster.db.Exec(c, cmdLine)
	}
	return cluster.Relay(peer, c, cmdLine)
}
