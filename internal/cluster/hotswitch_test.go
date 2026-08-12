package cluster

import (
	"errors"
	"testing"

	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

type fakeSwapper struct{ db database.DB }

func (f *fakeSwapper) CurrentDB() database.DB { return f.db }
func (f *fakeSwapper) SwapDB(db database.DB)  { f.db = db }

func TestPromoteToSeed(t *testing.T) {
	config.Properties.Dir = t.TempDir()
	config.Properties.ClusterHeartbeatInterval = 1 // fast raft election
	config.Properties.ClusterSyncViaOss = false
	config.Properties.AnnounceHost = "127.0.0.1"
	config.Properties.Port = 46379
	config.Properties.RaftListenAddr = "127.0.0.1:46666"
	config.Properties.RaftAdvertiseAddr = "127.0.0.1:46666"
	hotSwitched = false
	swapper := &fakeSwapper{db: dbimpl.NewStandaloneServer()}

	if err := promoteToSeed(swapper, true); err != nil {
		t.Fatal(err)
	}
	if _, ok := swapper.CurrentDB().(*Cluster); !ok {
		t.Fatalf("db not swapped to cluster, got %T", swapper.CurrentDB())
	}
	if config.Properties.ClusterID == "" {
		t.Fatal("cluster_id not generated")
	}
	if err := promoteToSeed(swapper, true); !errors.Is(err, errAlreadyInCluster) {
		t.Fatalf("second promote must fail, got %v", err)
	}
}

func TestJoinInterceptPromotesStandalone(t *testing.T) {
	config.Properties.Dir = t.TempDir()
	config.Properties.ClusterHeartbeatInterval = 1
	config.Properties.ClusterSyncViaOss = false
	config.Properties.AnnounceHost = "127.0.0.1"
	config.Properties.Port = 46380
	config.Properties.RaftListenAddr = "127.0.0.1:46667"
	config.Properties.RaftAdvertiseAddr = "127.0.0.1:46667"
	config.Properties.ClusterID = ""
	hotSwitched = false
	server := dbimpl.NewStandaloneServer()
	swapper := &fakeSwapper{db: server}
	RegisterJoinIntercept(swapper)

	conn := connection.NewFakeConn()
	// The first cluster.join on a standalone node promotes it to seed and
	// hands the command to the new cluster's join handler.
	ret := server.Exec(conn, utils.ToCmdLine("cluster.join", "127.0.0.1:6390", "127.0.0.1:16666"))
	if Current() == nil {
		t.Fatal("node not promoted to cluster")
	}
	// The promoted seed holds a generated cluster id, so the legacy 2-arg
	// join is rejected by the cluster's own handler — proving the command
	// reached the cluster instead of failing with "unknown command". (A
	// full accepted join would add the fake joiner as a raft voter and
	// lose quorum, so the handshake itself is covered elsewhere.)
	if !protocol.IsErrorReply(ret) {
		t.Fatalf("join should be rejected by cluster handler, got %s", ret.ToBytes())
	}
}
