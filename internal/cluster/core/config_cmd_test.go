package core

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
)

func TestConfigForwardedToConfSlotOwner(t *testing.T) {
	prevRequirePass := config.Properties.RequirePass
	defer func() { config.Properties.RequirePass = prevRequirePass }()
	nodes := MakeTestCluster([]string{"node0", "node1"})
	// pickNodeImpl maps slot % 2 -> node, so slot 0 is owned by node0
	conn := connection.NewFakeConn()
	ret := nodes["node1"].Exec(conn, utils.ToCmdLine("CONFIG", "SET", "requirepass", "hunter2"))
	asserts.AssertNotError(t, ret)

	ownerDB := nodes["node0"].db.(*dbimpl.Server)
	raw, ok := ownerDB.GetEntity(0, "conf:requirepass")
	if !ok || string(raw.Data.([]byte)) != "hunter2" {
		t.Fatalf("conf key not applied on slot-0 owner: ok=%v raw=%v", ok, raw)
	}
}
