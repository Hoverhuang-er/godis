package core

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
)

func TestConfKeysPinToSlotZero(t *testing.T) {
	cluster := &Cluster{}
	for _, key := range []string{"conf:requirepass", "conf:appendonly"} {
		if got := defaultGetSlotImpl(cluster, key); got != confSlotID {
			t.Errorf("conf key %s routed to slot %d, want %d", key, got, confSlotID)
		}
	}
	// regular keys still hash normally
	if got := defaultGetSlotImpl(cluster, "user-key"); got == confSlotID {
		t.Errorf("suspicious: user key hashed to slot 0")
	}
}

func TestRebalancePlanExcludesConfSlot(t *testing.T) {
	// All slots on n0 (> avgSlot+1 for 2 nodes), none on n1: n0 exports.
	slots := make([]uint32, 0, SlotCount)
	for i := uint32(0); i < uint32(SlotCount); i++ {
		slots = append(slots, i)
	}
	fsm := &raft.FSM{
		Node2Slot: map[string][]uint32{"n0": slots, "n1": {}},
		MasterSlaves: map[string]*raft.MasterSlave{
			"n0": {MasterId: "n0"},
			"n1": {MasterId: "n1"},
		},
	}
	cluster := &Cluster{raftNode: &raft.Node{FSM: fsm}}
	tasks, err := cluster.makeRebalancePlan()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) == 0 {
		t.Fatal("expected a rebalance plan")
	}
	for _, task := range tasks {
		for _, slot := range task.Slots {
			if slot == confSlotID {
				t.Fatalf("rebalance plan migrates conf slot %d", confSlotID)
			}
		}
	}
}
