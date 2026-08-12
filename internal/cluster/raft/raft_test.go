package raft

import (
	"bytes"
	"io"
	"os"
	"testing"

	"encoding/json"

	"github.com/hashicorp/raft"
)

func TestNodeLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	cfg := &RaftConfig{
		RedisAdvertiseAddr: "127.0.0.1:8000",
		RaftListenAddr:     "127.0.0.1:9000",
		Dir:                tmpDir,
	}

	node, err := StartNode(cfg)
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	if node == nil {
		t.Fatal("node is nil")
	}

	//HasExistingState
	has, err := node.HasExistingState()
	if err != nil {
		t.Fatalf("HasExistingState failed: %v", err)
	}
	if has {
		t.Fatalf("HasExistingState should be false for a new node")
	}

	//设置Failover回调
	called := false
	node.SetOnFailover(func(newMaster string) {
		called = true
		t.Log("Failover to new master:", newMaster, "called =", called)
	})

	//BootstrapCluster
	err = node.BootstrapCluster(10)
	if err != nil {
		t.Fatalf("BootstrapCluster failed: %v", err)
	}

	//Propose事件
	index, err := node.Propose(&LogEntry{
		Event: EventSeedStart,
		InitTask: &InitTask{
			Leader:    cfg.RedisAdvertiseAddr,
			SlotCount: 10,
		},
	})
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if index == 0 {
		t.Fatal("Propose returned index 0")
	}

	_, err = node.Propose(&LogEntry{
		Event: EventStartMigrate,
		MigratingTask: &MigratingTask{
			ID:         "migrate1",
			SrcNode:    "127.0.0.1:8000",
			TargetNode: "127.0.0.1:8001",
			Slots:      []uint32{1, 2, 3},
		},
	})
	if err != nil {
		t.Fatalf("Propose migrate failed: %v", err)
	}

	//添加节点到raft
	err = node.AddToRaft(cfg.RedisAdvertiseAddr, cfg.RaftListenAddr)
	if err == nil {
		t.Log("AddToRaft should fail for existing node")
	}

	//Evict节点
	err = node.HandleEvict(cfg.RedisAdvertiseAddr)
	if err == nil {
		t.Fatalf("HandleEvict should fail when removing the only voter")
	} else {
		t.Logf("HandleEvict failed as expected: %v", err)
	}

	err = node.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestRaftConfigID(t *testing.T) {
	cfg := &RaftConfig{RedisAdvertiseAddr: "test-id"}
	if cfg.ID() != "test-id" {
		t.Errorf("ID() should return RedisAdvertiseAddr")
	}
}

func TestFSMApplyEvents(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}

	//EventSeedStart
	entry := &LogEntry{
		Event: EventSeedStart,
		InitTask: &InitTask{
			Leader:    "node1",
			SlotCount: 3,
		},
	}
	data, _ := json.Marshal(entry)
	fsm.Apply(&raft.Log{Data: data})

	//EventStartMigrate
	entry = &LogEntry{
		Event: EventStartMigrate,
		MigratingTask: &MigratingTask{
			ID:         "m1",
			SrcNode:    "node1",
			TargetNode: "node2",
			Slots:      []uint32{1, 2},
		},
	}
	data, _ = json.Marshal(entry)
	fsm.Apply(&raft.Log{Data: data})

	//EventFinishMigrate
	entry = &LogEntry{
		Event: EventFinishMigrate,
		MigratingTask: &MigratingTask{
			ID:         "m1",
			SrcNode:    "node1",
			TargetNode: "node2",
			Slots:      []uint32{1, 2},
		},
	}
	data, _ = json.Marshal(entry)
	fsm.Apply(&raft.Log{Data: data})

	//EventFinishFailover
	fsm.Node2Slot["node1"] = []uint32{0, 1}
	entry = &LogEntry{
		Event: EventFinishFailover,
		FailoverTask: &FailoverTask{
			ID:          "f1",
			OldMasterId: "node1",
			NewMasterId: "node2",
		},
	}
	data, _ = json.Marshal(entry)
	fsm.Apply(&raft.Log{Data: data})

	//EventJoin
	entry = &LogEntry{
		Event: EventJoin,
		JoinTask: &JoinTask{
			NodeId: "node3",
			Master: "node2",
		},
	}
	data, _ = json.Marshal(entry)
	fsm.Apply(&raft.Log{Data: data})
}

func TestFSMSnapshotAndRestore(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    map[string][]uint32{"node1": {1, 2}},
		Slot2Node:    map[uint32]string{1: "node1", 2: "node1"},
		Migratings:   map[string]*MigratingTask{"m1": {ID: "m1"}},
		MasterSlaves: map[string]*MasterSlave{"node1": {MasterId: "node1", Slaves: []string{"node2"}}},
		SlaveMasters: map[string]string{"node2": "node1"},
		Failovers:    map[string]*FailoverTask{"f1": {ID: "f1"}},
	}
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	buf := &bytes.Buffer{}
	_ = snap.Persist(&mockSink{buf: buf})
	snap.Release()

	//Restore
	fsm2 := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}
	err = fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
}

type mockSink struct {
	buf *bytes.Buffer
	id  string
}

func (m *mockSink) ID() string {
	return m.id
}

func (m *mockSink) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}
func (m *mockSink) Close() error {
	return nil
}
func (m *mockSink) Cancel() error {
	return nil
}

func TestFSMUtils(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    map[string][]uint32{"node1": {1, 2}},
		Slot2Node:    map[uint32]string{1: "node1", 2: "node1"},
		MasterSlaves: map[string]*MasterSlave{"node1": {MasterId: "node1", Slaves: []string{"node2"}}},
		SlaveMasters: map[string]string{"node2": "node1"},
	}
	fsm.addSlots("node1", []uint32{3})
	fsm.removeSlots("node1", []uint32{1})
	fsm.failover("node1", "node2")
	fsm.addNode("node3", "node1")
	_ = fsm.GetMaster("node2")
}

// TestFSMFailoverUnknownMaster is a regression test for the nil-pointer panic
// seen in E2E: fsm.failover with an oldMasterId that has no MasterSlaves
// record (e.g. an already-failed-over, evicted, or ghost node) used to panic
// dereferencing fsm.MasterSlaves[oldMasterId].Slaves.
func TestFSMFailoverUnknownMaster(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    map[string][]uint32{"node1": {0, 1, 2}},
		Slot2Node:    map[uint32]string{0: "node1", 1: "node1", 2: "node1"},
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: map[string]*MasterSlave{"node1": {MasterId: "node1", Slaves: []string{"node2"}}},
		SlaveMasters: map[string]string{"node2": "node1"},
		Failovers:    make(map[string]*FailoverTask),
	}

	fsm.failover("ghost-master", "node2")
	fsm.failover("ghost-master", "node2")
	fsm.failover("node1", "node2")
	if got := fsm.GetMaster("node1"); got != "node2" {
		t.Errorf("node1 should now follow node2, got %q", got)
	}
	if ms := fsm.MasterSlaves["node2"]; ms == nil || ms.MasterId != "node2" {
		t.Errorf("node2 should be the new master, got %+v", ms)
	}
}

// TestFSMApplyFinishFailoverUnknownMaster is a regression test for the same
// panic reached through the raft Apply path with a committed
// EventFinishFailover whose old master is unknown to the FSM.
func TestFSMApplyFinishFailoverUnknownMaster(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    map[string][]uint32{"node1": {0}},
		Slot2Node:    map[uint32]string{0: "node1"},
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: map[string]*MasterSlave{"node1": {MasterId: "node1"}},
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}
	entry := &LogEntry{
		Event: EventFinishFailover,
		FailoverTask: &FailoverTask{
			ID:          "f-ghost",
			OldMasterId: "ghost-master",
			NewMasterId: "node2",
		},
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	// must not panic
	fsm.Apply(&raft.Log{Data: data})
}

func TestUtils(t *testing.T) {
	//这里只能简单调用下辅助方法
	cfg := &RaftConfig{RedisAdvertiseAddr: "node1", RaftListenAddr: "127.0.0.1:9000", Dir: t.TempDir()}
	node, err := StartNode(cfg)
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	_ = node.Self()
	_ = node.State()
	_, _ = node.CommittedIndex()
	_ = node.GetLeaderRedisAddress()
	_, _ = node.GetNodes()
	_ = node.GetSlaves("node1")
	_ = node.Close()
}
