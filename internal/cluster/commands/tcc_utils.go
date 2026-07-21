package commands

import (
	"sync"

	"github.com/Hoverhuang-er/godis/internal/cluster/core"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

type CmdLine = [][]byte

// node -> keys on the node
type RouteMap map[string][]string

func getRouteMap(cluster *core.Cluster, keys []string) RouteMap {
	m := make(RouteMap)
	for _, key := range keys {
		slot := cluster.GetSlot(key)
		node := cluster.PickNode(slot)
		m[node] = append(m[node], key)
	}
	return m
}

type TccTx struct {
	rawCmdLine CmdLine
	routeMap RouteMap
	cmdLines map[string]CmdLine // node -> CmdLine
}

// execute tcc
// returns node->result map
func doTcc(cluster *core.Cluster, c redis.Connection, tx *TccTx) (map[string]redis.Reply, protocol.ErrorReply) {
	txId := utils.RandString(6)
	var mu sync.Mutex
	var firstErr protocol.ErrorReply

	// prepareChan collects node results concurrently
	type prepResult struct {
		node string
	}

	// --- Phase 1: Concurrent prepare ---
	prepResults := make(chan prepResult, len(tx.routeMap))
	var prepWg sync.WaitGroup
	for node, cmdLine := range tx.cmdLines {
		prepWg.Add(1)
		node := node
		cmdLine := cmdLine
		go func() {
			defer prepWg.Done()
			prepareCmd := utils.ToCmdLine("prepare", txId)
			prepareCmd = append(prepareCmd, cmdLine...)
			reply := cluster.Relay(node, c, prepareCmd)
			mu.Lock()
			if firstErr == nil {
				if err := protocol.Try2ErrorReply(reply); err != nil {
					firstErr = protocol.MakeErrReply("prepare failed: " + err.Error())
				}
			}
			mu.Unlock()
			prepResults <- prepResult{node: node}
		}()
	}
	prepWg.Wait()
	close(prepResults)

	if firstErr != nil {
		requestRollback(cluster, c, txId, tx.routeMap)
		return nil, firstErr
	}

	// --- Phase 2: Concurrent commit ---
	commitCmd := utils.ToCmdLine("commit", txId)
	commitResults := make(chan relayNodeResult, len(tx.routeMap))
	var commitWg sync.WaitGroup
	firstErr = nil
	for node := range tx.routeMap {
		commitWg.Add(1)
		node := node
		go func() {
			defer commitWg.Done()
			reply := cluster.Relay(node, c, commitCmd)
			mu.Lock()
			if firstErr == nil {
				if err := protocol.Try2ErrorReply(reply); err != nil {
					firstErr = protocol.MakeErrReply("commit failed: " + err.Error())
				}
			}
			mu.Unlock()
			commitResults <- relayNodeResult{node: node, reply: reply}
		}()
	}
	commitWg.Wait()
	close(commitResults)

	if firstErr != nil {
		requestRollback(cluster, c, txId, tx.routeMap)
		return nil, firstErr
	}

	result := make(map[string]redis.Reply)
	for r := range commitResults {
		result[r.node] = r.reply
	}
	return result, nil
}

type relayNodeResult struct {
	node  string
	reply redis.Reply
}

func requestRollback(cluster *core.Cluster, c redis.Connection, txId string, routeMap RouteMap) {
	rollbackCmd := utils.ToCmdLine("rollback", txId)
	var wg sync.WaitGroup
	for node := range routeMap {
		wg.Add(1)
		node := node
		go func() {
			defer wg.Done()
			cluster.Relay(node, c, rollbackCmd)
		}()
	}
	wg.Wait()
}