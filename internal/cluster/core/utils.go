package core

import (
	"context"
	"errors"
	"hash/crc32"
	"net"
	"strings"
	"sync"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

const SlotCount int = 1024

const getCommittedIndexCommand = "raft.committedindex"

func init() {
	RegisterCmd(getCommittedIndexCommand, execRaftCommittedIndex)
}

// relay function relays command to peer or calls cluster.Exec
func (cluster *Cluster) Relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.SelfID() {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	cli, err := cluster.connections.BorrowPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.connections.ReturnPeerClient(cli)
	}()
	return cli.Send(cmdLine)
}

// LocalExec executes command at local node
func (cluster *Cluster) LocalExec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

// LocalExec executes command at local node
func (cluster *Cluster) LocalExecWithinLock(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return cluster.db.ExecWithLock(c, cmdLine)
}

func (cluster *Cluster) SlaveOf(master string) error {
	host, port, err := net.SplitHostPort(master)
	if err != nil {
		return errors.New("invalid master address")
	}
	c := connection.NewFakeConn()
	reply := cluster.db.Exec(c, utils.ToCmdLine("slaveof", host, port))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		return err
	}
	return nil
}

// GetPartitionKey extract hashtag
func GetPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

func defaultGetSlotImpl(cluster *Cluster, key string) uint32 {
	partitionKey := GetPartitionKey(key)
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(SlotCount)
}

func (cluster *Cluster) GetSlot(key string) uint32 {
	return cluster.getSlotImpl(key)
}

func defaultPickNodeImpl(cluster *Cluster, slotID uint32) string {
	return cluster.raftNode.FSM.PickNode(slotID)
}

// pickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is exporting the slot
func (cluster *Cluster) PickNode(slotID uint32) string {
	return cluster.pickNodeImpl(slotID)
}

// format: raft.committedindex
func execRaftCommittedIndex(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	index, err := cluster.raftNode.CommittedIndex()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeIntReply(int64(index))
}

// LocalExists returns existed ones from `keys` in local node
func (cluster *Cluster) LocalExists(keys []string) []string {
	var exists []string
	for _, key := range keys {
		_, ok := cluster.db.GetEntity(0, key)
		if ok {
			exists = append(exists, key)
		}
	}
	return exists
}

// parallelRelay sends commands to multiple nodes concurrently and returns results.
// Uses goroutines + channels for parallel execution.
func (cluster *Cluster) parallelRelay(
	routeMap map[string]CmdLine,
	c redis.Connection,
) map[string]redis.Reply {
	type nodeResult struct {
		node   string
		reply  redis.Reply
	}

	results := make(map[string]redis.Reply)
	resultCh := make(chan nodeResult, len(routeMap))

	var wg sync.WaitGroup
	for node, cmdLine := range routeMap {
		wg.Add(1)
		node := node
		cmdLine := cmdLine
		go func() {
			defer wg.Done()
			reply := cluster.Relay(node, c, cmdLine)
			resultCh <- nodeResult{node: node, reply: reply}
		}()
	}
	wg.Wait()
	close(resultCh)

	for r := range resultCh {
		results[r.node] = r.reply
	}
	return results
}

// parallelRelayWithError sends commands to multiple nodes concurrently.
// Returns on the first error reply, or aggregates all results.
func (cluster *Cluster) parallelRelayWithError(
	routeMap map[string]CmdLine,
	c redis.Connection,
) (map[string]redis.Reply, error) {
	type nodeResult struct {
		node  string
		reply redis.Reply
		err   error
	}

	results := make(map[string]redis.Reply)
	resultCh := make(chan nodeResult, len(routeMap))
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for node, cmdLine := range routeMap {
		wg.Add(1)
		node := node
		cmdLine := cmdLine
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}
			reply := cluster.Relay(node, c, cmdLine)
			err := protocol.Try2ErrorReply(reply)
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			resultCh <- nodeResult{node: node, reply: reply}
		}()
	}
	wg.Wait()
	close(resultCh)
	close(errCh)

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	for r := range resultCh {
		results[r.node] = r.reply
	}
	return results, nil
}

// RelayWorkerPool manages goroutine workers for processing relay requests.
// Each peer node gets a dedicated goroutine with a buffered channel for async relay.
type RelayWorkerPool struct {
	workers map[string]chan relayTask
	wg      sync.WaitGroup
	cluster *Cluster
}

type relayTask struct {
	cmdLine CmdLine
	result  chan relayResult
}

type relayResult struct {
	reply redis.Reply
	err   error
}

const relayChannelSize = 64

// NewRelayWorkerPool creates a pool with one goroutine per peer.
func (cluster *Cluster) NewRelayWorkerPool() *RelayWorkerPool {
	p := &RelayWorkerPool{
		workers: make(map[string]chan relayTask),
		cluster: cluster,
	}
	return p
}

// StartWorker starts a worker goroutine for the given peer.
func (p *RelayWorkerPool) StartWorker(peerAddr string) {
	if _, ok := p.workers[peerAddr]; ok {
		return
	}
	ch := make(chan relayTask, relayChannelSize)
	p.workers[peerAddr] = ch
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for task := range ch {
			// Borrow connection once, process multiple tasks
			cli, err := p.cluster.connections.BorrowPeerClient(peerAddr)
			if err != nil {
				task.result <- relayResult{err: err}
				continue
			}
			reply := cli.Send(task.cmdLine)
			_ = p.cluster.connections.ReturnPeerClient(cli)
			task.result <- relayResult{reply: reply}
		}
	}()
}

// SendAsync sends a command to a peer via the worker pool.
func (p *RelayWorkerPool) SendAsync(peerAddr string, cmdLine CmdLine) relayResult {
	ch, ok := p.workers[peerAddr]
	if !ok {
		p.StartWorker(peerAddr)
		ch = p.workers[peerAddr]
	}
	task := relayTask{
		cmdLine: cmdLine,
		result:  make(chan relayResult, 1),
	}
	ch <- task
	return <-task.result
}

// Stop closes all workers and waits for them to finish.
func (p *RelayWorkerPool) Stop() {
	for _, ch := range p.workers {
		close(ch)
	}
	p.wg.Wait()
}
