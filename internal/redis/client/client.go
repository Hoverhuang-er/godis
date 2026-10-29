package client

import (
	"errors"
	"fmt"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/sync/wait"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string

	status  int32
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
	backoff time.Duration   // reconnect backoff, only touched by handleRead
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second

	// Reconnect backoff: a peer that accepts and then closes immediately must
	// not spin the read loop at full speed.
	minReconnectBackoff = time.Second
	maxReconnectBackoff = 30 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) RemoteAddress() string {
	return client.addr
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()
	// stop new request
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean
	_ = client.conn.Close()
	close(client.waitingReqs)
}

// reconnect dials in the background until it succeeds or the client is closed.
// The outage itself is reported once by handleRead; retries stay silent and
// back off, so an unreachable or flapping peer cannot flood the log.
func (client *Client) reconnect() {
	for atomic.LoadInt32(&client.status) != closed {
		client.backoff = min(max(2*client.backoff, minReconnectBackoff), maxReconnectBackoff)
		time.Sleep(client.backoff)
		if atomic.LoadInt32(&client.status) == closed {
			return
		}

		conn, err := net.Dial("tcp", client.addr)
		if err != nil {
			continue
		}
		_ = client.conn.Close() // ignore possible errors from repeated closes
		client.conn = conn

		close(client.waitingReqs)
		for req := range client.waitingReqs {
			req.err = errors.New("connection closed")
			req.waiting.Done()
		}
		client.waitingReqs = make(chan *request, chanSize)
		return
	}
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			slog.Error(fmt.Sprintf("%v", err))
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {
	for atomic.LoadInt32(&client.status) != closed {
		for payload := range parser.ParseStream(client.conn) {
			if payload.Err != nil {
				if atomic.LoadInt32(&client.status) == closed {
					return
				}
				// backoff == 0 marks the first failure of an outage: report it
				// once, then keep retrying silently (see reconnect).
				if client.backoff == 0 {
					slog.Error("connection lost, reconnecting: "+payload.Err.Error(), "addr", client.addr)
				}
				break
			}
			if client.backoff != 0 {
				// Traffic proves the new connection works; announce it once.
				client.backoff = 0
				slog.Info("reconnected to: " + client.addr)
			}
			client.finishRequest(payload.Data)
		}
		if atomic.LoadInt32(&client.status) == closed {
			return
		}
		client.reconnect()
	}
}
