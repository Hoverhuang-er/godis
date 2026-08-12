package tcp

import (
	"bufio"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestListenAndServe(t *testing.T) {
	var err error
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n"))
		if err != nil {
			t.Error(err)
			return
		}
		bufReader := bufio.NewReader(conn)
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		if string(line) != val {
			t.Error("get wrong response")
			return
		}
	}
	_ = conn.Close()
	idleConns := make([]net.Conn, 0, 5)
	for i := 0; i < 5; i++ {
		// create idle connection
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			idleConns = append(idleConns, conn)
		}
	}
	// close idle conns so the handler goroutines can finish draining
	for _, c := range idleConns {
		_ = c.Close()
	}
	_ = listener.Close()
	time.Sleep(time.Second)
}

func TestClientCounter(t *testing.T) {
	var err error
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler())

	sleepUntil := time.Now().Add(3 * time.Second)
	subtime := func() time.Duration {
		return sleepUntil.Sub(time.Now())
	}

	for i := 0; i < 1000; i++ {
		go func() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
			}
			defer conn.Close()

			time.Sleep(subtime())
		}()
		time.Sleep(5 * time.Microsecond)
	}

	// poll until the counter drains; the 1000 goroutines close their
	// connections at roughly the same moment, so the drain is not instant
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ClientCounter == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("client counter error: %d", ClientCounter)
}
