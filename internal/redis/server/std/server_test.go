package std

import (
	"bufio"
	"github.com/Hoverhuang-er/godis/internal/tcp"
	"net"
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
	go tcp.ListenAndServe(listener, MakeHandler())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("PING\r\n"))
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
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	_ = listener.Close()
	time.Sleep(time.Second)
}
