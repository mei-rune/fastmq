package client

import (
	"net"

	"testing"
)

func TestConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":")
	if err != nil {
		t.Error(err)
		return
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Error(err)
			return
		}

		var bs [100]byte
		for {
			_, err := conn.Read(bs[:])
			if err != nil {
				return
			}
		}
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Error(err)
		return
	}

	conn, err := connect("", "127.0.0.1:"+port)
	if err == nil {
		t.Error("err is nil")

		conn.Close()
		return
	}
	t.Log(err)
}
