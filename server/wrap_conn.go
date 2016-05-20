package server

import (
	"bytes"
	"io"
	"net"
)

type wrapConn struct {
	net.Conn
	rd io.Reader
}

func (self *wrapConn) Read(b []byte) (int, error) {
	return self.rd.Read(b)
}

func wrap(buf []byte, conn net.Conn) net.Conn {
	return &wrapConn{Conn: conn,
		rd: io.MultiReader(bytes.NewReader(buf), conn)}
}

type Listener struct {
	network string
	addr    net.Addr
	closer  io.Closer
	c       chan net.Conn
}

// Accept waits for and returns the next connection to the listener.
func (self *Listener) Accept() (net.Conn, error) {
	conn, ok := <-self.c
	if ok {
		return conn, nil
	}
	return nil, &net.OpError{
		Op:     "accept",
		Net:    self.network,
		Source: self.addr,
		Addr:   self.addr,
		Err:    io.EOF,
	}
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (self *Listener) Close() error {
	if self.closer != nil {
		return self.closer.Close()
	}
	return nil
}

// Addr returns the listener's network address.
func (self *Listener) Addr() net.Addr {
	return self.addr
}
