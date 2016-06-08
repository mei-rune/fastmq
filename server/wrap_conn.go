package server

import (
	"io"
	"net"
)

type wrapConn struct {
	net.Conn
	buf []byte
}

func (self *wrapConn) Read(b []byte) (int, error) {
	if len(self.buf) == 0 {
		return self.Conn.Read(b)
	}

	if len(self.buf) <= len(b) {
		n := copy(b, self.buf)
		self.buf = nil
		return n, nil
	} else {
		n := copy(b, self.buf[:len(b)])
		self.buf = self.buf[len(b):]
		return n, nil
	}
}

func wrap(buf []byte, conn net.Conn) net.Conn {
	return &wrapConn{Conn: conn,
		buf: buf}
}

type Listener struct {
	addr   net.Addr
	closer io.Closer
	c      chan net.Conn
}

// Accept waits for and returns the next connection to the listener.
func (self *Listener) Accept() (net.Conn, error) {
	conn, ok := <-self.c
	if ok {
		return conn, nil
	}
	return nil, &net.OpError{
		Op:  "accept",
		Net: self.addr.Network(),
		//Source: self.addr,
		Addr: self.addr,
		Err:  io.EOF,
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
