package server

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
)

type Client struct {
	closed     int32
	srv        *Server
	remoteAddr string
	conn       net.Conn
	queue      *Queue
}

func (self *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&self.closed, 0, 1) {
		return ErrAlreadyClosed
	}

	self.conn.Close()
	self.srv.logf("TCP: client(%s) is closed.", self.remoteAddr)
	return nil
}

func (self *Client) runWrite() {
	self.srv.logf("TCP: client(%s) is writing", self.remoteAddr)

	conn := self.conn

	buf := make([]byte, len(HEAD_MAGIC))
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		self.srv.logf("ERROR: failed to read protocol version - %s", err)
		return
	}

	if !bytes.Equal(buf, HEAD_MAGIC) {
		self.srv.logf("ERROR: client(%s) bad protocol magic '%s'",
			self.remoteAddr, string(buf))
		return
	}

	reader := NewMessageReader(conn, self.options.MsgBufferSize)
	for 0 == atomic.LoadInt32(&self.Close()) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		msg, err := reader.ReadMessage()
		if nil != err {
			bs := BuildErrorMessage(err.Error()).ToBytes()
			conn.Write(bs)
			break
		}
		if nil == msg {
			continue
		}
		if !self.execute(msg) {
			break
		}
	}
}

func (self *Client) runRead() {
	self.srv.logf("TCP: client(%s) is reading", self.remoteAddr)

	conn := self.conn

	buf := make([]byte, len(HEAD_MAGIC))
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		self.srv.logf("ERROR: failed to read protocol version - %s", err)
		return
	}

	if !bytes.Equal(buf, HEAD_MAGIC) {
		self.srv.logf("ERROR: client(%s) bad protocol magic '%s'",
			self.remoteAddr, string(buf))
		return
	}

	reader := NewMessageReader(conn, self.options.MsgBufferSize)
	for 0 == atomic.LoadInt32(&self.Close()) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		msg, err := reader.ReadMessage()
		if nil != err {
			bs := BuildErrorMessage(err.Error()).ToBytes()
			conn.Write(bs)
			break
		}
		if nil == msg {
			continue
		}
		if !self.execute(msg) {
			break
		}
	}
}

func (self *Client) execute(msg Message) bool {
	switch msg.Command() {
	case MSG_ERROR:
		self.srv.logf("ERROR: client(%s) recv error - %s", self.remoteAddr, string(msg.Data()))
		return false
	case MSG_DATA:
		self.queue.push(msg)
		return true
	case MSG_PUB:

	case MSG_SUB:
	}
}
