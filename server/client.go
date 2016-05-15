package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
)

type Client struct {
	closed     int32
	srv        *Server
	remoteAddr string
	conn       net.Conn
}

func (self *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&self.closed, 0, 1) {
		return ErrAlreadyClosed
	}

	self.conn.Close()
	self.srv.logf("TCP: client(%s) is closed.", self.remoteAddr)
	return nil
}

func sendFull(conn net.Conn, data []byte) error {
	for len(data) == 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (self *Client) runWrite(c chan interface{}) {
	self.srv.logf("[write - %s] TCP: client(%s) is writing", self.remoteAddr, self.remoteAddr)

	conn := self.conn
	n, err := conn.Write(HEAD_MAGIC)
	if err != nil {
		self.srv.logf("[write - %s] fail to send magic bytes, %s", self.remoteAddr, err)
		return
	}
	if n != len(HEAD_MAGIC) {
		self.srv.logf("[client - %s] fail to send magic bytes", self.remoteAddr)
		return
	}

	var msg_ch chan Message

	for 0 == atomic.LoadInt32(&self.closed) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		select {
		case v, ok := <-c:
			if !ok {
				return
			}
			switch cmd := v.(type) {
			case *errorCommand:
				if err := sendFull(conn, cmd.msg.ToBytes()); err != nil {
					self.srv.logf("[client - %s] fail to send error message, %s", self.remoteAddr, err)
				}
				return
			case *subCommand:
				msg_ch = cmd.ch
			case *pubCommand:
				msg_ch = nil
			default:
				self.srv.logf("[client - %s] unknown command - %T", self.remoteAddr, v)
				return
			}
		case data, ok := <-msg_ch:
			if !ok {
				msg := BuildErrorMessage("message channel is closed.")
				if err := sendFull(conn, msg.ToBytes()); err != nil {
					self.srv.logf("[client - %s] fail to send closed message, %s", self.remoteAddr, err)
				}
				return
			} else {
				if err := sendFull(conn, data.ToBytes()); err != nil {
					self.srv.logf("[client - %s] fail to send data message, %s", self.remoteAddr, err)
					return
				}
			}
		}
	}
}

func (self *Client) runRead(c chan interface{}) {
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

	var ctx execCtx

	reader := NewMessageReader(conn, self.srv.options.MsgBufferSize)
	for 0 == atomic.LoadInt32(&self.closed) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		msg, err := reader.ReadMessage()
		if nil != err {
			c <- &errorCommand{msg: BuildErrorMessage(err.Error())}
			break
		}
		if nil == msg {
			continue
		}
		if !self.execute(&ctx, msg) {
			break
		}
	}
}

func (self *Client) execute(ctx *execCtx, msg Message) bool {
	switch msg.Command() {
	case MSG_ERROR:
		self.srv.logf("ERROR: client(%s) recv error - %s", self.remoteAddr, string(msg.Data()))
		return false
	case MSG_DATA:
		if ctx.producer == nil {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("state error.")}
			return true
		}

		if err := ctx.producer.Send(msg); err != nil {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("failed to send message, " + err.Error())}
			return true
		}
		return true
	case MSG_PUB:
		ss := bytes.Fields(msg.Data())
		if 2 != len(ss) {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}

		var queue Channel
		if bytes.Equal(ss[0], []byte("queue")) {
			queue = self.srv.createQueueIfNotExists(string(ss[1]))
		} else if bytes.Equal(ss[0], []byte("topic")) {
			queue = self.srv.createTopicIfNotExists(string(ss[1]))
		} else {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}

		ctx.producer = queue
		ctx.c <- &pubCommand{}
		return true
	case MSG_SUB:
		ss := bytes.Fields(msg.Data())
		if 2 != len(ss) {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}
		var queue Channel
		if bytes.Equal(ss[0], []byte("queue")) {
			queue = self.srv.createQueueIfNotExists(string(ss[1]))
		} else if bytes.Equal(ss[0], []byte("topic")) {
			queue = self.srv.createTopicIfNotExists(string(ss[1]))
		} else {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}
		if err := ctx.Reset(); err != nil {
			ctx.c <- &errorCommand{msg: BuildErrorMessage("failed to reset context, " + err.Error())}
			return true
		}
		ctx.consumer = queue.ListenOn()
		ctx.c <- &subCommand{ch: ctx.consumer.C}
		return true
	default:
		ctx.c <- &errorCommand{msg: BuildErrorMessage(fmt.Sprintf("unknown command - %v.", msg.Command()))}
		return true // don't exit, write thread will exit when recv error.
	}
}

type execCtx struct {
	c        chan interface{}
	producer Producer
	consumer *Consumer
}

func (self *execCtx) Reset() error {
	if nil != self.consumer {
		if err := self.consumer.Close(); err != nil {
			return err
		}
		self.consumer = nil
	}
	self.producer = nil
	return nil
}

type errorCommand struct {
	msg Message
}

type subCommand struct {
	ch chan Message
}

type pubCommand struct {
}
