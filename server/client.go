package server

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/runner-mei/fastmq"
)

type Client struct {
	mu         sync.Mutex
	name       string
	closed     int32
	srv        *Server
	remoteAddr string
	conn       net.Conn
}

func (self *Client) id() string {
	self.mu.Lock()
	id := self.name
	self.mu.Unlock()
	return id
}

func (self *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&self.closed, 0, 1) {
		return ErrAlreadyClosed
	}

	self.conn.Close()
	self.srv.logf("TCP: client(%s) [%s] is closed.", self.remoteAddr, self.id())
	return nil
}

func (self *Client) runWrite(c chan interface{}) {
	self.srv.logf("[write - %s] TCP: client(%s) is writing", self.remoteAddr, self.remoteAddr)

	conn := self.conn

	tick := time.NewTicker(1 * time.Minute)
	defer tick.Stop()

	var msg_ch chan mq.Message

	for 0 == atomic.LoadInt32(&self.closed) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		select {
		case v, ok := <-c:
			if !ok {
				return
			}
			switch cmd := v.(type) {
			case *errorCommand:
				if err := mq.SendFull(conn, cmd.msg.ToBytes()); err != nil {
					if 0 == atomic.LoadInt32(&self.closed) {
						self.srv.logf("[%s - %s] fail to send error message, %s", self.id(), self.remoteAddr, err)
					}
				}
				return
			case *subCommand:
				if err := mq.SendFull(conn, mq.MSG_ACK_BYTES); err != nil {
					self.srv.logf("[%s - %s] fail to send ack message, %s", self.id(), self.remoteAddr, err)
					return
				}

				msg_ch = cmd.ch
			case *pubCommand:
				msg_ch = nil

				if err := mq.SendFull(conn, mq.MSG_ACK_BYTES); err != nil {
					self.srv.logf("[%s - %s] fail to send ack message, %s", self.id(), self.remoteAddr, err)
					return
				}

			case *closeCommand:
				msg_ch = nil
				if err := mq.SendFull(conn, mq.MSG_ACK_BYTES); err != nil {
					self.srv.logf("[%s - %s] fail to send ack message, %s", self.id(), self.remoteAddr, err)
					return
				}
			default:
				self.srv.logf("[%s - %s] unknown command - %T", self.id(), self.remoteAddr, v)
				return
			}
		case data, ok := <-msg_ch:
			if !ok {
				msg := mq.BuildErrorMessage("message channel is closed.")
				if err := mq.SendFull(conn, msg.ToBytes()); err != nil {
					self.srv.logf("[%s - %s] fail to send closed message, %s", self.id(), self.remoteAddr, err)
				}
				return
			}
			if err := mq.SendFull(conn, data.ToBytes()); err != nil {
				self.srv.logf("[%s - %s] fail to send data message, %s", self.id(), self.remoteAddr, err)
				return
			}
		case <-tick.C:
			if err := mq.SendFull(conn, mq.MSG_NOOP_BYTES); err != nil {
				self.srv.logf("[%s - %s] fail to send noop message, %s", self.id(), self.remoteAddr, err)
				return
			}
		}
	}
}

func (self *Client) runRead(c chan interface{}) {
	self.srv.logf("TCP: client(%s) is reading", self.remoteAddr)

	conn := self.conn

	var ctx execCtx
	ctx.c = c
	ctx.srv = self.srv
	ctx.client = self
	defer ctx.Reset()

	reader := mq.NewMessageReader(conn, self.srv.options.MsgBufferSize)
	for 0 == atomic.LoadInt32(&self.closed) &&
		0 == atomic.LoadInt32(&self.srv.is_stopped) {
		msg, err := reader.ReadMessage()
		if nil != err {
			c <- &errorCommand{msg: mq.BuildErrorMessage(err.Error())}
			break
		}
		if nil == msg {
			continue
		}
		if !ctx.execute(msg) {
			break
		}
	}

	//fmt.Println(ctx.id)
	//fmt.Println("DataLength=", reader.DataLength())
}

type execCtx struct {
	srv      *Server
	client   *Client
	c        chan interface{}
	producer Producer
	consumer *Consumer
	//id       uint32
}

func (ctx *execCtx) execute(msg mq.Message) bool {
	defer ctx.srv.catchThrow("[" + ctx.client.name + "-" + ctx.client.remoteAddr + "] [" + mq.ToCommandName(msg.Command()) + "]")

	switch msg.Command() {
	case mq.MSG_NOOP:
		return true
	case mq.MSG_ID:
		ctx.client.mu.Lock()
		defer ctx.client.mu.Unlock()

		if msg.DataLength() == 0 {
			ctx.client.name = ""
		} else {
			ctx.client.name = string(bytes.TrimSpace(msg.Data()))
		}
		return true
	case mq.MSG_ERROR:
		ctx.srv.logf("ERROR: client(%s) recv error - %s", ctx.client.remoteAddr, string(msg.Data()))
		return false
	case mq.MSG_CLOSE:
		// fmt.Println("close ", ctx.id)
		if err := ctx.Reset(); err != nil {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("failed to reset context, " + err.Error())}
			return true
		}

		ctx.c <- &closeCommand{}
		return true
	case mq.MSG_DATA:
		//ctx.id = binary.BigEndian.Uint32(msg.Data())

		if ctx.producer == nil {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("state error.")}
			return true
		}

		if err := ctx.producer.Send(msg); err != nil {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("failed to send message, " + err.Error())}
			return true
		}
		return true
	case mq.MSG_PUB:
		ss := bytes.Fields(msg.Data())
		if 2 != len(ss) {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}

		var queue Channel
		if bytes.Equal(ss[0], []byte("queue")) {
			queue = ctx.srv.createQueueIfNotExists(string(ss[1]))
		} else if bytes.Equal(ss[0], []byte("topic")) {
			queue = ctx.srv.createTopicIfNotExists(string(ss[1]))
		} else {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}

		ctx.producer = queue
		ctx.c <- &pubCommand{}
		return true
	case mq.MSG_SUB:
		ss := bytes.Fields(msg.Data())
		if 2 != len(ss) {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}
		var queue Channel
		if bytes.Equal(ss[0], []byte("queue")) {
			queue = ctx.srv.createQueueIfNotExists(string(ss[1]))
		} else if bytes.Equal(ss[0], []byte("topic")) {
			queue = ctx.srv.createTopicIfNotExists(string(ss[1]))
		} else {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("invalid command - '" + string(msg.Data()) + "'.")}
			return true
		}
		if err := ctx.Reset(); err != nil {
			ctx.c <- &errorCommand{msg: mq.BuildErrorMessage("failed to reset context, " + err.Error())}
			return true
		}

		ctx.consumer = queue.ListenOn()
		ctx.c <- &subCommand{ch: ctx.consumer.C}
		return true
	default:
		ctx.c <- &errorCommand{msg: mq.BuildErrorMessage(fmt.Sprintf("unknown command - %v.", msg.Command()))}
		return true // don't exit, write thread will exit when recv error.
	}
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
	msg mq.Message
}

type subCommand struct {
	ch chan mq.Message
}

type pubCommand struct {
}

type closeCommand struct {
}
