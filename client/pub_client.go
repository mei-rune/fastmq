package client

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
)

var responsePool sync.Pool

func init() {
	responsePool.New = func() interface{} {
		return &SignelData{}
	}
}

type SimplePubClient struct {
	is_closed int32
	conn      net.Conn
}

func (self *SimplePubClient) Close() error {
	if !atomic.CompareAndSwapInt32(&self.is_closed, 0, 1) {
		return ErrAlreadyClosed
	}

	return self.conn.Close()
}

func (self *SimplePubClient) Stop() error {
	return SendFull(self.conn, MSG_CLOSE_BYTES)
}

func (self *SimplePubClient) Read() (Message, error) {
	return ReadMessage(self.conn)
}

func (self *SimplePubClient) Send(msg Message) error {
	return SendFull(self.conn, msg.ToBytes())
}

func NewSignelData(msg Message,
	err error) *SignelData {
	res := responsePool.Get().(*SignelData)
	res.Msg = msg
	res.Err = err
	return res
}

func ReleaseSignelData(res *SignelData) {
	res.Msg = nil
	res.Err = nil
	responsePool.Put(res)
}

type SignelData struct {
	Msg Message
	Err error
}

type PubClient struct {
	is_closed int32
	waitGroup sync.WaitGroup
	C         chan Message
}

func (self *PubClient) Close() error {
	if !atomic.CompareAndSwapInt32(&self.is_closed, 0, 1) {
		return ErrAlreadyClosed
	}

	close(self.C)
	self.waitGroup.Wait()
	return nil
}

func (self *PubClient) Stop() error {
	self.Send(Message(MSG_CLOSE_BYTES))
	return nil
}

func (self *PubClient) Send(msg Message) {
	self.C <- msg
}

func (self *PubClient) runItInGoroutine(cb func()) {
	self.waitGroup.Add(1)
	go func() {
		cb()
		self.waitGroup.Done()
	}()
}

func (self *PubClient) runLoop(builder *ClientBuilder, create func(builder *ClientBuilder) (net.Conn, error)) {
	err_count := 0
	for 0 == atomic.LoadInt32(&self.is_closed) {
		cli, err := create(builder)
		if err != nil {
			if (err_count % 100) < 5 {
				log.Println("["+builder.id+"] connect failed,", err)
			}
			err_count++
		} else {
			err_count = 0
			err = self.runOnce(builder, cli)
			if err != nil {
				log.Println("["+builder.id+"] run failed,", err)
			}
		}
	}
}

func (self *PubClient) runOnce(builder *ClientBuilder, conn net.Conn) (err error) {
	signal := make(chan *SignelData, 2)

	var wait sync.WaitGroup
	self.runItInGoroutine(func() {
		wait.Add(1)
		defer wait.Done()
		self.runRead(conn, signal)
	})
	defer wait.Wait()
	defer conn.Close()

	return self.runWrite(conn, signal)
}

func (self *PubClient) runRead(conn net.Conn, signal chan *SignelData) {
	defer close(signal)

	for 0 == atomic.LoadInt32(&self.is_closed) {
		msg, err := ReadMessage(conn)
		if err != nil {
			signal <- NewSignelData(msg, err)
			break
		}
		if msg.Command() == MSG_NOOP {
			continue
		}
		signal <- NewSignelData(msg, err)
	}
}

func (self *PubClient) runWrite(conn net.Conn, signal chan *SignelData) (err error) {
	defer conn.Close()

	for 0 == atomic.LoadInt32(&self.is_closed) {
		select {
		case msg, ok := <-self.C:
			if !ok {
				err = ErrAlreadyClosed
				goto exited
			}
			if err = SendFull(conn, msg.ToBytes()); err != nil {
				goto exited
			}
		case sig, ok := <-signal:
			if !ok {
				return nil
			}
			msg, e := sig.Msg, sig.Err
			ReleaseSignelData(sig)
			if e != nil {
				err = e
				goto exited
			}

			if msg.Command() == MSG_ERROR {
				err = ToError(msg)
				goto exited
			}
			log.Println("recv a unexcepted message -", ToCommandName(msg.Command()))
		}
	}

exited:
	conn.Close()
	for res := range signal {
		ReleaseSignelData(res)
	}
	return
}
