package client

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	mq "fastmq"
)

var responsePool sync.Pool

func init() {
	responsePool.New = func() interface{} {
		return &readResponse{}
	}
}

func newReadResponse(msg mq.Message,
	err error) *readResponse {
	res := responsePool.Get().(*readResponse)
	res.msg = msg
	res.err = err
	return res
}

func releaseReadResponse(res *readResponse) {
	res.msg = nil
	res.err = nil
	responsePool.Put(res)
}

type readResponse struct {
	msg mq.Message
	err error
}

type PubClient struct {
	is_closed int32
	waitGroup sync.WaitGroup
	read      chan *readResponse
	C         chan mq.Message
}

func (self *PubClient) Close() error {
	if !atomic.CompareAndSwapInt32(&self.is_closed, 0, 1) {
		return mq.ErrAlreadyClosed
	}

	close(self.C)
	self.waitGroup.Wait()
	return nil
}

func (self *PubClient) Id(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_ID, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte(name)).Append([]byte("\n"))
	self.C <- msg.Build()
	return nil
}

func (self *PubClient) Stop() error {
	return self.exec(mq.Message(mq.MSG_CLOSE_BYTES))
}

func (self *PubClient) Send(msg mq.Message) {
	self.C <- msg
}

func (self *PubClient) ToQueue(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_PUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("queue "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.exec(msg.Build())
}

func (self *PubClient) ToTopic(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_PUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("topic "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.exec(msg.Build())
}

func (self *PubClient) exec(msg mq.Message) error {
	self.C <- msg
	recvMsg, err := self.Read()
	if err != nil {
		return err
	}

	if mq.MSG_ACK == recvMsg.Command() {
		return nil
	}

	if mq.MSG_ERROR == recvMsg.Command() {
		return mq.ToError(recvMsg)
	}

	return errors.New("recv a unexcepted message, exepted is a ack message, actual is " + mq.ToCommandName(recvMsg.Command()))
}

func (self *PubClient) Read() (msg mq.Message, err error) {
	res, ok := <-self.read
	if !ok {
		return nil, mq.ErrAlreadyClosed
	}
	msg = res.msg
	err = res.err
	releaseReadResponse(res)
	return msg, err
}

func (self *PubClient) runItInGoroutine(cb func()) {
	self.waitGroup.Add(1)
	go func() {
		cb()
		self.waitGroup.Done()
	}()
}

func (self *PubClient) runRead(rd io.Reader, bufSize int) {
	defer close(self.read)

	var reader mq.Reader
	reader.Init(rd, bufSize)

	for {
		msg, err := reader.ReadMessage()
		if err != nil {
			self.read <- newReadResponse(msg, err)
			return
		}
		if msg.Command() == mq.MSG_NOOP {
			continue
		}

		self.read <- newReadResponse(msg, err)
	}
}

func (self *PubClient) runWrite(writer io.Writer) {
	for msg := range self.C {
		err := mq.SendFull(writer, msg.ToBytes())
		if err != nil {
			log.Println("failed to send message,", err)
			return
		}
	}
}

func ConnectPub(network, address string, capacity, bufSize int) (*PubClient, error) {
	if "" == address {
		return nil, errors.New("address is empty.")
	}
	if "" == network {
		network = "tcp"
	}

	conn, err := connect(network, address)
	if err != nil {
		return nil, err
	}

	v2 := &PubClient{
		read: make(chan *readResponse, capacity),
		C:    make(chan mq.Message, capacity),
	}

	v2.runItInGoroutine(func() {
		v2.runRead(conn, bufSize)
		conn.Close()
	})

	v2.runItInGoroutine(func() {
		v2.runWrite(conn)
		conn.Close()
	})

	return v2, nil
}

func sendMagic(conn net.Conn) error {
	return mq.SendFull(conn, mq.HEAD_MAGIC)
}

func recvMagic(conn net.Conn) error {
	var buf [4]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return err
	}
	if !bytes.Equal(buf[:], mq.HEAD_MAGIC) {
		return mq.ErrMagicNumber
	}
	return nil
}

func connect(network, address string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if err := sendMagic(conn); err != nil {
		return nil, err
	}
	return conn, recvMagic(conn)
}
