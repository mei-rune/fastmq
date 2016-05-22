package client

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	mq "github.com/runner-mei/fastmq"
)

var responsePool sync.Pool

func init() {
	responsePool.New = func() interface{} {
		return &SignelData{}
	}
}

func NewSignelData(msg mq.Message,
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
	Msg mq.Message
	Err error
}

type PubClient struct {
	is_closed int32
	waitGroup sync.WaitGroup
	Signal    chan *SignelData
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
	res, ok := <-self.Signal
	if !ok {
		return nil, mq.ErrAlreadyClosed
	}
	msg = res.Msg
	err = res.Err
	ReleaseSignelData(res)
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
	defer close(self.Signal)

	var reader mq.Reader
	reader.Init(rd, bufSize)

	for {
		msg, err := reader.ReadMessage()
		if err != nil {
			self.Signal <- NewSignelData(msg, err)
			return
		}
		if msg.Command() == mq.MSG_NOOP {
			continue
		}

		self.Signal <- NewSignelData(msg, err)
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
		Signal: make(chan *SignelData, capacity),
		C:      make(chan mq.Message, capacity),
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
