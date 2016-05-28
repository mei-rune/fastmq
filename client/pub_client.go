package client

import (
	"errors"
	"io"
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
	Signal    chan *SignelData
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

func (self *PubClient) Read() (msg Message, err error) {
	res, ok := <-self.Signal
	if !ok {
		return nil, ErrAlreadyClosed
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

	var reader Reader
	reader.Init(rd, bufSize)

	for {
		msg, err := reader.ReadMessage()
		if err != nil {
			self.Signal <- NewSignelData(msg, err)
			return
		}
		if msg.Command() == MSG_NOOP {
			continue
		}

		self.Signal <- NewSignelData(msg, err)
	}
}

func (self *PubClient) runWrite(writer io.Writer) {
	for msg := range self.C {
		err := SendFull(writer, msg.ToBytes())
		if err != nil {
			log.Println("failed to send message,", err)
			err_msg := NewMessageWriter(MSG_ERROR, len(err.Error())).Append([]byte(err.Error())).Build()
			select {
			case self.Signal <- NewSignelData(err_msg, err):
			default:
			}
			return
		}
	}
}

type ClientBuilder struct {
	network, address string
	capacity         int
	bufSize          int
	id               string
	signal           chan *SignelData
	c                chan Message
}

func (self *ClientBuilder) Id(name string) *ClientBuilder {
	self.id = name
	return self
}

func (self *ClientBuilder) ToQueue(name string) (*PubClient, error) {
	msg := NewMessageWriter(MSG_PUB, len(name)+HEAD_LENGTH+8).
		Append([]byte("queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).Build()
	return self.to(msg)
}

func (self *ClientBuilder) ToTopic(name string) (*PubClient, error) {
	msg := NewMessageWriter(MSG_PUB, len(name)+HEAD_LENGTH+8).
		Append([]byte("topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).Build()
	return self.to(msg)
}

func (self *ClientBuilder) to(msg Message) (*PubClient, error) {
	conn, err := connect(self.network, self.address)
	if err != nil {
		return nil, err
	}

	if self.id != "" {
		sendId(conn, self.id)
	}

	var head_buffer [8]byte
	err = exec(conn, msg, head_buffer)
	if err != nil {
		return nil, err
	}

	if self.capacity == 0 {
		self.capacity = 200
	}

	if self.bufSize == 0 {
		self.bufSize = 512
	}

	if self.signal == nil {
		self.signal = make(chan *SignelData, self.capacity)
	}

	if self.c == nil {
		self.c = make(chan Message, self.capacity)
	}

	v2 := &PubClient{
		Signal: self.signal, //make(chan *SignelData, capacity),
		C:      self.c,      //make(chan Message, capacity),
	}

	v2.runItInGoroutine(func() {
		v2.runRead(conn, self.bufSize)
		conn.Close()
	})

	v2.runItInGoroutine(func() {
		v2.runWrite(conn)
		conn.Close()
	})

	return v2, nil
}

func connect(network, address string) (net.Conn, error) {
	if "" == network {
		network = "tcp"
	}
	if "" == address {
		return nil, errors.New("address is missing.")
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if err := SendMagic(conn); err != nil {
		return nil, err
	}
	return conn, ReadMagic(conn)
}

func sendId(conn net.Conn, name string) error {
	msg := NewMessageWriter(MSG_ID, len(name)+HEAD_LENGTH+8).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	return SendFull(conn, msg.ToBytes())
}

func exec(conn net.Conn, msg Message, head_buf [8]byte) error {
	err := SendFull(conn, msg.ToBytes())
	if err != nil {
		return err
	}
	recvMsg, err := ReadMessage(conn, head_buf)
	if err != nil {
		return err
	}

	if MSG_ACK == recvMsg.Command() {
		return nil
	}

	if MSG_ERROR == recvMsg.Command() {
		return ToError(recvMsg)
	}

	return errors.New("recv a unexcepted message, exepted is a ack message, actual is " +
		ToCommandName(recvMsg.Command()))
}

func Connect(network, address string) *ClientBuilder {
	return &ClientBuilder{network: network, address: address}
}
