package client

import (
	"errors"
	"net"
)

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

func (self *ClientBuilder) SetBufSize(size int) *ClientBuilder {
	self.bufSize = size
	return self
}

func (self *ClientBuilder) SetQueueCapacity(capacity int) *ClientBuilder {
	self.capacity = capacity
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

func (self *ClientBuilder) SubscribeQueue(name string, cb func(cli *Subscription, msg Message)) error {
	msg := NewMessageWriter(MSG_SUB, len(name)+HEAD_LENGTH+8).
		Append([]byte("queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).Build()
	return self.subscribe(msg, cb)
}

func (self *ClientBuilder) SubscribeTopic(name string, cb func(cli *Subscription, msg Message)) error {
	msg := NewMessageWriter(MSG_SUB, len(name)+HEAD_LENGTH+8).
		Append([]byte("topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).Build()
	return self.subscribe(msg, cb)
}

func (self *ClientBuilder) subscribe(msg Message, cb func(cli *Subscription, msg Message)) error {
	conn, err := connect(self.network, self.address)
	if err != nil {
		return err
	}

	if self.id != "" {
		sendId(conn, self.id)
	}

	var head_buffer [8]byte
	err = exec(conn, msg, head_buffer)
	if err != nil {
		return err
	}

	if self.capacity == 0 {
		self.capacity = 200
	}

	if self.bufSize == 0 {
		self.bufSize = 512
	}

	var sub = Subscription{conn: conn}
	defer conn.Close()

	return sub.subscribe(self.bufSize, cb)
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
