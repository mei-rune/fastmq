package client

import (
	"errors"
	"io"
	"net"
)

type SubClient struct {
	conn   net.Conn
	reader Reader
}

func (self *SubClient) Close() error {
	return self.conn.Close()
}

func (self *SubClient) Stop() error {
	if err := SendFull(self.conn, MSG_CLOSE_BYTES); err != nil {
		return err
	}
	return self.recvAck()
}

func (self *SubClient) recvAck() error {
	var recvMessage Message
	var err error

	for i := 0; i < 100; i++ {
		recvMessage, err = self.reader.ReadMessage()
		if nil != err {
			return err
		}
		if nil != recvMessage {
			if MSG_ACK == recvMessage.Command() {
				return nil
			}

			if MSG_ERROR == recvMessage.Command() {
				return ToError(recvMessage)
			}

			//log.Println("recv ", ToCommandName(recvMessage.Command()))
			return ErrUnexceptedMessage
		}
	}
	return ErrMoreThanMaxRead
}

func (self *SubClient) Id(name string) error {
	msg := NewMessageWriter(MSG_ID, len(name)+HEAD_LENGTH+8)
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.send(msg.Build())
}

func (self *SubClient) send(msg Message) error {
	return SendFull(self.conn, msg.ToBytes())
}

func (self *SubClient) read() (Message, error) {
	return self.reader.ReadMessage()
}

func (self *SubClient) exec(msg Message) error {
	if err := self.send(msg); err != nil {
		return nil
	}
	return self.recvAck()
}

func (self *SubClient) SubscribeQueue(name string, cb func(cli *Subscription, msg Message)) error {
	msg := NewMessageWriter(MSG_SUB, len(name)+HEAD_LENGTH+8)
	msg.Append([]byte("queue "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	if err := self.exec(msg.Build()); err != nil {
		return err
	}

	var sub = Subscription{cli: self}
	return sub.subscribe(cb)
}

func (self *SubClient) SubscribeTopic(name string, cb func(cli *Subscription, msg Message)) error {
	msg := NewMessageWriter(MSG_SUB, len(name)+HEAD_LENGTH+8)
	msg.Append([]byte("topic "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	if err := self.exec(msg.Build()); err != nil {
		return err
	}

	var sub = Subscription{cli: self}
	return sub.subscribe(cb)
}

type Subscription struct {
	closed bool
	cli    *SubClient
}

func (self *Subscription) Stop() error {
	if self.closed {
		return nil
	}
	self.closed = true
	return SendFull(self.cli.conn, MSG_CLOSE_BYTES)
}

func (self *Subscription) subscribe(cb func(cli *Subscription, msg Message)) error {
	var recvMessage Message
	var err error
	for {
		recvMessage, err = self.cli.reader.ReadMessage()
		if nil != err {
			if io.EOF == err {
				return nil
			}
			return err
		}

		if nil != recvMessage {
			if recvMessage.Command() == MSG_ACK {
				if self.closed {
					return nil
				} else {
					return ErrUnexceptedAck
				}
			}

			cb(self, recvMessage)
		}
	}
}

func ConnectSub(network, address string, bufferSize int) (*SubClient, error) {
	if "" == address {
		return nil, errors.New("address is empty.")
	}
	if "" == network {
		network = "tcp"
	}

	if bufferSize < HEAD_LENGTH {
		bufferSize = 512
	}

	conn, err := connect(network, address)
	if err != nil {
		return nil, err
	}

	cli := &SubClient{conn: conn}
	cli.reader.Init(conn, bufferSize)
	return cli, nil
}
