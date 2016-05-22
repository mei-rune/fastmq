package client

import (
	"errors"
	"io"
	"log"
	"net"

	mq "fastmq"
)

type SubClient struct {
	conn   net.Conn
	reader mq.Reader
}

func (self *SubClient) Close() error {
	return self.conn.Close()
}

func (self *SubClient) Stop() error {
	if err := mq.SendFull(self.conn, mq.MSG_CLOSE_BYTES); err != nil {
		return err
	}
	return self.recvAck()
}

func (self *SubClient) recvAck() error {
	var recvMessage mq.Message
	var err error

	for i := 0; i < 100; i++ {
		recvMessage, err = self.reader.ReadMessage()
		if nil != err {
			return err
		}
		if nil != recvMessage {
			if mq.MSG_ACK == recvMessage.Command() {
				return nil
			}

			if mq.MSG_ERROR == recvMessage.Command() {
				return mq.ToError(recvMessage)
			}

			log.Println("recv ", mq.ToCommandName(recvMessage.Command()))
			return mq.ErrUnexceptedMessage
		}
	}
	return mq.ErrMoreThanMaxRead
}

func (self *SubClient) Id(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_ID, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.send(msg.Build())
}

func (self *SubClient) send(msg mq.Message) error {
	return mq.SendFull(self.conn, msg.ToBytes())
}

func (self *SubClient) read() (mq.Message, error) {
	return self.reader.ReadMessage()
}

func (self *SubClient) exec(msg mq.Message) error {
	if err := self.send(msg); err != nil {
		return nil
	}
	return self.recvAck()
}

func (self *SubClient) SubscribeQueue(name string, cb func(cli *Subscription, msg mq.Message)) error {
	msg := mq.NewMessageWriter(mq.MSG_SUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("queue "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	if err := self.exec(msg.Build()); err != nil {
		return err
	}

	var sub = Subscription{cli: self}
	return sub.subscribe(cb)
}

func (self *SubClient) SubscribeTopic(name string, cb func(cli *Subscription, msg mq.Message)) error {
	msg := mq.NewMessageWriter(mq.MSG_SUB, len(name)+mq.HEAD_LENGTH+8)
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
	return mq.SendFull(self.cli.conn, mq.MSG_CLOSE_BYTES)
}

func (self *Subscription) subscribe(cb func(cli *Subscription, msg mq.Message)) error {
	var recvMessage mq.Message
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
			if recvMessage.Command() == mq.MSG_ACK {
				if self.closed {
					return nil
				} else {
					return mq.ErrUnexceptedAck
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

	if bufferSize < mq.HEAD_LENGTH {
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
