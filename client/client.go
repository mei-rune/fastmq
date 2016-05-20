package client

import (
	"bytes"
	"errors"
	"io"
	"net"

	mq "fastmq"
)

type Client struct {
	capacity int
	conn     net.Conn
	reader   mq.Reader
}

func (self *Client) connect(network, address string) error {
	conn, err := net.Dial(network, address)
	if err != nil {
		return err
	}
	self.conn = conn
	if err := self.sendMagic(); err != nil {
		return err
	}
	if err := self.recvMagic(); err != nil {
		return err
	}

	self.reader.Init(conn, self.capacity)
	return nil
}

func (self *Client) Close() error {
	return self.conn.Close()
}

func (self *Client) sendClose() error {
	return mq.SendFull(self.conn, mq.MSG_CLOSE_BYTES)
}

func (self *Client) sendMagic() error {
	return mq.SendFull(self.conn, mq.HEAD_MAGIC)
}

func (self *Client) recvMagic() error {
	var buf [4]byte
	if _, err := io.ReadFull(self.conn, buf[:]); err != nil {
		return err
	}
	if !bytes.Equal(buf[:], mq.HEAD_MAGIC) {
		return mq.ErrMagicNumber
	}
	return nil
}

func (self *Client) recvAck() error {
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

			return mq.ErrUnexceptedMessage
		}
	}
	return mq.ErrMoreThanMaxRead
}

func (self *Client) Id(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_ID, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.Send(msg.Build())
}

func (self *Client) Send(msg mq.Message) error {
	return mq.SendFull(self.conn, msg.ToBytes())
}

func (self *Client) SwitchToQueue(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_PUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("queue "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.exec(msg.Build())
}

func (self *Client) SwitchToTopic(name string) error {
	msg := mq.NewMessageWriter(mq.MSG_PUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("topic "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	return self.exec(msg.Build())
}

func (self *Client) exec(msg mq.Message) error {
	if err := mq.SendFull(self.conn, msg.ToBytes()); err != nil {
		return err
	}
	return self.recvAck()
}

func (self *Client) SubscribeQueue(name string, cb func(cli SubscribeClient, msg mq.Message)) error {
	msg := mq.NewMessageWriter(mq.MSG_SUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("queue "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	if err := self.exec(msg.Build()); err != nil {
		return err
	}
	var sub subClient
	sub.cli = self
	return sub.subscribe(cb)
}

func (self *Client) SubscribeTopic(name string, cb func(cli SubscribeClient, msg mq.Message)) error {
	msg := mq.NewMessageWriter(mq.MSG_SUB, len(name)+mq.HEAD_LENGTH+8)
	msg.Append([]byte("topic "))
	msg.Append([]byte(name)).Append([]byte("\n"))
	if err := self.exec(msg.Build()); err != nil {
		return err
	}

	var sub subClient
	sub.cli = self
	return sub.subscribe(cb)
}

type SubscribeClient interface {
	io.Closer
}

type subClient struct {
	closed bool
	cli    *Client
}

func (self *subClient) Close() error {
	if self.closed {
		return nil
	}
	self.closed = true
	return self.cli.sendClose()
}

func (self *subClient) subscribe(cb func(cli SubscribeClient, msg mq.Message)) error {
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

type SubscribeOption struct {
	BufferSize int
	IsTopic    bool
	Name       string

	Network string
	Address string
}

// bufferSize int, bs []byte, conn net.Conn

func Subscribe(options *SubscribeOption, cb func(cli SubscribeClient, msg mq.Message)) error {
	if "" == options.Name {
		if options.IsTopic {
			return errors.New("topic name is empty.")
		}
		return errors.New("queue name is empty.")
	}

	if "" == options.Address {
		return errors.New("address is empty.")
	}
	if "" == options.Network {
		options.Network = "tcp"
	}

	if options.BufferSize < mq.HEAD_LENGTH {
		options.BufferSize = 512
	}

	var cli = &Client{capacity: options.BufferSize}
	if err := cli.connect(options.Network, options.Address); err != nil {
		return err
	}

	if options.IsTopic {
		return cli.SubscribeTopic(options.Name, cb)
	} else {
		return cli.SubscribeQueue(options.Name, cb)
	}
}
