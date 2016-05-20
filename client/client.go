package client

import (
	"bytes"
	"errors"
	"io"
	"net"

	mq "fastmq"
)

var ErrReadTooMuch = errors.New("read count is too much")

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
	if err := self.sendMagic(); err != nil {
		return err
	}
	if err := self.recvMagic(); err != nil {
		return err
	}

	self.conn = conn
	self.reader.Init(conn, self.capacity)
	return nil
}

func (self *Client) Close() error {
	return self.conn.Close()
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

			return errors.New("message isn't ack.")
		}
	}
	return ErrReadTooMuch
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

func (self *Client) subscribe(cb func(cli *Client, msg mq.Message)) error {
	var recvMessage mq.Message
	var err error
	for {
		recvMessage, err = self.reader.ReadMessage()
		if nil != err {
			if io.EOF == err {
				return nil
			}
			return err
		}
		if nil != recvMessage {
			cb(self, recvMessage)
		}
	}
}

type SubscribeClient interface {
	io.Closer
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

	var cli = Client{capacity: options.BufferSize}
	if err := cli.connect(options.Network, options.Address); err != nil {
		return err
	}

	subMessage := mq.NewMessageWriter(mq.MSG_SUB, len(options.Name)+mq.HEAD_LENGTH+6)
	if options.IsTopic {
		subMessage.Append([]byte("topic "))
	} else {
		subMessage.Append([]byte("queue "))
	}
	subMessage.Append([]byte(options.Name)).Append([]byte("\n"))

	if err := cli.exec(subMessage.Build()); err != nil {
		return err
	}

	return cli.subscribe(func(cli *Client, msg mq.Message) {
		cb(cli, msg)
	})
}
