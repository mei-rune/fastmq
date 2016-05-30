package client

import (
	"io"
	"net"
)

type Subscription struct {
	closed bool
	conn   net.Conn
}

func (self *Subscription) Stop() error {
	if self.closed {
		return nil
	}
	self.closed = true
	return SendFull(self.conn, MSG_CLOSE_BYTES)
}

func (self *Subscription) subscribe(bufSize int, cb func(cli *Subscription, msg Message)) error {
	var reader MessageReader
	var recvMessage Message
	var err error

	reader.Init(self.conn, bufSize)
	for {
		recvMessage, err = reader.ReadMessage()
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

// func ConnectSub(network, address string, bufferSize int) (*SubClient, error) {
// 	if "" == address {
// 		return nil, errors.New("address is empty.")
// 	}
// 	if "" == network {
// 		network = "tcp"
// 	}

// 	if bufferSize < HEAD_LENGTH {
// 		bufferSize = 512
// 	}

// 	conn, err := connect(network, address)
// 	if err != nil {
// 		return nil, err
// 	}

// 	cli := &SubClient{conn: conn}
// 	cli.reader.Init(conn, bufferSize)
// 	return cli, nil
// }
