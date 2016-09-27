package client

import (
	"io"
	"net"
)

type ErrDisconnect struct {
	err error
}

func (e *ErrDisconnect) Error() string {
	return e.err.Error()
}

func IsConnected(e error) bool {
	_, ok := e.(*ErrDisconnect)
	return ok
}

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
	var reader FixedMessageReader
	var recvMessage Message
	var err error

	reader.Init(self.conn)
	for {
		recvMessage, err = reader.ReadMessage()
		if nil != err {
			if io.EOF == err {
				return nil
			}
			return &ErrDisconnect{err}
		}

		if nil != recvMessage {
			if recvMessage.Command() == MSG_ACK {
				if self.closed {
					return nil
				}
				return &ErrDisconnect{ErrUnexceptedAck}
			}

			cb(self, recvMessage)
		}
	}
}
