package client

import (
	"io"
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

func (self *PubClient) DrainRead() error {
	for {
		select {
		case res, ok := <-self.Signal:
			if !ok {
				return ErrAlreadyClosed
			}
			ReleaseSignelData(res)
		default:
			return nil
		}
	}
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

	var reader MessageReader
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
			select {
			case self.Signal <- NewSignelData(nil, err):
			default:
			}
			return
		}
	}
}
