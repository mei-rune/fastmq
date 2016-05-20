package server

import (
	mq "fastmq"
	"sync"
	"sync/atomic"
)

type Consumer struct {
	topic        *Topic
	id           int
	C            chan mq.Message
	DiscardCount uint32
	Count        uint32
}

func (self *Consumer) addDiscard() {
	atomic.AddUint32(&self.Count, 1)
}

func (self *Consumer) add() {
	atomic.AddUint32(&self.DiscardCount, 1)
}

func (self *Consumer) Close() error {
	if nil == self.topic {
		return nil
	}
	self.topic.remove(self.id)
	self.topic = nil
	return nil
}

type Producer interface {
	Send(msg mq.Message) error
}

type Channel interface {
	Producer

	ListenOn() *Consumer
}

type Queue struct {
	name string
	C    chan mq.Message
}

func (self *Queue) Send(msg mq.Message) error {
	self.C <- msg
	return nil
}

func (self *Queue) ListenOn() *Consumer {
	return &Consumer{C: self.C}
}

func creatQueue(srv *Server, name string, capacity int) *Queue {
	return &Queue{name: name, C: make(chan mq.Message, capacity)}
}

type Topic struct {
	name          string
	capacity      int
	last_id       int
	channels      []*Consumer
	channels_lock sync.RWMutex
}

func (self *Topic) Send(msg mq.Message) error {
	self.channels_lock.RLock()
	defer self.channels_lock.RUnlock()

	for _, consumer := range self.channels {
		select {
		case consumer.C <- msg:
			consumer.add()
		default:
			consumer.addDiscard()
		}
	}
	return nil
}

func (self *Topic) ListenOn() *Consumer {
	listener := &Consumer{topic: self, C: make(chan mq.Message, self.capacity)}

	self.channels_lock.Lock()
	self.last_id++
	listener.id = self.last_id
	self.channels = append(self.channels, listener)
	self.channels_lock.Unlock()
	return listener
}

func (self *Topic) remove(id int) (ret *Consumer) {
	self.channels_lock.Lock()
	for idx, consumer := range self.channels {
		if consumer.id == id {
			ret = consumer

			copy(self.channels[idx:], self.channels[idx+1:])
			self.channels = self.channels[:len(self.channels)-1]
			break
		}
	}
	self.channels_lock.Unlock()
	return ret
}

func creatTopic(srv *Server, name string, capacity int) *Topic {
	return &Topic{name: name, capacity: capacity}
}
