package server

import (
	"container/list"
	mq "fastmq"
	"sync"
)

type Consumer struct {
	topic *Topic
	el    *list.Element
	C     chan mq.Message
}

func (self *Consumer) Close() error {
	if nil == self.topic {
		return nil
	}
	self.topic.channels_lock.Lock()
	self.topic.channels.Remove(self.el)
	self.topic.channels_lock.Unlock()
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
	channels      *list.List
	channels_lock sync.RWMutex
}

func (self *Topic) Send(msg mq.Message) error {
	self.channels_lock.RLock()
	defer self.channels_lock.RUnlock()

	for el := self.channels.Front(); el != nil; el = el.Next() {
		if conn, ok := el.Value.(*Consumer); ok {
			conn.C <- msg
		}
	}
	return nil
}

func (self *Topic) ListenOn() *Consumer {
	listener := &Consumer{topic: self, C: make(chan mq.Message, self.capacity)}

	self.channels_lock.Lock()
	listener.el = self.channels.PushBack(listener)
	self.channels_lock.Unlock()
	return listener
}

func creatTopic(srv *Server, name string, capacity int) *Topic {
	return &Topic{name: name, capacity: capacity, channels: list.New()}
}
