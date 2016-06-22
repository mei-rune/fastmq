package server

import (
	mq_client "github.com/runner-mei/fastmq/client"
)

type Watcher interface {
	OnNewQueue(name string)
	OnRemoveQueue(name string)
	OnNewTopic(name string)
	OnRemoveTopic(name string)
}

type watcher struct {
	topic Producer
	watch Watcher
}

func (w *watcher) onNewQueue(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("new queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
	w.watch.OnNewQueue(name)
}

func (w *watcher) onRemoveQueue(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("del queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
	w.watch.OnRemoveQueue(name)
}

func (w *watcher) onNewTopic(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("new topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
	w.watch.OnNewTopic(name)
}

func (w *watcher) onRemoveTopic(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("del topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
	w.watch.OnRemoveTopic(name)
}

type dummyWatcher struct{}

func (self *dummyWatcher) OnNewQueue(name string) {
}

func (self *dummyWatcher) OnRemoveQueue(name string) {
}

func (self *dummyWatcher) OnNewTopic(name string) {
}

func (self *dummyWatcher) OnRemoveTopic(name string) {
}

var DummyWatcher Watcher = &dummyWatcher{}
