package server

import (
	mq_client "github.com/runner-mei/fastmq/client"
)

type watcher struct {
	topic Producer
}

func (w *watcher) onNewQueue(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("new queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
}

func (w *watcher) onRemoveQueue(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("del queue ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
}

func (w *watcher) onNewTopic(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("new topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
}

func (w *watcher) onRemoveTopic(name string) {
	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(name)+1).
		Append([]byte("del topic ")).
		Append([]byte(name)).
		Append([]byte("\n")).
		Build()
	w.topic.Send(msg)
}
