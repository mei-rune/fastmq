package client

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Handler struct {
	read_connect_last_at  int64
	write_connect_last_at int64

	read_connect_total  uint32
	write_connect_total uint32
	read_connect_ok     uint32
	write_connect_ok    uint32

	closed         int32
	wait           sync.WaitGroup
	c              chan Message
	processMessage func(msg Message, c chan Message)

	builder    *ClientBuilder
	typ        string
	recv_qname string
	send_qname string
	last_error error
}

func (self *Handler) Stats() interface{} {
	return map[string]interface{}{
		"read_connect_last_at":  time.Unix(0, atomic.LoadInt64(&self.read_connect_last_at)),
		"read_connect_total":    atomic.LoadUint32(&self.read_connect_total),
		"read_connect_ok":       atomic.LoadUint32(&self.read_connect_ok),
		"write_connect_last_at": time.Unix(0, atomic.LoadInt64(&self.write_connect_last_at)),
		"write_connect_total":   atomic.LoadUint32(&self.write_connect_total),
		"write_connect_ok":      atomic.LoadUint32(&self.write_connect_ok),
		"last_error":            self.last_error,
	}
}

func (self *Handler) Close() {
	if !atomic.CompareAndSwapInt32(&self.closed, 0, 1) {
		return
	}

	close(self.c)
	self.wait.Wait()
}

func (self *Handler) catchThrow(err *error) {
	if o := recover(); nil != o {
		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf("[panic] %v", o))
		for i := 1; ; i += 1 {
			pc, file, line, ok := runtime.Caller(i)
			if !ok {
				break
			}
			funcinfo := runtime.FuncForPC(pc)
			if nil != funcinfo {
				buffer.WriteString(fmt.Sprintf("    %s:%d %s\r\n", file, line, funcinfo.Name()))
			} else {
				buffer.WriteString(fmt.Sprintf("    %s:%d\r\n", file, line))
			}
		}

		errMsg := buffer.String()
		log.Println(errMsg)
		*err = errors.New(errMsg)
	}
}

func (self *Handler) RunItInGoroutine(cb func()) {
	self.wait.Add(1)
	go func() {
		cb()
		self.wait.Done()
	}()
}

func (self *Handler) runLoop(builder *ClientBuilder, id string,
	cb func(builder *ClientBuilder) error) {

	builder.Id(id)

	conn_err_count := 0
	for {
		if err := cb(builder); err != nil {
			conn_err_count++
			self.last_error = err

			if conn_err_count < 5 || 0 == conn_err_count%50 {
				log.Println("failed to connect mq server,", err)
			}
			if conn_err_count > 5 {
				time.Sleep(2 * time.Second)
			}
		} else {
			conn_err_count = 0
		}

		if 0 != atomic.LoadInt32(&self.closed) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (self *Handler) runWrite(builder *ClientBuilder) (err error) {
	defer self.catchThrow(&err)

	log.Println("[mq] [" + self.send_qname + "] connect to mq server......")
	atomic.StoreInt64(&self.write_connect_last_at, time.Now().UnixNano())
	atomic.AddUint32(&self.write_connect_total, 1)

	w, e := builder.To(self.typ, self.send_qname)
	if e != nil {
		return e
	}
	defer w.Close()

	atomic.AddUint32(&self.write_connect_ok, 1)

	for msg := range self.c {
		if err = w.Send(msg); err != nil {
			log.Println("[mq] ["+self.send_qname+"] send message fialed,", err)
			return nil
		}
	}

	log.Println("[mq] [" + self.send_qname + "] mq server is closed")
	return nil
}

func (self *Handler) runRead(builder *ClientBuilder) (err error) {
	defer self.catchThrow(&err)

	log.Println("[mq] [" + self.recv_qname + "] subscribe to mq server......")
	atomic.StoreInt64(&self.read_connect_last_at, time.Now().UnixNano())
	atomic.AddUint32(&self.read_connect_total, 1)

	err = builder.Subscribe(self.typ, self.recv_qname,
		func(subscription *Subscription, msg Message) {
			if MSG_DATA != msg.Command() {
				log.Println("[mq] ["+self.recv_qname+"] recv unexcepted message - ", ToCommandName(msg.Command()))
				return
			}

			self.processMessage(msg, self.c)
		})

	if IsConnected(err) {
		atomic.AddUint32(&self.read_connect_ok, 1)
		log.Println("[mq] ["+self.recv_qname+"] mq is disconnected, ", err)
		return nil
	}
	return err
}

func NewQueueHandler(builder *ClientBuilder, id, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	return NewHandler(builder, id, QUEUE, rqueue, squeue, cb)
}

func NewTopicHandler(builder *ClientBuilder, id, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	return NewHandler(builder, id, TOPIC, rqueue, squeue, cb)
}

func NewHandler(builder *ClientBuilder, id, typ, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	handler := &Handler{
		builder:        builder,
		typ:            typ,
		recv_qname:     rqueue,
		send_qname:     squeue,
		c:              make(chan Message, 1000),
		processMessage: cb}

	handler.RunItInGoroutine(func() {
		handler.runLoop(builder, id+".listener", handler.runRead)
	})

	handler.RunItInGoroutine(func() {
		handler.runLoop(builder, id+".sender", handler.runWrite)
	})

	return handler
}
