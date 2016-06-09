package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Base struct {
	closed int32
	wait   sync.WaitGroup
}

func (self *Base) CloseWith(closeHandle func() error) error {
	if !atomic.CompareAndSwapInt32(&self.closed, 0, 1) {
		return nil
	}
	var err error
	if nil != closeHandle {
		err = closeHandle()
	}
	self.wait.Wait()
	return err
}

func (self *Base) IsClosed() bool {
	return 0 != atomic.LoadInt32(&self.closed)
}

func (self *Base) CatchThrow(err *error) {
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

func (self *Base) RunItInGoroutine(cb func()) {
	self.wait.Add(1)
	go func() {
		cb()
		self.wait.Done()
	}()
}

type Handler struct {
	read_connect_last_at  int64
	write_connect_last_at int64

	read_connect_total  uint32
	write_connect_total uint32
	read_connect_ok     uint32
	write_connect_ok    uint32

	*Base
	c              chan Message
	processMessage func(msg Message, c chan Message)

	Typ        string
	RecvQname  string
	SendQname  string
	last_error error
}

func (self *Handler) Stats() map[string]interface{} {
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

func (self *Handler) Shutdown() error {
	close(self.c)
	return nil
}

func (self *Handler) Close() error {
	return self.CloseWith(self.Shutdown)
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
	defer self.CatchThrow(&err)

	log.Println("[mq] [" + self.SendQname + "] connect to mq server......")
	atomic.StoreInt64(&self.write_connect_last_at, time.Now().UnixNano())
	atomic.AddUint32(&self.write_connect_total, 1)

	w, e := builder.To(self.Typ, self.SendQname)
	if e != nil {
		return e
	}
	defer w.Close()

	atomic.AddUint32(&self.write_connect_ok, 1)

	for msg := range self.c {
		if err = w.Send(msg); err != nil {
			log.Println("[mq] ["+self.SendQname+"] send message fialed,", err)
			return nil
		}
	}

	log.Println("[mq] [" + self.SendQname + "] mq server is closed")
	return nil
}

func (self *Handler) runRead(builder *ClientBuilder) (err error) {
	defer self.CatchThrow(&err)

	log.Println("[mq] [" + self.RecvQname + "] subscribe to mq server......")
	atomic.StoreInt64(&self.read_connect_last_at, time.Now().UnixNano())
	atomic.AddUint32(&self.read_connect_total, 1)

	err = builder.Subscribe(self.Typ, self.RecvQname,
		func(subscription *Subscription, msg Message) {
			if MSG_DATA != msg.Command() {
				log.Println("[mq] ["+self.RecvQname+"] recv unexcepted message - ", ToCommandName(msg.Command()))
				return
			}

			self.processMessage(msg, self.c)
		})

	if IsConnected(err) {
		atomic.AddUint32(&self.read_connect_ok, 1)
		log.Println("[mq] ["+self.RecvQname+"] mq is disconnected, ", err)
		return nil
	}
	return err
}

func NewQueueHandler(builder *ClientBuilder, id, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	return NewHandler(&Base{}, builder, id, QUEUE, rqueue, squeue, cb)
}

func NewTopicHandler(builder *ClientBuilder, id, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	return NewHandler(&Base{}, builder, id, TOPIC, rqueue, squeue, cb)
}

func NewHandler(base *Base, builder *ClientBuilder, id, typ, rqueue, squeue string,
	cb func(msg Message, c chan Message)) *Handler {
	handler := &Handler{
		Base:           base,
		Typ:            typ,
		RecvQname:      rqueue,
		SendQname:      squeue,
		c:              make(chan Message, 1000),
		processMessage: cb}

	handler.RunItInGoroutine(func() {
		handler.runLoop(builder.Clone(), id+".listener", handler.runRead)
	})

	handler.RunItInGoroutine(func() {
		handler.runLoop(builder.Clone(), id+".sender", handler.runWrite)
	})

	return handler
}

type HandlerObject interface {
	io.Closer
}

type QueueMgr struct {
	Base
	Url           string
	Qtype         string
	Qname         string
	qmatchType    string
	qmatchName    string
	shutdown      chan struct{}
	handlers_lock sync.Mutex
	handlers      map[string]HandlerObject
	create        func(mgr *QueueMgr, name string)
}

func (self *QueueMgr) Close() error {
	return self.CloseWith(self.CloseDirect)
}

func (self *QueueMgr) CloseDirect() error {
	close(self.shutdown)

	self.handlers_lock.Lock()
	defer self.handlers_lock.Unlock()

	var err error
	for _, q := range self.handlers {
		if e := q.Close(); e != nil {
			err = e
		}
	}
	self.handlers = map[string]HandlerObject{}
	return err
}

func (self *QueueMgr) RunLoop(builder *ClientBuilder, id string,
	cb func(builder *ClientBuilder) error) {
	conn_err_count := 0
	for {
		if err := cb(builder); err != nil {
			conn_err_count++

			if conn_err_count < 5 || 0 == conn_err_count%50 {
				log.Println("failed to connect mq server,", err)
			} else {
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

func (self *QueueMgr) RunPoll() {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	count := uint(0)
	for 0 == atomic.LoadInt32(&self.closed) {
		select {
		case <-tick.C:
			count++
			if count < 60 || count%30 == 0 {
				self.PollList()
			}
		case <-self.shutdown:
			return
		}
	}
}

func (self *QueueMgr) PollList() {
	var url string
	if self.qmatchType == QUEUE {
		if strings.HasSuffix(self.Url, "/") {
			url = url + "mq/queues"
		} else {
			url = url + "/mq/queues"
		}
	} else {
		if strings.HasSuffix(self.Url, "/") {
			url = url + "mq/topics"
		} else {
			url = url + "/mq/topics"
		}
	}
	res, err := http.Get(url)
	if nil != err {
		log.Println("[mq] list queues failed,", err)
		return
	}
	defer res.Body.Close()

	bs, err := ioutil.ReadAll(res.Body)
	if nil != err {
		log.Println("[mq] list queues failed,", err)
		return
	}

	var queues []string
	err = json.Unmarshal(bs, &queues)
	if nil != err {
		log.Println("[mq] list queues failed,", err, "\r\n\t", string(bs))
		return
	}

	for _, queue := range queues {
		self.create(self, queue)
	}
}

func (self *QueueMgr) RunRead(builder *ClientBuilder) (err error) {
	log.Println("[mq] subscribe to mq server at ", self.Qtype, self.Qname, "......")

	qmatchType := []byte(self.qmatchType)
	qmatchName := []byte(self.qmatchName)
	err = builder.Subscribe(self.Qtype, self.Qname,
		func(subscription *Subscription, msg Message) {
			if MSG_DATA != msg.Command() {
				log.Println("[mq] recv unexcepted message - ", ToCommandName(msg.Command()))
				return
			}
			data := msg.Data()
			if len(data) <= 0 {
				return
			}

			fields := bytes.Fields(data)
			if len(fields) != 3 {
				return
			}

			if bytes.Equal(fields[0], []byte("new")) &&
				bytes.Equal(fields[1], qmatchType) &&
				bytes.HasPrefix(fields[2], qmatchName) {
				self.create(self, string(fields[2]))
			}
		})

	if IsConnected(err) {
		log.Println("[mq] mq is disconnected, ", err)
		return nil
	}
	return err
}

func (self *QueueMgr) CreateHandlerIfNotExists(name string, cb func(name string) (HandlerObject, error)) {
	self.handlers_lock.Lock()
	defer self.handlers_lock.Unlock()

	if nil != self.handlers {
		if _, ok := self.handlers[name]; ok {
			return
		}
	} else {
		self.handlers = map[string]HandlerObject{}
	}

	obj, err := cb(name)
	if err != nil {
		log.Println(err)
	} else {
		self.handlers[name] = obj
	}
}

func NewQueueMgr(url, typ, qname, matchType, matchName string, cb func(mgr *QueueMgr, name string)) *QueueMgr {
	if typ == "" {
		typ = TOPIC
	}
	if qname == "" {
		qname = SYS_EVENTS
	}

	return &QueueMgr{
		Url:        url,
		Qtype:      typ,
		Qname:      qname,
		qmatchType: matchType,
		qmatchName: matchName,
		shutdown:   make(chan struct{}),
		create:     cb}
}
