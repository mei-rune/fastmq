package server

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	mq_client "github.com/runner-mei/fastmq/client"
)

var ErrHandlerType = errors.New("handler isn't http.Handler.")
var ErrAlreadyClosed = errors.New("server is already closed.")

type ByPass interface {
	On(conn net.Conn)
	Close() error
}

var ConnectionHandle func(srv *Server) (ByPass, error)

type Server struct {
	options      Options
	is_stopped   int32
	waitGroup    sync.WaitGroup
	listener     net.Listener
	bypass       ByPass
	watcher      watcher
	clients_lock sync.Mutex
	clients      *list.List
	queues_lock  sync.RWMutex
	queues       map[string]*Queue

	topics_lock sync.RWMutex
	topics      map[string]*Topic
}

func (self *Server) Close() error {
	if !atomic.CompareAndSwapInt32(&self.is_stopped, 0, 1) {
		return ErrAlreadyClosed
	}
	if nil != self.bypass {
		self.bypass.Close()
	}

	err := self.listener.Close()
	func() {
		self.clients_lock.Lock()
		defer self.clients_lock.Unlock()
		for el := self.clients.Front(); el != nil; el = el.Next() {
			if conn, ok := el.Value.(io.Closer); ok {
				conn.Close()
			}
		}
	}()

	func() {
		self.queues_lock.Lock()
		defer self.queues_lock.Unlock()
		for _, v := range self.queues {
			v.Close()
		}
	}()

	func() {
		self.topics_lock.Lock()
		defer self.topics_lock.Unlock()
		for _, v := range self.topics {
			v.Close()
		}
	}()

	self.waitGroup.Wait()
	return err
}

func (self *Server) Wait() {
	self.waitGroup.Wait()
}

//
// func (self *Server) createHandler() http.Handler {
// 	return self
// }

// 	handler := self.options.Handler
// 	if nil == handler {
// 		handler = httprouter.New()
// 		//self.options.Handler = handler
// 	}
//
// 	handler.NotFound = http.DefaultServeMux
//
// 	handler.GET("/mq/queues", self.queuesIndex)
// 	handler.GET("/mq/topics", self.topicsIndex)
// 	handler.GET("/mq/clients", self.clientsIndex)
// 	return handler
// }

func (self *Server) GetOptions() *Options {
	return &self.options
}

func (self *Server) Addr() net.Addr {
	return self.listener.Addr()
}

func (self *Server) GetQueues() []string {
	self.queues_lock.RLock()
	defer self.queues_lock.RUnlock()
	var results []string
	for k, _ := range self.queues {
		results = append(results, k)
	}
	return results
}

func (self *Server) GetTopics() []string {
	self.topics_lock.RLock()
	defer self.topics_lock.RUnlock()
	var results []string
	for k, _ := range self.topics {
		results = append(results, k)
	}
	return results
}

func (self *Server) GetClients() []map[string]interface{} {
	self.clients_lock.Lock()
	defer self.clients_lock.Unlock()
	var results []map[string]interface{}

	for el := self.clients.Front(); el != nil; el = el.Next() {
		if cli, ok := el.Value.(*Client); ok {
			cli.mu.Lock()
			results = append(results, map[string]interface{}{
				"name":        cli.name,
				"remote_addr": cli.remoteAddr,
			})
			cli.mu.Unlock()
		}
	}

	return results
}

func (self *Server) log(args ...interface{}) {
	self.options.Logger.Println(args...)
}

func (self *Server) logf(format string, args ...interface{}) {
	self.options.Logger.Printf(format, args...)
}

func (self *Server) catchThrow(ctx string) {
	if e := recover(); nil != e {
		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf("[panic] %s %v", ctx, e))
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
		self.logf(buffer.String())
	}
}

func (self *Server) RunItInGoroutine(cb func()) {
	self.waitGroup.Add(1)
	go func() {
		cb()
		self.waitGroup.Done()
	}()
}

func (self *Server) runLoop(listener net.Listener) {
	self.logf("TCP: listening on %s", listener.Addr())

	defer listener.Close()

	for 0 == atomic.LoadInt32(&self.is_stopped) {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				self.logf("NOTICE: temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}

			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				self.logf("ERROR: listener.Accept() - %s", err)
			}
			break
		}

		self.handleConnection(clientConn)
	}

	self.logf("TCP: closing %s", listener.Addr())
}

func (self *Server) handleConnection(clientConn net.Conn) {
	remoteAddr := clientConn.RemoteAddr().String()

	self.RunItInGoroutine(func() {
		////////////////////// begin check magic bytes  //////////////////////////
		buf := make([]byte, len(mq_client.HEAD_MAGIC))
		_, err := io.ReadFull(clientConn, buf)
		if err != nil {
			self.logf("ERROR: client(%s) failed to read protocol version - %s",
				remoteAddr, err)
			clientConn.Close()
			return
		}
		if !bytes.Equal(buf, mq_client.HEAD_MAGIC) {
			if nil != self.bypass {
				self.bypass.On(wrap(buf, clientConn))
			} else {
				self.logf("ERROR: client(%s) bad protocol magic '%s'",
					remoteAddr, string(buf))
				clientConn.Close()
			}
			return
		}
		if err := mq_client.SendFull(clientConn, mq_client.HEAD_MAGIC); err != nil {
			self.logf("ERROR: client(%s) fail to send magic bytes, %s", remoteAddr, err)
			return
		}
		////////////////////// end check magic bytes  //////////////////////////

		client := &Client{
			srv:        self,
			remoteAddr: remoteAddr,
			conn:       clientConn,
		}

		defer self.catchThrow("[" + remoteAddr + "]")

		self.clients_lock.Lock()
		el := self.clients.PushBack(client)
		self.clients_lock.Unlock()

		defer func() {
			self.clients_lock.Lock()
			self.clients.Remove(el)
			self.clients_lock.Unlock()

			client.Close()
		}()

		ch := make(chan interface{}, 10)
		self.RunItInGoroutine(func() {
			defer self.catchThrow("[" + remoteAddr + "]")

			client.runWrite(ch)
			client.Close()
		})

		client.runRead(ch)
		close(ch)
	})
}

func (self *Server) CreateQueueIfNotExists(name string) *Queue {
	self.queues_lock.RLock()
	queue, ok := self.queues[name]
	self.queues_lock.RUnlock()

	if ok {
		return queue
	}

	self.queues_lock.Lock()
	queue, ok = self.queues[name]
	if ok {
		self.queues_lock.Unlock()
		return queue
	}

	queue = creatQueue(self, name, self.options.MsgQueueCapacity)
	self.queues[name] = queue
	self.queues_lock.Unlock()

	self.watcher.onNewQueue(name)
	return queue
}

func (self *Server) CreateTopicIfNotExists(name string) *Topic {
	self.topics_lock.RLock()
	topic, ok := self.topics[name]
	self.topics_lock.RUnlock()

	if ok {
		return topic
	}

	self.topics_lock.Lock()
	topic, ok = self.topics[name]
	if ok {
		self.topics_lock.Unlock()
		return topic
	}
	topic = creatTopic(self, name, self.options.MsgQueueCapacity)
	self.topics[name] = topic
	self.topics_lock.Unlock()

	self.watcher.onNewTopic(name)
	return topic
}

func NewServer(opts *Options) (*Server, error) {
	opts.ensureDefault()

	listener, err := net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, err
	}

	srv := &Server{
		options:  *opts,
		listener: listener,
		clients:  list.New(),
		queues:   map[string]*Queue{},
		topics:   map[string]*Topic{},
	}

	if opts.HttpEnabled {
		if nil == ConnectionHandle {
			ConnectionHandle = StandardConnection
		}

		srv.bypass, err = ConnectionHandle(srv)
		if nil != err {
			listener.Close()
			return nil, err
		}
	}

	srv.watcher.topic = DummyProducer
	srv.watcher.topic = srv.CreateTopicIfNotExists(mq_client.SYS_EVENTS)

	srv.RunItInGoroutine(func() {
		srv.runLoop(listener)
	})

	return srv, nil
}
