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

	mq "fastmq"
)

var ErrAlreadyClosed = errors.New("server is already closed.")

type Server struct {
	options      Options
	is_stopped   int32
	waitGroup    sync.WaitGroup
	listener     net.Listener
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

	self.waitGroup.Wait()
	return err
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

func (self *Server) runItInGoroutine(cb func()) {
	self.waitGroup.Add(1)
	go func() {
		cb()
		self.waitGroup.Done()
	}()
}

func (self *Server) runLoop(listener net.Listener) {
	self.logf("TCP: listening on %s", listener.Addr())

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

	self.runItInGoroutine(func() {

		////////////////////// begin check magic bytes  //////////////////////////
		buf := make([]byte, len(mq.HEAD_MAGIC))
		_, err := io.ReadFull(clientConn, buf)
		if err != nil {
			self.logf("ERROR: client(%s) failed to read protocol version - %s",
				remoteAddr, err)
			clientConn.Close()
			return
		}
		if !bytes.Equal(buf, mq.HEAD_MAGIC) {
			self.logf("ERROR: client(%s) bad protocol magic '%s'",
				remoteAddr, string(buf))
			clientConn.Close()
			return
		}
		if err := mq.SendFull(clientConn, mq.HEAD_MAGIC); err != nil {
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
		self.runItInGoroutine(func() {
			defer self.catchThrow("[" + remoteAddr + "]")

			client.runWrite(ch)
			client.Close()
		})

		client.runRead(ch)
		close(ch)
	})
}

func (self *Server) createQueueIfNotExists(name string) *Queue {
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
	return queue
}

func (self *Server) createTopicIfNotExists(name string) *Topic {
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

	srv.runItInGoroutine(func() {
		srv.runLoop(listener)
	})

	return srv, nil
}
