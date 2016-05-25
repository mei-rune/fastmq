package server

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/runner-mei/fastmq"
)

var ErrAlreadyClosed = errors.New("server is already closed.")

type Server struct {
	options      Options
	is_stopped   int32
	waitGroup    sync.WaitGroup
	listener     net.Listener
	bypass       *Listener
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
		close(self.bypass.c)
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

func (self *Server) createListener() net.Listener {
	if self.bypass == nil {
		self.bypass = &Listener{
			network: self.listener.Addr().Network(),
			addr:    self.listener.Addr(),
			closer:  nil,
			c:       make(chan net.Conn, 100),
		}
	}
	return self.bypass
}

func (self *Server) createHandler() http.Handler {
	return self
}

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

func (self *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/mq/queues") {
		self.queuesIndex(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/mq/topics") {
		self.topicsIndex(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/mq/clients") {
		self.clientsIndex(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/mq/queue/") {
		self.doHandler(w, r, "/mq/queue/",
			func(name string) *Consumer {
				return self.CreateQueueIfNotExists(name).ListenOn()
			},
			func(name string) Producer {
				return self.CreateQueueIfNotExists(name)
			})
	} else if strings.HasPrefix(r.URL.Path, "/mq/topic/") {
		self.doHandler(w, r, "/mq/topic/",
			func(name string) *Consumer {
				return self.CreateTopicIfNotExists(name).ListenOn()
			},
			func(name string) Producer {
				return self.CreateTopicIfNotExists(name)
			})
	} else if self.options.Handler != nil {
		self.options.Handler.ServeHTTP(w, r)
	} else {
		http.DefaultServeMux.ServeHTTP(w, r)
	}
}

func (self *Server) doHandler(w http.ResponseWriter, r *http.Request,
	prefix string, recv_cb func(name string) *Consumer,
	send_cb func(name string) Producer) {
	url_path := strings.TrimPrefix(r.URL.Path, prefix)
	url_path = strings.TrimSuffix(url_path, "/")
	query_params := r.URL.Query()

	if r.Method == "GET" {
		timeout := GetTimeout(query_params, 1*time.Second)
		timer := time.NewTimer(timeout)
		consumer := recv_cb(url_path)
		defer consumer.Close()

		select {
		case msg, ok := <-consumer.C:
			timer.Stop()
			if !ok {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("queue is closed."))
				return
			}

			w.Header().Add("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			// fmt.Println("===================", msg.DataLength(), mq.ToCommandName(msg.Command()))
			if msg.DataLength() > 0 {
				w.Write(msg.Data())
			}
		case <-timer.C:
			w.WriteHeader(http.StatusNoContent)
		}
	} else if r.Method == "PUT" || r.Method == "POST" {
		bs, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		r.Body.Close()

		timeout := GetTimeout(query_params, 0)
		msg := mq.NewMessageWriter(mq.MSG_DATA, len(bs)+10).Append(bs).Build()
		send := send_cb(url_path)
		if timeout == 0 {
			err = send.Send(msg)
		} else {
			err = send.SendTimeout(msg, timeout)
		}
		w.Header().Add("Content-Type", "text/plain")
		if err != nil {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte(err.Error()))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		}
	} else {
		if nil != r.Body {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}

		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Method must is PUT or GET."))
	}
}

func GetTimeout(query_params url.Values, value time.Duration) time.Duration {
	s := query_params.Get("timeout")
	if "" == s {
		return value
	}
	t, e := time.ParseDuration(s)
	if nil != e {
		return value
	}
	return t
}

func (self *Server) queuesIndex(w http.ResponseWriter, r *http.Request) {
	self.queues_lock.RLock()
	defer self.queues_lock.RUnlock()
	var results []string
	for k, _ := range self.queues {
		results = append(results, k)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func (self *Server) topicsIndex(w http.ResponseWriter, r *http.Request) {
	self.topics_lock.RLock()
	defer self.topics_lock.RUnlock()
	var results []string
	for k, _ := range self.topics {
		results = append(results, k)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func (self *Server) clientsIndex(w http.ResponseWriter, r *http.Request) {
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

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
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
			if nil != self.bypass {
				self.bypass.c <- wrap(buf, clientConn)
			} else {
				self.logf("ERROR: client(%s) bad protocol magic '%s'",
					remoteAddr, string(buf))
				clientConn.Close()
			}
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
		bypass := srv.createListener()
		srv.runItInGoroutine(func() {
			if err := http.Serve(bypass,
				srv.createHandler()); err != nil {
				if e, ok := err.(*net.OpError); !ok || e == nil || e.Err != io.EOF {
					srv.log("[http]", err)
				}
				srv.Close()
			}
		})
	}

	srv.runItInGoroutine(func() {
		srv.runLoop(listener)
	})

	return srv, nil
}
