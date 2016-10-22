package server

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"strconv"
	"strings"
	"time"

	mq_client "github.com/runner-mei/fastmq/client"
)

func StandardConnection(srv *Server) (ByPass, error) {
	engine := &standardEngine{srv: srv,
		hasPrefix:    "" != srv.GetOptions().HttpPrefix,
		prefix:       srv.GetOptions().HttpPrefix,
		is_redirect:  "" != srv.GetOptions().HttpRedirectUrl,
		redirect_url: srv.GetOptions().HttpRedirectUrl}
	if nil != srv.GetOptions().HttpHandler {
		handler, ok := srv.GetOptions().HttpHandler.(http.Handler)
		if !ok {
			return nil, ErrHandlerType
		}
		engine.handler = handler
	}

	if engine.handler == nil {
		engine.handler = http.DefaultServeMux
	}

	srv.RunItInGoroutine(func() {
		if err := http.Serve(engine.createListener(), engine); err != nil {
			if e, ok := err.(*net.OpError); !ok || e == nil || e.Err != io.EOF {
				srv.log("[http]", err)
			}
			srv.Close()
		}
	})

	if "" != srv.options.SSLAddress {
		srv.RunItInGoroutine(func() {
			if err := http.ListenAndServeTLS(srv.options.SSLAddress,
				srv.options.SSLCertFile, srv.options.SSLKeyFile, engine); err != nil {
				if e, ok := err.(*net.OpError); !ok || e == nil || e.Err != io.EOF {
					srv.log("[https]", err)
				}
				srv.Close()
			}
		})
	}
	return engine, nil
}

type standardEngine struct {
	srv          *Server
	listener     *Listener
	handler      http.Handler
	hasPrefix    bool
	prefix       string
	is_redirect  bool
	redirect_url string
}

func (self *standardEngine) Close() error {
	close(self.listener.c)
	return self.listener.Close()
}

func (self *standardEngine) On(conn net.Conn) {
	select {
	case self.listener.c <- conn:
	default:
		conn.Close()
		self.srv.log("listen pool is overflow.")
	}
}

func (self *standardEngine) createListener() net.Listener {
	if self.listener == nil {
		self.listener = &Listener{
			addr:   self.srv.Addr(),
			closer: nil,
			c:      make(chan net.Conn, 50),
		}
	}
	return self.listener
}

func (self *standardEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	url_path := r.URL.Path
	if self.hasPrefix {
		if !strings.HasPrefix(url_path, self.prefix) {
			if strings.HasPrefix(url_path, "/debug/") {
				if url_path == "/debug/pprof" {
					http.Redirect(w, r, "/debug/pprof/", http.StatusMovedPermanently)
					return
				}

				http.DefaultServeMux.ServeHTTP(w, r)
				return
			}

			if self.is_redirect {
				http.Redirect(w, r, self.redirect_url, http.StatusMovedPermanently)
				return
			}

			http.DefaultServeMux.ServeHTTP(w, r)
			return
		}
		url_path = strings.TrimPrefix(url_path, self.prefix)
		if !strings.HasPrefix(url_path, "mq/") {
			self.handler.ServeHTTP(w, r)
			return
		}

		url_path = strings.TrimPrefix(url_path, "mq/")
	} else {
		if !strings.HasPrefix(url_path, "/mq/") {
			self.handler.ServeHTTP(w, r)
			return
		}
		url_path = strings.TrimPrefix(url_path, "/mq/")
	}

	if url_path == "queues" {
		self.queuesIndex(w, r)
	} else if url_path == "topics" {
		self.topicsIndex(w, r)
	} else if url_path == "clients" {
		self.clientsIndex(w, r)
	} else if strings.HasPrefix(url_path, "queues/") {
		url_path = strings.TrimPrefix(url_path, "queues/")
		if "" == url_path {
			self.queuesIndex(w, r)
			return
		}

		self.doHandler(w, r, url_path,
			func(name string) *Consumer {
				return self.srv.CreateQueueIfNotExists(name).ListenOn()
			},
			func(name string) Producer {
				return self.srv.CreateQueueIfNotExists(name)
			})
	} else if strings.HasPrefix(url_path, "topics/") {
		url_path = strings.TrimPrefix(url_path, "topics/")
		if "" == url_path {
			self.queuesIndex(w, r)
			return
		}

		self.doHandler(w, r, strings.TrimPrefix(url_path, "topics/"),
			func(name string) *Consumer {
				return self.srv.CreateTopicIfNotExists(name).ListenOn()
			},
			func(name string) Producer {
				return self.srv.CreateTopicIfNotExists(name)
			})
	} else {
		http.DefaultServeMux.ServeHTTP(w, r)
	}
}

func readMore(c chan mq_client.Message, msg mq_client.Message) []mq_client.Message {
	results := append(make([]mq_client.Message, 0, 12), msg)
	for i := 0; i < 100; i++ {
		select {
		case m, ok := <-c:
			if !ok {
				return results
			}
			results = append(results, m)
		default:
			return results
		}
	}
	return results
}
func (self *standardEngine) doHandler(w http.ResponseWriter, r *http.Request,
	url_path string, recv_cb func(name string) *Consumer,
	send_cb func(name string) Producer) {
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

			if query_params.Get("batch") != "true" {
				w.Header().Add("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				if msg.DataLength() > 0 {
					w.Write(msg.Data())
				}
			} else {
				msgList := readMore(consumer.C, msg)
				w.Header().Add("X-HW-Batch", strconv.FormatInt(int64(len(msgList)), 10))
				w.WriteHeader(http.StatusOK)

				w.Write([]byte("["))
				is_frist := true
				for _, m := range msgList {
					if m.DataLength() > 0 {
						if is_frist {
							is_frist = false
						} else {
							w.Write([]byte(","))
						}

						w.Write(m.Data())
					}
				}
				w.Write([]byte("]"))
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
		msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(bs)+10).Append(bs).Build()
		send := send_cb(url_path)
		if timeout == 0 {
			err = send.Send(msg)
		} else {
			err = send.SendTimeout(msg, timeout)
		}
		// fmt.Println("===================", msg.DataLength(), mq_client.ToCommandName(msg.Command()), err, timeout, url_path)
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

func (self *standardEngine) queuesIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(self.srv.GetQueues())
}

func (self *standardEngine) topicsIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(self.srv.GetTopics())
}

func (self *standardEngine) clientsIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(self.srv.GetClients())
}
