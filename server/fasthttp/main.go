package fast

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	mq_client "github.com/runner-mei/fastmq/client"
	mq_server "github.com/runner-mei/fastmq/server"
	"github.com/valyala/fasthttp"
)

var ErrHandlerType = errors.New("handler isn't fasthttp.RequestHandler.")

func FastConnection(srv *mq_server.Server) (mq_server.ByPass, error) {
	engine := &fastEngine{srv: srv,
		prefix:       []byte(srv.GetOptions().HttpPrefix),
		is_redirect:  "" != srv.GetOptions().HttpRedirectUrl,
		redirect_url: srv.GetOptions().HttpRedirectUrl}
	if nil != srv.GetOptions().HttpHandler {
		handler, ok := srv.GetOptions().HttpHandler.(fasthttp.RequestHandler)
		if !ok {
			return nil, ErrHandlerType
		}
		engine.handler = handler
	}

	if engine.handler == nil {
		engine.handler = func(ctx *fasthttp.RequestCtx) {
			ctx.NotFound()
		}
	}

	if len(engine.prefix) == 0 {
		engine.prefix = nil
	}

	return engine, nil
}

type fastEngine struct {
	srv          *mq_server.Server
	handler      fasthttp.RequestHandler
	prefix       []byte
	is_redirect  bool
	redirect_url string
}

func (self *fastEngine) Close() error {
	return nil
}

func (self *fastEngine) On(conn net.Conn) {
	go func() {
		err := fasthttp.ServeConn(conn, func(ctx *fasthttp.RequestCtx) {
			url_path := ctx.Path()
			if nil != self.prefix {
				if !bytes.HasPrefix(url_path, self.prefix) {
					if self.is_redirect {
						ctx.Redirect(self.redirect_url, fasthttp.StatusMovedPermanently)
						return
					}
					ctx.NotFound()
					return
				}
				url_path = bytes.TrimPrefix(url_path, self.prefix)
			}

			if bytes.Equal(url_path, []byte("/mq/queues")) {
				self.queuesIndex(ctx)
			} else if bytes.Equal(url_path, []byte("/mq/topics")) {
				self.topicsIndex(ctx)
			} else if bytes.Equal(url_path, []byte("/mq/clients")) {
				self.clientsIndex(ctx)
			} else if bytes.HasPrefix(url_path, []byte("/mq/queues/")) {
				url_path = bytes.TrimPrefix(url_path, []byte("/mq/queues/"))
				if len(url_path) == 0 {
					self.queuesIndex(ctx)
					return
				}

				self.doHandler(ctx, bytes.TrimPrefix(url_path, []byte("/mq/queues/")),
					func(name []byte) *mq_server.Consumer {
						return self.srv.CreateQueueIfNotExists(string(name)).ListenOn()
					},
					func(name []byte) mq_server.Producer {
						return self.srv.CreateQueueIfNotExists(string(name))
					})
			} else if bytes.HasPrefix(url_path, []byte("/mq/topics/")) {
				url_path = bytes.TrimPrefix(url_path, []byte("/mq/topics/"))
				if len(url_path) == 0 {
					self.queuesIndex(ctx)
					return
				}

				self.doHandler(ctx, bytes.TrimPrefix(url_path, []byte("/mq/topics/")),
					func(name []byte) *mq_server.Consumer {
						return self.srv.CreateTopicIfNotExists(string(name)).ListenOn()
					},
					func(name []byte) mq_server.Producer {
						return self.srv.CreateTopicIfNotExists(string(name))
					})
			} else {
				self.handler(ctx)
			}
		})
		if err != nil {
			conn.Close()
			log.Println(err)
		}
	}()
}

func (self *fastEngine) doHandler(ctx *fasthttp.RequestCtx,
	url_path []byte, recv_cb func(name []byte) *mq_server.Consumer,
	send_cb func(name []byte) mq_server.Producer) {
	url_path = bytes.TrimSuffix(url_path, []byte("/"))
	uri := ctx.URI()

	method := ctx.Method()
	if bytes.Equal(method, []byte("GET")) {
		timeout := GetTimeout(uri, 1*time.Second)
		timer := time.NewTimer(timeout)
		consumer := recv_cb(url_path)
		defer consumer.Close()

		select {
		case msg, ok := <-consumer.C:
			timer.Stop()
			if !ok {
				ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
				ctx.Write([]byte("queue is closed."))
				return
			}

			ctx.Response.Header.Set("Content-Type", "text/plain")
			ctx.SetStatusCode(fasthttp.StatusOK)
			if msg.DataLength() > 0 {
				ctx.Write(msg.Data())
			}
		case <-timer.C:
			ctx.SetStatusCode(fasthttp.StatusNoContent)
		}
	} else if bytes.Equal(method, []byte("PUT")) || bytes.Equal(method, []byte("POST")) {
		bs := ctx.PostBody()
		timeout := GetTimeout(uri, 0)
		msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, len(bs)+10).Append(bs).Build()
		send := send_cb(url_path)
		var err error
		if timeout == 0 {
			err = send.Send(msg)
		} else {
			err = send.SendTimeout(msg, timeout)
		}
		// fmt.Println("===================", msg.DataLength(), mq_client.ToCommandName(msg.Command()), err, timeout, url_path)
		ctx.Response.Header.Set("Content-Type", "text/plain")
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusRequestTimeout)
			ctx.Write([]byte(err.Error()))
		} else {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.Write([]byte("OK"))
		}
	} else {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.Write([]byte("Method must is PUT or GET."))
	}
}

func GetTimeout(uri *fasthttp.URI, value time.Duration) time.Duration {
	s := uri.QueryArgs().Peek("timeout")
	if len(s) == 0 {
		return value
	}
	t, e := time.ParseDuration(string(s))
	if nil != e {
		return value
	}
	return t
}

func (self *fastEngine) queuesIndex(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetQueues())
}

func (self *fastEngine) topicsIndex(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetTopics())
}

func (self *fastEngine) clientsIndex(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetClients())
}

func init() {
	mq_server.ConnectionHandle = FastConnection
}
