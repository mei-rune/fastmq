package fast

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	mq_client "github.com/runner-mei/fastmq/client"
	mq_server "github.com/runner-mei/fastmq/server"
	"github.com/valyala/fasthttp"
)

var ErrHandlerType = errors.New("handler isn't fasthttp.RequestHandler.")

func FastConnection(srv *mq_server.Server) (mq_server.ByPass, error) {
	engine := &fastEngine{srv: srv}
	if nil != srv.GetOptions().Handler {
		handler, ok := srv.GetOptions().Handler.(fasthttp.RequestHandler)
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

	return engine, nil
}

type fastEngine struct {
	srv     *mq_server.Server
	handler fasthttp.RequestHandler
}

func (self *fastEngine) Close() error {
	return nil
}

func (self *fastEngine) On(conn net.Conn) {
	err := fasthttp.ServeConn(conn, func(ctx *fasthttp.RequestCtx) {
		url_path := ctx.Path()
		if bytes.HasPrefix(url_path, []byte("/mq/queues")) {
			self.queuesIndex(ctx)
		} else if bytes.HasPrefix(url_path, []byte("/mq/topics")) {
			self.topicsIndex(ctx)
		} else if bytes.HasPrefix(url_path, []byte("/mq/clients")) {
			self.clientsIndex(ctx)
		} else if bytes.HasPrefix(url_path, []byte("/mq/queue/")) {
			self.doHandler(ctx, []byte("/mq/queue/"),
				func(name []byte) *mq_server.Consumer {
					return self.srv.CreateQueueIfNotExists(string(name)).ListenOn()
				},
				func(name []byte) mq_server.Producer {
					return self.srv.CreateQueueIfNotExists(string(name))
				})
		} else if bytes.HasPrefix(url_path, []byte("/mq/topic/")) {
			self.doHandler(ctx, []byte("/mq/topic/"),
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
}

func (self *fastEngine) doHandler(ctx *fasthttp.RequestCtx,
	prefix []byte, recv_cb func(name []byte) *mq_server.Consumer,
	send_cb func(name []byte) mq_server.Producer) {
	url_path := bytes.TrimPrefix(ctx.Path(), prefix)
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
				ctx.SetStatusCode(http.StatusServiceUnavailable)
				ctx.Write([]byte("queue is closed."))
				return
			}

			ctx.Response.Header.Set("Content-Type", "text/plain")
			ctx.SetStatusCode(http.StatusOK)
			if msg.DataLength() > 0 {
				ctx.Write(msg.Data())
			}
		case <-timer.C:
			ctx.SetStatusCode(http.StatusNoContent)
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
			ctx.SetStatusCode(http.StatusRequestTimeout)
			ctx.Write([]byte(err.Error()))
		} else {
			ctx.SetStatusCode(http.StatusOK)
			ctx.Write([]byte("OK"))
		}
	} else {
		ctx.SetStatusCode(http.StatusMethodNotAllowed)
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
	ctx.SetStatusCode(http.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetQueues())
}

func (self *fastEngine) topicsIndex(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(http.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetTopics())
}

func (self *fastEngine) clientsIndex(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(http.StatusOK)
	json.NewEncoder(ctx).Encode(self.srv.GetClients())
}

func init() {
	mq_server.ConnectionHandle = FastConnection
}
