package server

import (
	"bytes"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

func CreateTcp() (net.Conn, net.Conn) {
	srv, err := net.Listen("tcp", ":")
	if nil != err {
		panic(err)
	}
	defer srv.Close()

	_, port, err := net.SplitHostPort(srv.Addr().String())
	if nil != err {
		panic(err)
	}

	ch := make(chan net.Conn, 1)

	go func() {
		conn, err := srv.Accept()
		if nil != err {
			panic(err)
		}
		ch <- conn
	}()

	conn1, err := net.Dial("tcp", "127.0.0.1:"+port)
	if nil != err {
		panic(err)
	}

	return conn1, <-ch
}

func TestClientPublishMessage(t *testing.T) {
	for _, s := range []string{"topic", "queue"} {
		func() {
			srv, err := NewServer(&Options{})
			if nil != err {
				t.Error(err)
				return
			}
			defer srv.Close()

			conn1, conn2 := CreateTcp()
			defer func() {
				conn1.Close()
				conn2.Close()
			}()

			client := &Client{
				srv:        srv,
				remoteAddr: "aa",
				conn:       conn1,
			}
			defer client.Close()

			ch := make(chan interface{}, 10)
			go func() {
				client.runRead(ch)
			}()

			var channel Channel
			if "topic" == s {
				channel = srv.createTopicIfNotExists("a")
			} else {
				channel = srv.createQueueIfNotExists("a")
			}
			sub := channel.ListenOn()
			defer sub.Close()

			_, err = conn2.Write(NewMessageWriter(MSG_PUB, 10).Append([]byte(s + " a")).Build().ToBytes())
			if nil != err {
				t.Error(err)
				return
			}

			pingBytes := NewMessageWriter(MSG_DATA, 10).Append([]byte("aa")).Build().ToBytes()
			for i := 0; i < 100; i++ {
				_, err = conn2.Write(pingBytes)
				if nil != err {
					t.Error(err)
					return
				}
			}

			command := <-ch
			if _, ok := command.(*pubCommand); !ok {
				t.Errorf("it isn't pub - %T", command)
				return
			}

			for i := 0; i < 100; i++ {
				recvMessage := <-sub.C
				if !bytes.Equal(pingBytes, recvMessage.ToBytes()) {
					t.Error(recvMessage)
				}
			}
		}()
	}
}

func TestClientSubscribeMessage(t *testing.T) {
	go http.ListenAndServe(":", nil)

	for _, s := range []string{"topic", "queue"} {
		func() {
			srv, err := NewServer(&Options{})
			if nil != err {
				t.Error(err)
				return
			}
			defer srv.Close()

			conn1, conn2 := CreateTcp()
			defer func() {
				conn1.Close()
				conn2.Close()
			}()

			client := &Client{
				srv:        srv,
				remoteAddr: "aa",
				conn:       conn1,
			}
			defer client.Close()

			ch := make(chan interface{}, 10)
			go func() {
				client.runRead(ch)
			}()

			go func() {
				client.runWrite(ch)
			}()

			// sub := channel.ListenOn()
			// defer sub.Close()

			_, err = conn2.Write(NewMessageWriter(MSG_SUB, 10).Append([]byte(s + " a")).Build().ToBytes())
			if nil != err {
				t.Error(err)
				return
			}

			var channel Channel
			if "topic" == s {
				channel = srv.createTopicIfNotExists("a")
				time.Sleep(1 * time.Second) // wait for client is subscribed
			} else {
				channel = srv.createQueueIfNotExists("a")
			}

			pingMessage := NewMessageWriter(MSG_DATA, 10).Append([]byte("aa")).Build()
			go func() {
				for i := 0; i < 100; i++ {
					channel.Send(pingMessage)
				}
			}()

			rd := NewMessageReader(conn2, 100)
			for i := 0; i < 100; i++ {
				var recvMessage Message
				var err error
				for {
					recvMessage, err = rd.ReadMessage()
					if nil != err {
						t.Error(err)
						return
					}
					if nil != recvMessage {
						break
					}
				}

				if !bytes.Equal(pingMessage.ToBytes(), recvMessage.ToBytes()) {
					t.Error(recvMessage)
				}
			}
		}()
	}
}
