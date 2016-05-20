package server

import (
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	mq "fastmq"
	mq_client "fastmq/client"
)

func TestServerPublishMessage(t *testing.T) {
	//go http.ListenAndServe(":", nil)
	for _, s := range []string{"topic", "queue"} {
		func() {
			srv, err := NewServer(&Options{})
			if nil != err {
				t.Error(err)
				return
			}
			defer srv.Close()

			pingMessage := mq.NewMessageWriter(mq.MSG_DATA, 10).Append([]byte("aa")).Build()
			go func() {
				if "topic" == s {
					time.Sleep(1 * time.Second)
				}
				conn, err := net.Dial("tcp", "127.0.0.1"+srv.options.TCPAddress)
				if nil != err {
					t.Error(err)
					return
				}
				if err := mq.SendFull(conn, mq.HEAD_MAGIC); err != nil {
					t.Error(err)
					return
				}

				var buf = make([]byte, len(mq.HEAD_MAGIC))
				if _, err := io.ReadFull(conn, buf); err != nil {
					t.Error(err)
					return
				}

				if !bytes.Equal(buf, mq.HEAD_MAGIC) {
					t.Error("magic is error - ", buf)
					return
				}

				_, err = conn.Write(mq.NewMessageWriter(mq.MSG_PUB, 10).Append([]byte(s + " a")).Build().ToBytes())
				if nil != err {
					t.Error(err)
					return
				}

				if err = readAck(conn); nil != err {
					t.Error(err)
					return
				}

				for i := 0; i < 100; i++ {
					_, err = conn.Write(pingMessage.ToBytes())
					if nil != err {
						t.Error(err)
						return
					}
				}
			}()

			msg_count := 0
			is_topic := "topic" == s
			err = mq_client.Subscribe(&mq_client.SubscribeOption{
				IsTopic: is_topic,
				Name:    "a",
				Network: "tcp",
				Address: "127.0.0.1" + srv.options.TCPAddress},
				func(cli mq_client.SubscribeClient, recvMessage mq.Message) {

					if !bytes.Equal(pingMessage.ToBytes(), recvMessage.ToBytes()) {
						t.Log("excepted is", pingMessage.ToBytes())
						t.Error("actual is", recvMessage, mq.ToCommandName(recvMessage[0]))
					}

					msg_count++
					if msg_count >= 100 {
						cli.Close()
					}
					// fmt.Println(msg_count)
				})
			if err != nil {
				t.Error(err)
			}
		}()
	}
}

func TestServerHttp(t *testing.T) {
	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()

	res, err := http.Get("http://127.0.0.1" + srv.options.TCPAddress + "/mq/clients")
	if nil != err {
		t.Error(err)
		return
	}

	bs, _ := ioutil.ReadAll(res.Body)

	if res.StatusCode != http.StatusOK {
		t.Error("status code is", res.Status)
		t.Error(string(bs))
		return
	}

	if "null" != string(bytes.TrimSpace(bs)) {
		t.Error("body is", string(bs))
		return
	}
}
