package server

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	mq "fastmq"
	mq_client "fastmq/client"
	"fmt"
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

			sub, err := mq_client.ConnectSub("tcp", "127.0.0.1"+srv.options.TCPAddress, 512)
			if nil != err {
				t.Error(err)
				return
			}

			msg_count := 0
			cb := func(cli *mq_client.Subscription, recvMessage mq.Message) {
				if !bytes.Equal(pingMessage.ToBytes(), recvMessage.ToBytes()) {
					t.Log("excepted is", pingMessage.ToBytes())
					t.Error("actual is", recvMessage, mq.ToCommandName(recvMessage[0]))
				}

				msg_count++
				if msg_count >= 100 {
					cli.Stop()
				}
				// fmt.Println(msg_count)
			}

			if "topic" == s {
				err = sub.SubscribeTopic("a", cb)
			} else {
				err = sub.SubscribeQueue("a", cb)
			}

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

const message_total = 100000

var sleep = flag.Duration("test.sleep", 0, "")

func TestBenchmarkMqServer(t *testing.T) {
	go http.ListenAndServe(":", nil)

	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()

	address := "127.0.0.1" + srv.options.TCPAddress

	subClient, err := mq_client.ConnectSub("", address, 512)
	if nil != err {
		log.Fatalln(err)
		return
	}

	if err = subClient.Id("subscribe"); err != nil {
		t.Error(err)
		return
	}

	var wait sync.WaitGroup
	go func() {
		defer wait.Done()
		defer subClient.Close()

		var start_at time.Time
		var message_count uint32 = 0
		isStarted := false

		cb := func(cli *mq_client.Subscription, msg mq.Message) {
			if !isStarted {
				start_at = time.Now()
				isStarted = true
			}
			if mq.MSG_NOOP == msg.Command() {
				return
			}

			if 4 != msg.DataLength() {
				// switch msg.Command() {
				// case mq.MSG_ERROR:
				// 	t.Error(string(msg.Data()))
				// case mq.MSG_ACK
				// }
				t.Error("length is error - ", mq.ToCommandName(msg.Command()), msg.ToBytes())
				fmt.Println("length is error - ", mq.ToCommandName(msg.Command()), msg.ToBytes())
				return
			}

			id := binary.BigEndian.Uint32(msg.Data())

			if id == math.MaxUint32 {
				if message_count != 0 {
					fmt.Println("recv:", message_count, ", elapsed:", time.Now().Sub(start_at))
				}

				cli.Stop()
				return
			}
			if id != message_count {
				t.Error(message_count, "-", id)
			}
			message_count++

			if 0 != *sleep {
				time.Sleep(*sleep)
				//fmt.Println(id)
			}
		}
		err = subClient.SubscribeQueue("a", cb)
		if nil != err {
			t.Error(err)
			return
		}

		if message_total != message_count {
			t.Error("excepted message count is", message_total, ", actual is", message_count)
		}
	}()
	wait.Add(1)

	time.Sleep(1 * time.Second)

	sendClient, err := mq_client.ConnectPub("", address, 200, 512)
	if nil != err {
		t.Error(err)
		return
	}
	if err = sendClient.Id("send"); err != nil {
		t.Error(err)
		return
	}

	err = sendClient.ToQueue("a")
	if nil != err {
		t.Error(err)
		return
	}

	var buf [4]byte
	for i := uint32(0); i < message_total; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		msg := mq.NewMessageWriter(mq.MSG_DATA, 4+1).Append(buf[:]).Build()
		sendClient.Send(msg.ToBytes())
	}

	binary.BigEndian.PutUint32(buf[:], math.MaxUint32)
	endMsg := mq.NewMessageWriter(mq.MSG_DATA, 4+1).Append(buf[:]).Build()
	sendClient.Send(endMsg.ToBytes())

	t.Log("test is ok")
	fmt.Println("send is ok")

	if err := sendClient.Stop(); err != nil {
		t.Error(err)
	}

	//sendClient.Close()

	wait.Wait()
}
