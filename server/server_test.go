package server

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	mq_client "github.com/runner-mei/fastmq/client"
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

			pingMessage := mq_client.NewMessageWriter(mq_client.MSG_DATA, 10).Append([]byte("aa")).Build()
			go func() {
				if "topic" == s {
					time.Sleep(1 * time.Second)
				}
				conn, err := net.Dial("tcp", "127.0.0.1"+srv.options.TCPAddress)
				if nil != err {
					t.Error(err)
					return
				}
				if err := mq_client.SendFull(conn, mq_client.HEAD_MAGIC); err != nil {
					t.Error(err)
					return
				}

				var buf = make([]byte, len(mq_client.HEAD_MAGIC))
				if _, err := io.ReadFull(conn, buf); err != nil {
					t.Error(err)
					return
				}

				if !bytes.Equal(buf, mq_client.HEAD_MAGIC) {
					t.Error("magic is error - ", buf)
					return
				}

				_, err = conn.Write(mq_client.NewMessageWriter(mq_client.MSG_PUB, 10).Append([]byte(s + " a")).Build().ToBytes())
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

			sub := mq_client.Connect("tcp", "127.0.0.1"+srv.options.TCPAddress)

			msg_count := 0
			cb := func(cli *mq_client.Subscription, recvMessage mq_client.Message) {
				if !bytes.Equal(pingMessage.ToBytes(), recvMessage.ToBytes()) {
					if recvMessage.Command() == mq_client.MSG_ERROR {
						t.Error("actual is", mq_client.ToError(recvMessage))
					} else {
						t.Log("excepted is", pingMessage.ToBytes())
						t.Error("actual is", recvMessage, mq_client.ToCommandName(recvMessage[0]))
					}
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
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

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

func TestServerHttpQueuePush(t *testing.T) {
	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	res, err := http.Post("http://127.0.0.1"+srv.options.TCPAddress+"/mq/queue/aa", "text/plain", strings.NewReader("AAA"))
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

	if "OK" != string(bytes.TrimSpace(bs)) {
		t.Error("body is", string(bs))
		return
	}

	q := srv.CreateQueueIfNotExists("aa")
	//fmt.Println(len(q.C))
	//fmt.Println(len(q.consumer.C))
	select {
	case msg := <-q.C:
		if "AAA" != string(msg.Data()) {
			t.Error("body is", string(bs))
			return
		}
	default:
		//fmt.Println("===========")
		//case <-time.After(10 * time.Second):
		t.Error("msg isnot recv")
	}
}

func TestServerHttpQueueGet(t *testing.T) {
	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, 8+3).Append([]byte("AAA")).Build()
	srv.CreateQueueIfNotExists("aa").C <- msg

	//fmt.Println(string(msg.Data()))
	res, err := http.Get("http://127.0.0.1" + srv.options.TCPAddress + "/mq/queue/aa")
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

	if "AAA" != string(bytes.TrimSpace(bs)) {
		t.Error("body is", string(bs))
		return
	}
}

const message_total = 100000

var sleep = flag.Duration("test.sleep", 0, "")

func TestBenchmarkMqServer(t *testing.T) {
	//go http.ListenAndServe(":", nil)

	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	address := "127.0.0.1" + srv.options.TCPAddress

	subClient := mq_client.Connect("", address)
	subClient.Id("subscribe")

	var wait sync.WaitGroup
	go func() {
		defer wait.Done()

		var start_at time.Time
		var message_count uint32 = 0
		isStarted := false

		cb := func(cli *mq_client.Subscription, msg mq_client.Message) {
			if !isStarted {
				start_at = time.Now()
				isStarted = true
			}
			if mq_client.MSG_NOOP == msg.Command() {
				return
			}

			if 4 != msg.DataLength() {
				// switch msg.Command() {
				// case mq_client.MSG_ERROR:
				// 	t.Error(string(msg.Data()))
				// case mq_client.MSG_ACK
				// }
				t.Error("length is error - ", mq_client.ToCommandName(msg.Command()), msg.ToBytes())
				fmt.Println("length is error - ", mq_client.ToCommandName(msg.Command()), msg.ToBytes())
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

	sendClient, err := mq_client.Connect("", address).Id("send").ToQueue("a")
	if nil != err {
		t.Error(err)
		return
	}
	// if err = sendClient.Id("send"); err != nil {
	// 	t.Error(err)
	// 	return
	// }

	// err = sendClient.ToQueue("a")
	// if nil != err {
	// 	t.Error(err)
	// 	return
	// }

	var buf [4]byte
	for i := uint32(0); i < message_total; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, 4+1).Append(buf[:]).Build()
		sendClient.Send(msg.ToBytes())
	}

	binary.BigEndian.PutUint32(buf[:], math.MaxUint32)
	endMsg := mq_client.NewMessageWriter(mq_client.MSG_DATA, 4+1).Append(buf[:]).Build()
	sendClient.Send(endMsg.ToBytes())

	t.Log("test is ok")
	fmt.Println("send is ok")

	if err := sendClient.Stop(); err != nil {
		t.Error(err)
	}

	//sendClient.Close()

	wait.Wait()
}
