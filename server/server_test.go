package server

import (
	"bytes"
	"encoding/binary"
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

func TestBenchmarkMqServer(t *testing.T) {
	go http.ListenAndServe(":", nil)

	srv, err := NewServer(&Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()

	address := "127.0.0.1" + srv.options.TCPAddress

	subClient, err := mq_client.Connect("", address, 512)
	if nil != err {
		log.Fatalln(err)
		return
	}

	var wait sync.WaitGroup
	go func() {
		defer wait.Done()
		defer subClient.Close()

		var start_at time.Time
		var message_count uint32 = 0
		isStarted := false

		cb := func(cli mq_client.SubscribeClient, msg mq.Message) {
			if !isStarted {
				start_at = time.Now()
				isStarted = true
			}

			id := binary.BigEndian.Uint32(msg.Data())

			if id == math.MaxUint32 {
				if message_count != 0 {
					fmt.Println("recv:", message_count, ", elapsed:", time.Now().Sub(start_at))
					t.Log("recv:", message_count, ", elapsed:", time.Now().Sub(start_at))
					message_count = 0

					//	fmt.Println("message count = ", message_count)
					//} else {
					//	fmt.Println("message count = 0")
				}

				cli.Close()
				return
			}
			if id != message_count {
				t.Error(message_count, "-", id)
			}
			message_count++
		}
		err = subClient.SubscribeQueue("a", cb)
		if nil != err {
			t.Error(err)
			return
		}
	}()
	wait.Add(1)

	time.Sleep(1 * time.Second)

	sendClient, err := mq_client.Connect("", address, 512)
	if nil != err {
		t.Error(err)
		return
	}

	err = sendClient.SwitchToQueue("a")
	if nil != err {
		t.Error(err)
		return
	}

	var buf [4]byte
	for i := uint32(0); i < 10000; i++ {
		binary.BigEndian.PutUint32(buf[:], i)
		msg := mq.NewMessageWriter(mq.MSG_DATA, 4+1).Append(buf[:]).Build()
		if err = sendClient.Send(msg.ToBytes()); nil != err {
			t.Error(err)
			return
		}
	}

	binary.BigEndian.PutUint32(buf[:], math.MaxUint32)
	endMsg := mq.NewMessageWriter(mq.MSG_DATA, 4+1).Append(buf[:]).Build()
	for i := uint32(0); i < 1000; i++ {
		if err = sendClient.Send(endMsg.ToBytes()); nil != err {
			t.Error(err)
			return
		}
	}
	sendClient.Close()

	t.Log("test is ok")

	wait.Wait()
}
