package fast

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	mq_client "github.com/runner-mei/fastmq/client"
	mq_server "github.com/runner-mei/fastmq/server"
)

func TestServerHttp(t *testing.T) {
	srv, err := mq_server.NewServer(&mq_server.Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	res, err := http.Get("http://127.0.0.1" + srv.GetOptions().TCPAddress + "/mq/clients")
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
	srv, err := mq_server.NewServer(&mq_server.Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	res, err := http.Post("http://127.0.0.1"+srv.GetOptions().TCPAddress+"/mq/queues/aa", "text/plain", strings.NewReader("AAA"))
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
	srv, err := mq_server.NewServer(&mq_server.Options{HttpEnabled: true})
	if nil != err {
		t.Error(err)
		return
	}
	defer srv.Close()
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	msg := mq_client.NewMessageWriter(mq_client.MSG_DATA, 8+3).Append([]byte("AAA")).Build()
	srv.CreateQueueIfNotExists("aa").C <- msg

	//fmt.Println(string(msg.Data()))
	res, err := http.Get("http://127.0.0.1" + srv.GetOptions().TCPAddress + "/mq/queues/aa")
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
