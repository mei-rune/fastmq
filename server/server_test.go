package server

import (
	"bytes"
	"io"
	"net"
	_ "net/http/pprof"
	"testing"

	mq "fastmq"
)

func TestServerPublishMessage(t *testing.T) {
	for _, s := range []string{"topic", "queue"} {
		func() {
			srv, err := NewServer(&Options{})
			if nil != err {
				t.Error(err)
				return
			}
			defer srv.Close()

			conn, err := net.Dial("tcp", "127.0.0.1"+srv.options.TCPAddress)
			if nil != err {
				t.Error(err)
				return
			}
			if err := mq.SendFull(conn, mq.HEAD_MAGIC); err != nil {
				t.Error(err)
				return
			}

			var buf = make([]byte, mq.HEAD_LENGTH)
			if err := io.ReadFull(conn, buf); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(buf, mq.HEAD_MAGIC) {
				t.Error("magic is error - ", buf)
				return
			}

			go func() {
				_, err = conn2.Write(mq.NewMessageWriter(mq.MSG_PUB, 10).Append([]byte(s + " a")).Build().ToBytes())
				if nil != err {
					t.Error(err)
					return
				}

				pingBytes := mq.NewMessageWriter(mq.MSG_DATA, 10).Append([]byte("aa")).Build().ToBytes()
				for i := 0; i < 100; i++ {
					_, err = conn2.Write(pingBytes)
					if nil != err {
						t.Error(err)
						return
					}
				}
			}()

		}()
	}
}
