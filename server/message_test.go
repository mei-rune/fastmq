package server

import (
	"bytes"
	"strings"
	"testing"
)

func TestMessageReadLength(t *testing.T) {
	for _, s := range []struct {
		input    string
		excepted Message
	}{
		//       123456789
		{input: "p     0\n",
			excepted: NewMessageWriter('p', 0).Build()},
		{input: "p     1\n1",
			excepted: NewMessageWriter('p', 0).Append([]byte{'1'}).Build()},
		{input: "p    11\n11111111111",
			excepted: NewMessageWriter('p', 0).Append(bytes.Repeat([]byte{'1'}, 11)).Build()},
		{input: "p   111\n" + strings.Repeat("1", 111),
			excepted: NewMessageWriter('p', 0).Append(bytes.Repeat([]byte{'1'}, 111)).Build()},
		{input: "p  1111\n" + strings.Repeat("1", 1111),
			excepted: NewMessageWriter('p', 0).Append(bytes.Repeat([]byte{'1'}, 1111)).Build()},
		{input: "p 11111\n" + strings.Repeat("1", 11111),
			excepted: NewMessageWriter('p', 0).Append(bytes.Repeat([]byte{'1'}, 11111)).Build()},
	} {

		if !bytes.Equal(s.excepted.ToBytes(), []byte(s.input)) {
			t.Error("[", s.input, "] Data is error - ", msg.Data(), s.excepted.Data())
			continue
		}

		//fmt.Println("'"+string(s.excepted.ToBytes())+"'", len(s.excepted.ToBytes()))
		reader := NewMessageReader(strings.NewReader(s.input), 100)
		msg, err := reader.ReadMessage()
		if nil != err {
			t.Error("[", s.input, "]", err)
			continue
		}
		if nil == msg {
			msg, err = reader.ReadMessage()
			if nil != err {
				t.Error("[", s.input, "]", err)
				continue
			}
			if nil == msg {
				t.Error("[", s.input, "] read is error")
				continue
			}
		}
		if msg.Command() != s.excepted.Command() {
			t.Error("[", s.input, "] command is error - ", msg.Command(), s.excepted.Command())
			continue
		}
		if msg.DataLength() != s.excepted.DataLength() {
			t.Error("[", s.input, "] DataLength is error - ", msg.DataLength(), s.excepted.DataLength())
			continue
		}

		if !bytes.Equal(msg.Data(), s.excepted.Data()) {
			//fmt.Println("'"+string(msg.ToBytes())+"'", len(msg.ToBytes()))
			t.Error("[", s.input, "] Data is error - ", msg.Data(), s.excepted.Data())
			continue
		}

	}
}
