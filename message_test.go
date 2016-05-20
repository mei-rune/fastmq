package client

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func assertEq(t *testing.T, reader *Reader, input string, excepted Message) {
	msg, err := reader.ReadMessage()
	if nil != err {
		t.Error("[", input, "]", err)
		return
	}
	if nil == msg {
		msg, err = reader.ReadMessage()
		if nil != err {
			t.Error("[", input, "]", err)
			return
		}
		if nil == msg {
			t.Error("[", input, "] read is error")
			return
		}
	}
	if msg.Command() != excepted.Command() {
		t.Error("[", input, "] command is error - ", msg.Command(), excepted.Command())
		return
	}
	if msg.DataLength() != excepted.DataLength() {
		t.Error("[", input, "] DataLength is error - ", msg.DataLength(), excepted.DataLength())
		return
	}

	if !bytes.Equal(msg.Data(), excepted.Data()) {
		//fmt.Println("'"+string(msg.ToBytes())+"'", len(msg.ToBytes()))
		t.Error("[", input, "] Data is error - ", msg.Data(), excepted.Data())
		return
	}
}

func TestMessageReadLength(t *testing.T) {
	var ok_tests = []struct {
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
	}

	for _, s := range ok_tests {
		if !bytes.Equal(s.excepted.ToBytes(), []byte(s.input)) {
			t.Error("[", s.input, "] Data is error")
			continue
		}

		rd := NewMessageReader(strings.NewReader(s.input), 100)
		assertEq(t, rd, s.input, s.excepted)

		msg, err := rd.ReadMessage()
		if io.EOF != err {
			t.Error(err)
		}
		if nil != msg {
			t.Error("don't read message.")
		}
	}

	for _, s := range ok_tests {
		for i := 1; i < 10; i++ {
			data := strings.Repeat(s.input, i)

			rd := NewMessageReader(strings.NewReader(data), 100)
			for j := 0; j < i; j++ {
				assertEq(t, rd, s.input, s.excepted)
			}
		}
	}
}

func TestMessageReadMuti(t *testing.T) {
	var ok_tests = []struct {
		input    string
		excepted Message
	}{
		//       123456789
		{input: "p     0\np     0\n",
			excepted: NewMessageWriter('p', 0).Build()},
		{input: "p     1\n1p     1\n1",
			excepted: NewMessageWriter('p', 0).Append([]byte{'1'}).Build()},
		{input: "p    11\n11111111111p    11\n11111111111",
			excepted: NewMessageWriter('p', 0).Append(bytes.Repeat([]byte{'1'}, 11)).Build()},
	}

	for _, s := range ok_tests {
		rd := NewMessageReader(strings.NewReader(s.input), 100)
		assertEq(t, rd, s.input, s.excepted)
		assertEq(t, rd, s.input, s.excepted)
	}
}
