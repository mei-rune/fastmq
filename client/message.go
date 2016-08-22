package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// message format
// 'a' 'a' 'v' version
// command  ' ' length ‘\n’ data
// command is a byte.
// version is a byte.
// length  is a fixed length string (5 byte).
// data    is a byte array, length is between 0 to 65523 (65535-12).
//

const MAX_ENVELOPE_LENGTH = 65535
const MAX_MESSAGE_LENGTH = 65523
const HEAD_LENGTH = 8
const MAGIC_LENGTH = 4
const SYS_EVENTS = "_sys.events"

var (
	HEAD_MAGIC                  = []byte{'a', 'a', 'v', '1'}
	MSG_DATA_EMPTY_HEADER_BYTES = []byte{MSG_DATA, ' ', ' ', ' ', ' ', ' ', '0', '\n'}
	MSG_NOOP_BYTES              = []byte{MSG_NOOP, ' ', ' ', ' ', ' ', ' ', '0', '\n'}
	MSG_ACK_BYTES               = []byte{MSG_ACK, ' ', ' ', ' ', ' ', ' ', '0', '\n'}
	MSG_CLOSE_BYTES             = []byte{MSG_CLOSE, ' ', ' ', ' ', ' ', ' ', '0', '\n'}

	ErrTimeout           = errors.New("timeout")
	ErrAlreadyClosed     = errors.New("already closed.")
	ErrMoreThanMaxRead   = errors.New("more than maximum read.")
	ErrUnexceptedMessage = errors.New("recv a unexcepted message.")
	ErrUnexceptedAck     = errors.New("recv a unexcepted ack message.")
	ErrEmptyString       = errors.New("empty error message.")
	ErrMagicNumber       = errors.New("magic number is error.")
	ErrLengthExceed      = errors.New("message length is exceed.")
	ErrLengthNotDigit    = errors.New("length field of message isn't number.")
	ErrQueueFull         = errors.New("queue is full.")
)

const (
	MSG_ID    = 'i'
	MSG_ERROR = 'e'
	MSG_DATA  = 'd'
	MSG_PUB   = 'p'
	MSG_SUB   = 's'
	MSG_ACK   = 'a'
	MSG_NOOP  = 'n'
	MSG_CLOSE = 'c'
	MSG_KILL  = 'k'
)

func ToCommandName(cmd byte) string {
	switch cmd {
	case MSG_ID:
		return "MSG_ID"
	case MSG_ERROR:
		return "MSG_ERROR"
	case MSG_DATA:
		return "MSG_DATA"
	case MSG_PUB:
		return "MSG_PUB"
	case MSG_SUB:
		return "MSG_SUB"
	case MSG_ACK:
		return "MSG_ACK"
	case MSG_NOOP:
		return "MSG_NOOP"
	case MSG_CLOSE:
		return "MSG_CLOSE"
	case MSG_KILL:
		return "MSG_KILL"
	default:
		return "UNKNOWN-" + string(cmd)
	}
}

type Message []byte

func (self Message) Command() byte {
	return self[0]
}

func (self Message) DataLength() int {
	return len(self) - HEAD_LENGTH
}

func (self Message) Data() []byte {
	return self[HEAD_LENGTH:]
}

func (self Message) ToBytes() []byte {
	return self
}

type MessageReader interface {
	ReadMessage() (Message, error)
}

type errReader struct {
	err error
}

func (self *errReader) Read(bs []byte) (int, error) {
	return 0, self.err
}

type BufferedMessageReader struct {
	conn   io.Reader
	buffer []byte
	start  int
	end    int

	buffer_size int
}

func (self *BufferedMessageReader) DataLength() int {
	return self.end - self.start
}

func (self *BufferedMessageReader) Init(conn io.Reader, size int) {
	self.conn = conn
	self.buffer = MakeBytes(size)
	self.start = 0
	self.end = 0
	self.buffer_size = size
}

func readLength(bs []byte) (int, error) {
	// fmt.Println(bs)
	start := 0
	for ' ' == bs[start] {
		start++
	}
	if start >= 5 {
		return 0, ErrLengthNotDigit
	}

	length := int(bs[start] - '0')
	if length > 9 {
		//fmt.Println(bs[start], start)
		return 0, ErrLengthNotDigit
	}
	start++
	for ; start < 5; start++ {
		if l := int(bs[start] - '0'); l > 9 {
			//fmt.Println(bs[start], start)
			return 0, ErrLengthNotDigit
		} else {
			length *= 10
			length += l
		}
	}
	return length, nil
}

func (self *BufferedMessageReader) ensureCapacity(size int) {
	//fmt.Println("ensureCapacity", size, self.buffer_size)
	if self.buffer_size > size {
		size = self.buffer_size
	}
	//fmt.Println("ensureCapacity", size, self.buffer_size)
	tmp := MakeBytes(size)
	self.end = copy(tmp, self.buffer[self.start:self.end])
	self.start = 0
	self.buffer = tmp
	//fmt.Println(len(tmp))
}

func (self *BufferedMessageReader) nextMessage() (bool, Message, error) {
	length := self.end - self.start
	if length < HEAD_LENGTH {
		buf_reserve := len(self.buffer) - self.end
		if buf_reserve < (HEAD_LENGTH + 16) {
			self.ensureCapacity(256)
		}

		return false, nil, nil
	}

	msg_data_length, err := readLength(self.buffer[self.start+2:])
	if err != nil {
		return false, nil, err
	}
	//fmt.Println(msg_data_length, length, self.end, self.start, len(self.buffer))

	if msg_data_length > MAX_MESSAGE_LENGTH {
		return false, nil, ErrLengthExceed
	}

	msg_total_length := msg_data_length + HEAD_LENGTH
	if msg_total_length <= length {
		bs := self.buffer[self.start : self.start+msg_total_length]
		self.start += msg_total_length
		return true, Message(bs), nil
	}

	msg_residue := msg_total_length - length
	buf_reserve := len(self.buffer) - self.end
	if msg_residue > buf_reserve {
		if msg_total_length > 2*(MAX_MESSAGE_LENGTH+HEAD_LENGTH) {
			return false, nil, fmt.Errorf("ensureCapacity failed: %v", msg_total_length)
		}

		self.ensureCapacity(msg_total_length)
	}
	return false, nil, nil
}

func (self *BufferedMessageReader) ReadMessage() (Message, error) {
	ok, msg, err := self.nextMessage()
	if ok {
		return msg, nil
	}
	if err != nil {
		return nil, err
	}

	n, err := self.conn.Read(self.buffer[self.end:])
	if err != nil {
		if n <= 0 {
			return nil, err
		}
		self.conn = &errReader{err: err}
	}

	self.end += n
	ok, msg, err = self.nextMessage()
	if ok {
		return msg, nil
	}
	return nil, err
}

func NewMessageReader(conn io.Reader, size int) *BufferedMessageReader {
	return &BufferedMessageReader{
		conn:        conn,
		buffer:      MakeBytes(size),
		start:       0,
		end:         0,
		buffer_size: size,
	}
}

type MeesageBuilder struct {
	buffer []byte
}

func (self *MeesageBuilder) Init(cmd byte, capacity int) {
	self.buffer = MakeBytes(HEAD_LENGTH + capacity)
	self.buffer[0] = cmd
	self.buffer[1] = ' '
	self.buffer[7] = '\n'
	self.buffer = self.buffer[:HEAD_LENGTH]
}

func (self *MeesageBuilder) Append(bs []byte) *MeesageBuilder {
	if len(self.buffer)+len(bs) > MAX_MESSAGE_LENGTH {
		panic(ErrLengthExceed)
	}

	self.buffer = append(self.buffer, bs...)
	return self
}

func (self *MeesageBuilder) Write(bs []byte) (int, error) {
	if len(self.buffer)+len(bs) > MAX_MESSAGE_LENGTH {
		return 0, ErrLengthExceed
	}

	self.buffer = append(self.buffer, bs...)
	return len(bs), nil
}

func (self *MeesageBuilder) WriteString(s string) (int, error) {
	if len(self.buffer)+len(s) > MAX_MESSAGE_LENGTH {
		return 0, ErrLengthExceed
	}

	self.buffer = append(self.buffer, s...)
	return len(s), nil
}

func (self *MeesageBuilder) Build() Message {
	return BuildMessage(self.buffer)
}

func BuildMessageWith(cmd byte, buffer *bytes.Buffer) Message {
	bs := buffer.Bytes()
	bs[0] = cmd
	return BuildMessage(bs)
}

func BuildMessage(buffer []byte) Message {
	length := len(buffer) - HEAD_LENGTH
	//if length < 65535 {
	//	buffer = append(buffer, '\n')
	//	length++
	//}

	switch {
	case length > 65535:
		panic(ErrLengthExceed)
	case length >= 10000:
		buffer[2] = '0' + byte(length/10000)
		buffer[3] = '0' + byte((length%10000)/1000)
		buffer[4] = '0' + byte((length%1000)/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 1000:
		buffer[2] = ' '
		buffer[3] = '0' + byte(length/1000)
		buffer[4] = '0' + byte((length%1000)/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 100:
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = '0' + byte(length/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 10:
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = ' '
		buffer[5] = '0' + byte(length/10)
		buffer[6] = '0' + byte(length%10)
	default:
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = ' '
		buffer[5] = ' '
		buffer[6] = '0' + byte(length)
	}
	return Message(buffer)
}

func NewMessageWriter(cmd byte, capacity int) *MeesageBuilder {
	builder := &MeesageBuilder{}
	builder.Init(cmd, capacity)
	return builder
}

func BuildErrorMessage(msg string) Message {
	var builder MeesageBuilder
	builder.Init(MSG_ERROR, len(msg))
	builder.WriteString(msg)
	return builder.Build()
}

func SendFull(conn io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func ToError(msg Message) error {
	if MSG_ERROR != msg.Command() {
		panic(errors.New("it isn't a error message."))
	}
	if msg.DataLength() == 0 {
		return ErrEmptyString
	}
	return errors.New(string(msg.Data()))
}

type FixedReader struct {
	conn io.Reader
}

func (self *FixedReader) Init(conn io.Reader) {
	self.conn = conn
}

func (self *FixedReader) ReadMessage() (Message, error) {
	return ReadMessage(self.conn)
}

func ReadMessage(rd io.Reader) (Message, error) {
	var head_buf [HEAD_LENGTH]byte

	_, err := io.ReadFull(rd, head_buf[:])
	if err != nil {
		return nil, err
	}

	msg_data_length, err := readLength(head_buf[2:])
	if err != nil {
		return nil, err
	}

	msg_bytes := MakeBytes(HEAD_LENGTH + msg_data_length)
	_, err = io.ReadFull(rd, msg_bytes[HEAD_LENGTH:])
	if err != nil {
		return nil, err
	}
	copy(msg_bytes, head_buf[:])
	return msg_bytes, nil
}

func SendMagic(w io.Writer) error {
	return SendFull(w, HEAD_MAGIC)
}

func ReadMagic(r io.Reader) error {
	var buf [MAGIC_LENGTH]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	if !bytes.Equal(buf[:], HEAD_MAGIC) {
		return ErrMagicNumber
	}
	return nil
}

type FixedMessageReader struct {
	conn   io.Reader
	buffer [2 * HEAD_LENGTH]byte
	length int
}

func (self *FixedMessageReader) ReadMessage() (Message, error) {
	//if self.stage == 0 {
	if self.length < HEAD_LENGTH {
		n, err := self.conn.Read(self.buffer[self.length:])
		if err != nil {
			if n <= 0 {
				return nil, err
			}
			self.conn = &errReader{err: err}
		}
		self.length += n
		if self.length < HEAD_LENGTH {
			return nil, err
		}
	}

	msg_data_length, err := readLength(self.buffer[2:])
	if err != nil {
		return nil, err
	}

	bs := MakeBytes(msg_data_length + HEAD_LENGTH)
	//}

	message_length := (msg_data_length + HEAD_LENGTH)
	if message_length <= 2*HEAD_LENGTH {
		copy(bs, self.buffer[:message_length])
		self.length -= message_length
		return Message(bs), nil
	}

	_, err = io.ReadFull(self.conn, bs[self.length:message_length])
	self.length = 0
	if err != nil {
		return nil, err
	}

	return Message(bs), nil
}

type BatchMessages struct {
	buffer *bytes.Buffer
}

func (self *BatchMessages) Init(buffer *bytes.Buffer) {
	buffer.Reset()
	self.buffer = buffer
}

func (self *BatchMessages) New(cmd byte) Builder {
	offset := self.buffer.Len()
	self.buffer.Write(MSG_DATA_EMPTY_HEADER_BYTES)
	return Builder{self.buffer, offset, cmd}
}

func (self *BatchMessages) ToBytes() []byte { return self.buffer.Bytes() }

type Builder struct {
	buffer *bytes.Buffer
	offset int
	cmd    byte
}

func (self Builder) WriteString(data string) (int, error) {
	return self.buffer.WriteString(data)
}

func (self Builder) Write(data []byte) (int, error) {
	return self.buffer.Write(data)
}

func (self Builder) Truncate(n int) {
	self.buffer.Truncate(n)
}

func (self Builder) Len() int {
	return self.buffer.Len()
}

func (self Builder) Bytes() []byte {
	return self.buffer.Bytes()
}

func (self Builder) WriteByte(b byte) error {
	return self.buffer.WriteByte(b)
}

func (self Builder) Close() error {
	bs := self.buffer.Bytes()[self.offset:]
	bs[0] = self.cmd
	BuildMessage(bs)
	return nil
}
