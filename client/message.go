package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// message format
// 'a' 'a' 'v' version
// command  ' ' length ‘\n’ data
// command is a byte.
// version is a byte.
// length  is a fixed length string (5 byte).
// data    is a byte array, length is between 0 to 65523 (65535-12).
//

const MAGIC_LENGTH = 4
const MAX_ENVELOPE_LENGTH = math.MaxInt32
const MAX_MESSAGE_LENGTH = math.MaxInt32 - HEAD_LENGTH
const HEAD_LENGTH = 8
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

// Message - 一个消息的数据
type Message []byte

// Command - 返回命令号
func (msg Message) Command() byte {
	return msg[0]
}

// DataLength - 获取消息的数据部份的长度
func (msg Message) DataLength() int {
	return len(msg) - HEAD_LENGTH
}

// Data - 获取消息的数据部份
func (msg Message) Data() []byte {
	return msg[HEAD_LENGTH:]
}

// ToBytes - 转换为字节数组
func (msg Message) ToBytes() []byte {
	return msg
}

// MessageReader - 读消息的接口
type MessageReader interface {
	ReadMessage() (Message, error)
}

type errReader struct {
	err error
}

func (r *errReader) Read(bs []byte) (int, error) {
	return 0, r.err
}

// type BufferedMessageReader struct {
// 	conn   io.Reader
// 	buffer []byte
// 	start  int
// 	end    int

// 	buffer_size int
// }

// func (self *BufferedMessageReader) DataLength() int {
// 	return self.end - self.start
// }

// func (self *BufferedMessageReader) Init(conn io.Reader, size int) {
// 	self.conn = conn
// 	self.buffer = MakeBytes(size)
// 	self.start = 0
// 	self.end = 0
// 	self.buffer_size = size
// }

// func (self *BufferedMessageReader) ensureCapacity(size int) {
// 	//fmt.Println("ensureCapacity", size, self.buffer_size)
// 	if self.buffer_size > size {
// 		size = self.buffer_size
// 	}
// 	//fmt.Println("ensureCapacity", size, self.buffer_size)
// 	tmp := MakeBytes(size)
// 	self.end = copy(tmp, self.buffer[self.start:self.end])
// 	self.start = 0
// 	self.buffer = tmp
// 	//fmt.Println(len(tmp))
// }

// func (self *BufferedMessageReader) nextMessage() (bool, Message, error) {
// 	length := self.end - self.start
// 	if length < HEAD_LENGTH {
// 		buf_reserve := len(self.buffer) - self.end
// 		if buf_reserve < (HEAD_LENGTH + 16) {
// 			self.ensureCapacity(256)
// 		}

// 		return false, nil, nil
// 	}

// 	msg_data_length, err := readLength(self.buffer[self.start:])
// 	if err != nil {
// 		return false, nil, err
// 	}
// 	//fmt.Println(msg_data_length, length, self.end, self.start, len(self.buffer))

// 	//if msg_data_length > MAX_MESSAGE_LENGTH {
// 	//	return false, nil, ErrLengthExceed
// 	//}

// 	msg_total_length := msg_data_length + HEAD_LENGTH
// 	if msg_total_length <= length {
// 		bs := self.buffer[self.start : self.start+msg_total_length]
// 		self.start += msg_total_length
// 		return true, Message(bs), nil
// 	}

// 	msg_residue := msg_total_length - length
// 	buf_reserve := len(self.buffer) - self.end
// 	if msg_residue > buf_reserve {
// 		//if msg_total_length > 2*(MAX_MESSAGE_LENGTH+HEAD_LENGTH) {
// 		//	return false, nil, fmt.Errorf("ensureCapacity failed: %v", msg_total_length)
// 		//}

// 		self.ensureCapacity(msg_total_length)
// 	}
// 	return false, nil, nil
// }

// func (self *BufferedMessageReader) ReadMessage() (Message, error) {
// 	ok, msg, err := self.nextMessage()
// 	if ok {
// 		return msg, nil
// 	}
// 	if err != nil {
// 		return nil, err
// 	}

// 	n, err := self.conn.Read(self.buffer[self.end:])
// 	if err != nil {
// 		if n <= 0 {
// 			return nil, err
// 		}
// 		self.conn = &errReader{err: err}
// 	}

// 	self.end += n
// 	ok, msg, err = self.nextMessage()
// 	if ok {
// 		return msg, nil
// 	}
// 	return nil, err
// }

// func NewMessageReader(conn io.Reader, size int) *BufferedMessageReader {
// 	return &BufferedMessageReader{
// 		conn:        conn,
// 		buffer:      MakeBytes(size),
// 		start:       0,
// 		end:         0,
// 		buffer_size: size,
// 	}
// }

// MessageBuilder - 消息的创建工厂
type MessageBuilder struct {
	buffer []byte
}

// Init - 初始化消息工厂
func (builder *MessageBuilder) Init(cmd byte, capacity int) {
	builder.buffer = MakeBytes(HEAD_LENGTH + uint(capacity))
	builder.buffer[0] = cmd
	builder.buffer[1] = ' '
	builder.buffer[7] = '\n'
	builder.buffer = builder.buffer[:HEAD_LENGTH]
}

// Append - 将字节追加到消息体的未尾
func (builder *MessageBuilder) Append(bs []byte) *MessageBuilder {
	if len(builder.buffer)+len(bs) > MAX_MESSAGE_LENGTH {
		panic(ErrLengthExceed)
	}

	builder.buffer = append(builder.buffer, bs...)
	return builder
}

// Write - 将字节追加到消息体的未尾
func (builder *MessageBuilder) Write(bs []byte) (int, error) {
	if len(builder.buffer)+len(bs) > MAX_MESSAGE_LENGTH {
		return 0, ErrLengthExceed
	}

	builder.buffer = append(builder.buffer, bs...)
	return len(bs), nil
}

// WriteString - 将字节追加到消息体的未尾
func (builder *MessageBuilder) WriteString(s string) (int, error) {
	if len(builder.buffer)+len(s) > MAX_MESSAGE_LENGTH {
		return 0, ErrLengthExceed
	}

	builder.buffer = append(builder.buffer, s...)
	return len(s), nil
}

// Build - 创建消息
func (builder *MessageBuilder) Build() Message {
	return BuildMessage(builder.buffer)
}

func BuildMessageWith(cmd byte, buffer *bytes.Buffer) Message {
	bs := buffer.Bytes()
	bs[0] = cmd
	return BuildMessage(bs)
}

func BuildMessage(buffer []byte) Message {
	msg, err := CreateMessage(buffer)
	if err != nil {
		panic(err)
	}
	return msg
}

func CreateMessageWith(cmd byte, buffer *bytes.Buffer) (Message, error) {
	bs := buffer.Bytes()
	bs[0] = cmd
	return CreateMessage(bs)
}

func CreateMessage(buffer []byte) (Message, error) {
	length := len(buffer) - HEAD_LENGTH
	//if length < 65535 {
	//	buffer = append(buffer, '\n')
	//	length++
	//}

	switch {
	case uint64(length) > math.MaxUint32:
		return nil, ErrLengthExceed
	case length > 65535:
		buffer[1] = 1
		binary.BigEndian.PutUint32(buffer[4:], uint32(length))
	case length >= 10000:
		buffer[1] = ' '
		buffer[2] = '0' + byte(length/10000)
		buffer[3] = '0' + byte((length%10000)/1000)
		buffer[4] = '0' + byte((length%1000)/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 1000:
		buffer[1] = ' '
		buffer[2] = ' '
		buffer[3] = '0' + byte(length/1000)
		buffer[4] = '0' + byte((length%1000)/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 100:
		buffer[1] = ' '
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = '0' + byte(length/100)
		buffer[5] = '0' + byte((length%100)/10)
		buffer[6] = '0' + byte(length%10)
	case length >= 10:
		buffer[1] = ' '
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = ' '
		buffer[5] = '0' + byte(length/10)
		buffer[6] = '0' + byte(length%10)
	default:
		buffer[1] = ' '
		buffer[2] = ' '
		buffer[3] = ' '
		buffer[4] = ' '
		buffer[5] = ' '
		buffer[6] = '0' + byte(length)
	}
	return Message(buffer), nil
}

func readLength(bs []byte) (uint, error) {
	if 1 == bs[1] {
		return uint(binary.BigEndian.Uint32(bs[4:])), nil
	}

	start := 2
	for ' ' == bs[start] {
		start++
	}
	if start >= HEAD_LENGTH {
		return 0, ErrLengthNotDigit
	}

	length := uint(bs[start] - '0')
	if length > 9 {
		return 0, ErrLengthNotDigit
	}
	start++
	for ; start < (HEAD_LENGTH - 1); start++ {
		l := uint(bs[start] - '0')
		if l > 9 {
			return 0, ErrLengthNotDigit
		}
		length *= 10
		length += l
	}
	return length, nil
}

func NewMessageWriter(cmd byte, capacity int) *MessageBuilder {
	builder := &MessageBuilder{}
	builder.Init(cmd, capacity)
	return builder
}

func BuildErrorMessage(msg string) Message {
	var builder MessageBuilder
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

func (r *FixedReader) Init(conn io.Reader) {
	r.conn = conn
}

func (r *FixedReader) ReadMessage() (Message, error) {
	return ReadMessage(r.conn)
}

func ReadMessage(rd io.Reader) (Message, error) {
	var headBuffer [HEAD_LENGTH]byte

	_, err := io.ReadFull(rd, headBuffer[:])
	if err != nil {
		return nil, err
	}

	dataLength, err := readLength(headBuffer[:])
	if err != nil {
		return nil, err
	}

	msgBytes := MakeBytes(HEAD_LENGTH + dataLength)
	_, err = io.ReadFull(rd, msgBytes[HEAD_LENGTH:])
	if err != nil {
		return nil, err
	}
	copy(msgBytes, headBuffer[:])
	return msgBytes, nil
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
	length uint
}

func (r *FixedMessageReader) Init(conn io.Reader) *FixedMessageReader {
	r.conn = conn
	return r
}

func (r *FixedMessageReader) ReadMessage() (Message, error) {
	if r.length < HEAD_LENGTH {
		n, err := r.conn.Read(r.buffer[r.length:])
		if err != nil {
			if n <= 0 {
				return nil, err
			}
			r.conn = &errReader{err: err}
		}
		r.length += uint(n)
		if r.length < HEAD_LENGTH {
			return nil, err
		}
	}

	dataLength, err := readLength(r.buffer[:])
	if err != nil {
		return nil, err
	}

	messageLength := (dataLength + HEAD_LENGTH)
	bs := MakeBytes(messageLength)

	if messageLength <= r.length {
		copy(bs, r.buffer[:messageLength])
		copy(r.buffer[:], r.buffer[messageLength:])
		r.length -= messageLength
		return Message(bs), nil
	}

	copy(bs, r.buffer[:r.length])
	_, err = io.ReadFull(r.conn, bs[r.length:messageLength])
	r.length = 0
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

func NewMessageReader(r io.Reader, capacity int) MessageReader {
	return &FixedMessageReader{conn: r}
}
