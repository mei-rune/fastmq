package client

import (
	"errors"
	"io"
	"net"
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

var (
	HEAD_MAGIC      = []byte{'a', 'a', 'v', '1'}
	MSG_NOOP_BYTES  = []byte{MSG_NOOP, ' ', ' ', ' ', ' ', ' ', '0', '\n'}
	MSG_ACK_BYTES   = []byte{MSG_ACK, ' ', ' ', ' ', ' ', ' ', '0', '\n'}
	MSG_CLOSE_BYTES = []byte{MSG_CLOSE, ' ', ' ', ' ', ' ', ' ', '0', '\n'}

	ErrMoreThanMaxRead   = errors.New("more than maximum read.")
	ErrUnexceptedMessage = errors.New("recv a unexcepted message.")
	ErrUnexceptedAck     = errors.New("recv a unexcepted ack message.")
	ErrEmptyString       = errors.New("empty error message.")
	ErrMagicNumber       = errors.New("magic number is error.")
	ErrLengthExceed      = errors.New("message length is exceed.")
	ErrLengthNotDigit    = errors.New("length field of message isn't number.")
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

type Reader struct {
	conn   io.Reader
	buffer []byte
	start  int
	end    int

	buffer_size int
}

func (self *Reader) Init(conn io.Reader, size int) {
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

func (self *Reader) ensureCapacity(size int) {
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

func (self *Reader) nextMessage() (bool, Message, error) {
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
		self.ensureCapacity(msg_total_length)
	}
	return false, nil, nil
}

func (self *Reader) ReadMessage() (Message, error) {
	ok, msg, err := self.nextMessage()
	if ok {
		return msg, nil
	}
	if err != nil {
		return nil, err
	}

	n, err := self.conn.Read(self.buffer[self.end:])
	if err != nil {
		return nil, err
	}

	self.end += n
	ok, msg, err = self.nextMessage()
	if ok {
		return msg, nil
	}
	return nil, err
}

func NewMessageReader(conn io.Reader, size int) *Reader {
	return &Reader{
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
	length := len(self.buffer) - HEAD_LENGTH
	//if length < 65535 {
	//	self.buffer = append(self.buffer, '\n')
	//	length++
	//}

	switch {
	case length > 65535:
		panic(ErrLengthExceed)
	case length >= 10000:
		self.buffer[2] = '0' + byte(length/10000)
		self.buffer[3] = '0' + byte((length%10000)/1000)
		self.buffer[4] = '0' + byte((length%1000)/100)
		self.buffer[5] = '0' + byte((length%100)/10)
		self.buffer[6] = '0' + byte(length%10)
	case length >= 1000:
		self.buffer[2] = ' '
		self.buffer[3] = '0' + byte(length/1000)
		self.buffer[4] = '0' + byte((length%1000)/100)
		self.buffer[5] = '0' + byte((length%100)/10)
		self.buffer[6] = '0' + byte(length%10)
	case length >= 100:
		self.buffer[2] = ' '
		self.buffer[3] = ' '
		self.buffer[4] = '0' + byte(length/100)
		self.buffer[5] = '0' + byte((length%100)/10)
		self.buffer[6] = '0' + byte(length%10)
	case length >= 10:
		self.buffer[2] = ' '
		self.buffer[3] = ' '
		self.buffer[4] = ' '
		self.buffer[5] = '0' + byte(length/10)
		self.buffer[6] = '0' + byte(length%10)
	default:
		self.buffer[2] = ' '
		self.buffer[3] = ' '
		self.buffer[4] = ' '
		self.buffer[5] = ' '
		self.buffer[6] = '0' + byte(length)
	}
	return Message(self.buffer)
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

func SendFull(conn net.Conn, data []byte) error {
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
