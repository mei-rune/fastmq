package server

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type Options struct {
	// basic options
	ID         int64
	Verbose    bool
	TCPAddress string

	// msg and command options
	MsgBufferSize    int
	MsgTimeout       time.Duration
	MsgQueueCapacity int

	HttpEnabled     bool
	HttpPrefix      string
	HttpRedirectUrl string
	HttpHandler     interface{}

	Logger *log.Logger
}

func (self *Options) ensureDefault() {
	if self.ID == 0 {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}

		h := md5.New()
		io.WriteString(h, hostname)
		self.ID = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	}

	if self.TCPAddress == "" {
		self.TCPAddress = ":4150"
	}

	if self.MsgBufferSize <= 8 {
		self.MsgBufferSize = 8
	}

	if self.MsgTimeout <= 0 {
		self.MsgTimeout = 1 * time.Second
	}
	if self.MsgQueueCapacity <= 0 {
		self.MsgQueueCapacity = 200
	}

	if self.HttpHandler != nil {
		self.HttpEnabled = true
	}

	if self.Logger == nil {
		self.Logger = log.New(os.Stderr, "[aa] ", log.Ldate|log.Ltime|log.Lmicroseconds)
	}
}
