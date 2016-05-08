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
	ID         int64  `flag:"worker-id" cfg:"id"`
	Verbose    bool   `flag:"verbose"`
	TCPAddress string `flag:"tcp-address"`

	// msg and command options
	MsgBufferSize int
	MsgTimeout    time.Duration `flag:"msg-timeout" arg:"1ms"`

	Logger *log.Logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:         defaultID,
		TCPAddress: "0.0.0.0:4150",
		Logger:     log.New(os.Stderr, "[aa] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}
