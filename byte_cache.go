package client

import "sync"

var (
	bytes_512b sync.Pool
	bytes_1k   sync.Pool
	bytes_2k   sync.Pool
	bytes_4k   sync.Pool
)

func init() {
	bytes_512b.New = func() interface{} {
		return make([]byte, 512-48)
	}
	bytes_1k.New = func() interface{} {
		return make([]byte, 1024-48)
	}
	bytes_2k.New = func() interface{} {
		return make([]byte, 2048-48)
	}
	bytes_4k.New = func() interface{} {
		return make([]byte, 4096-48)
	}
}

func MakeBytes(size int) []byte {
	switch {
	case size <= 512-46:
		return bytes_512b.Get().([]byte)
	case size <= 1024-46:
		return bytes_1k.Get().([]byte)
	case size <= 2048-46:
		return bytes_2k.Get().([]byte)
	case size <= 4096-46:
		return bytes_4k.Get().([]byte)
	}
	return make([]byte, size)
}

func FreeBytes(bs []byte) {
	size := len(bs)
	if size > 4*1024 {
		return
	}
	if size >= 4*1024-48 {
		bytes_4k.Put(bs)
		return
	}
	if size >= 2*1024-48 {
		bytes_2k.Put(bs)
		return
	}
	if size >= 1*1024-48 {
		bytes_1k.Put(bs)
		return
	}
	if size > 512-48 {
		bytes_512b.Put(bs)
		return
	}
}
