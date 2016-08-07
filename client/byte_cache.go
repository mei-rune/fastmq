package client

import "sync"

var (
	bytes_64b  sync.Pool
	bytes_128b sync.Pool
	bytes_256b sync.Pool
	bytes_512b sync.Pool
	bytes_1k   sync.Pool
	bytes_2k   sync.Pool
	bytes_4k   sync.Pool
)

func init() {
	bytes_64b.New = func() interface{} {
		return make([]byte, 64)
	}
	bytes_128b.New = func() interface{} {
		return make([]byte, 128)
	}
	bytes_256b.New = func() interface{} {
		return make([]byte, 256)
	}
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
	case size <= 64:
		return bytes_64b.Get().([]byte)[:size]
	case size <= 128:
		return bytes_128b.Get().([]byte)[:size]
	case size <= 256:
		return bytes_256b.Get().([]byte)[:size]
	case size <= 512-48:
		return bytes_512b.Get().([]byte)[:size]
	case size <= 1024-48:
		return bytes_1k.Get().([]byte)[:size]
	case size <= 2048-48:
		return bytes_2k.Get().([]byte)[:size]
	case size <= 4096-48:
		return bytes_4k.Get().([]byte)[:size]
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
