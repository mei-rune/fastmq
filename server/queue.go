package server

import "io"

type Channel interface {
	Send(msg Message) error

	On(cb func(msg Message)) (io.Closer, error)
}

type Queue struct {
}

func creatQueue(srv *Server, name string) *Queue {

}
