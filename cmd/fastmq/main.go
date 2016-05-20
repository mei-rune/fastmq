package main

import (
	"fastmq/server"
	"log"
)

func main() {
	opt := &server.Options{HttpEnabled: true}

	srv, err := server.NewServer(opt)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer srv.Close()

	srv.Wait()
}
