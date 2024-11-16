package main

import (
	"fmt"
	"io"
	"net"
)

func main() {

	// Creating a new server / listener
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Listening on PORT: 6379")

	// Listening for new connections (this is a blocking connection) and whenever
	// a connection is made then an acceptance is established using Accept()
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// Create an infinite for-loop so that we can keep listening to the port
	// constantly, receive commands from clients and respond to them
	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection dropped to Bluedis server.")
				return
			}
			fmt.Println(err)
			return
		}
		fmt.Println(value)

		// Not responding to request but sending OK regardless of command received
		// from client
		conn.Write([]byte("+OK\r\n"))
	}
}
