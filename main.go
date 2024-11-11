package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {

	// Creating a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Listening for new connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// Create an infinite for-loop so that we can keep listening to the port
	// constantly, receive commands from clients and respond to them
	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)
		if err != nil {
			// While reading from a connection, we usually hit the EOF error after the
			// entire data has been read. This needs to be handled properly
			if err == io.EOF {
				break
			}

			// If we are getting an error which is not of the type EOF then we need
			// to print it out and crash the program
			fmt.Println("error reading from client: ", err.Error())
			os.Exit(1)
		}

		// Not responding to request but sending OK regardless of command received
		// from client
		conn.Write([]byte("+OK\r\n"))
	}
}
