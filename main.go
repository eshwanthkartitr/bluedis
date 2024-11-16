package main

import (
	"fmt"
	"io"
	"net"
	"strings"
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

	// Writer allocation for writing back to redis-cli
	writer := NewWriter(conn)

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

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args)
		err = writer.Write(result)
		if err != nil {
			fmt.Println(err)
		}
	}
}
