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

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	// Persistance added and database automatically reconstructs from AOF
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		fmt.Println("Arguments passed :",args)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}
		handler(args)
	})

	// When a connection drops, we continue listening for a new connection
	for {
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
					fmt.Println("Client disconnected from Bluedis server.")
					break
				}
				fmt.Println(err)
				break
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
			// Redis sends an initial command when connecting, handling it
			if command == "COMMAND" || command == "RETRY" {
				fmt.Println("Client connected to Bluedis server!")
				writer.Write(Value{typ: "string", str: ""})
				continue
			}
			if !ok {
				fmt.Println("Invalid command: ", command)
				writer.Write(Value{typ: "string", str: ""})
				continue
			}

			// Append "write" commands to AOF
			if command == "SET" || command == "HSET" || command == "LPUSH" || command == "RPUSH" || command == "LPOP" || command == "RPOP" {
				aof.Write(value)
			}
			

			result := handler(args)
			err = writer.Write(result)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

}
