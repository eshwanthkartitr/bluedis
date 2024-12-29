package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"
	"strconv"
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
		if value.typ == "array" && len(value.array) > 0 {
			command := strings.ToUpper(value.array[0].bulk)
			args := value.array[1:]

			switch command {
			case "SET":
				if len(args) >= 2 {
					key := args[0].bulk
					val := args[1].bulk
					SETsMu.Lock()
					currentVal := Values{Content : val, HasExpiry: false,}
					SETs[key] = currentVal
					SETsMu.Unlock()
					// Handle EX/PX during reconstruction
					for i := 2; i < len(args); i += 2 {
						if i+1 < len(args) {
							switch strings.ToUpper(args[i].bulk) {
							case "EX":
								seconds, _ := strconv.Atoi(args[i+1].bulk)
								SETsMu.Lock()
								currentVal := SETs[key]
								currentVal.Begone = time.Now().Add(time.Duration(seconds) * time.Second)
								currentVal.HasExpiry= true
								SETs[key] = currentVal
								SETsMu.Unlock()
							case "PX":
								milliseconds, _ := strconv.ParseInt(args[i+1].bulk, 10, 64)
								SETsMu.Lock()
								currentVal := SETs[key]
								currentVal.Begone = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
								currentVal.HasExpiry= true
								SETs[key] = currentVal
								SETsMu.Unlock()
							}
						}
					}
				}
			case "EXPIRE":
				if len(args) >= 2 {
					key := args[0].bulk
					seconds, _ := strconv.Atoi(args[1].bulk)
					expiryTime := time.Now().Add(time.Duration(seconds) * time.Second)
					SETsMu.Lock()
					fmt.Println("EXPIRE: key=", key, "expiryTime=", expiryTime, "SETs[key]=", SETs[key])
					if val, ok := SETs[key]; ok {
						val.HasExpiry = true
						val.Begone = expiryTime
						SETs[key] = val
					}
					SETsMu.Unlock()
				}
			case "DEL":
				for _, arg := range args {
					SETsMu.Lock()
					delete(SETs, arg.bulk)
					SETsMu.Unlock()
				}
			}
		}
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

			if command  == "EXPIRE" {
				// Expire command
				result := expireHandler(args)
				fmt.Println(args)
				if result.typ == "integer" && result.num == 1 {
					num,err := strconv.Atoi(args[1].bulk)
					aof.WriteExpire(args[0].bulk,num,args[2].bulk) // Write EXPIRE to AOF if successful
					if err != nil {
						fmt.Println(err)
					}
				}
				writer.Write(result)
				continue

				// expire(args)
				continue
			}

			if command == "DEL"{
				result := Delete(args)
				if result.typ == "integer" && result.num > 0 {
					keys := make([]string, len(args))
					for i, arg := range args {
						keys[i] = arg.bulk
					}
					aof.WriteDel(keys) // DEL to AOF if successful
				}
				writer.Write(result)
				continue
			}

			// Append "write" commands to AOF
			if command == "SET" || command == "HSET" || command == "LPUSH" || command == "RPUSH" || command == "LPOP" || command == "RPOP" || command == "BLPOP" {
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