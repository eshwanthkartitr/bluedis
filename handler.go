package main

import (
	"fmt"
	"sync"
)

// Redis commands are case-sensitive
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'set' command",
		}
	}

	key := args[0].bulk
	value := args[1].bulk

	// In Go, maps can be concurrently accessed by multiple goroutines and this
	// can cause race conditions. Our server is supposed to handle requests
	// concurrently. Therefore, during map updation, we setup a
	// Mutex lock and after the updation, we unlock it. This lock works at a thread
	// level and restricts other threads to modify the resource.

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'set' command",
		}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{
		typ:  "bulk",
		bulk: value,
	}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hset' command",
		}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	// If the hash does not already exist, then create a new entry
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{
		typ: "string",
		str: "OK",
	}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hget' command",
		}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{
		typ:  "bulk",
		bulk: value,
	}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hgetall' command",
		}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: fmt.Sprintf("%s:%s", k, v)})
	}

	return Value{
		typ:   "array",
		array: values,
	}
}
