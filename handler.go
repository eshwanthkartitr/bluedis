package main

import (
	"fmt"
	"sync"
	"strconv" // Add this import statement
)

// Redis commands are case-sensitive
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
    "LPUSH":   lpush,
    "LPOP":    lpop,
    //"RPUSH":   rpush,
    //"RPOP":    rpop,
    //"LLEN":    llen,
    //"LRANGE":  lrange,
    //"BLPOP":   blpop,
}


func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
    fmt.Println("Ping received")
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
			str: "ERR wrong number of arguments for 'get' command",
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

// Define listStore as a map where the keys are strings and the values are pointers to DoublyLinkedList
var listStore = make(map[string]*DoublyLinkedList)
var listStoreMu sync.Mutex

func lpush(args []Value) Value {
    if len(args) != 2 {
        return Value{typ: "error", str: "ERR wrong number of arguments for 'lpush' command"}
    }

    key := args[0].bulk
    value := args[1].bulk

    listStoreMu.Lock()
    list, exists := listStore[key]
    if !exists {
        list = NewDoublyLinkedList()
        listStore[key] = list
    }
    listStoreMu.Unlock()

    list.mu.Lock()
    length := list.PushLeft(value)
    list.mu.Unlock()

    return Value{typ: "integer", num: length}
}

func lpop(args []Value) Value {
    if len(args) < 1 || len(args) > 2 {
        return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command"}
    }

    key := args[0].bulk
    count := 1 // Default to popping one element
    if len(args) == 2 {
        var err error
        count, err = strconv.Atoi(args[1].bulk)
        if err != nil || count <= 0 {
            return Value{typ: "error", str: "ERR invalid count argument for 'lpop' command"}
        }
    }

    listStoreMu.Lock()
    list, exists := listStore[key]
    if !exists || list.Length() == 0 {
        listStoreMu.Unlock()
        return Value{typ: "null"}
    }
    listStoreMu.Unlock()

    list.mu.Lock()
    result := make([]Value, 0, count)
    for i := 0; i < count && list.Length() > 0; i++ {
        value, _ := list.PopLeft()
        result = append(result, Value{typ: "bulk", bulk: fmt.Sprintf("%v", value)})
    }
    list.mu.Unlock()

    // If only one element is popped, return it as a bulk string wrapped in a Value
    if len(result) == 1 {
        return Value{typ: "bulk", bulk: result[0].bulk}
    }
    // Otherwise, return an array of bulk strings
    return Value{typ: "array", array: result}
}


// func rpush(args []Value) Value {
//     if len(args) < 2 {
//         return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command"}
//     }

//     key := args[0].bulk
//     elements := args[1:]

//     listStoreMu.Lock()
//     list, exists := listStore[key]
//     if !exists {
//         list = NewDoublyLinkedList()
//         listStore[key] = list
//     }
//     for _, element := range elements {
//         list.PushRight(element.bulk)
//     }
//     length := list.Length()
//     listStoreMu.Unlock()

//     return Value{
// 		typ: "integer",
// 		num: length,
// 	}
// }

// func rpop(args []Value) Value {
//     if len(args) < 1 || len(args) > 2 {
//         return Value{typ: "error", str: "ERR wrong number of arguments for 'rpop' command"}
//     }

//     key := args[0].bulk
//     count := 1
//     if len(args) == 2 {
//         var err error
//         count, err = strconv.Atoi(args[1].bulk)
//         if err != nil || count <= 0 {
//             return Value{typ: "error", str: "ERR invalid count argument for 'rpop' command"}
//         }
//     }

//     listStoreMu.Lock()
//     list, exists := listStore[key]
//     if !exists || list.Length() == 0 {
//         listStoreMu.Unlock()
//         return Value{typ: "null"}
//     }

//     result := make([]Value, 0, count)
//     for i := 0; i < count && list.Length() > 0; i++ {
//         value, _ := list.PopRight()
//         result = append(result, Value{typ: "bulk", bulk: fmt.Sprintf("%v", value)})
//     }
//     listStoreMu.Unlock()

//     if len(result) == 1 {
//         return result[0]
//     }
//     return Value{
// 		typ: "array",
// 	 	array: result,
// 	}
// }

// func llen(args []Value) Value {
//     if len(args) != 1 {
//         return Value{typ: "error", str: "ERR wrong number of arguments for 'llen' command"}
//     }

//     key := args[0].bulk

//     listStoreMu.Lock()
//     list, exists := listStore[key]
//     length := 0
//     if exists {
//         length = list.Length()
//     }
//     listStoreMu.Unlock()

//     return Value{
// 		typ: "integer",
// 		num: length,
// 	}
// }

// func lrange(args []Value) Value {
//     if len(args) != 3 {
//         return Value{typ: "error", str: "ERR wrong number of arguments for 'lrange' command"}
//     }

//     key := args[0].bulk
//     start, err1 := strconv.Atoi(args[1].bulk)
//     end, err2 := strconv.Atoi(args[2].bulk)
//     if err1 != nil || err2 != nil {
//         return Value{typ: "error", str: "ERR invalid arguments for 'lrange' command"}
//     }

//     listStoreMu.Lock()
//     list, exists := listStore[key]
//     if !exists {
//         listStoreMu.Unlock()
//         return Value{
// 			typ: "array", 
// 			array: []Value{},
// 		}
//     }

//     values := list.ExtractRange(start, end)
//     result := make([]Value, len(values))
//     for i, v := range values {
//         result[i] = Value{typ: "bulk", bulk: fmt.Sprintf("%v", v)}
//     }
//     listStoreMu.Unlock()

//     return Value{
// 		typ: "array",
// 		array: result,
// 	}
// }

// func blpop(args []Value) Value {
//     if len(args) < 2 {
//         return Value{typ: "error", str: "ERR wrong number of arguments for 'blpop' command"}
//     }

//     keys := args[:len(args)-1]
//     timeout, err := strconv.Atoi(args[len(args)-1].bulk)
//     if err != nil || timeout < 0 {
//         return Value{typ: "error", str: "ERR invalid timeout argument for 'blpop' command"}
//     }

//     // Add blocking logic for BLPOP here (simplified version provided for now)
//     // Iterates over provided keys to find the first non-empty list.
//     listStoreMu.Lock()
//     for _, key := range keys {
//         list, exists := listStore[key.bulk]
//         if exists && list.Length() > 0 {
//             value, _ := list.PopLeft()
//             listStoreMu.Unlock()
//             return Value{
// 				typ: "bulk",
// 				bulk: fmt.Sprintf("%v", value),
// 			}
//         }
//     }
//     listStoreMu.Unlock()

//     // Simulate blocking logic for `timeout` duration
//     // Placeholder for actual timeout-based wait mechanism.
//     return Value{typ: "null"}
// }
