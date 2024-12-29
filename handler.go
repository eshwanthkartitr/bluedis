package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
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
	"RPUSH":   rpush,
	"RPOP":    rpop,
	"LLEN":    llen,
	"LRANGE":  lrange,
	"BLPOP":   blpop,
	"EXPIRE":  expireHandler,
	"DEL":     Delete,
}

func Delete(args []Value) Value {
	if len(args) < 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'del' command",
		}
	}
	deletedCount := 0
	for _, arg := range args {
		key := arg.bulk
		SETsMu.Lock()
		if _, ok := SETs[key]; ok {
			delete(SETs, key)
			fmt.Println("DEL: key=", key)
			deletedCount++
		}
		SETsMu.Unlock() // Also delete from expiry map
	}
	fmt.Println("DEL: deletedCount=", deletedCount)
	return Value{
		typ: "integer",
		num: deletedCount,
	}
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

type Values struct {
	Content   string
	Begone    time.Time
	HasExpiry bool
}

var SETs = make(map[string]Values)
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'set' command",
		}
	}

	key := args[0].bulk
	value := Values{Content: args[1].bulk}
	expiry := false

	for i := 2; i < len(args); i += 2 {
		if i+1 < len(args) {
			switch strings.ToUpper(args[i].bulk) {
			case "PX":
				expiry = true
				ms, err := strconv.ParseInt(args[i+1].bulk, 10, 64)
				if err != nil {
					return Value{typ: "error", str: "ERR invalid PX value"}
				}
				value.Begone = time.Now().Add(time.Duration(ms) * time.Millisecond)
			case "EX":
				expiry = true
				s, err := strconv.Atoi(args[i+1].bulk)
				if err != nil {
					return Value{typ: "error", str: "ERR invalid EX value"}
				}
				value.Begone = time.Now().Add(time.Duration(s) * time.Second)
			}
		}
	}

	value.HasExpiry = expiry

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	fmt.Printf("SET: key=%s, value=%s, expiry=%v, Begone=%v\n", key, value.Content, value.HasExpiry, value.Begone)

	return Value{typ: "string", str: "OK"}
}

func expireHandler(args []Value) Value {
	if len(args) < 2 || len(args) > 3 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'expire' command",
		}
	}

	key := args[0].bulk
	seconds, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{
			typ: "error",
			str: "ERR value is not an integer or out of range",
		}
	}

	var flag string
	if len(args) == 3 {
		flag = strings.ToUpper(args[2].bulk)
		if flag != "NX" && flag != "XX" && flag != "GT" && flag != "LT" {
			return Value{
				typ: "error",
				str: "ERR invalid flag value",
			}
		}
	}

	SETsMu.Lock()
	defer SETsMu.Unlock()
	value, ok := SETs[key]
	if !ok {
		return Value{typ: "integer", num: 0} // Key does not exist
	}

	now := time.Now()
	newExpiry := now.Add(time.Duration(seconds) * time.Second)

	applyExpiry := false
	switch flag {
	case "":
		applyExpiry = true
	case "NX":
		if !value.HasExpiry {
			applyExpiry = true
		}
	case "XX":
		if value.HasExpiry {
			applyExpiry = true
		}
	case "GT":
		if !value.HasExpiry || newExpiry.After(value.Begone) {
			applyExpiry = true
		}
	case "LT":
		if !value.HasExpiry || newExpiry.Before(value.Begone) {
			applyExpiry = true
		}
	}

	fmt.Println("EXPIRE: key=", key, "expiryTime=", newExpiry, "SETs[key]=", SETs[key])

	if applyExpiry {
		value.HasExpiry = true
		value.Begone = newExpiry
		SETs[key] = value
		return Value{typ: "integer", num: 1}
	}

	fmt.Println("EXPIRE: key=", key, "expiryTime=", newExpiry, "SETs[key]=", SETs[key])

	return Value{typ: "integer", num: 0}
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

	if ok && value.HasExpiry && time.Now().After(value.Begone) {
		// Key needs to be-gone for good
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()
		return Value{typ: "null"}
	}

	if !ok {
		return Value{typ: "null"}
	}

	return Value{
		typ:  "bulk",
		bulk: value.Content,
	}
}

var HSETs = make(map[string]map[string]string)
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
	defer HSETsMu.Unlock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = make(map[string]string)
	}
	HSETs[hash][key] = value

	return Value{typ: "string", str: "OK"}
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

	resp := []Value{}
	for k, v := range value {
		resp = append(resp, Value{typ: "bulk", bulk: k})
		resp = append(resp, Value{typ: "bulk", bulk: v})
	}

	return Value{
		typ:   "array",
		array: resp,
	}
}

// Define listStore as a map where the keys are strings and the values are pointers to DoublyLinkedList.
var listStore = make(map[string]*DoublyLinkedList)
var listStoreMu sync.Mutex

func lpush(args []Value) Value {
	// fmt.Println("Received LPUSH command with arguments:", args)

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

	length := list.PushLeft(value)

	// fmt.Println("List length after LPUSH:", length)

	return Value{typ: "integer", num: length}
}

func lpop(args []Value) Value {
	// fmt.Println("Received LPOP command with arguments:", args)

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
		fmt.Println("List does not exist or is empty")
		return Value{typ: "null"}
	}
	listStoreMu.Unlock()

	result := make([]Value, 0, count)
	for i := 0; i < count && list.Length() > 0; i++ {
		value, ok := list.PopLeft()
		if !ok {
			fmt.Println("Failed to pop from list")
			return Value{typ: "null"}
		}
		result = append(result, Value{typ: "bulk", bulk: fmt.Sprintf("%v", value)})
	}

	// fmt.Println("List length after LPOP:", list.Length())
	// fmt.Println("Result to return:", result)

	// If only one element is popped, return it as a bulk string wrapped in a Value.
	if len(result) == 1 {
		return Value{typ: "bulk", bulk: result[0].bulk}
	}
	// Otherwise, return an array of bulk strings.
	return Value{typ: "array", array: result}
}

func rpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk
	elements := args[1:]

	listStoreMu.Lock()
	list, exists := listStore[key]
	if !exists {
		list = NewDoublyLinkedList()
		listStore[key] = list
	}
	for _, element := range elements {
		list.PushRight(element.bulk)
	}
	length := list.Length()
	listStoreMu.Unlock()

	return Value{
		typ: "integer",
		num: length,
	}
}

func rpop(args []Value) Value {
	if len(args) < 1 || len(args) > 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpop' command"}
	}

	key := args[0].bulk
	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(args[1].bulk)
		if err != nil || count <= 0 {
			return Value{typ: "error", str: "ERR invalid count argument for 'rpop' command"}
		}
	}

	listStoreMu.Lock()
	list, exists := listStore[key]
	if !exists || list.Length() == 0 {
		listStoreMu.Unlock()
		return Value{typ: "null"}
	}

	result := make([]Value, 0, count)
	for i := 0; i < count && list.Length() > 0; i++ {
		value, _ := list.PopRight()
		result = append(result, Value{typ: "bulk", bulk: fmt.Sprintf("%v", value)})
	}
	listStoreMu.Unlock()

	if len(result) == 1 {
		return result[0]
	}
	return Value{
		typ:   "array",
		array: result,
	}
}

func llen(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].bulk

	listStoreMu.Lock()
	list, exists := listStore[key]
	length := 0
	if exists {
		length = list.Length()
	}
	listStoreMu.Unlock()

	return Value{
		typ: "integer",
		num: length,
	}
}

func lrange(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lrange' command"}
	}

	key := args[0].bulk
	start, err1 := strconv.Atoi(args[1].bulk)
	end, err2 := strconv.Atoi(args[2].bulk)
	if err1 != nil || err2 != nil {
		return Value{typ: "error", str: "ERR invalid arguments for 'lrange' command"}
	}

	listStoreMu.Lock()
	list, exists := listStore[key]
	if !exists {
		listStoreMu.Unlock()
		return Value{
			typ:   "array",
			array: []Value{},
		}
	}

	values := list.ExtractRange(start, end)
	result := make([]Value, len(values))
	for i, v := range values {
		result[i] = Value{typ: "bulk", bulk: fmt.Sprintf("%v", v)}
	}
	listStoreMu.Unlock()

	return Value{
		typ:   "array",
		array: result,
	}
}

func blpop(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'blpop' command"}
	}

	// Extract keys and timeout.
	keys := args[:len(args)-1]
	timeout, err := strconv.Atoi(args[len(args)-1].bulk)
	if err != nil || timeout < 0 {
		return Value{typ: "error", str: "ERR invalid timeout argument for 'blpop' command"}
	}

	// Create a ticker for polling and a timer for timeout.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var timer *time.Timer
	var timerC <-chan time.Time

	if timeout > 0 {
		timer = time.NewTimer(time.Duration(timeout) * time.Second)
		defer timer.Stop()
		timerC = timer.C
	}

	// Main loop
	for {
		// Check all keys under lock.
		listStoreMu.Lock()
		for _, key := range keys {
			list, exists := listStore[key.bulk]
			if exists && list.Length() > 0 {
				value, _ := list.PopLeft()
				listStoreMu.Unlock()

				return Value{
					typ: "array",
					array: []Value{
						{typ: "bulk", bulk: key.bulk},
						{typ: "bulk", bulk: fmt.Sprintf("%v", value)},
					},
				}
			}
		}
		listStoreMu.Unlock()

		// Wait for either timeout or next tick.
		select {
		case <-timerC:
			return Value{typ: "null"}
		case <-ticker.C:
			// Continue to next iteration.
		}

		// If timeout is 0, return immediately.
		if timeout == 0 {
			return Value{typ: "null"}
		}
	}
}
