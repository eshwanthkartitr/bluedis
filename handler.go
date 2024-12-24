// handler.go
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