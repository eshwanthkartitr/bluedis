package main

import (
	"fmt"
	"sync"
	"time"
	"strconv"
	"strings"
)

// Redis commands are case-sensitive
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"EXPIRE": expire,
	"DEL": Delete,
}

func Delete(args []Value) Value {
	if len(args) < 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'del' command",
		}
	}
	for i:=0;i<len(args);i++ {
		key := args[i].bulk

		SETsMu.Lock()
		_, ok := SETs[key]
		if ok {
			delete(SETs, key)
		}
		SETsMu.Unlock()
	}
	return Value{
		typ: "string",
		str: "OK",
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

var SETs = map[string]Values{}
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

    if len(args) == 4 && (args[2].bulk == "PX" || args[2].bulk == "px") {
        expiry = true
        px, err := time.ParseDuration(args[3].bulk + "ms")
        if err != nil {
            return Value{
                typ: "error",
                str: "ERR invalid PX value",
            }
        }
        value.Begone = time.Now().Add(px)
    }
	if len(args) == 4 && (args[2].bulk == "EX" || args[2].bulk == "ex") {
        expiry = true
        px, err := time.ParseDuration(args[3].bulk + "s")
        if err != nil {
            return Value{
                typ: "error",
                str: "ERR invalid PX value",
            }
        }
        value.Begone = time.Now().Add(px)
    }

    value.HasExpiry = expiry

    SETsMu.Lock()
    SETs[key] = value
    SETsMu.Unlock()

    fmt.Printf("SET: key=%s, value=%s, expiry=%v, Begone=%v\n", key, value.Content, value.HasExpiry, value.Begone)

    return Value{typ: "string", str: "OK"}
}

// 

func expire(args []Value) Value {
    if len(args) < 2 || len(args) > 3 {
        return Value{
            typ: "error",
            str: "ERR wrong number of arguments for 'expire' command",
        }
    }
	fmt.Println("Expire command Intiated")

    key := args[0].bulk
	fmt.Println(strconv.Atoi(args[1].bulk))
    seconds, err := strconv.Atoi(args[1].bulk)
    if err != nil {
        return Value{
            typ: "error",
            str: "ERR value is not an integer or out of range",
        }
    }
	fmt.Println("Seconds: ", seconds)

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
	fmt.Println("why go here",key)

	SETsMu.Lock()
    value, ok := SETs[key]
	fmt.Println(ok)
    if !ok {
        return Value{typ:  "bulk",bulk: "0"}  // Key is gone
    }
	fmt.Println("simple")
	fmt.Println(flag)

    now := time.Now()
    newExpiry := now.Add(time.Duration(seconds) * time.Second)

    switch flag {
    case "NX":
		fmt.Println(flag)
        if value.HasExpiry {
            return Value{typ: "bulk", bulk: "0"} // Key is already going
        }
    case "XX":
        if !value.HasExpiry {
            return Value{typ: "bulk", bulk: "0"} // Key is not going anywhere
        }
    case "GT":
        if value.HasExpiry && !newExpiry.After(value.Begone) {
            return Value{typ: "bulk", bulk: "0"} // New Begone is not greater
        }
    case "LT":
        if value.HasExpiry && !newExpiry.Before(value.Begone) {
            return Value{typ: "bulk", bulk: "0"} // New Begone is not lesser
        }
    }
	fmt.Println("simple")

    value.HasExpiry = true
    value.Begone = newExpiry
    SETs[key] = value
	fmt.Println("value",value)
	defer SETsMu.Unlock() 

    return Value{typ: "bulk", bulk: "1"}
}

func get(args []Value) Value {
    if len(args) != 1 {
        return Value{
            typ: "error",
            str: "ERR wrong number of arguments for 'get' command",
        }
    }

    key := args[0].bulk

    SETsMu.Lock()
    value, ok := SETs[key]
    fmt.Printf("GET: key=%s, value=%s, hasExpiry=%v, Begone=%v, now=%v\n", key, value.Content, value.HasExpiry, value.Begone, time.Now())
    if ok && value.HasExpiry && time.Now().After(value.Begone) {
        // Key needs to be-gone for good
        delete(SETs, key)
        ok = false
    }
    defer SETsMu.Unlock() 

    if !ok {
        return Value{typ: "null"}
    }

    return Value{
        typ:  "bulk",
        bulk: value.Content,
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
