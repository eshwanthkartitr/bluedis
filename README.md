# Bluedis

Trying to build a Redis clone for learning the internals of how Redis works and 
how to build a low-latency system. The reference article can be found 
[here](https://www.build-redis-from-scratch.dev/en/aof)

## High Level Architecture
![SVG-Image](diagram.svg)

## Usage Instructions
1. Install the `redis-cli` on your machine.
```bash
# Ubuntu example
sudo apt-get install redis

# Arch Linux example
sudo pacman -S redis
```

2. Compile and run the program. You should receive a listening message of PORT
6379 which is also the default port of Redis. Make sure nothing else is running 
on that port.
```go
go build
./bluedis
```

3. Run the redis-cli and type out redis commands through another terminal. The 
following redis commands work
- get
- set
- hget
- hset
- hgetall

### Example Usage
- Example 1 (For testing SET, GET)
```bash
set name ritesh # sets the name to ritesh
get name        # returns ritesh
```
- Example 2 (For testing HGET, HSET, HGETALL)
```bash

```
- Example 3 (For testing AOF)    
Restart the `Bluedis` server after executing some `SET` commands. Then try to 
`GET` them. It ought to get back your data thereby proving persistance.

## Check-list
- [X] Build the server
- [X] Reading RESP
- [x] Writing RESP
- [x] Redis Commands
- [x] Data Persistance with Append-Only File

## Notes

Redis receives commands through a serialization protocol called RESP 
(Redis Serialization Protocol).

For example
```bash
# Command
SET admin ritesh

# Serialization
*3\r\n$3\r\nset\r\n$5\r\nadmin\r\n$5\r\nritesh

# Better simplified version
*3
$3
set
$5
admin
$5
ritesh
```

Another example
```bash
# Command
GET admin

# Serialization
$5\r\nritesh\r\n

# Better simplified version
$5
ritesh
```

For handling input operations, currently I am handling two types of commands
1. ReadArray
2. ReadBulk

```bash
# Serialization
*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n

# Better version
*2
$5
hello
$5
world
```

