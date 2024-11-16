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
hset users u1 ritesh # sets u1 as ritesh
hget users u1        # returns ritesh
hset users u2 abhi   # set u2 as abhi
hgetall users        # should return both users
```
- Example 3 (For testing AOF)    
Restart the `Bluedis` server after executing some `SET` commands. Then try to 
`GET` them. It ought to get back your data thereby proving persistance.

## Roadmap
- [X] Build the server
- [X] Reading RESP
- [x] Writing RESP
- [x] Redis Commands
- [x] Data Persistance with Append-Only File
