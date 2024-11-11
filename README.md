# Bluedis

Trying to build a Redis clone for learning the internals of how Redis works and 
how to build a low-latency system. The reference article can be found 
[here](https://www.build-redis-from-scratch.dev/en/aof)

## High Level Architecture
![SVG-Image]("diagram.svg")

## Check-list
- [X] Build the server
- [ ] Reading RESP
- [ ] Writing RESP
- [ ] Redis Commands
- [ ] Data Persistance

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
