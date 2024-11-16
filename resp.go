package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// This struct will support in serialization and deserialization process for
// RESP (Redis Serialization Protocol)
type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	// The buffer created during the connection to PORT 6379 would be passed to
	// this function for generating responses
	return &Resp{
		reader: bufio.NewReader(rd),
	}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	// Read line from buffer. We read one byte at a time until we reach '\r',
	// which indicates the end of the line. Then we return the line without the
	// last two bytes, which are '\r\n' and number of bytes in the line.
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1

		line = append(line, b)

		// At any point if the second last character of the line buffer is carriage
		// return then we can break out of the for loop and return the line by
		// trimming out the last two characters.
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}

	// After reading the line successfully, we try to convert the required string
	// bits into an integer with base 10 and size 64. Otherwise, we then further
	// typecase the 64 bit integer to int-type which is system default and return.

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	// Method to read from the buffer recursively. This is needed to read the
	// Value again and again for each step of the input we received so that we
	// can parse it according to the character at the beginning of the line.

	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	// Read the first byte to determine the RESP type which is to be parsed as per
	// the switch statement.
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (v Value, err error) {

	// Steps for reading the Array:
	// 1. Skip the first byte because we have already read it in the Read method
	// 2. Read the integer that represents the number of elements in the array
	// 3. Iterate over the array and for each line, call the Read method to parse
	// the type according too the character at the beginning of the line
	// 4. With each iteration, append the parsed value to the array in the Value
	// object and return it

	v.typ = "array"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// for each line, parse and read the Value
	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// add parsed value to array
		v.array[i] = val
	}

	return v, nil
}

func (r *Resp) readBulk() (v Value, err error) {
	// Steps for reading Bulk data:
	// 1. Skip the first byte because we have already read it in the Read method
	// 2. Read the integer that represents the number of bytes in the bulk string
	// 3. Read the bulk string, followed by the '\r\n' that indicates the end of
	// the bulk string
	// 4. Return the Value object

	v.typ = "bulk"
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)
	r.reader.Read(bulk)
	v.bulk = string(bulk)

	// Read the trailing CRLF so that the pointer is effectively moved to the
	// next bulk string correctly. Otherwise, the pointer would be stuck at '\r'
	// and Read method would not work properly
	r.readLine()

	return v, nil
}
