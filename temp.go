package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Proof of concept on how the parser function ought to work. All RESP commands
// will be handled in a similar fashion.
func temp1() {
	input := "$5\r\nRitesh\r\n"
	reader := bufio.NewReader(strings.NewReader(input))

	// The first byte should be a $ sign. So making sure that is the case
	b, _ := reader.ReadByte()
	if b != '$' {
		fmt.Println("Invalid type, expected bulk strings only")
		os.Exit(1)
	}

	// After the first byte has been read, it is imperative that the second byte
	// being read is a number which can be parsed as an int
	size, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}

	// ParseInt takes - string, base (decimal, hex, etc) and bitSize
	// It is guaranteed to return an int64 but the int type is 32 bit or 64 and
	// is platform dependent. Therefore, this additional argument is taken for
	// handling things properly
	strSize, err := strconv.ParseInt(string(size), 10, 64)
	if err != nil {
		panic(err)
	}

	// Consuming the next \r\n
	reader.ReadByte()
	reader.ReadByte()

	name := make([]byte, strSize)
	reader.Read(name) // reading the remaining number of bytes into a byte slice

	// Converting the bytes into a human readable string output
	fmt.Println(string(name))

}
