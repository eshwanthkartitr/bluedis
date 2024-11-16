package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File      // Hold the file descriptor
	rd   *bufio.Reader // Read RESP commands for the file for reconstruction
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// At the time of initialization, we spawn a goroutine which syncs the AOF
	// to disk every 1 second. If we have not setup 1 second then the program
	// becomes OS dependent on when to flush the file contents to the disk but as
	// we have setup 1 second, we can lose data only within this span in the worst
	// case scenario.
	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	// Alternate approach is to sync the file everytime a command is successfully
	// executed but this can result in poor performance for write operations
	// because IO ops are expensive.

	return aof, nil
}

func (aof *Aof) Close() error {
	// Closing the file when the server is shutting down. If we do not acquire
	// the lock then we can run into problems where some garbage value gets
	// written as a race condition between server shutting down and a goroutine
	// trying to write to the AOF

	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// We are writing to the AOF file in RESP format using the Marshal() method
	// so that if we have to reconstruct then we can run all the commands of that
	// file in a loop without any pre-processing requirement
	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	resp := NewResp(aof.file)
	for {
		value, err := resp.Read()
		if err == nil {
			callback(value)
		}
		// We will eventually hit EOF with any file
		if err == io.EOF {
			break
		}

		return err
	}

	return nil
}
