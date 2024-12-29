package aof

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IAmRiteshKoushik/bluedis/resp"
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

func (aof *Aof) Write(value resp.Value) error {
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

func (aof *Aof) Read(callback func(value resp.Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart)
	resp := resp.NewResp(aof.file)

	for {
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		callback(value)
	}

	return nil
}

func (aof *Aof) WriteExpire(key string, ttl int, condition string) error {
	args := []resp.Value{
		{Typ: "bulk", Bulk: "EXPIRE"},
		{Typ: "bulk", Bulk: key},
		{Typ: "bulk", Bulk: strconv.Itoa(ttl)},
	}
	if condition != "" {
		args = append(args, resp.Value{Typ: "bulk", Bulk: condition})
	}
	value := resp.Value{Typ: "array", Array: args}
	return aof.Write(value)
}

// WriteDel converts the DEL command and its arguments into RESP format
func (aof *Aof) WriteDel(keys []string) error {
	args := []resp.Value{{Typ: "bulk", Bulk: "DEL"}}
	for _, key := range keys {
		args = append(args, resp.Value{Typ: "bulk", Bulk: key})
	}
	value := resp.Value{Typ: "array", Array: args}
	return aof.Write(value)
}

// WriteSet converts the SET command and its arguments into RESP format,
// optionally including expiry information.
func (aof *Aof) WriteSet(key, value string, args ...string) error {
	commandArgs := []resp.Value{{Typ: "bulk", Bulk: "SET"}, {Typ: "bulk", Bulk: key}, {Typ: "bulk", Bulk: value}}

	// Check for expiry arguments (EX or PX)
	for i := 0; i < len(args); i++ {
		commandArgs = append(commandArgs, resp.Value{Typ: "bulk", Bulk: args[i]})
		if i+1 < len(args) {
			commandArgs = append(commandArgs, resp.Value{Typ: "bulk", Bulk: args[i+1]})
			break // Consume both EX/PX and the time
		}
	}

	respValue := resp.Value{Typ: "array", Array: commandArgs}
	return aof.Write(respValue)
}
