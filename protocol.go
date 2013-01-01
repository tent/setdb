package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/jmhodges/levigo"
)

type client struct {
	cn net.Conn
	r  *bufio.Reader
	w  chan []byte

	writeQueueSize int // current queue size in bytes
}

var (
	clients    = make(map[string]*client) // ip:port -> client mapping
	clientsMtx = &sync.RWMutex{}
)

func listen() {
	l, err := net.Listen("tcp", ":12345")
	maybeFatal(err)
	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO: log error
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(cn net.Conn) {
	c := &client{cn: cn, r: bufio.NewReader(cn), w: make(chan []byte)}
	defer close(c.w)

	addr := cn.RemoteAddr().String()
	clientsMtx.Lock()
	clients[addr] = c
	clientsMtx.Unlock()
	defer func() {
		clientsMtx.Lock()
		delete(clients, addr)
		clientsMtx.Unlock()
	}()

	o := make(chan []byte)
	go responseQueue(c, o)
	go responseWriter(c, o)

	protocolHandler(c)
}

func protocolHandler(c *client) {
	// Read a length (looks like "$3\r\n")
	readLength := func(prefix byte) (length int, err error) {
		b, err := c.r.ReadByte()
		if err != nil {
			return
		}
		if b != prefix {
			writeProtocolError(c.w, "invalid length")
			return
		}
		l, overflowed, err := c.r.ReadLine() // Read bytes will look like "123"
		if err != nil {
			return
		}
		if overflowed {
			writeProtocolError(c.w, "length line too long")
			return
		}
		if len(l) == 0 {
			writeProtocolError(c.w, "missing length")
			return
		}
		length, err = strconv.Atoi(string(l))
		if err != nil {
			writeProtocolError(c.w, "length is not a valid integer")
			return
		}
		return
	}

	runCommand := func(args [][]byte) (err error) {
		if len(args) == 0 {
			writeProtocolError(c.w, "missing command")
			return
		}

		// lookup the command
		command, ok := commands[strings.ToLower(string(args[0]))]
		if !ok {
			writeError(c.w, "unknown command '"+string(args[0])+"'")
			return
		}

		// check command arity, negative arity means >= n
		if (command.arity < 0 && len(args)-1 < -command.arity) || (command.arity >= 0 && len(args)-1 > command.arity) {
			writeError(c.w, "wrong number of arguments for '"+string(args[0])+"' command")
			return
		}

		// call the command and respond
		var wb *levigo.WriteBatch
		if command.writes {
			wb = levigo.NewWriteBatch()
			defer wb.Close()
		}
		command.lockKeys(args[1:])
		res := command.function(args[1:], wb)
		if command.writes {
			if _, ok := res.(error); !ok { // only write the batch if the return value is not an error
				err = DB.Write(DefaultWriteOptions, wb)
			}
			if err != nil {
				writeError(c.w, "data write error: "+err.Error())
				return
			}
		}
		command.unlockKeys(args[1:])
		writeReply(c.w, res)

		return
	}

	processInline := func() error {
		line, err := c.r.ReadBytes('\n')
		if err != nil {
			return err
		}
		return runCommand(bytes.Split(line[:len(line)-2], []byte(" ")))
	}

	scratch := make([]byte, 2)
	args := [][]byte{}
	// Client event loop, each iteration handles a command
	for {
		// check if we're using the old inline protocol
		b, err := c.r.Peek(1)
		if err != nil {
			return
		}
		if b[0] != '*' {
			err = processInline()
			if err != nil {
				return
			}
			continue
		}

		// Step 1: get the number of arguments
		argCount, err := readLength('*')
		if err != nil {
			return
		}

		// read the arguments
		for i := 0; i < argCount; i++ {
			length, err := readLength('$')
			if err != nil {
				return
			}

			// Read the argument bytes
			args = append(args, make([]byte, length))
			_, err = io.ReadFull(c.r, args[i])
			if err != nil {
				return
			}

			// The argument has a trailing \r\n that we need to discard
			c.r.Read(scratch) // TODO: make sure these bytes are read
		}

		err = runCommand(args)
		if err != nil {
			return
		}

		// Truncate arguments for the next run
		args = args[:0]
	}
}

func responseQueue(c *client, out chan<- []byte) {
	defer close(out)

	queue := [][]byte{}

receive:
	for {
		// ensure that the queue always has an item in it
		if len(queue) == 0 {
			v, ok := <-c.w
			if !ok {
				break // in is closed, we're done
			}
			queue = append(queue, v)
			c.writeQueueSize += len(v)
		}

		select {
		case v, ok := <-c.w:
			if !ok {
				break receive // in is closed, we're done
			}
			queue = append(queue, v)
			c.writeQueueSize += len(v)
		case out <- queue[0]:
			c.writeQueueSize -= len(queue[0])
			queue = queue[1:]
		}
	}

	for _, v := range queue {
		out <- v
	}
}

func responseWriter(c *client, out <-chan []byte) {
	for v := range out {
		c.cn.Write(v)
	}
}

func writeReply(w chan<- []byte, reply interface{}) {
	if _, ok := reply.([]interface{}); !ok && reply == nil {
		writeNil(w)
		return
	}
	switch reply.(type) {
	case string:
		writeString(w, reply.(string))
	case []byte:
		writeBulk(w, reply.([]byte))
	case int:
		writeInt(w, int64(reply.(int)))
	case int64:
		writeInt(w, reply.(int64))
	case uint32:
		writeInt(w, int64(reply.(uint32)))
	case error:
		writeError(w, reply.(error).Error())
	case []interface{}:
		writeMultibulk(w, reply.([]interface{}))
	case *cmdReplyStream:
		writeMultibulkStream(w, reply.(*cmdReplyStream))
	case map[string]bool:
		writeMultibulkStringMap(w, reply.(map[string]bool))
	default:
		panic("Invalid reply type")
	}
}

func writeProtocolError(w chan<- []byte, msg string) {
	writeError(w, "Protocol error: "+msg)
}

func writeNil(w chan<- []byte) {
	w <- []byte("$-1\r\n")
}

func writeInt(w chan<- []byte, n int64) {
	w <- []byte(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func writeString(w chan<- []byte, s string) {
	w <- []byte("+" + s + "\r\n")
}

func writeBulk(w chan<- []byte, b []byte) {
	if b == nil {
		w <- []byte("$-1\r\n")
	}
	// TODO: find a more efficient way of doing this
	w <- []byte("$" + strconv.Itoa(len(b)) + "\r\n")
	w <- b
	w <- []byte("\r\n")
}

func writeMultibulkStream(w chan<- []byte, reply *cmdReplyStream) {
	writeMultibulkLength(w, reply.size)
	for r := range reply.items {
		writeReply(w, r)
	}
}

func writeMultibulk(w chan<- []byte, reply []interface{}) {
	if reply == nil {
		writeMultibulkLength(w, -1)
	}
	writeMultibulkLength(w, int64(len(reply)))
	for _, r := range reply {
		writeReply(w, r)
	}
}

func writeMultibulkStringMap(w chan<- []byte, reply map[string]bool) {
	writeMultibulkLength(w, int64(len(reply)))
	for r, _ := range reply {
		writeBulk(w, []byte(r))
	}
}

func writeMultibulkLength(w chan<- []byte, n int64) {
	w <- []byte("*" + strconv.FormatInt(n, 10) + "\r\n")
}

func writeError(w chan<- []byte, msg string) {
	w <- []byte("-ERR " + msg + "\r\n")
}
