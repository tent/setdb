package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
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

func handleClient(client net.Conn) {
	r := bufio.NewReader(client)

	// Read a length (looks like "$3\r\n")
	readLength := func(prefix byte) (length int, err error) {
		c, err := r.ReadByte()
		if err != nil {
			return
		}
		if c != prefix {
			protocolError(client, "invalid length")
			return
		}
		b, overflowed, err := r.ReadLine() // Read bytes will look like "123"
		if err != nil {
			return
		}
		if overflowed {
			protocolError(client, "length line too long")
			return
		}
		if len(b) == 0 {
			protocolError(client, "missing length")
			return
		}
		length, err = strconv.Atoi(string(b))
		if err != nil {
			protocolError(client, "length is not a valid integer")
			return
		}
		return
	}

	runCommand := func(args [][]byte) (err error) {
		if len(args) == 0 {
			protocolError(client, "missing command")
			return
		}

		// lookup the command
		command, ok := commands[strings.ToLower(string(args[0]))]
		if !ok {
			writeError(client, "unknown command '"+string(args[0])+"'")
			return
		}

		// check command arity, negative arity means >= n
		if (command.arity < 0 && len(args)-1 < -command.arity) || (command.arity >= 0 && len(args)-1 > command.arity) {
			writeError(client, "wrong number of arguments for '"+string(args[0])+"' command")
			return
		}

		// call the command and respond
		res := command.function(args[1:])
		err = writeReply(client, res)
		if err != nil {
			return
		}

		return
	}

	processInline := func() error {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		args := bytes.Split(line[:len(line)-2], []byte(" "))
		return runCommand(args)
	}

	// Client event loop, each iteration handles a command
	for {
		// check if we're using the old inline protocol
		b, err := r.Peek(1)
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

		args := make([][]byte, argCount)

		// read the arguments
		for i := 0; i < argCount; i++ {
			length, err := readLength('$')
			if err != nil {
				return
			}

			// Read the argument bytes
			args[i] = make([]byte, length)
			_, err = io.ReadFull(r, args[i])
			if err != nil {
				return
			}

			// The argument has a trailing \r\n that we need to discard
			r.Read(make([]byte, 2))
		}

		err = runCommand(args)
		if err != nil {
			return
		}
	}
}

func writeReply(w io.Writer, reply cmdReply) (err error) {
	if reply == nil {
		return writeNil(w)
	}
	switch reply.(type) {
	case string:
		err = writeString(w, reply.(string))
	case []byte:
		err = writeBulk(w, reply.([]byte))
	case int:
		err = writeInt(w, reply.(int))
	case error:
		err = writeError(w, reply.(error).Error())
	case []cmdReply:
		err = writeMultibulk(w, reply.([]cmdReply))
	default:
		panic("Invalid reply type")
	}
	return
}

func writeNil(w io.Writer) error {
	_, err := w.Write([]byte("*-1\r\n"))
	return err
}

func writeInt(w io.Writer, n int) error {
	_, err := w.Write([]byte(":" + strconv.Itoa(n) + "\r\n"))
	return err
}

func writeString(w io.Writer, s string) error {
	_, err := w.Write([]byte("+" + s + "\r\n"))
	return err
}

func writeBulk(w io.Writer, b []byte) error {
	if b == nil {
		_, err := w.Write([]byte("$-1\r\n"))
		return err
	}
	// TODO: find a more efficient way of doing this
	_, err := w.Write([]byte("$" + strconv.Itoa(len(b)) + "\r\n"))
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\r\n"))
	return err
}

func writeMultibulk(w io.Writer, reply []cmdReply) error {
	_, err := w.Write([]byte("*" + strconv.Itoa(len(reply)) + "\r\n"))
	if err != nil {
		return err
	}
	for _, r := range reply {
		err = writeReply(w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeError(w io.Writer, msg string) error {
	_, err := w.Write([]byte("-ERR " + msg + "\r\n"))
	return err
}

func protocolError(conn net.Conn, msg string) {
	writeError(conn, "Protocol error: "+msg)
}
