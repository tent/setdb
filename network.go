package main

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
)

func writeReply(w io.Writer, reply cmdReply) (err error) {
	switch reply.(type) {
	case string:
		err = writeString(w, reply.(string))
	case []byte:
		err = writeBulk(w, reply.([]byte))
	case error:
		err = writeError(w, reply.(error).Error())
	}
	return
}

func writeString(w io.Writer, s string) error {
	_, err := w.Write([]byte("+" + s + "\r\n"))
	return err
}

func writeBulk(w io.Writer, b []byte) error {
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

func writeError(w io.Writer, msg string) error {
	_, err := w.Write([]byte("-ERR " + msg + "\r\n"))
	return err
}

func protocolError(conn net.Conn, msg string) {
	writeError(conn, "Protocol error: "+msg)
}

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
	readLength := func(prefix byte) (int, error) {
		c, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if c != prefix {
			protocolError(client, "invalid length")
			return 0, err
		}
		b, overflowed, err := r.ReadLine() // Read bytes will look like "123"
		if err != nil {
			return 0, err
		}
		if overflowed {
			protocolError(client, "length line too long")
			return 0, err
		}
		if len(b) == 0 {
			protocolError(client, "missing length")
			return 0, err
		}
		l, err := strconv.Atoi(string(b))
		if err != nil {
			protocolError(client, "length is not a valid integer")
			return 0, err
		}
		return l, nil
	}

	// Client event loop, each iteration handles a command
	for {
		// Step 1: get the number of arguments
		argCount, err := readLength('*')
		if err != nil {
			return
		}

		args := make([][]byte, argCount)

		// Step 2: read the arguments
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

		if len(args) == 0 {
			protocolError(client, "missing command")
			return
		}

		// lookup the command
		command, ok := commands[strings.ToLower(string(args[0]))]
		if !ok {
			writeError(client, "unknown command '"+string(args[0])+"'")
			continue
		}

		// check command arity, negative arity means >= n
		if (command.arity < 0 && len(args)-1 < -command.arity) || (command.arity >= 0 && len(args)-1 > command.arity) {
			writeError(client, "wrong number of arguments for '"+string(args[0])+"' command")
			continue
		}

		// call the command and respond
		res := command.function(args[1:])
		err = writeReply(client, res)
		if err != nil {
			return
		}
	}
}
