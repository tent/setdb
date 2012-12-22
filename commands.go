package main

import (
	"github.com/jmhodges/levigo"
)

// A cmdReply is a response to a command, and wraps one of these types:
//
// string - single line reply, automatically prefixed with "+"
// error - error message, automatically prefixed with "-"
// int - integer number, automatically encoded and prefixed with ":"
// []byte - bulk reply, automatically prefixed with the length like "$3\r\n"
// nil []byte - nil response (must be part of multi-bulk reply), encoded as "$-1\r\n"
// nil - nil multi-bulk reply, encoded as "*-1"
// []cmdReply - multi-bulk reply, automatically serialized, members can be nil, []byte, or int
type cmdReply interface{}

type cmdFunc func(args [][]byte, wb *levigo.WriteBatch) cmdReply

type cmdDesc struct {
	name     string
	function cmdFunc
	arity    int  // the number of required arguments, -n means >= n
	writes   bool // false if the command doesn't write data (the WriteBatch will not be passed in)
}

var commandList = []cmdDesc{
	{"ping", ping, 0, false},
	{"echo", echo, 1, false},
	{"zadd", zadd, -3, true},
	{"zscore", zscore, 2, false},
}

var commands = make(map[string]cmdDesc, len(commandList))

func ping(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return "PONG"
}

func echo(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return args[0]
}

func init() {
	for _, c := range commandList {
		commands[c.name] = c
	}
}
