package main

// A cmdReply is a response to a command, and wraps one of these types:
//
// nil - nil response, encoded as "$-1\r\n"
// string - single line reply, automatically prefixed with "+"
// error - error message, automatically prefixed with "-"
// int - integer number, automatically encoded and prefixed with ":"
// []byte - bulk reply, automatically prefixed with the length like "$3\r\n"
// []cmdReply - multi-bulk reply, automatically serialized, members can be nil, []byte, or int
type cmdReply interface{}

type cmdFunc func(args [][]byte) cmdReply

type cmdDesc struct {
	name     string
	function cmdFunc
	arity    int // the number of required arguments, -n means >= n
}

var commandList = []cmdDesc{
	{"ping", pingCommand, 0},
	{"echo", echoCommand, 1},
}

var commands = make(map[string]cmdDesc, len(commandList))

func pingCommand(args [][]byte) cmdReply {
	return "PONG"
}

func echoCommand(args [][]byte) cmdReply {
	return args[0]
}

func init() {
	for _, c := range commandList {
		commands[c.name] = c
	}
}
