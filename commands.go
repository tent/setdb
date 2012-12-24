package main

import (
	"fmt"

	"github.com/jmhodges/levigo"
	"github.com/titanous/setdb/lockring"
)

var KeyMutex = lockring.NewLockRing(1024)

// Key/Value type identifiers, only append to this list
const (
	MetaKey byte = iota
	StringKey
	HashKey
	ListKey
	SetKey
	ZSetKey
	ZScoreKey
	StringLengthValue
	HashLengthValue
	ListLengthValue
	SetCardValue
	ZCardValue
)

var InvalidKeyTypeError = fmt.Errorf("Operation against a key holding the wrong kind of value")
var InvalidDataError = fmt.Errorf("Invalid data")
var SyntaxError = fmt.Errorf("syntax error")

// A cmdReply is a response to a command, and wraps one of these types:
//
// string - single line reply, automatically prefixed with "+"
// error - error message, automatically prefixed with "-"
// int - integer number, automatically encoded and prefixed with ":"
// []byte - bulk reply, automatically prefixed with the length like "$3\r\n"
// nil []byte - nil response (must be part of multi-bulk reply), encoded as "$-1\r\n"
// nil - nil multi-bulk reply, encoded as "*-1"
// []cmdReply - multi-bulk reply, automatically serialized, members can be nil, []byte, or int
// *cmdReplyStream - multi-bulk reply sent over a channel
type cmdReply interface{}

// if the number of items is known before the items,
// they do not need to be buffered into memory, and can be streamed over a channel
type cmdReplyStream struct {
	size  int64         // the number of items that will be sent
	items chan cmdReply // a multi-bulk reply item, one of nil, []byte, or int
}

type cmdFunc func(args [][]byte, wb *levigo.WriteBatch) cmdReply

type cmdDesc struct {
	name     string
	function cmdFunc
	arity    int  // the number of required arguments, -n means >= n
	writes   bool // false if the command doesn't write data (the WriteBatch will not be passed in)
	firstKey int  // first argument that is a key (-1 for none)
	lastKey  int  // last argument that is a key (-1 for unbounded)
	keyStep  int  // step to get all the keys from first to last. For instance MSET is 2 since the arguments are KEY VAL KEY VAL...
}

var commandList = []cmdDesc{
	{"del", Del, -1, true, 0, -1, 1},
	{"echo", Echo, 1, false, -1, 0, 0},
	{"get", Get, 1, false, 0, 0, 0},
	{"ping", Ping, 0, false, -1, 0, 0},
	{"set", Set, 2, true, 0, 0, 0},
	{"sadd", Sadd, -2, true, 0, 0, 0},
	{"scard", Scard, 1, false, 0, 0, 0},
	{"sismember", Sismember, 2, false, 0, 0, 0},
	{"smembers", Smembers, 1, false, 0, 0, 0},
	{"spop", Spop, 1, true, 0, 0, 0},
	{"srem", Srem, -2, true, 0, 0, 0},
	{"zadd", Zadd, -3, true, 0, 0, 0},
	{"zcard", Zcard, 1, false, 0, 0, 0},
	{"zincrby", Zincrby, 3, true, 0, 0, 0},
	{"zrange", Zrange, -3, false, 0, 0, 0},
	{"zrem", Zrem, -2, true, 0, 0, 0},
	{"zrevrange", Zrevrange, -3, false, 0, 0, 0},
	{"zscore", Zscore, 2, false, 0, 0, 0},
}

// extract the keys from the command args
func (c *cmdDesc) getKeys(args [][]byte) [][]byte {
	// if no keys are expected, or the argument with the first key doesn't exist
	if c.firstKey < 0 || len(args) <= c.firstKey {
		return nil
	}
	// shortcut: if the keystep is 0 or 1, we can slice the array
	if c.keyStep <= 1 {
		return args[c.firstKey : c.lastKey+1]
	}
	keys := make([][]byte, 0, 1)
	for i := c.firstKey; i < len(args) && (c.lastKey == -1 || i <= c.lastKey); i += c.keyStep {
		keys = append(keys, args[i])
	}
	return keys
}

// acquires a read or write lock for the keys in arguments using the cmdDesc
func (c *cmdDesc) lockKeys(args [][]byte) {
	if !c.writes {
		return
	}
	for _, k := range c.getKeys(args) {
		KeyMutex.Lock(k)
	}
}

func (c *cmdDesc) unlockKeys(args [][]byte) {
	if !c.writes {
		return
	}
	for _, k := range c.getKeys(args) {
		KeyMutex.Unlock(k)
	}
}

var commands = make(map[string]cmdDesc, len(commandList))

func Ping(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return "PONG"
}

func Echo(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return args[0]
}

func Del(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	deleted := 0
	k := make([]byte, 1, len(args[0])) // make a reusable slice with room for the first metakey

	for _, key := range args {
		k = bufMetaKey(k, key)
		res, err := DB.Get(DefaultReadOptions, k)
		if err != nil {
			return err
		}
		if res == nil {
			continue
		}
		if len(res) == 0 {
			return InvalidDataError
		}
		del(key, res[0], wb)
		wb.Delete(k)
		deleted++
	}
	return deleted
}

func del(key []byte, t byte, wb *levigo.WriteBatch) {
	switch t {
	case StringLengthValue:
		DelString(key, wb)
	case SetCardValue:
		DelSet(key, wb)
	case ZCardValue:
		DelZset(key, wb)
	default:
		panic("unknown key type")
	}
}

// set buf to the metaKey for key
func bufMetaKey(buf []byte, key []byte) []byte {
	buf[0] = MetaKey
	return append(buf[:1], key...)
}

func metaKey(k []byte) []byte {
	key := make([]byte, 1+len(k))
	key[0] = MetaKey
	copy(key[1:], k)
	return key
}

func init() {
	for _, c := range commandList {
		commands[c.name] = c
	}
}
