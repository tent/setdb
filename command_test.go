package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/jmhodges/levigo"
	. "launchpad.net/gocheck"
)

// Hook gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type CommandSuite struct{}

var _ = Suite(&CommandSuite{})

func (s CommandSuite) SetUpSuite(c *C) {
	openDB()
}

func (s CommandSuite) TearDownSuite(c *C) {
	os.RemoveAll("db")
}

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

var tests = []struct {
	command  string
	args     string
	response interface{}
}{
	{"ping", "", "PONG"},
	{"echo", "foo", []byte("foo")},
	{"zadd", "foo 1 bar", uint32(1)},
	{"zadd", "foo 1 bar", uint32(0)},
	{"zadd", "foo 2 bar", uint32(0)},
	{"zadd", "foo 1 baz", uint32(1)},
	{"zadd", "foo 1 baz 2 bar", uint32(0)},
	{"zadd", "foo 5.1 asdf 2 buzz 1 baz 2 bar", uint32(2)},
	{"zadd", "asdf 0.1 bar", uint32(1)},
	{"zadd", "fooz 4e29 bar 0.2 baz", uint32(2)},
	{"zscore", "foo bar", []byte("2")},
	{"zscore", "foo baz", []byte("1")},
	{"zscore", "asdf bar", []byte("0.1")},
	{"zscore", "fooz bar", []byte("4e+29")},
	{"zscore", "fooz bag", nil},
	{"zincrby", "foo 0.1 bar", []byte("2.1")},
	{"zincrby", "foo 1.1 bazz", []byte("1.1")},
	{"zcard", "foo", uint32(5)},
	{"zcard", "fooz", uint32(2)},
	{"zcard", "asdf", uint32(1)},
	{"zcard", "asdfa", uint32(0)},
	{"zrem", "foo bar baz", uint32(2)},
	{"zrem", "foo bar", uint32(0)},
	{"zrem", "asdfa bar", 0},
	{"zcard", "foo", uint32(3)},
	{"zrem", "asdf bar", uint32(1)},
	{"zcard", "asdf", uint32(0)},
	{"zrange", "foo 0 1", []cmdReply{[]byte("bazz"), []byte("buzz")}},
	{"zrange", "foo 0 -1 withscores", []cmdReply{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2"), []byte("asdf"), []byte("5.1")}},
	{"zrange", "foo 0 -2 WITHSCORES", []cmdReply{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2")}},
	{"zrange", "foo -1 -1", []cmdReply{[]byte("asdf")}},
	{"zrange", "foo 10 12", []cmdReply{}},
	{"zrange", "foo 2 1", []cmdReply{}},
	{"zrange", "foo -10 -1 withscores", []cmdReply{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2"), []byte("asdf"), []byte("5.1")}},
	{"zrevrange", "foo 0 1", []cmdReply{[]byte("asdf"), []byte("buzz")}},
	{"zrevrange", "foo 0 -1 withscores", []cmdReply{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2"), []byte("bazz"), []byte("1.1")}},
	{"zrevrange", "foo 0 -2 WITHSCORES", []cmdReply{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2")}},
	{"zrevrange", "foo -1 -1", []cmdReply{[]byte("bazz")}},
	{"zrevrange", "foo 10 12", []cmdReply{}},
	{"zrevrange", "foo 2 1", []cmdReply{}},
	{"zrevrange", "foo -10 -1 withscores", []cmdReply{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2"), []byte("bazz"), []byte("1.1")}},
	{"zadd", "asdf 1 bar", uint32(1)},
	{"del", "foo asdf", 2},
	{"del", "foo asdf", 0},
	{"zcard", "foo", uint32(0)},
	{"zcard", "asdf", uint32(0)},
	{"set", "foo bar", "OK"},
	{"get", "foo", []byte("bar")},
	{"set", "foo baz", "OK"},
	{"get", "foo", []byte("baz")},
	{"del", "foo", 1},
	{"zadd", "asdf 1 bar", uint32(1)},
	{"set", "asdf foo", "OK"},
	{"get", "asdf", []byte("foo")},
	{"sadd", "aset 1 2 3 4 5", uint32(5)},
	{"sadd", "aset 1", uint32(0)},
	{"scard", "aset", uint32(5)},
	{"sadd", "aset 6", uint32(1)},
	{"scard", "aset", uint32(6)},
	{"srem", "aset 4 5", uint32(2)},
	{"srem", "aset 4 5", uint32(0)},
	{"scard", "aset", uint32(4)},
	{"sismember", "aset 6", 1},
	{"sismember", "aset 7", 0},
	{"smembers", "aset", []cmdReply{[]byte("1"), []byte("2"), []byte("3"), []byte("6")}},
	{"smembers", "bset", []cmdReply{}},
	{"del", "aset", 1},
	{"scard", "aset", uint32(0)},
	{"sadd", "bset a", uint32(1)},
	{"spop", "bset", []byte("a")},
	{"scard", "bset", uint32(0)},
	{"hset", "hash foo bar", 1},
	{"hget", "hash foo", []byte("bar")},
	{"hget", "hash0 baz", []byte(nil)},
	{"hset", "hash foo baz", 0},
	{"hget", "hash foo", []byte("baz")},
	{"hlen", "hash", uint32(1)},
	{"hlen", "haz", uint32(0)},
	{"hset", "hash bar baz", 1},
	{"hlen", "hash", uint32(2)},
	{"hmset", "hash2 foo 1 bar 2 baz 3", "OK"},
	{"hmget", "hash2 foo bar test", []cmdReply{[]byte("1"), []byte("2"), []byte(nil)}},
	{"hgetall", "hash2", []cmdReply{[]byte("bar"), []byte("2"), []byte("baz"), []byte("3"), []byte("foo"), []byte("1")}},
	{"hgetall", "hash3", []cmdReply{}},
	{"hkeys", "hash2", []cmdReply{[]byte("bar"), []byte("baz"), []byte("foo")}},
	{"hkeys", "hash3", []cmdReply{}},
	{"hvals", "hash2", []cmdReply{[]byte("2"), []byte("3"), []byte("1")}},
	{"hvals", "hash3", []cmdReply{}},
	{"hexists", "hash2 bar", 1},
	{"hexists", "hash2 bax", 0},
	{"hsetnx", "hash2 foox 3", 1},
	{"hget", "hash2 foox", []byte("3")},
	{"hsetnx", "hash2 foox 4", 0},
	{"hget", "hash2 foox", []byte("3")},
	{"hincrby", "hash2 fooz 3", []byte("3")},
	{"hget", "hash2 fooz", []byte("3")},
	{"hincrby", "hash2 fooz -20", []byte("-17")},
	{"hget", "hash2 fooz", []byte("-17")},
	{"hincrbyfloat", "hash2 fooa 10.50", []byte("10.5")},
	{"hget", "hash2 fooa", []byte("10.5")},
	{"hset", "hash2 fooa 5.0e3", 0},
	{"hincrbyfloat", "hash2 fooa 2.0e2", []byte("5200")},
	{"hget", "hash2 fooa", []byte("5200")},
}

func (s CommandSuite) TestCommands(c *C) {
	for _, t := range tests {
		cmd := commands[t.command]
		var wb *levigo.WriteBatch
		if cmd.writes {
			wb = levigo.NewWriteBatch()
		}
		var args [][]byte
		if t.args != "" {
			args = bytes.Split([]byte(t.args), []byte(" "))
		}
		cmd.lockKeys(args)
		res := cmd.function(args, wb)
		if cmd.writes {
			err := DB.Write(DefaultWriteOptions, wb)
			MaybeFail(c, err)
			wb.Close()
		}
		cmd.unlockKeys(args)
		if stream, ok := res.(*cmdReplyStream); ok {
			items := make([]cmdReply, 0, int(stream.size))
			for item := range stream.items {
				items = append(items, item)
			}
			res = items
		}
		c.Assert(res, DeepEquals, t.response, Commentf("%s %s, obtained=%s expected=%s", t.command, t.args, res, t.response))
	}
}
