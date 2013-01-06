package main

import (
	"bytes"
	"encoding/base64"
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
	os.RemoveAll("db")
	openDB()
}

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

var stringDump, _ = base64.StdEncoding.DecodeString("AAVIZWxsbwYAKiSfaMhQwxA=")
var hashDump, _ = base64.StdEncoding.DecodeString("BAIGZmllbGQxBUhlbGxvBmZpZWxkMgVXb3JsZAYAgLPX8O51AWE=")
var setDump, _ = base64.StdEncoding.DecodeString("AgIFSGVsbG8FV29ybGQGADOH1ks5wqwF")
var zsetDump, _ = base64.StdEncoding.DecodeString("AwMDdHdvATMDdW5vATEDb25lATEGAGHZ/WW1zUvC")
var listDump, _ = base64.StdEncoding.DecodeString("AQIFSGVsbG8FV29ybGQGACzxp+PtJo1E")

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
	{"zunionstore", "dz 2 foo fooz WEIGHTS 2 4 aggregate sum", uint32(5)},
	{"zrange", "dz 0 -1 withscores", []interface{}{[]byte("bazz"), []byte("2.2"), []byte("baz"), []byte("2.8"), []byte("buzz"), []byte("4"), []byte("asdf"), []byte("10.2"), []byte("bar"), []byte("1.6e+30")}},
	{"zinterstore", "dz 2 foo fooz WEIGHTS 2 4 aggregate min", uint32(2)},
	{"zrange", "dz 0 -1 withscores", []interface{}{[]byte("baz"), []byte("0.8"), []byte("bar"), []byte("4.2")}},
	{"sadd", "zs bar", uint32(1)},
	{"zinterstore", "dz 2 foo zs aggregate max", uint32(1)},
	{"zrange", "dz 0 -1 withscores", []interface{}{[]byte("bar"), []byte("2.1")}},
	{"zrem", "foo bar baz", uint32(2)},
	{"zrem", "foo bar", uint32(0)},
	{"zrem", "asdfa bar", 0},
	{"zcard", "foo", uint32(3)},
	{"zrem", "asdf bar", uint32(1)},
	{"zcard", "asdf", uint32(0)},
	{"exists", "asdf", 0},
	{"zrange", "foo 0 1", []interface{}{[]byte("bazz"), []byte("buzz")}},
	{"zrange", "foo 0 -1 withscores", []interface{}{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2"), []byte("asdf"), []byte("5.1")}},
	{"zrange", "foo 0 -2 WITHSCORES", []interface{}{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2")}},
	{"zrange", "foo -1 -1", []interface{}{[]byte("asdf")}},
	{"zrange", "foo 10 12", []interface{}{}},
	{"zrange", "foo 2 1", []interface{}{}},
	{"zrange", "foo -10 -1 withscores", []interface{}{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2"), []byte("asdf"), []byte("5.1")}},
	{"zrevrange", "foo 0 1", []interface{}{[]byte("asdf"), []byte("buzz")}},
	{"zrevrange", "foo 0 -1 withscores", []interface{}{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2"), []byte("bazz"), []byte("1.1")}},
	{"zrevrange", "foo 0 -2 WITHSCORES", []interface{}{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2")}},
	{"zrevrange", "foo -1 -1", []interface{}{[]byte("bazz")}},
	{"zrevrange", "foo 10 12", []interface{}{}},
	{"zrevrange", "foo 2 1", []interface{}{}},
	{"zrevrange", "foo -10 -1 withscores", []interface{}{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2"), []byte("bazz"), []byte("1.1")}},
	{"zrangebyscore", "foo -inf +inf withscores", []interface{}{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2"), []byte("asdf"), []byte("5.1")}},
	{"zrangebyscore", "foo -inf +inf withscores limit 0 2", []interface{}{[]byte("bazz"), []byte("1.1"), []byte("buzz"), []byte("2")}},
	{"zrangebyscore", "foo -inf +inf limit 1 1", []interface{}{[]byte("buzz")}},
	{"zrangebyscore", "foo 1.1 1.2", []interface{}{[]byte("bazz")}},
	{"zrangebyscore", "foo (2 +inf", []interface{}{[]byte("asdf")}},
	{"zrangebyscore", "foo 2 (3", []interface{}{[]byte("buzz")}},
	{"zrevrangebyscore", "foo +inf -inf withscores", []interface{}{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2"), []byte("bazz"), []byte("1.1")}},
	{"zrevrangebyscore", "foo +inf -inf withscores limit 0 2", []interface{}{[]byte("asdf"), []byte("5.1"), []byte("buzz"), []byte("2")}},
	{"zrevrangebyscore", "foo +inf -inf limit 1 1", []interface{}{[]byte("buzz")}},
	{"zrevrangebyscore", "foo 1.2 1.1", []interface{}{[]byte("bazz")}},
	{"zrevrangebyscore", "foo +inf (2", []interface{}{[]byte("asdf")}},
	{"zrevrangebyscore", "foo (3 2", []interface{}{[]byte("buzz")}},
	{"zadd", "deletetest 1 one 2 two 3 three", uint32(3)},
	{"zremrangebyscore", "deletetest 1 2", uint32(2)},
	{"zcard", "deletetest", uint32(1)},
	{"zrange", "deletetest 0 -1", []interface{}{[]byte("three")}},
	{"zremrangebyscore", "deletetest -inf +inf", uint32(1)},
	{"exists", "deletetest", 0},
	{"zadd", "asdf 1 bar", uint32(1)},
	{"del", "foo asdf", 2},
	{"del", "foo asdf", 0},
	{"zcard", "foo", uint32(0)},
	{"exists", "foo", 0},
	{"zcard", "asdf", uint32(0)},
	{"exists", "asdf", 0},
	{"set", "foo bar", "OK"},
	{"get", "foo", []byte("bar")},
	{"set", "foo baz", "OK"},
	{"get", "foo", []byte("baz")},
	{"del", "foo", 1},
	{"exists", "foo", 0},
	{"zadd", "asdf 1 bar", uint32(1)},
	{"set", "asdf foo", "OK"},
	{"get", "asdf", []byte("foo")},
	{"sadd", "aset 1 2 3 4 5", uint32(5)},
	{"sadd", "set2 1 a 3", uint32(3)},
	{"sadd", "set3 1 b 4", uint32(3)},
	{"sunion", "aset set2 set3", []interface{}{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("a"), []byte("b")}},
	{"sinter", "aset set2 set3", []interface{}{[]byte("1")}},
	{"sinter", "aset bset set2 set3", []interface{}{}},
	{"sdiff", "aset set2 set3", []interface{}{[]byte("2"), []byte("5")}},
	{"sunionstore", "destset aset set2 set3", uint32(7)},
	{"smembers", "destset", []interface{}{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("a"), []byte("b")}},
	{"sinterstore", "destset aset set2 set3", uint32(1)},
	{"smembers", "destset", []interface{}{[]byte("1")}},
	{"sdiffstore", "destset aset set2 set3", uint32(2)},
	{"smembers", "destset", []interface{}{[]byte("2"), []byte("5")}},
	{"sadd", "aset 1", uint32(0)},
	{"scard", "aset", uint32(5)},
	{"sadd", "aset 6", uint32(1)},
	{"scard", "aset", uint32(6)},
	{"srem", "aset 4 5", uint32(2)},
	{"srem", "aset 4 5", uint32(0)},
	{"scard", "aset", uint32(4)},
	{"sismember", "aset 6", 1},
	{"sismember", "aset 7", 0},
	{"smembers", "aset", []interface{}{[]byte("1"), []byte("2"), []byte("3"), []byte("6")}},
	{"smembers", "bset", []interface{}{}},
	{"smove", "aset newset 1", 1},
	{"sismember", "aset 1", 0},
	{"sismember", "newset 1", 1},
	{"smove", "aset newset 1", 0},
	{"del", "aset", 1},
	{"exists", "aset", 0},
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
	{"hmget", "hash2 foo bar test", []interface{}{[]byte("1"), []byte("2"), []byte(nil)}},
	{"hgetall", "hash2", []interface{}{[]byte("bar"), []byte("2"), []byte("baz"), []byte("3"), []byte("foo"), []byte("1")}},
	{"hgetall", "hash3", []interface{}{}},
	{"hkeys", "hash2", []interface{}{[]byte("bar"), []byte("baz"), []byte("foo")}},
	{"hkeys", "hash3", []interface{}{}},
	{"hvals", "hash2", []interface{}{[]byte("2"), []byte("3"), []byte("1")}},
	{"hvals", "hash3", []interface{}{}},
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
	{"keys", "hash*", []interface{}{[]byte("hash"), []byte("hash2")}},
	{"del", "hash2", 1},
	{"hlen", "hash2", uint32(0)},
	{"exists", "hash", 1},
	{"exists", "hash2", 0},
	{"type", "hash", "hash"},
	{"type", "asdf", "string"},
	{"type", "newset", "set"},
	{"type", "fooz", "zset"},
	{"type", "aaaaa", "none"},
	{"lpush", "mylist world hello", uint32(2)},
	{"llen", "mylist", uint32(2)},
	{"lpop", "mylist", []byte("hello")},
	{"llen", "mylist", uint32(1)},
	{"lpop", "mylist", []byte("world")},
	{"llen", "mylist", uint32(0)},
	{"rpush", "mylist hello world test", uint32(3)},
	{"rpushx", "mylist test2", uint32(4)},
	{"rpushx", "myotherlist test", uint32(0)},
	{"lpushx", "mylist test", uint32(5)},
	{"lpushx", "myotherlist test", uint32(0)},
	{"rpop", "mylist", []byte("test2")},
	{"rpoplpush", "mylist myotherlist", []byte("test")},
	{"rpop", "myotherlist", []byte("test")},
	{"llen", "myotherlist", uint32(0)},
	{"exists", "myotherlist", 0},
	{"lrange", "mylist 0 -1", []interface{}{[]byte("test"), []byte("hello"), []byte("world")}},
	{"lrange", "mylist -1 -1", []interface{}{[]byte("world")}},
	{"lrange", "mylist -2 -5", []interface{}{}},
	{"lrange", "mylist 1 2", []interface{}{[]byte("hello"), []byte("world")}},
	{"restore", "r 0 " + string(stringDump), "OK"},
	{"dump", "r", stringDump},
	{"get", "r", []byte("Hello")},
	{"restore", "r 0 " + string(hashDump), "OK"},
	{"dump", "r", hashDump},
	{"hlen", "r", uint32(2)},
	{"hgetall", "r", []interface{}{[]byte("field1"), []byte("Hello"), []byte("field2"), []byte("World")}},
	{"restore", "r 0 " + string(setDump), "OK"},
	{"dump", "r", setDump},
	{"scard", "r", uint32(2)},
	{"smembers", "r", []interface{}{[]byte("Hello"), []byte("World")}},
	{"restore", "r 0 " + string(zsetDump), "OK"},
	{"dump", "r", zsetDump},
	{"zcard", "r", uint32(3)},
	{"zrange", "r 0 -1 withscores", []interface{}{[]byte("one"), []byte("1"), []byte("uno"), []byte("1"), []byte("two"), []byte("3")}},
	{"restore", "r 0 " + string(listDump), "OK"},
	{"dump", "r", listDump},
	{"llen", "r", uint32(2)},
	{"lrange", "r 0 -1", []interface{}{[]byte("Hello"), []byte("World")}},
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
			if cmd.arity > 0 {
				args = bytes.SplitN([]byte(t.args), []byte(" "), cmd.arity)
			} else {
				args = bytes.Split([]byte(t.args), []byte(" "))
			}
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
			items := make([]interface{}, 0, int(stream.size))
			for item := range stream.items {
				items = append(items, item)
			}
			res = items
		}
		c.Assert(res, DeepEquals, t.response, Commentf("%s %s, obtained=%s expected=%s", t.command, t.args, res, t.response))
	}
}
