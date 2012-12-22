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

type ZSetSuite struct{}

var _ = Suite(&ZSetSuite{})

func (s ZSetSuite) SetUpSuite(c *C) {
	openDB()
}

func (s ZSetSuite) TearDownSuite(c *C) {
	os.RemoveAll("db")
}

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

var zsetTests = []struct {
	command  string
	args     string
	response interface{}
}{
	{"zadd", "foo 1 bar", 1},
	{"zadd", "foo 1 bar", 0},
	{"zadd", "foo 2 bar", 0},
	{"zadd", "foo 1 baz", 1},
	{"zadd", "foo 1 baz 2 bar", 0},
	{"zadd", "foo 5.1 asdf 2 buzz 1 baz 2 bar", 2},
	{"zadd", "asdf 0.1 bar", 1},
	{"zadd", "fooz 4e29 bar 0.2 baz", 2},
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
	{"zcard", "asdfa", 0},
}

func (s ZSetSuite) TestZset(c *C) {
	for _, t := range zsetTests {
		cmd := commands[t.command]
		var wb *levigo.WriteBatch
		if cmd.writes {
			wb = levigo.NewWriteBatch()
		}
		res := cmd.function(bytes.Split([]byte(t.args), []byte(" ")), wb)
		if cmd.writes {
			err := DB.Write(DefaultWriteOptions, wb)
			MaybeFail(c, err)
			wb.Close()
		}

		c.Assert(res, DeepEquals, t.response, Commentf("%s %s, obtained=%s expected=%s", t.command, t.args, res, t.response))
	}
}
