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

var zaddTests = []struct {
	args    string
	newKeys int
}{
	{"foo 1 bar", 1},
	{"foo 1 bar", 0},
	{"foo 2 bar", 0},
	{"foo 1 baz", 1},
	{"foo 1 baz 2 bar", 0},
	{"foo 5.1 asdf 2 buzz 1 baz 2 bar", 2},
	{"asdf 0.1 bar", 1},
	{"fooz 4e29 bar 0.2 baz", 2},
}

func (s ZSetSuite) TestZadd(c *C) {
	for _, t := range zaddTests {
		wb := levigo.NewWriteBatch()
		res := zadd(bytes.Split([]byte(t.args), []byte(" ")), wb)
		if err, ok := res.(error); ok {
			MaybeFail(c, err)
		}
		err := DB.Write(DefaultWriteOptions, wb)
		MaybeFail(c, err)

		c.Assert(res.(int), Equals, t.newKeys)

		wb.Close()
	}
}

var zscoreTests = []struct {
	key    string
	member string
	score  string
}{
	{"foo", "bar", "2"},
	{"foo", "baz", "1"},
	{"asdf", "bar", "0.1"},
	{"fooz", "bar", "4e+29"},
}

func (s ZSetSuite) TestZscore(c *C) {
	for _, t := range zscoreTests {
		res := zscore([][]byte{[]byte(t.key), []byte(t.member)}, nil)
		if err, ok := res.(error); ok {
			MaybeFail(c, err)
		}

		c.Assert(string(res.([]byte)), Equals, t.score)
	}
}
