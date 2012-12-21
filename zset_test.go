package main

import (
	"testing"
	"os"

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
	key    string
	member string
	score  float64
	newKey bool
}{
	{"foo", "bar", 1, true},
	{"foo", "bar", 1, false},
	{"foo", "bar", 2, false},
	{"foo", "baz", 1, true},
	{"asdf", "bar", 0.1, true},
}

func (s ZSetSuite) TestZadd(c *C) {
	for _, t := range zaddTests {
		res, err := zadd([]byte(t.key), t.score, []byte(t.member))
		MaybeFail(c, err)
		c.Assert(res, Equals, t.newKey)
	}
}

var zscoreTests = []struct {
	key    string
	member string
	score  float64
}{
	{"foo", "bar", 2},
	{"foo", "baz", 1},
	{"asdf", "bar", 0.1},
}

func (s ZSetSuite) TestZscore(c *C) {
	for _, t := range zscoreTests {
		res, err := zscore([]byte(t.key), []byte(t.member))
		MaybeFail(c, err)
		c.Assert(*res, Equals, t.score)
	}
}
