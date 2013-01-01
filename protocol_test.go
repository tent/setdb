package main

import (
	"net"

	. "launchpad.net/gocheck"
)

type ProtocolSuite struct{}

var _ = Suite(&ProtocolSuite{})

func (s ProtocolSuite) TestPing(c *C) {
	a, b := net.Pipe()
	go handleClient(b)
	defer a.Close()

	a.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	res := make([]byte, 7)
	a.Read(res)
	c.Assert(res, DeepEquals, []byte("+PONG\r\n"))
}
