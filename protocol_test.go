package main

import (
	"net"
	"testing"

	. "launchpad.net/gocheck"
)

type ProtocolSuite struct{}

var _ = Suite(&ProtocolSuite{})

func (s ProtocolSuite) TestPing(c *C) {
	a, b := net.Pipe()
	defer a.Close()
	go handleClient(b)

	a.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	res := make([]byte, 7)
	a.Read(res)
	c.Assert(res, DeepEquals, []byte("+PONG\r\n"))
}

func (s ProtocolSuite) TestArity(c *C) {
	a, b := net.Pipe()
	defer a.Close()
	go handleClient(b)

	tests := []struct {
		cmd      string
		expected string
	}{
		// invalid positive arity
		{
			"*1\r\n$6\r\nLRANGE\r\n",
			"-ERR wrong number of arguments for 'LRANGE' command\r\n",
		},
		// valid positive arity
		{
			"*4\r\n$6\r\nLRANGE\r\n$3\r\nfoo\r\n$1\r\n0\r\n$2\r\n-1\r\n",
			"*0\r\n",
		},
		// invalid negative arity
		{
			"*1\r\n$5\r\nLPUSH\r\n",
			"-ERR wrong number of arguments for 'LPUSH' command\r\n",
		},
		// valid negative arity
		{
			"*3\r\n$5\r\nLPUSH\r\n$3\r\nfoo\r\n$1\r\nA\r\n",
			":1\r\n",
		},
	}

	var res []byte
	for _, t := range tests {
		a.Write([]byte(t.cmd))
		res = make([]byte, len(t.expected))
		a.Read(res)
		c.Assert(res, DeepEquals, []byte(t.expected))
	}
}

func BenchmarkProtocolParserSimple(b *testing.B) {
	b.StopTimer()
	client, server := net.Pipe()
	defer client.Close()
	go handleClient(server)

	// eat anything that gets written to the pipe
	res := make([]byte, 1024)
	go func() {
		for {
			_, err := client.Read(res)
			if err != nil {
				break
			}
		}
	}()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		client.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	}
}

func BenchmarkProtocolParserInline(b *testing.B) {
	b.StopTimer()
	client, server := net.Pipe()
	defer client.Close()
	go handleClient(server)

	// eat anything that gets written to the pipe
	res := make([]byte, 1024)
	go func() {
		for {
			_, err := client.Read(res)
			if err != nil {
				break
			}
		}
	}()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		client.Write([]byte("PING\r\n"))
	}
}
