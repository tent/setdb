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
