package main

import (
	"bytes"
	"strconv"

	"github.com/jmhodges/levigo"
	"github.com/titanous/rdb"
)

func Restore(args [][]byte, wb *levigo.WriteBatch) interface{} {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return InvalidIntError
	}
	err = rdb.DecodeDump(args[2], 0, args[0], ttl, &rdbDecoder{wb: wb})
	if err != nil {
		return err
	}
	return "OK"
}

func Dump(args [][]byte, wb *levigo.WriteBatch) interface{} {
	buf := &bytes.Buffer{}
	e := &rdbEncoder{rdb.NewEncoder(buf)}
	err := e.encodeKey(args[0], true)
	if err != nil {
		return err
	}
	if buf.Len() == 0 {
		return nil
	}
	return buf.Bytes()
}
