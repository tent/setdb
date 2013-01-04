package main

import (
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
