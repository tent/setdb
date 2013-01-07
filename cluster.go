package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jmhodges/levigo"
	"github.com/titanous/bconv"
	"github.com/titanous/rdb"
)

func Restore(args [][]byte, wb *levigo.WriteBatch) interface{} {
	ttl, err := bconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return InvalidIntError
	}
	err = rdb.DecodeDump(args[2], 0, args[0], ttl, &rdbDecoder{wb: wb})
	if err != nil {
		return err
	}
	return ReplyOK
}

func Dump(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := dumpKey(args[0])
	if err != nil {
		return err
	}
	return res
}

func dumpKey(key []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	e := &rdbEncoder{rdb.NewEncoder(buf)}
	err := e.encodeKey(key, true)
	if err != nil {
		return nil, err
	}
	if buf.Len() == 0 {
		return nil, nil
	}
	return buf.Bytes(), nil
}

func Migrate(args [][]byte, wb *levigo.WriteBatch) interface{} {
	timeout, err := bconv.ParseInt(args[4], 10, 64)
	if err != nil {
		return InvalidIntError
	}

	data, err := dumpKey(args[2])
	if err != nil {
		return err
	}
	if data == nil {
		return ReplyNOKEY
	}

	t := time.Duration(timeout) * time.Millisecond
	r, err := redis.DialTimeout("tcp", string(args[0])+":"+string(args[1]), t, t, t)
	defer r.Close()
	if err != nil {
		return IOError{fmt.Errorf("error or timeout connecting to target instance")}
	}

	res, err := redis.String(r.Do("SELECT", args[3]))
	if _, ok := err.(redis.Error); ok {
		return fmt.Errorf("Target instance replied with error: %s", err)
	}
	if err != nil || res != "OK" {
		return IOError{fmt.Errorf("error or timeout performing SELECT of database %s on target instance", args[3])}
	}

	res, err = redis.String(r.Do("RESTORE", args[2], "0", data))
	if _, ok := err.(redis.Error); ok {
		return fmt.Errorf("Target instance replied with error: %s", err)
	}
	if err != nil || res != "OK" {
		return IOError{fmt.Errorf("error or timeout performing RESTORE of key on target instance")}
	}

	_, err = delKey(metaKey(args[2]), wb)
	if err != nil {
		return IOError{fmt.Errorf("error deleting key from local instance: %s", err)}
	}

	return ReplyOK
}
