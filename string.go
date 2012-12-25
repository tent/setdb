package main

import (
	"encoding/binary"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for strings
//
// MetadataKey | key = StringLengthValue | string length uint32
//
// For each key:
// StringKey | key = value

func Set(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	key := metaKey(args[0])
	res, err := DB.Get(DefaultReadOptions, key)
	if err != nil {
		return err
	}
	// If there is a non-string key here, let's delete it first
	if len(res) > 0 && res[0] != StringLengthValue {
		del(args[0], res[0], wb)
	}

	// store the string length in the metakey
	meta := res
	if len(res) != 5 {
		meta = make([]byte, 5)
	}
	meta[0] = StringLengthValue
	binary.BigEndian.PutUint32(meta[1:], uint32(len(args[1])))
	wb.Put(key, meta)

	// store the string
	wb.Put(stringKey(args[0]), args[1])

	return "OK"
}

func Get(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, stringKey(args[0]))
	if err != nil {
		return err
	}
	return res
}

func DelString(key []byte, wb *levigo.WriteBatch) {
	wb.Delete(stringKey(key))
}

func stringKey(k []byte) []byte {
	key := make([]byte, 5+len(k))
	key[0] = StringKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
	return key
}

// APPEND
// BITCOUNT
// BITOP
// DECR
// DECRBY
// GET
// GETBIT
// GETRANGE
// GETSET
// INCR
// INCRBY
// INCRBYFLOAT
// MGET
// MSET
// MSETNX
// PSETEX
// SET
// SETBIT
// SETEX
// SETNX
// SETRANGE
// STRLEN
