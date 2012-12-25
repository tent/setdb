package main

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for hashes
//
// MetadataKey | key = HashLengthValue | count of fields uint32
//
// For each field:
// HashKey | key length uint32 | key | field = value

func Hset(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return hset(args, true, wb)
}

func Hsetnx(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return hset(args, false, wb)
}

func hset(args [][]byte, overwrite bool, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	length, err := hlen(mk, nil)
	if err != nil {
		return err
	}
	var res []byte
	key := NewKeyBufferWithSuffix(HashKey, args[0], args[1]).Key()
	if length > 0 {
		res, err = DB.Get(DefaultReadOptions, key)
		if err != nil {
			return err
		}
	}
	if overwrite || res == nil {
		wb.Put(key, args[2])
	}
	if res == nil {
		setHlen(mk, length+1, wb)
		return 1
	}
	return 0
}

func Hget(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, NewKeyBufferWithSuffix(HashKey, args[0], args[1]).Key())
	if err != nil {
		return err
	}
	return res
}

func Hexists(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, NewKeyBufferWithSuffix(HashKey, args[0], args[1]).Key())
	if err != nil {
		return err
	}
	if res == nil {
		return 0
	}
	return 1
}

func Hlen(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	length, err := hlen(metaKey(args[0]), nil)
	if err != nil {
		return err
	}
	return length
}

func Hdel(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	length, err := hlen(mk, nil)
	if err != nil {
		return err
	}
	if length == 0 {
		return 0
	}

	var deleted uint32
	key := NewKeyBuffer(HashKey, args[0], len(args[1]))
	for _, field := range args[1:] {
		key.SetSuffix(field)
		res, err := DB.Get(ReadWithoutCacheFill, key.Key())
		if err != nil {
			return err
		}
		if res == nil {
			continue
		}
		wb.Delete(key.Key())
		deleted++
	}
	if deleted == length {
		wb.Delete(mk)
	} else if deleted > 0 {
		setHlen(mk, length-deleted, wb)
	}
	return deleted
}

func Hmset(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	if (len(args)-1)%2 != 0 {
		return fmt.Errorf("wrong number of arguments for 'hmset' command")
	}

	mk := metaKey(args[0])
	length, err := hlen(mk, nil)
	if err != nil {
		return err
	}

	var added uint32
	key := NewKeyBuffer(HashKey, args[0], len(args[1]))
	for i := 1; i < len(args); i += 2 {
		key.SetSuffix(args[i])
		var res []byte
		if length > 0 {
			res, err = DB.Get(DefaultReadOptions, key.Key())
			if err != nil {
				return err
			}
		}
		if res == nil {
			added++
		}
		wb.Put(key.Key(), args[i+1])
	}
	if added > 0 {
		setHlen(mk, length+added, wb)
	}
	return "OK"
}

func Hmget(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	stream := &cmdReplyStream{int64(len(args) - 1), make(chan cmdReply)}
	go func() {
		key := NewKeyBuffer(HashKey, args[0], len(args[1]))
		for _, field := range args[1:] {
			key.SetSuffix(field)
			res, err := DB.Get(DefaultReadOptions, key.Key())
			if err != nil {
				stream.items <- err
				continue
			}
			stream.items <- res
		}
		close(stream.items)
	}()
	return stream
}

func Hincrby(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	length, err := hlen(mk, nil)
	if err != nil {
		return err
	}
	key := NewKeyBufferWithSuffix(HashKey, args[0], args[1]).Key()
	res, err := DB.Get(DefaultReadOptions, key)
	if err != nil {
		return err
	}

	var current int64
	if res != nil {
		current, err = strconv.ParseInt(string(res), 10, 64)
		if err != nil {
			return fmt.Errorf("hash value is not an integer")
		}
	}
	increment, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return fmt.Errorf("value is not an integer or out of range")
	}
	result := []byte(strconv.FormatInt(current+increment, 10))
	wb.Put(key, result)

	// if is a new key, increment the hash length
	if res == nil {
		setHlen(mk, length+1, wb)
	}
	return result
}

func Hincrbyfloat(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	length, err := hlen(mk, nil)
	if err != nil {
		return err
	}
	key := NewKeyBufferWithSuffix(HashKey, args[0], args[1]).Key()
	res, err := DB.Get(DefaultReadOptions, key)
	if err != nil {
		return err
	}

	var current float64
	if res != nil {
		current, err = strconv.ParseFloat(string(res), 64)
		if err != nil {
			return fmt.Errorf("hash value is not a valid float")
		}
	}
	increment, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return fmt.Errorf("value is not a valid float")
	}
	result := []byte(strconv.FormatFloat(current+increment, 'f', -1, 64))
	wb.Put(key, result)

	// if is a new key, increment the hash length
	if res == nil {
		setHlen(mk, length+1, wb)
	}
	return result
}

func Hgetall(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return hgetall(args[0], true, true)
}

func Hkeys(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return hgetall(args[0], true, false)
}

func Hvals(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return hgetall(args[0], false, true)
}

func hgetall(key []byte, fields bool, values bool) cmdReply {
	// use a snapshot so that the length is consistent with the iterator
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)

	length, err := hlen(metaKey(key), opts)
	if err != nil {
		return err
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}
	if length == 0 {
		return []cmdReply{}
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}

	if fields && values {
		length *= 2
	}

	stream := &cmdReplyStream{int64(length), make(chan cmdReply)}
	go func() {
		iterKey := NewKeyBuffer(HashKey, key, 0)
		it := DB.NewIterator(opts)
		for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
			k := it.Key()
			if !iterKey.IsPrefixOf(k) {
				break
			}
			if fields {
				stream.items <- k[len(iterKey.Key()):]
			}
			if values {
				stream.items <- it.Value()
			}
		}
		close(stream.items)
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}()
	return stream
}

func DelHash(key []byte, wb *levigo.WriteBatch) {
	it := DB.NewIterator(ReadWithoutCacheFill)
	defer it.Close()
	iterKey := NewKeyBuffer(HashKey, key, 0)
	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		k := it.Key()
		if !iterKey.IsPrefixOf(k) {
			break
		}
		wb.Delete(k)
	}
}

func hlen(key []byte, opts *levigo.ReadOptions) (uint32, error) {
	if opts == nil {
		opts = DefaultReadOptions
	}
	res, err := DB.Get(opts, key)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	if len(res) > 0 && res[0] != HashLengthValue {
		return 0, InvalidKeyTypeError
	}
	if len(res) < 5 {
		return 0, InvalidDataError
	}
	return binary.BigEndian.Uint32(res[1:]), nil
}

func setHlen(key []byte, length uint32, wb *levigo.WriteBatch) {
	data := make([]byte, 5)
	data[0] = HashLengthValue
	binary.BigEndian.PutUint32(data[1:], length)
	wb.Put(key, data)
}
