package main

import (
	"encoding/binary"
	"math"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for lists
//
// MetaKey | key = ListLengthValue | uint32 list length | 1 byte flags | int64 sequence number of the leftmost element | int64 sequence number of the rightmost element
//
// For each list item:
// ListKey | key length uint32 | key | int64 sequence number = value

type listDetails struct {
	flags  byte
	length uint32
	left   int64
	right  int64
}

const (
	listLooseSeq byte = 1 << iota
)

func Lrange(args [][]byte, wb *levigo.WriteBatch) interface{} {
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)

	l, err := llen(metaKey(args[0]), opts)
	if err != nil {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return err
	}
	if l.length == 0 {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return []interface{}{}
	}

	start, end, err := parseRange(args[1:], int64(l.length))
	if err != nil {
		return err
	}
	// the start comes after the end, so we're not going to find anything
	if start > end {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return []interface{}{}
	}

	count := end + 1 - start
	stream := &cmdReplyStream{count, make(chan interface{})}

	go func() {
		defer close(stream.items)
		it := DB.NewIterator(opts)
		defer it.Close()

		iterKey := NewKeyBuffer(ListKey, args[0], 8)
		binary.BigEndian.PutUint64(iterKey.SuffixForRead(8), uint64(l.left+1+start-math.MinInt64))
		it.Seek(iterKey.Key())
		for i := int64(0); it.Valid() && i < count; i++ {
			stream.items <- it.Value()
			it.Next()
		}
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}()
	return stream
}

func Llen(args [][]byte, wb *levigo.WriteBatch) interface{} {
	l, err := llen(metaKey(args[0]), nil)
	if err != nil {
		return err
	}
	return l.length
}

// A LPUSH onto a list takes the seq number of the leftmost element,
// decrements it and inserts the item.
func Lpush(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpush(args, true, true, wb)
	if err != nil {
		return err
	}
	return res
}

func Lpushx(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpush(args, true, false, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpush(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpush(args, false, true, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpushx(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpush(args, false, false, wb)
	if err != nil {
		return err
	}
	return res
}

func lpush(args [][]byte, left bool, create bool, wb *levigo.WriteBatch) (interface{}, error) {
	mk := metaKey(args[0])
	l, err := llen(mk, nil)
	if err != nil {
		return nil, err
	}
	if create || l.length > 0 {
		key := NewKeyBuffer(ListKey, args[0], 8)
		for _, value := range args[1:] {
			l.length++
			var seq int64
			if left {
				seq = l.left
				l.left--
			} else {
				seq = l.right
				l.right++
			}
			// To sort negative ints in order before positive, we subtract math.MinInt64
			// which wraps the numbers around and sorts correctly
			binary.BigEndian.PutUint64(key.SuffixForRead(8), uint64(seq-math.MinInt64))
			wb.Put(key.Key(), value)
		}
		setLlen(mk, l, wb)
	}
	return l.length, nil
}

func Lpop(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpop(args[0], true, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpop(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpop(args[0], false, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpoplpush(args [][]byte, wb *levigo.WriteBatch) interface{} {
	res, err := lpop(args[0], false, wb)
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	_, err = lpush([][]byte{args[1], res.([]byte)}, true, true, wb)
	if err != nil {
		return err
	}
	return res
}

func lpop(key []byte, left bool, wb *levigo.WriteBatch) (interface{}, error) {
	mk := metaKey(key)
	l, err := llen(mk, nil)
	if err != nil {
		return nil, err
	}
	if l.length == 0 {
		return nil, nil
	}

	iterKey := NewKeyBuffer(ListKey, key, 0)
	it := DB.NewIterator(ReadWithoutCacheFill)
	defer it.Close()
	if !left {
		iterKey.ReverseIterKey()
	}
	it.Seek(iterKey.Key())
	if !left {
		it.Prev()
	}
	if !it.Valid() {
		return nil, nil
	}
	k := it.Key()
	if !iterKey.IsPrefixOf(k) {
		return nil, nil
	}
	res := it.Value()

	wb.Delete(k)
	l.length--
	if l.length == 0 {
		wb.Delete(mk)
	} else {
		// decode the sequence number from the list item key
		seq := int64(binary.BigEndian.Uint64(k[len(key)+5:])) + math.MinInt64
		if left {
			l.left = seq
		} else {
			l.right = seq
		}
		setLlen(mk, l, wb)
	}

	return res, nil
}

func llen(key []byte, opts *levigo.ReadOptions) (*listDetails, error) {
	if opts == nil {
		opts = DefaultReadOptions
	}
	res, err := DB.Get(opts, key)
	if err != nil {
		return nil, err
	}
	l := &listDetails{right: 1}
	if res == nil {
		return l, nil
	}
	if len(res) < 22 || res[0] != ListLengthValue {
		return nil, InvalidDataError
	}
	l.length = binary.BigEndian.Uint32(res[1:])
	l.flags = res[5]
	l.left = int64(binary.BigEndian.Uint64(res[6:]))
	l.right = int64(binary.BigEndian.Uint64(res[14:]))
	return l, nil
}

func setLlen(key []byte, l *listDetails, wb *levigo.WriteBatch) {
	data := make([]byte, 22)
	data[0] = ListLengthValue
	binary.BigEndian.PutUint32(data[1:], l.length)
	data[5] = l.flags
	binary.BigEndian.PutUint64(data[6:], uint64(l.left))
	binary.BigEndian.PutUint64(data[14:], uint64(l.right))
	wb.Put(key, data)
}

// BLPOP
// BRPOP
// BRPOPLPUSH
// LINDEX
// LINSERT
// LRANGE
// LREM
// LSET
// LTRIM
