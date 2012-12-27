package main

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for lists
//
// MetaKey | key = ListLengthValue | 1 byte flags | uint32 list length | int64 sequence number of the leftmost element | int64 sequence number of the rightmost element
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

func Lrange(args [][]byte, wb *levigo.WriteBatch) cmdReply {
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
		return []cmdReply{}
	}

	start, end, err := parseRange(args[1:], int64(l.length))
	if err != nil {
		return err
	}
	// the start comes after the end, so we're not going to find anything
	if start > end {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return []cmdReply{}
	}

	count := end + 1 - start
	stream := &cmdReplyStream{count, make(chan cmdReply)}

	go func() {
		it := DB.NewIterator(opts)
		defer it.Close()

		iterKey := NewKeyBuffer(ListKey, args[0], 8)
		binary.BigEndian.PutUint64(iterKey.SuffixForRead(8), uint64(l.left+1+start-math.MinInt64))
		it.Seek(iterKey.Key())
		for i := int64(0); it.Valid() && i < count; i++ {
			stream.items <- it.Value()
			it.Next()
		}
		close(stream.items)
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}()
	return stream
}

func Llen(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	l, err := llen(metaKey(args[0]), nil)
	if err != nil {
		return err
	}
	return l.length
}

func Linsert(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	l, err := llen(mk, nil)
	if err != nil {
		return err
	}
	if l.length == 0 {
		return -1
	}

	var before bool
	position := bytes.ToLower(args[1])
	if bytes.Equal(position, []byte("before")) {
		before = true
	} else if !bytes.Equal(position, []byte("after")) {
		return SyntaxError
	}

	it := DB.NewIterator(DefaultReadOptions)
	defer it.Close()

	iterKey := NewKeyBuffer(ListKey, args[0], 0)
	it.Seek(iterKey.Key())
	for i := uint32(0); it.Valid() && i < l.length; i++ {
		k := it.Key()
		if !iterKey.IsPrefixOf(k) {
			break
		}

		// check if this is the pivot value
		if !bytes.Equal(it.Value(), args[2]) {
			it.Next()
		}

		// if we are inserting at index 0 or -1, this can be simplified to a lpush/rpush
		if (before && i == 0) || (!before && i == l.length-1) {
			res, err = lpush(args[0], before, false, wb)
			if err != nil {
				return err
			}
			return res
		}

		if before {
			it.Next()
		} else {
			it.Prev()
		}
		if !it.Valid() {
			return InvalidDataError
		}
		nextKey := it.Key()

		var insertKey []byte

		if before {
			insertKey = nextKey
		// if there is already additional sequence bytes at the end, 
		// we increment or add a new one
		// lastSeq isn't maxed out, increment the seq byte
			if len(insertKey) > len(insertKey) && int8(insertKey[len(insertKey)-1]+math.MinInt8) < math.MaxInt8 {
				insertKey[len(insertKey)-1] += 1
			} else {
				insertKey = append(insertKey, 0x80) // 0x80 == 0-math.MinInt8
			}
		} else {
			insertKey = k
			keyLen := len(iterKey.Key()) + 8
			// if the next key shares the same initial sequence,
			// figure out what extra sequence needs to be set to sort before it
			if len(nextKey) > keyLen && bytes.Equal(k[:keyLen], nextKey[:keyLen] {
				// if the nextKey has the same amount of sequence bytes, find out if there is room between them
				if len(k) == len(nextKey) {
					curSeq := int8(k[len(k)-1]+math.MinInt8)
					nextSeq := int8(nextKey[len(nextKey)-1]+math.MinInt8)
					if curSeq > math.MinInt8 && nextSeq - curSeq > 1 {
						insertKey[len(insertKey)-1] -= 1
					}
				}
			}
		 }
		break
	}
	return -1
}

// A LPUSH onto a list takes the seq number of the leftmost element, 
// decrements it and inserts the item.
func Lpush(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpush(args, true, true, wb)
	if err != nil {
		return err
	}
	return res
}

func Lpushx(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpush(args, true, false, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpush(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpush(args, false, true, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpushx(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpush(args, false, false, wb)
	if err != nil {
		return err
	}
	return res
}

func lpush(args [][]byte, left bool, create bool, wb *levigo.WriteBatch) (cmdReply, error) {
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

func Lpop(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpop(args[0], true, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpop(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := lpop(args[0], false, wb)
	if err != nil {
		return err
	}
	return res
}

func Rpoplpush(args [][]byte, wb *levigo.WriteBatch) cmdReply {
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

func lpop(key []byte, left bool, wb *levigo.WriteBatch) (cmdReply, error) {
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
	l.flags = res[1]
	l.length = binary.BigEndian.Uint32(res[2:])
	l.left = int64(binary.BigEndian.Uint64(res[6:]))
	l.right = int64(binary.BigEndian.Uint64(res[14:]))
	return l, nil
}

func setLlen(key []byte, l *listDetails, wb *levigo.WriteBatch) {
	data := make([]byte, 22)
	data[0] = ListLengthValue
	data[1] = l.flags
	binary.BigEndian.PutUint32(data[2:], l.length)
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
