package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for zsets
//
// MetadataKey | key = ZCardValue | count of members uint32
//
// For each member:
// ZSetKey   | key length uint32 | key | member = score float64
// ZScoreKey | key length uint32 | key | score float64 | member = empty

func Zadd(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	if (len(args)-1)%2 != 0 {
		return fmt.Errorf("wrong number of arguments for 'zadd' command")
	}
	return zadd(args, wb, false)
}

func Zincrby(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return zadd(args, wb, true)
}

func zadd(args [][]byte, wb *levigo.WriteBatch, incr bool) cmdReply {
	var newMembers uint32
	var score float64
	scoreBytes := make([]byte, 8)
	setKey := NewKeyBuffer(ZSetKey, args[0], len(args[2]))
	scoreKey := NewKeyBuffer(ZScoreKey, args[0], 8+len(args[2]))

	mk := metaKey(args[0])
	card, err := zcard(mk, nil)
	if err != nil {
		return err
	}

	// Iterate through each of the score/member pairs
	for i := 1; i < len(args); i += 2 {
		var err error
		score, err = strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return fmt.Errorf("'%s' is not a valid float", string(args[1]))
		}

		// Check if the member exists
		setKey.SetSuffix(args[i+1])
		var res []byte
		if card > 0 {
			res, err = DB.Get(DefaultReadOptions, setKey.Key())
			if err != nil {
				return err
			}
		}

		// set the score key with 8 empty bytes before the member for the score
		setZScoreKeyMember(scoreKey, args[i+1])
		if res != nil { // We got a score from the db, so the member already exists
			if len(res) != 8 {
				return InvalidDataError
			}
			actualScore := math.Float64frombits(binary.BigEndian.Uint64(res))
			if incr { // this is a ZINCRBY, so increment the score
				score += actualScore
			}
			if score == actualScore { // Member already exists with the same score, do nothing
				continue
			}

			// Delete score key for member
			setZScoreKeyScore(scoreKey, actualScore)
			wb.Delete(scoreKey.Key())
		} else { // No score found, we're adding a new member
			newMembers++
		}

		// Store the set and score keys
		binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
		setZScoreKeyScore(scoreKey, score)
		wb.Put(setKey.Key(), scoreBytes)
		wb.Put(scoreKey.Key(), []byte{}) // The score key is only used for sorting, the value is empty
	}

	// Update the set metadata with the new cardinality
	if newMembers > 0 {
		data := make([]byte, 5)
		data[0] = ZCardValue

		// Increment the cardinality
		binary.BigEndian.PutUint32(data[1:], card+newMembers)
		wb.Put(mk, data)
	}

	if incr { // This is a ZINCRBY, return the new score
		return ftoa(score)
	}
	return newMembers
}

func Zscore(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, NewKeyBufferWithSuffix(ZSetKey, args[0], args[1]).Key())
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	if len(res) != 8 {
		return InvalidDataError
	}

	return ftoa(btof(res))
}

func Zcard(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	c, err := zcard(metaKey(args[0]), nil)
	if err != nil {
		return err
	}
	return c
}

func zcard(key []byte, opts *levigo.ReadOptions) (uint32, error) {
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
	if len(res) > 0 && res[0] != ZCardValue {
		return 0, InvalidKeyTypeError
	}
	if len(res) < 5 {
		return 0, InvalidDataError
	}
	return binary.BigEndian.Uint32(res[1:]), nil
}

func Zrem(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	card, err := zcard(mk, nil)
	if err != nil {
		return err
	}
	if card == 0 {
		return 0
	}

	var deleted uint32
	setKey := NewKeyBuffer(ZSetKey, args[0], len(args[1]))
	scoreKey := NewKeyBuffer(ZScoreKey, args[0], 8+len(args[1]))
	// Delete each of the members
	for _, member := range args[1:] {
		setKey.SetSuffix(member)
		res, err := DB.Get(ReadWithoutCacheFill, setKey.Key())
		if err != nil {
			return nil
		}
		if res == nil {
			continue
		}
		if len(res) != 8 {
			return InvalidDataError
		}

		score := btof(res)
		setZScoreKeyMember(scoreKey, member)
		setZScoreKeyScore(scoreKey, score)
		wb.Delete(setKey.Key())
		wb.Delete(scoreKey.Key())
		deleted++
	}
	if deleted == card { // We deleted all of the members, so delete the meta key
		wb.Delete(mk)
	} else if deleted > 0 { // Decrement the cardinality
		data := make([]byte, 5)
		data[0] = ZCardValue

		// Increment the cardinality
		binary.BigEndian.PutUint32(data[1:], card-deleted)
		wb.Put(mk, data)
	}

	return deleted
}

func Zrange(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return zrange(args, false)
}

func Zrevrange(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	return zrange(args, true)
}

func zrange(args [][]byte, reverse bool) cmdReply {
	// use a snapshot for this read so that the zcard is consistent
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)

	count, err := zcard(metaKey(args[0]), opts)
	if err != nil {
		return err
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}
	if count == 0 {
		return []cmdReply{}
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	end, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || err2 != nil {
		return fmt.Errorf("value is not an integer or out of range")
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}

	// if the index is negative, it is counting from the end, 
	// so add it to the length to get the absolute index
	if start < 0 {
		start += int64(count)
	}
	if end < 0 {
		end += int64(count)
	}

	if end > int64(count) { // limit the end to the last member
		end = int64(count) - 1
	}
	// the start comes after the end, so we're not going to find anything
	if start > end {
		return []cmdReply{}
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}

	var withscores bool
	items := end + 1 - start
	if len(args) >= 4 {
		if len(args[3]) != 10 || len(args) > 4 { // withscores flag
			return SyntaxError
			DB.ReleaseSnapshot(snapshot)
			opts.Close()
		}
		withscores = true
		items *= 2
	}
	stream := &cmdReplyStream{items, make(chan cmdReply)}

	go func() {
		it := DB.NewIterator(opts)
		defer it.Close()

		var i int64
		iterKey := NewKeyBuffer(ZScoreKey, args[0], 0)
		if reverse {
			iterKey.ReverseIterKey()
		}
		for it.Seek(iterKey.Key()); it.Valid() && i <= end; i++ {
			if reverse {
				it.Prev()
			}
			if i >= start {
				score, member := parseZScoreKey(it.Key(), len(args[0]))
				stream.items <- member
				if withscores {
					stream.items <- ftoa(score)
				}
			}
			if !reverse {
				it.Next()
			}
		}
		close(stream.items)
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}()

	return stream
}

func DelZset(key []byte, wb *levigo.WriteBatch) {
	// TODO: count keys to verify everything works as expected?
	it := DB.NewIterator(ReadWithoutCacheFill)
	defer it.Close()
	iterKey := NewKeyBuffer(ZSetKey, key, 0)
	scoreKey := NewKeyBuffer(ZScoreKey, key, 0)

	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		k := it.Key()
		// If the prefix of the current key doesn't match the iteration key,
		// we have reached the end of the zset
		if !iterKey.IsPrefixOf(k) {
			break
		}
		wb.Delete(k)
		setZScoreKeyMember(scoreKey, k[len(iterKey.Key()):])
		setZScoreKeyScore(scoreKey, btof(it.Value()))
		wb.Delete(scoreKey.Key())
	}
}

func setZScoreKeyScore(key *KeyBuffer, score float64) {
	writeByteSortableFloat(key.SuffixForRead(8), score)
}

func setZScoreKeyMember(key *KeyBuffer, member []byte) {
	key.SetSuffix(append(make([]byte, 8, 8+len(member)), member...))
}

func parseZScoreKey(b []byte, keyLen int) (float64, []byte) {
	return readByteSortableFloat(b[keyLen+5:]), b[keyLen+13:]
}

func btof(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func ftoa(f float64) []byte {
	b := []byte(strconv.FormatFloat(f, 'g', -1, 64))
	if len(b) > 1 && b[1] == 'I' { // -Inf/+Inf to lowercase
		b[1] = 'i'
	}
	if b[0] == 'N' { // NaN to lowercase
		b[0], b[2] = 'n', 'n'
	}
	if b[0] == '+' { // +inf to inf
		b = b[1:]
	}

	return b
}

/* Natural sorting of floating point numbers
 *
 * ENCODING
 * If the number is positive, flip the sign (first bit to 1)
 * Else the number is negative, flip all the bits
 * 
 * DECODING
 * If the first byte is >= 0x80 (0b10000000), decode float, flip the sign
 * Else flip all the bits, decode float
 */

func writeByteSortableFloat(b []byte, f float64) {
	if math.Signbit(f) {
		binary.BigEndian.PutUint64(b, math.Float64bits(f))
		for i, v := range b[:8] {
			b[i] = v ^ 255
		}
	} else {
		binary.BigEndian.PutUint64(b, math.Float64bits(-f))
	}
}

func readByteSortableFloat(b []byte) float64 {
	if b[0] < 0x80 {
		for i, v := range b[:8] {
			b[i] = v ^ 255
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	}
	return -math.Float64frombits(binary.BigEndian.Uint64(b))
}

// ZCOUNT
// ZINTERSTORE
// ZUNIONSTORE
// ZREVRANGEBYSCORE
// ZRANGEBYSCORE
// ZRANK
// ZREMRANGEBYRANK
// ZREMRANGEBYSCORE
// ZREVRANK
