package main

import (
	"encoding/binary"
	"math"

	"github.com/jmhodges/levigo"
)

/* Keys stored in LevelDB for zsets
 *
 * MetadataKey | key = ZCardValue | count of members uint32 
 *
 * For each member:
 * ZSetKey   | key length uint32 | key | member = score float64
 * ZScoreKey | key length uint32 | key | score float64 | member = empty
 */
func zadd(key []byte, score float64, member []byte) (newMember bool, err error) {
	// Check if the key exists
	setKey, scoreKey := zsetKey(key, member), zscoreKey(key, member, score)
	res, err := DB.Get(DefaultReadOptions, setKey)
	if err != nil {
		return
	}

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	if res != nil { // We have a score, so the member already exists
		if len(res) != 8 {
			return false, InvalidDataError
		}
		actualScore := math.Float64frombits(binary.BigEndian.Uint64(res))
		if score == actualScore { // Member already exists with the same score, do nothing
			return
		}

		// Delete score key for member
		wb.Delete(scoreKey)
	} else { // No score found, we're adding a new member
		newMember = true
		var card uint32

		metaKey := zmetaKey(key)
		res, err = DB.Get(DefaultReadOptions, metaKey)
		if res != nil && (len(res) < 5 || res[0] != ZCardValue) {
			return false, InvalidDataError
		}
		if res != nil { // This zset exists, get the current cardinality
			card = binary.BigEndian.Uint32(res[1:])
		}
		if res == nil {
			res = make([]byte, 5)
			res[0] = ZCardValue
		}

		// Increment the cardinality
		binary.BigEndian.PutUint32(res[1:], card+1)
		wb.Put(metaKey, res)
	}

	scoreBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
	wb.Put(setKey, scoreBytes)
	wb.Put(scoreKey, []byte{})

	err = DB.Write(DefaultWriteOptions, wb)

	return
}

func zscore(key, member []byte) (*float64, error) {
	setKey := zsetKey(key, member)
	res, err := DB.Get(DefaultReadOptions, setKey)
	if err != nil || res == nil {
		return nil, err
	}
	if len(res) != 8 {
		return nil, InvalidDataError
	}

	score := math.Float64frombits(binary.BigEndian.Uint64(res))
	return &score, nil
}

func zmetaKey(k []byte) []byte {
	key := make([]byte, 1+len(k))
	key[0] = MetaKey
	copy(key[1:], k)
	return key
}

// ZSetKey | key length uint32 | key | member
func zsetKey(k, member []byte) []byte {
	key := make([]byte, 5+len(k)+len(member))
	key[0] = ZSetKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
	copy(key[5+len(k):], member)
	return key
}

// ZScoreKey | key length uint32 | key | score float64 | member
func zscoreKey(k, member []byte, score float64) []byte {
	key := make([]byte, 13+len(k)+len(member))
	key[0] = ZScoreKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
	writeByteSortableFloat(key[5+len(k):], score)
	copy(key[13+len(k):], member)
	return key
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

// ZADD
// ZREM
// ZCARD
// ZCOUNT
// ZINTERSTORE
// ZUNIONSTORE
// ZREVRANGE
// ZREVRANGEBYSCORE
// ZRANGE
// ZRANGEBYSCORE

// ZINCRBY
// ZRANK
// ZREMRANGEBYRANK
// ZREMRANGEBYSCORE
// ZREVRANK
// ZSCORE
