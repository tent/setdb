package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

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
	var newMembers int
	var score float64
	scoreBytes := make([]byte, 8)

	// Iterate through each of the score/member pairs
	for i := 1; i < len(args); i += 2 {
		var err error
		score, err = strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return fmt.Errorf("'%s' is not a valid float", string(args[1]))
		}

		// Check if the member exists
		setKey := zsetKey(args[0], args[i+1])
		res, err := DB.Get(DefaultReadOptions, setKey)
		if err != nil {
			return err
		}

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
			wb.Delete(zscoreKey(args[0], args[i+1], actualScore))
		} else { // No score found, we're adding a new member
			newMembers++
		}

		// Store the set and score keys
		binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
		wb.Put(setKey, scoreBytes)
		wb.Put(zscoreKey(args[0], args[i+1], score), []byte{}) // The score key is only used for sorting, the value is empty
	}

	// Update the set metadata with the new cardinality
	if newMembers > 0 {
		var card uint32
		metaKey := zmetaKey(args[0])
		res, err := DB.Get(DefaultReadOptions, metaKey)
		if err != nil {
			return err
		}
		if res != nil && len(res) > 0 && res[0] != ZCardValue {
			return InvalidKeyTypeError
		}
		if res != nil && len(res) < 5 {
			return InvalidDataError
		}
		if res != nil { // This zset exists, get the current cardinality
			card = binary.BigEndian.Uint32(res[1:])
		}
		if res == nil {
			res = make([]byte, 5)
			res[0] = ZCardValue
		}

		// Increment the cardinality
		binary.BigEndian.PutUint32(res[1:], card+uint32(newMembers))
		wb.Put(metaKey, res)
	}

	if incr { // This is a ZINCRBY, return the new score
		return ftoa(score)
	}
	return newMembers
}

func Zscore(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, zsetKey(args[0], args[1]))
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
	res, err := DB.Get(DefaultReadOptions, zmetaKey(args[0]))
	if err != nil {
		return err
	}
	if res == nil {
		return 0
	}
	if len(res) > 0 && res[0] != ZCardValue {
		return InvalidKeyTypeError
	}
	if len(res) < 5 {
		return InvalidDataError
	}
	return binary.BigEndian.Uint32(res[1:])
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
