package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

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
	var newMembers uint32
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
		card, err := zcard(args[0])
		if err != nil {
			return err
		}
		res := make([]byte, 5)
		res[0] = ZCardValue

		// Increment the cardinality
		binary.BigEndian.PutUint32(res[1:], card+newMembers)
		wb.Put(zmetaKey(args[0]), res)
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
	c, err := zcard(args[0])
	if err != nil {
		return err
	}
	return c
}

func zcard(key []byte) (uint32, error) {
	res, err := DB.Get(DefaultReadOptions, zmetaKey(key))
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
	card, err := zcard(args[0])
	if err != nil {
		return err
	}
	if card == 0 {
		return 0
	}

	var deleted uint32
	// Delete each of the members
	for _, member := range args[1:] {
		setKey := zsetKey(args[0], member)
		res, err := DB.Get(DefaultReadOptions, setKey)
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
		wb.Delete(setKey)
		wb.Delete(zscoreKey(args[0], member, score))
		deleted++
	}
	if deleted == card { // We deleted all of the members, so delete the meta key
		wb.Delete(zmetaKey(args[0]))
	} else if deleted > 0 { // Decrement the cardinality
		data := make([]byte, 5)
		data[0] = ZCardValue

		// Increment the cardinality
		binary.BigEndian.PutUint32(data[1:], card-deleted)
		wb.Put(zmetaKey(args[0]), data)
	}

	return deleted
}

func Zrange(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	count, err := zcard(args[0])
	if err != nil {
		return err
	}
	if count == 0 {
		return []cmdReply{}
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	end, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || err2 != nil {
		return fmt.Errorf("value is not an integer or out of range")
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
	}

	var withscores bool
	items := end + 1 - start
	if len(args) >= 4 {
		if strings.ToLower(string(args[3])) != "withscores" || len(args) > 4 {
			return fmt.Errorf("syntax error")
		}
		withscores = true
		items *= 2
	}
	res := make([]cmdReply, 0, items)

	it := DB.NewIterator(DefaultReadOptions)
	defer it.Close()
	var i int64
	for it.Seek(zscoreIterKey(args[0], false)); it.Valid(); it.Next() {
		if i < start {
			i++
			continue
		}

		score, member := parseZscoreKey(it.Key(), len(args[0]))
		if withscores {
			res = append(res, member, ftoa(score))
		} else {
			res = append(res, member)
		}

		i++
		if i > end {
			break
		}
	}

	return res
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

func zscoreIterKey(k []byte, reverse bool) []byte {
	key := make([]byte, 5+len(k))
	key[0] = ZScoreKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
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

func parseZscoreKey(b []byte, keyLen int) (float64, []byte) {
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
// ZREVRANGE
// ZREVRANGEBYSCORE
// ZRANGEBYSCORE
// ZRANK
// ZREMRANGEBYRANK
// ZREMRANGEBYSCORE
// ZREVRANK
