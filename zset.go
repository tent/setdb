package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
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

func Zadd(args [][]byte, wb *levigo.WriteBatch) interface{} {
	if (len(args)-1)%2 != 0 {
		return fmt.Errorf("wrong number of arguments for 'zadd' command")
	}
	return zadd(args, wb, false)
}

func Zincrby(args [][]byte, wb *levigo.WriteBatch) interface{} {
	return zadd(args, wb, true)
}

func zadd(args [][]byte, wb *levigo.WriteBatch, incr bool) interface{} {
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
		setZcard(mk, card+newMembers, wb)
	}

	if incr { // This is a ZINCRBY, return the new score
		return ftoa(score)
	}
	return newMembers
}

func Zscore(args [][]byte, wb *levigo.WriteBatch) interface{} {
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

func Zcard(args [][]byte, wb *levigo.WriteBatch) interface{} {
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

func Zrem(args [][]byte, wb *levigo.WriteBatch) interface{} {
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
		setZcard(mk, card-deleted, wb)
	}

	return deleted
}

func Zunionstore(args [][]byte, wb *levigo.WriteBatch) interface{} {
	return combineZset(args, zsetUnion, wb)
}

func Zinterstore(args [][]byte, wb *levigo.WriteBatch) interface{} {
	return combineZset(args, zsetInter, wb)
}

const (
	zsetUnion int = iota
	zsetInter
)

const (
	zsetAggSum int = iota
	zsetAggMin
	zsetAggMax
)

func combineZset(args [][]byte, op int, wb *levigo.WriteBatch) interface{} {
	var count uint32
	res := []interface{}{}
	members := make(chan *iterZsetMember)
	var setKey, scoreKey *KeyBuffer
	scoreBytes := make([]byte, 8)

	if wb != nil {
		d := Del(args[0:1], wb)
		if err, ok := d.(error); ok {
			return err
		}
		setKey = NewKeyBuffer(ZSetKey, args[0], 0)
		scoreKey = NewKeyBuffer(ZScoreKey, args[0], 0)
	}

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return InvalidIntError
	}

	aggregate := zsetAggSum
	weights := make([]float64, numKeys)
	scores := make([]float64, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		weights[i] = 1
	}

	argOffset := 2 + numKeys
	if len(args) > argOffset {
		if len(args) > argOffset+numKeys {
			if bytes.Equal(bytes.ToLower(args[argOffset]), []byte("weights")) {
				argOffset += numKeys + 1
				for i, w := range args[numKeys+3 : argOffset] {
					weights[i], err = strconv.ParseFloat(string(w), 64)
					if err != nil {
						return fmt.Errorf("weight value is not a float")
					}
				}
			} else {
				return SyntaxError
			}
		}
		if len(args) > argOffset {
			if len(args) == argOffset+2 && bytes.Equal(bytes.ToLower(args[argOffset]), []byte("aggregate")) {
				agg := bytes.ToLower(args[argOffset+1])
				switch {
				case bytes.Equal(agg, []byte("sum")):
					aggregate = zsetAggSum
				case bytes.Equal(agg, []byte("min")):
					aggregate = zsetAggMin
				case bytes.Equal(agg, []byte("max")):
					aggregate = zsetAggMax
				default:
					return SyntaxError
				}
			} else {
				return SyntaxError
			}
		}
	}

	go multiZsetIter(args[2:numKeys+2], members, op != zsetUnion)

COMBINEOUTER:
	for m := range members {
		if op == zsetInter {
			for _, k := range m.exists {
				if !k {
					continue COMBINEOUTER
				}
			}
		}

		scores = scores[0:0]
		for i, k := range m.exists {
			if k {
				scores = append(scores, m.scores[i])
			}
		}
		var score float64
		for i, s := range scores {
			scores[i] = s * weights[i]
		}
		switch aggregate {
		case zsetAggSum:
			for _, s := range scores {
				score += s
			}
		case zsetAggMin:
			sort.Float64s(scores)
			score = scores[0]
		case zsetAggMax:
			sort.Float64s(scores)
			score = scores[len(scores)-1]
		}

		if wb != nil {
			setKey.SetSuffix(m.member)
			binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
			setZScoreKeyMember(scoreKey, m.member)
			setZScoreKeyScore(scoreKey, score)
			wb.Put(setKey.Key(), scoreBytes)
			wb.Put(scoreKey.Key(), []byte{})
			count++
		} else {
			res = append(res, m.member, ftoa(score))
		}
	}

	if wb != nil {
		if count > 0 {
			setZcard(metaKey(args[0]), count, wb)
		}
		return count
	}

	return res
}

type iterZsetMember struct {
	member []byte
	exists []bool
	scores []float64
}

type zsetMember struct {
	member []byte
	score  float64
	key    int
}

type zsetMembers []*zsetMember

func (m zsetMembers) Len() int           { return len(m) }
func (m zsetMembers) Less(i, j int) bool { return bytes.Compare(m[i].member, m[j].member) == -1 }
func (m zsetMembers) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// See set.go's multiSetIter() for details on how this works
func multiZsetIter(keys [][]byte, out chan<- *iterZsetMember, stopEarly bool) {
	defer close(out)
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	defer opts.Close()
	opts.SetSnapshot(snapshot)
	defer DB.ReleaseSnapshot(snapshot)

	members := make(zsetMembers, len(keys))
	iterKeys := make([]*KeyBuffer, len(keys))
	iterators := make([]*levigo.Iterator, len(keys))
	for i, k := range keys {
		if card, _ := zcard(metaKey(k), opts); card > 0 {
			iterKeys[i] = NewKeyBuffer(ZSetKey, k, 0)
		} else {
			// If the zset is not found, we'll assume that it is actually a set.
			// There is a slight edge case that an error could be raised by
			// zcard(), but since this function is run by a goroutine, there
			// isn't a clean way of handling it.
			iterKeys[i] = NewKeyBuffer(SetKey, k, 0)
		}
		it := DB.NewIterator(opts)
		defer it.Close()
		it.Seek(iterKeys[i].Key())
		iterators[i] = it
	}

	getMember := func(i int) ([]byte, float64) {
		// If the iterator is done, we remove the iterator and ignore it in future runs
		if iterators[i] == nil || !iterators[i].Valid() {
			iterators[i] = nil
			return nil, 0
		}
		k := iterators[i].Key()
		if !iterKeys[i].IsPrefixOf(k) {
			iterators[i] = nil
			return nil, 0
		}

		// Default non-sorted set members to score 1.0
		var score float64 = 1
		if iterKeys[i].Type() == ZSetKey {
			score = btof(iterators[i].Value())
		}
		// Strip the key prefix from the key and return the member and score
		return k[len(iterKeys[i].Key()):], score
	}

	// Initialize the members list
	for i := 0; i < len(members); i++ {
		m := &zsetMember{key: i}
		m.member, m.score = getMember(i)
		members[i] = m
	}

MULTIOUTER:
	for {
		im := &iterZsetMember{exists: make([]bool, len(members)), scores: make([]float64, len(members))}
		first := true
		sort.Sort(members)

		for _, m := range members {
			// The member will be nil if the key it is from has no more members
			if m.member == nil {
				if m.key == 0 && stopEarly {
					break MULTIOUTER
				}
				continue
			}
			// The first member is the one that we will send out on this iteration
			if first {
				im.member = m.member
				first = false
			}
			if first || bytes.Compare(im.member, m.member) == 0 {
				im.exists[m.key] = true
				im.scores[m.key] = m.score
				iterators[m.key].Next()
				m.member, m.score = getMember(m.key)
			} else {
				// If the member isn't first or the same as the one we are
				// looking for, it's not in the list
				break
			}
		}
		// When the result member is nil, there are no members left in any of the sets
		if im.member == nil {
			break
		}
		out <- im
	}
}

func Zrange(args [][]byte, wb *levigo.WriteBatch) interface{} {
	return zrange(args, false)
}

func Zrevrange(args [][]byte, wb *levigo.WriteBatch) interface{} {
	return zrange(args, true)
}

func zrange(args [][]byte, reverse bool) interface{} {
	// use a snapshot for this read so that the zcard is consistent
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)

	count, err := zcard(metaKey(args[0]), opts)
	if err != nil {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return err
	}
	if count == 0 {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return []interface{}{}
	}

	start, end, err := parseRange(args[1:], int64(count))
	if err != nil {
		return err
	}
	// the start comes after the end, so we're not going to find anything
	if start > end {
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
		return []interface{}{}
	}

	var withscores bool
	items := end + 1 - start
	if len(args) >= 4 {
		if !bytes.Equal(bytes.ToLower(args[3]), []byte("withscores")) || len(args) > 4 { // withscores flag
			DB.ReleaseSnapshot(snapshot)
			opts.Close()
			return SyntaxError
		}
		withscores = true
		items *= 2
	}
	stream := &cmdReplyStream{items, make(chan interface{})}

	go func() {
		defer close(stream.items)
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

func setZcard(key []byte, card uint32, wb *levigo.WriteBatch) {
	data := make([]byte, 5)
	data[0] = ZCardValue
	binary.BigEndian.PutUint32(data[1:], card)
	wb.Put(key, data)
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

func ZunionInterKeys(args [][]byte) [][]byte {
	numKeys, err := strconv.Atoi(string(args[1]))
	// don't return any keys if the response will be a syntax error
	if err != nil || len(args) < 2+numKeys {
		return nil
	}
	keys := make([][]byte, 1, 1+numKeys)
	keys[0] = args[0]
KEYLOOP:
	for _, k := range args[2 : 2+numKeys] {
		for _, key := range keys {
			// skip keys that are already in the array
			if bytes.Equal(k, key) {
				continue KEYLOOP
			}
		}
		keys = append(keys, k)
	}
	return keys
}

// ZCOUNT
// ZREVRANGEBYSCORE
// ZRANGEBYSCORE
// ZRANK
// ZREMRANGEBYRANK
// ZREMRANGEBYSCORE
// ZREVRANK
