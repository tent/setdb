package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sort"

	"github.com/jmhodges/levigo"
)

// Keys stored in LevelDB for sets
//
// MetadataKey | key = SetCardValue | count of members uint32
//
// For each member:
// SetKey | key length uint32 | key | member = empty

func Sadd(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	var newMembers uint32
	key := NewKeyBuffer(SetKey, args[0], len(args[1]))
	mk := metaKey(args[0])
	card, err := scard(mk, nil)
	if err != nil {
		return err
	}

	for _, member := range args[1:] {
		key.SetSuffix(member)
		if card > 0 {
			res, err := DB.Get(DefaultReadOptions, key.Key())
			if err != nil {
				return err
			}
			if res != nil {
				continue
			}
		}
		wb.Put(key.Key(), []byte{})
		newMembers++
	}
	if newMembers > 0 {
		setCard(mk, card+newMembers, wb)
	}
	return newMembers
}

func Scard(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	card, err := scard(metaKey(args[0]), nil)
	if err != nil {
		return err
	}
	return card
}

func Srem(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	card, err := scard(mk, nil)
	if err != nil {
		return err
	}
	if card == 0 {
		return card
	}
	var deleted uint32
	key := NewKeyBuffer(SetKey, args[0], len(args[1]))
	for _, member := range args[1:] {
		key.SetSuffix(member)
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
	if deleted == card {
		wb.Delete(mk)
	} else if deleted > 0 { // decrement the cardinality
		setCard(mk, card-deleted, wb)
	}
	return deleted
}

func Sismember(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, NewKeyBufferWithSuffix(SetKey, args[0], args[1]).Key())
	if err != nil {
		return err
	}
	if res == nil {
		return 0
	}
	return 1
}

func Smembers(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	// use a snapshot so that the cardinality is consistent with the iterator
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)

	card, err := scard(metaKey(args[0]), opts)
	if err != nil {
		return err
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}
	if card == 0 {
		return []cmdReply{}
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}

	// send the reply back over a channel since there could be a lot of items
	stream := &cmdReplyStream{int64(card), make(chan cmdReply)}
	go func() {
		it := DB.NewIterator(opts)
		defer it.Close()
		iterKey := NewKeyBuffer(SetKey, args[0], 0)

		for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
			// If the prefix of the current key doesn't match the iteration key,
			// we have reached the end of the set
			key := it.Key()
			if !iterKey.IsPrefixOf(key) {
				break
			}
			stream.items <- parseMemberFromSetKey(key)
		}
		close(stream.items)
		DB.ReleaseSnapshot(snapshot)
		opts.Close()
	}()
	return stream
}

func Spop(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	mk := metaKey(args[0])
	card, err := scard(mk, nil)
	if err != nil {
		return err
	}
	if card == 0 {
		return nil
	}
	key := NewKeyBuffer(SetKey, args[0], 1)
	member := srand(key)
	if member == nil {
		return nil
	}
	key.SetSuffix(member)
	wb.Delete(key.Key())
	if card == 1 { // we're removing the last remaining member
		wb.Delete(mk)
	} else {
		setCard(mk, card-1, wb)
	}
	return member
}

func Smove(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	resp, err := DB.Get(DefaultReadOptions, NewKeyBufferWithSuffix(SetKey, args[0], args[2]).Key())
	if err != nil {
		return err
	}
	if resp == nil {
		return 0
	}

	res := Srem([][]byte{args[0], args[2]}, wb)
	if err, ok := res.(error); ok {
		return err
	}
	res = Sadd([][]byte{args[1], args[2]}, wb)
	if err, ok := res.(error); ok {
		return err
	}
	return 1
}

func Sunion(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	union := make([]cmdReply, 0)
	members := make(chan *iterSetMember)
	go multiSetIter(args, members)

	for m := range members {
		union = append(union, m.member)
	}

	return union
}

type iterSetMember struct {
	member []byte
	exists []bool
}

type setMember struct {
	member []byte
	key    int
}

type setMembers []*setMember

func (m setMembers) Len() int           { return len(m) }
func (m setMembers) Less(i, j int) bool { return bytes.Compare(m[i].member, m[j].member) == -1 }
func (m setMembers) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// Take a list of keys and send a member to out for each unique key with a list
// of the keys that have that member.
// An iterator for each key is created (members are lexographically sorted), and
// the first member from each key is added to a list. This list is sorted, so
// that the member that would iterate first is at the beginnning of the list.
// The member list is then checked for any other keys that have the same member.
// The first member is sent to out, and all keys that had that member are iterated
// forward. This is repeated until all keys have run out of members.
func multiSetIter(keys [][]byte, out chan *iterSetMember) {
	// Set up a snapshot so that we have a consistent view of the data
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	defer opts.Close()
	opts.SetSnapshot(snapshot)
	defer DB.ReleaseSnapshot(snapshot)

	members := make(setMembers, len(keys)) // a list of the current member for each key iterator
	iterKeys := make([]*KeyBuffer, len(keys))
	iterators := make([]*levigo.Iterator, len(keys))
	for i, k := range keys {
		iterKeys[i] = NewKeyBuffer(SetKey, k, 0)
		it := DB.NewIterator(opts)
		defer it.Close()
		it.Seek(iterKeys[i].Key())
		iterators[i] = it
	}

	getMember := func(i int) []byte {
		// If the iterator is done, we remove the iterator and ignore it in future runs
		if iterators[i] == nil || !iterators[i].Valid() {
			iterators[i] = nil
			return nil
		}
		k := iterators[i].Key()
		if !iterKeys[i].IsPrefixOf(k) {
			iterators[i] = nil
			return nil
		}
		// Strip the key prefix from the key and return the member
		return k[len(iterKeys[i].Key()):]
	}

	// Initialize the members list
	for i := 0; i < len(members); i++ {
		members[i] = &setMember{getMember(i), i}
	}

	// This loop runs until we run out of keys
	for {
		im := &iterSetMember{exists: make([]bool, len(members))}
		first := true
		sort.Sort(members)

		for _, m := range members {
			// The member will be nil if the key it is from has no more members
			if m.member == nil {
				continue
			}
			// The first member is the one that we will send out on this iteration
			if first {
				im.member = m.member
				first = false
			}
			if first || bytes.Compare(im.member, m.member) == 0 {
				im.exists[m.key] = true
				iterators[m.key].Next()
				m.member = getMember(m.key)
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
	close(out)
}

func DelSet(key []byte, wb *levigo.WriteBatch) {
	it := DB.NewIterator(ReadWithoutCacheFill)
	defer it.Close()
	iterKey := NewKeyBuffer(SetKey, key, 0)
	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		k := it.Key()
		// If the prefix of the current key doesn't match the iteration key,
		// we have reached the end of the set
		if !iterKey.IsPrefixOf(k) {
			break
		}
		wb.Delete(k)
	}
}

func scard(key []byte, opts *levigo.ReadOptions) (uint32, error) {
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
	if len(res) > 0 && res[0] != SetCardValue {
		return 0, InvalidKeyTypeError
	}
	if len(res) < 5 {
		return 0, InvalidDataError
	}
	return binary.BigEndian.Uint32(res[1:]), nil
}

func srand(key *KeyBuffer) []byte {
	it := DB.NewIterator(DefaultReadOptions)
	defer it.Close()
	rand.Read(key.SuffixForRead(1))
	it.Seek(key.Key())
	if !it.Valid() {
		return nil
	}
	k := it.Key()
	// check if we are in the set
	// if we aren't it's possible that we ended up at the end, so go back a key
	if !key.IsPrefixOf(k) {
		it.Prev()
		k = it.Key()
		if !key.IsPrefixOf(k) {
			return nil
		}
	}
	return parseMemberFromSetKey(k)
}

func setCard(key []byte, card uint32, wb *levigo.WriteBatch) {
	data := make([]byte, 5)
	data[0] = SetCardValue
	binary.BigEndian.PutUint32(data[1:], card)
	wb.Put(key, data)
}

func parseMemberFromSetKey(key []byte) []byte {
	keyLen := binary.BigEndian.Uint32(key[1:])
	return key[5+int(keyLen):]
}

// SDIFF
// SDIFFSTORE
// SINTER
// SINTERSTORE
// SRANDMEMBER
// SUNIONSTORE
