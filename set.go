package main

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/jmhodges/levigo"
)

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
// SMOVE
// SRANDMEMBER
// SUNION
// SUNIONSTORE
