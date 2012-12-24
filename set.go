package main

import (
	"bytes"
	"encoding/binary"

	"github.com/jmhodges/levigo"
)

func Sadd(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	var newMembers uint32
	empty := []byte{}

	for _, member := range args[1:] {
		key := setKey(args[0], member)
		res, err := DB.Get(DefaultReadOptions, key)
		if err != nil {
			return err
		}
		if res != nil {
			continue
		}
		wb.Put(key, empty)
		newMembers++
	}
	if newMembers > 0 {
		card, err := scard(args[0], nil)
		if err != nil {
			return err
		}
		setCard(args[0], card+newMembers, wb)
	}
	return newMembers
}

func Scard(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	card, err := scard(args[0], nil)
	if err != nil {
		return err
	}
	return card
}

func Srem(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	card, err := scard(args[0], nil)
	if err != nil {
		return err
	}
	if card == 0 {
		return card
	}
	var deleted uint32
	for _, member := range args[1:] {
		key := setKey(args[0], member)
		res, err := DB.Get(DefaultReadOptions, key)
		if err != nil {
			return err
		}
		if res == nil {
			continue
		}
		wb.Delete(key)
		deleted++
	}
	if deleted == card {
		wb.Delete(metaKey(args[0]))
	} else if deleted > 0 { // decrement the cardinality
		setCard(args[0], card-deleted, wb)
	}
	return deleted
}

func Sismember(args [][]byte, wb *levigo.WriteBatch) cmdReply {
	res, err := DB.Get(DefaultReadOptions, setKey(args[0], args[1]))
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
	defer DB.ReleaseSnapshot(snapshot)
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)
	defer opts.Close()

	card, err := scard(args[0], opts)
	if err != nil {
		return err
	}
	if card == 0 {
		return []cmdReply{}
	}

	members := make([]cmdReply, 0, int(card))
	it := DB.NewIterator(opts)
	defer it.Close()
	iterKey := setIterKey(args[0])
	for it.Seek(iterKey); it.Valid(); it.Next() {
		// If the prefix of the current key doesn't match the iteration key,
		// we have reached the end of the set
		key := it.Key()
		if !bytes.Equal(iterKey, key[:len(iterKey)]) {
			break
		}
		members = append(members, parseMemberFromSetKey(key))
	}
	return members
}

func DelSet(key []byte, wb *levigo.WriteBatch) {
	it := DB.NewIterator(DefaultReadOptions)
	defer it.Close()
	iterKey := setIterKey(key)
	for it.Seek(iterKey); it.Valid(); it.Next() {
		key := it.Key()
		// If the prefix of the current key doesn't match the iteration key,
		// we have reached the end of the set
		if !bytes.Equal(iterKey, key[:len(iterKey)]) {
			break
		}
		wb.Delete(key)
	}
}

func scard(key []byte, opts *levigo.ReadOptions) (uint32, error) {
	if opts == nil {
		opts = DefaultReadOptions
	}
	res, err := DB.Get(opts, metaKey(key))
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

func setCard(key []byte, card uint32, wb *levigo.WriteBatch) {
	data := make([]byte, 5)
	data[0] = SetCardValue
	binary.BigEndian.PutUint32(data[1:], card)
	wb.Put(metaKey(key), data)
}

func setKey(k, member []byte) []byte {
	key := make([]byte, 5+len(k)+len(member))
	key[0] = SetKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
	copy(key[5+len(k):], member)
	return key
}

func setIterKey(k []byte) []byte {
	key := make([]byte, 5+len(k))
	key[0] = SetKey
	binary.BigEndian.PutUint32(key[1:], uint32(len(k)))
	copy(key[5:], k)
	return key
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
// SPOP
// SRANDMEMBER
// SUNION
// SUNIONSTORE
