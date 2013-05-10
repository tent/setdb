package main

import (
	"encoding/binary"
	"math"

	"github.com/cupcake/rdb"
	"github.com/jmhodges/levigo"
)

type rdbDecoder struct {
	wb *levigo.WriteBatch
	i  int64
	rdb.NopDecoder
}

func (p *rdbDecoder) Set(key, value []byte, expiry int64) {
	Del([][]byte{key}, p.wb)
	setStringLen(metaKey(key), len(value), p.wb)
	p.wb.Put(stringKey(key), value)
}

func (p *rdbDecoder) StartHash(key []byte, length, expiry int64) {
	Del([][]byte{key}, p.wb)
	setHlen(metaKey(key), uint32(length), p.wb)
}

func (p *rdbDecoder) Hset(key, field, value []byte) {
	p.wb.Put(NewKeyBufferWithSuffix(HashKey, key, field).Key(), value)
}

func (p *rdbDecoder) StartSet(key []byte, cardinality, expiry int64) {
	Del([][]byte{key}, p.wb)
	setCard(metaKey(key), uint32(cardinality), p.wb)
}

func (p *rdbDecoder) Sadd(key, member []byte) {
	p.wb.Put(NewKeyBufferWithSuffix(SetKey, key, member).Key(), []byte{})
}

func (p *rdbDecoder) StartList(key []byte, length, expiry int64) {
	p.i = 0
	Del([][]byte{key}, p.wb)
	setLlen(metaKey(key), &listDetails{length: uint32(length), right: length + 2}, p.wb)
}

func (p *rdbDecoder) Rpush(key, value []byte) {
	p.i++
	k := NewKeyBuffer(ListKey, key, 8)
	binary.BigEndian.PutUint64(k.SuffixForRead(8), uint64(p.i-math.MinInt64))
	p.wb.Put(k.Key(), value)
}

func (p *rdbDecoder) StartZSet(key []byte, cardinality, expiry int64) {
	Del([][]byte{key}, p.wb)
	setZcard(metaKey(key), uint32(cardinality), p.wb)
}

func (p *rdbDecoder) Zadd(key []byte, score float64, member []byte) {
	scoreBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
	scoreKey := NewKeyBuffer(ZScoreKey, key, len(member)+8)
	setZScoreKeyMember(scoreKey, member)
	setZScoreKeyScore(scoreKey, score)
	p.wb.Put(NewKeyBufferWithSuffix(ZSetKey, key, member).Key(), scoreBytes)
	p.wb.Put(scoreKey.Key(), []byte{})
}

type rdbEncoder struct {
	r *rdb.Encoder
}

func (e *rdbEncoder) encodeKey(key []byte, dump bool) error {
	snapshot := DB.NewSnapshot()
	opts := levigo.NewReadOptions()
	opts.SetSnapshot(snapshot)
	defer DB.ReleaseSnapshot(snapshot)
	defer opts.Close()

	res, err := DB.Get(opts, metaKey(key))
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	if len(res) < 5 {
		return InvalidDataError
	}

	length := binary.BigEndian.Uint32(res[1:])
	switch res[0] {
	case StringLengthValue:
		e.r.EncodeType(rdb.TypeString)
	case HashLengthValue:
		e.r.EncodeType(rdb.TypeHash)
	case SetCardValue:
		e.r.EncodeType(rdb.TypeSet)
	case ZCardValue:
		e.r.EncodeType(rdb.TypeZSet)
	case ListLengthValue:
		e.r.EncodeType(rdb.TypeList)
	default:
		panic("unknown key type")
	}

	if !dump {
		e.r.EncodeString(key)
	}

	switch res[0] {
	case StringLengthValue:
		e.encodeString(key, opts)
	case HashLengthValue:
		e.encodeHash(key, length, opts)
	case SetCardValue:
		e.encodeSet(key, length, opts)
	case ZCardValue:
		e.encodeZSet(key, length, opts)
	case ListLengthValue:
		e.encodeList(key, length, opts)
	}

	if dump {
		e.r.EncodeDumpFooter()
	}

	return nil
}

func (e *rdbEncoder) encodeString(key []byte, opts *levigo.ReadOptions) error {
	res, err := DB.Get(opts, stringKey(key))
	if err != nil {
		return err
	}
	return e.r.EncodeString(res)
}

func (e *rdbEncoder) encodeHash(key []byte, length uint32, opts *levigo.ReadOptions) error {
	err := e.r.EncodeLength(length)
	if err != nil {
		return err
	}

	iterKey := NewKeyBuffer(HashKey, key, 0)
	it := DB.NewIterator(opts)
	defer it.Close()

	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		k := it.Key()
		if !iterKey.IsPrefixOf(k) {
			break
		}
		err = e.r.EncodeString(k[len(iterKey.Key()):])
		if err != nil {
			return err
		}
		err = e.r.EncodeString(it.Value())
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *rdbEncoder) encodeList(key []byte, length uint32, opts *levigo.ReadOptions) error {
	err := e.r.EncodeLength(length)
	if err != nil {
		return err
	}

	iterKey := NewKeyBuffer(ListKey, key, 0)
	it := DB.NewIterator(opts)
	defer it.Close()

	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		if !iterKey.IsPrefixOf(it.Key()) {
			break
		}
		err = e.r.EncodeString(it.Value())
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *rdbEncoder) encodeSet(key []byte, cardinality uint32, opts *levigo.ReadOptions) error {
	err := e.r.EncodeLength(cardinality)
	if err != nil {
		return err
	}

	iterKey := NewKeyBuffer(SetKey, key, 0)
	it := DB.NewIterator(opts)
	defer it.Close()

	for it.Seek(iterKey.Key()); it.Valid(); it.Next() {
		k := it.Key()
		if !iterKey.IsPrefixOf(k) {
			break
		}
		err = e.r.EncodeString(k[len(iterKey.Key()):])
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *rdbEncoder) encodeZSet(key []byte, cardinality uint32, opts *levigo.ReadOptions) error {
	err := e.r.EncodeLength(cardinality)
	if err != nil {
		return err
	}

	iterKey := NewKeyBuffer(ZScoreKey, key, 0)
	iterKey.ReverseIterKey()
	it := DB.NewIterator(opts)
	defer it.Close()
	it.Seek(iterKey.Key())

	for it.Prev(); it.Valid(); it.Prev() {
		k := it.Key()
		if !iterKey.IsPrefixOf(k) {
			break
		}
		score, member := parseZScoreKey(k, len(iterKey.Key())-5)
		err = e.r.EncodeString(member)
		if err != nil {
			return err
		}
		err = e.r.EncodeFloat(score)
		if err != nil {
			return err
		}
	}

	return nil
}
