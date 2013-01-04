package main

import (
	"encoding/binary"
	"math"

	"github.com/jmhodges/levigo"
)

type rdbDecoder struct {
	wb *levigo.WriteBatch
	i  int64
}

func (p *rdbDecoder) StartRDB() {
}

func (p *rdbDecoder) StartDatabase(n int) {
}

func (p *rdbDecoder) EndDatabase(n int) {
}

func (p *rdbDecoder) EndRDB() {
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

func (p *rdbDecoder) EndHash(key []byte) {
}

func (p *rdbDecoder) StartSet(key []byte, cardinality, expiry int64) {
	Del([][]byte{key}, p.wb)
	setCard(metaKey(key), uint32(cardinality), p.wb)
}

func (p *rdbDecoder) Sadd(key, member []byte) {
	p.wb.Put(NewKeyBufferWithSuffix(SetKey, key, member).Key(), []byte{})
}

func (p *rdbDecoder) EndSet(key []byte) {
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

func (p *rdbDecoder) EndList(key []byte) {
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

func (p *rdbDecoder) EndZSet(key []byte) {
}
