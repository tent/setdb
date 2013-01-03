package main

import (
	"encoding/binary"
	"math"

	"github.com/jmhodges/levigo"
)

type rdbParser struct {
	wb *levigo.WriteBatch
	i  int64
}

func (p *rdbParser) StartRDB() {
}

func (p *rdbParser) StartDatabase(n int) {
}

func (p *rdbParser) EndDatabase(n int) {
}

func (p *rdbParser) EndRDB() {
}

func (p *rdbParser) Set(key, value []byte, expiry int64) {
	Del([][]byte{key}, p.wb)
	setStringLen(metaKey(key), len(value), p.wb)
	p.wb.Put(stringKey(key), value)
}

func (p *rdbParser) StartHash(key []byte, length, expiry int64) {
	Del([][]byte{key}, p.wb)
	setHlen(metaKey(key), uint32(length), p.wb)
}

func (p *rdbParser) Hset(key, field, value []byte) {
	p.wb.Put(NewKeyBufferWithSuffix(HashKey, key, field).Key(), value)
}

func (p *rdbParser) EndHash(key []byte) {
}

func (p *rdbParser) StartSet(key []byte, cardinality, expiry int64) {
	Del([][]byte{key}, p.wb)
	setCard(metaKey(key), uint32(cardinality), p.wb)
}

func (p *rdbParser) Sadd(key, member []byte) {
	p.wb.Put(NewKeyBufferWithSuffix(SetKey, key, member).Key(), []byte{})
}

func (p *rdbParser) EndSet(key []byte) {
}

func (p *rdbParser) StartList(key []byte, length, expiry int64) {
	p.i = 0
	Del([][]byte{key}, p.wb)
	setLlen(metaKey(key), &listDetails{length: uint32(length), right: length + 2}, p.wb)
}

func (p *rdbParser) Rpush(key, value []byte) {
	p.i++
	k := NewKeyBuffer(ListKey, key, 8)
	binary.BigEndian.PutUint64(k.SuffixForRead(8), uint64(p.i-math.MinInt64))
	p.wb.Put(k.Key(), value)
}

func (p *rdbParser) EndList(key []byte) {
}

func (p *rdbParser) StartZSet(key []byte, cardinality, expiry int64) {
	Del([][]byte{key}, p.wb)
	setZcard(metaKey(key), uint32(cardinality), p.wb)
}

func (p *rdbParser) Zadd(key []byte, score float64, member []byte) {
	scoreBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))
	scoreKey := NewKeyBuffer(ZScoreKey, key, len(member)+8)
	setZScoreKeyMember(scoreKey, member)
	setZScoreKeyScore(scoreKey, score)
	p.wb.Put(NewKeyBufferWithSuffix(ZSetKey, key, member).Key(), scoreBytes)
	p.wb.Put(scoreKey.Key(), []byte{})
}

func (p *rdbParser) EndZSet(key []byte) {
}
