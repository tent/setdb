package main

import (
	"bytes"
	"encoding/binary"
)

const keyPrefixSize = 5

// KeyBuffer is a reusable key for LevelDB
type KeyBuffer struct {
	buf        []byte
	keyLen     int
	reverseKey bool
}

// Create a new key of type t with key and room for extra bytes.
func NewKeyBuffer(t byte, key []byte, extra int) *KeyBuffer {
	k := &KeyBuffer{make([]byte, keyPrefixSize, keyPrefixSize+len(key)+extra), len(key), false}
	k.buf[0] = t
	k.SetKey(key)
	return k
}

func NewKeyBufferWithSuffix(t byte, key []byte, suffix []byte) *KeyBuffer {
	k := &KeyBuffer{make([]byte, keyPrefixSize, keyPrefixSize+len(key)+len(suffix)), len(key), false}
	k.buf[0] = t
	k.SetKey(key)
	k.SetSuffix(suffix)
	return k
}

func (b *KeyBuffer) SetKey(key []byte) {
	if len(key) == 0 {
		return
	}
	b.keyLen = len(key)
	binary.BigEndian.PutUint32(b.buf[1:], uint32(len(key)))
	b.buf = append(b.buf[:keyPrefixSize], key...)
}

// Add extra after the key, will overwrite any existing extra
func (b *KeyBuffer) SetSuffix(s []byte) {
	b.buf = append(b.buf[:keyPrefixSize+b.keyLen], s...)
}

// Return a slice of size n suitable for using with an io.Reader
// The read bytes will overwrite the first n bytes of suffix (if they exist)
func (b *KeyBuffer) SuffixForRead(n int) []byte {
	start := keyPrefixSize + b.keyLen
	if len(b.buf) < start+n {
		b.buf = append(b.buf[:start], make([]byte, n)...) // resize the slice to be large enough
	}
	return b.buf[start : start+n]
}

// Check if k starts with the key (without the suffix)
func (b *KeyBuffer) IsPrefixOf(k []byte) bool {
	keyLen := keyPrefixSize + b.keyLen
	// the last byte is 0xff, so truncate a byte early
	if b.reverseKey {
		keyLen--
	}
	if len(k) > keyLen && bytes.Equal(b.buf[:keyLen], k[:keyLen]) {
		return true
	}
	return false
}

// Change the key so that it sorts to come after the last prefix key
//
// To get a key that will sort *after* given prefix, we increment the last
// byte that is not 0xff and truncated after the byte that was incremented
func (b *KeyBuffer) ReverseIterKey() {
	b.reverseKey = true
	for i := len(b.buf) - 1; i >= 0; i-- {
		if b.buf[i] == 0xff {
			continue
		}
		b.buf[i] += 1
		b.buf = b.buf[:i+1]
		break
	}
}

func (b *KeyBuffer) Type() byte {
	return b.buf[0]
}

func (b *KeyBuffer) Key() []byte {
	return b.buf
}
