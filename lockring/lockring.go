package lockring

import (
	"hash/crc32"
	"sync"
)

type LockRing struct {
	size  uint32
	locks []sync.Mutex
}

func New(n uint32) *LockRing {
	return &LockRing{n, make([]sync.Mutex, n)}
}

func (l *LockRing) Lock(k []byte) {
	l.lockForKey(k).Lock()
}

func (l *LockRing) Unlock(k []byte) {
	l.lockForKey(k).Unlock()
}

func (l *LockRing) lockForKey(k []byte) *sync.Mutex {
	return &l.locks[int(crc32.ChecksumIEEE(k)%l.size)]
}
