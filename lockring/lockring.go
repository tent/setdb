package lockring

import (
	"hash/crc32"
	"sync"
)

type LockRing struct {
	size  uint32
	locks []sync.RWMutex
}

func NewLockRing(n uint32) *LockRing {
	return &LockRing{n, make([]sync.RWMutex, n)}
}

func (l *LockRing) Lock(k []byte) {
	l.lockForKey(k).Lock()
}

func (l *LockRing) Unlock(k []byte) {
	l.lockForKey(k).Unlock()
}

func (l *LockRing) RLock(k []byte) {
	l.lockForKey(k).RLock()
}

func (l *LockRing) RUnlock(k []byte) {
	l.lockForKey(k).RUnlock()
}

func (l *LockRing) lockForKey(k []byte) *sync.RWMutex {
	return &l.locks[int(crc32.ChecksumIEEE(k)%l.size)]
}
