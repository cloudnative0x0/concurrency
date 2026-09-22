package rw_mutex

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type RWMutex struct {
	_ noCopy

	// gate protects every field below it. It is held only for short state and
	// queue updates; goroutines never park while holding gate.
	gate atomic.Uint32

	activeReaders uint32
	writerActive  bool

	readerHead   *waiter
	readerTail   *waiter
	readerQueued uint32

	writerHead   *waiter
	writerTail   *waiter
	writerQueued uint32
}

type waiter struct {
	ready chan struct{}
	next  *waiter
}

// RLock acquires a read lock. Readers may run together, but a new reader waits
// once a writer has queued so that a steady read load cannot starve writers.
func (rw *RWMutex) RLock() {
	rw.lockGate()
	if !rw.writerActive && rw.writerQueued == 0 {
		rw.activeReaders++
		rw.unlockGate()
		return
	}

	w := &waiter{ready: make(chan struct{})}
	rw.enqueueReader(w)
	rw.unlockGate()
	<-w.ready
}

// TryRLock attempts to acquire a read lock without waiting.
func (rw *RWMutex) TryRLock() bool {
	if !rw.tryLockGate() {
		return false
	}
	if rw.writerActive || rw.writerQueued != 0 {
		rw.unlockGate()
		return false
	}

	rw.activeReaders++
	rw.unlockGate()
	return true
}

// RUnlock releases one read lock. Calling RUnlock without a matching RLock
// panics.
func (rw *RWMutex) RUnlock() {
	rw.lockGate()
	if rw.activeReaders == 0 {
		rw.unlockGate()
		panic("rw_mutex: RUnlock of unlocked RWMutex")
	}

	rw.activeReaders--
	if rw.activeReaders != 0 || rw.writerHead == nil {
		rw.unlockGate()
		return
	}

	// The final reader transfers ownership directly to the oldest writer.
	w := rw.dequeueWriter()
	rw.writerActive = true
	rw.unlockGate()
	close(w.ready)
}

// Lock acquires the mutex for exclusive writing.
func (rw *RWMutex) Lock() {
	rw.lockGate()
	if !rw.writerActive && rw.activeReaders == 0 {
		rw.writerActive = true
		rw.unlockGate()
		return
	}

	w := &waiter{ready: make(chan struct{})}
	rw.enqueueWriter(w)
	rw.unlockGate()
	<-w.ready
}

// TryLock attempts to acquire the mutex for writing without waiting.
func (rw *RWMutex) TryLock() bool {
	if !rw.tryLockGate() {
		return false
	}
	if rw.writerActive || rw.activeReaders != 0 {
		rw.unlockGate()
		return false
	}

	rw.writerActive = true
	rw.unlockGate()
	return true
}

// Unlock releases a write lock. Calling Unlock without a matching Lock panics.
func (rw *RWMutex) Unlock() {
	rw.lockGate()
	if !rw.writerActive {
		rw.unlockGate()
		panic("rw_mutex: Unlock of unlocked RWMutex")
	}

	if rw.readerHead != nil {
		// Readers accumulated during this writer phase are released as one batch.
		// If writers are still queued, newly arriving readers remain blocked and
		// the final reader in this batch hands ownership to the next writer.
		readers, count := rw.detachReaders()
		rw.writerActive = false
		rw.activeReaders = count
		rw.unlockGate()
		wakeAll(readers)
		return
	}

	if rw.writerHead != nil {
		// No reader phase is waiting, so hand ownership directly to a writer.
		w := rw.dequeueWriter()
		// writerActive deliberately remains true during the handoff.
		rw.unlockGate()
		close(w.ready)
		return
	}

	rw.writerActive = false
	rw.unlockGate()
}

// RLocker returns a sync.Locker backed by RLock and RUnlock.
func (rw *RWMutex) RLocker() sync.Locker {
	return (*readLocker)(rw)
}

type readLocker RWMutex

func (r *readLocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *readLocker) Unlock() { (*RWMutex)(r).RUnlock() }

func (rw *RWMutex) enqueueReader(w *waiter) {
	if rw.readerTail == nil {
		rw.readerHead = w
		rw.readerTail = w
	} else {
		rw.readerTail.next = w
		rw.readerTail = w
	}
	rw.readerQueued++
}

func (rw *RWMutex) enqueueWriter(w *waiter) {
	if rw.writerTail == nil {
		rw.writerHead = w
		rw.writerTail = w
	} else {
		rw.writerTail.next = w
		rw.writerTail = w
	}
	rw.writerQueued++
}

func (rw *RWMutex) dequeueWriter() *waiter {
	w := rw.writerHead
	rw.writerHead = w.next
	w.next = nil
	rw.writerQueued--
	if rw.writerHead == nil {
		rw.writerTail = nil
	}
	return w
}

func (rw *RWMutex) detachReaders() (*waiter, uint32) {
	head := rw.readerHead
	count := rw.readerQueued
	rw.readerHead = nil
	rw.readerTail = nil
	rw.readerQueued = 0
	return head, count
}

func wakeAll(head *waiter) {
	for head != nil {
		next := head.next
		head.next = nil
		close(head.ready)
		head = next
	}
}

func (rw *RWMutex) lockGate() {
	for !rw.gate.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
}

func (rw *RWMutex) tryLockGate() bool {
	return rw.gate.CompareAndSwap(0, 1)
}

func (rw *RWMutex) unlockGate() {
	rw.gate.Store(0)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
