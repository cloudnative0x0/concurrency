package mutex

import (
	"runtime"
	"sync/atomic"
)

type Mutex struct {
	_ noCopy

	// state is 0 when the mutex is free and mutexLocked while it is owned.
	state atomic.Uint32

	// queueGuard is a tiny spin lock used only while changing the waiter list.
	// A goroutine never waits for ownership of Mutex while holding queueGuard.
	queueGuard atomic.Uint32
	head       *waiter
	tail       *waiter
}

const mutexLocked uint32 = 1

type waiter struct {
	ready chan struct{}
	next  *waiter
}

// Lock acquires m. If another goroutine owns m, Lock blocks until ownership is
// handed to it.
func (m *Mutex) Lock() {
	// Fast path: the common uncontended case is one atomic instruction.
	if m.state.CompareAndSwap(0, mutexLocked) {
		return
	}

	m.lockSlow()
}

func (m *Mutex) lockSlow() {
	w := &waiter{ready: make(chan struct{})}

	m.lockQueue()

	// Unlock may have made the mutex free after the first CAS failed but before
	// this goroutine reached the queue. Rechecking under queueGuard closes that
	// race without parking unnecessarily.
	if m.state.CompareAndSwap(0, mutexLocked) {
		m.unlockQueue()
		return
	}

	if m.tail == nil {
		m.head = w
		m.tail = w
	} else {
		m.tail.next = w
		m.tail = w
	}
	m.unlockQueue()

	<-w.ready
}

// TryLock attempts to acquire m without waiting.
func (m *Mutex) TryLock() bool {
	return m.state.CompareAndSwap(0, mutexLocked)
}

// Unlock releases m. Unlocking an unlocked Mutex panics.
//
// A Mutex is not tied to a goroutine ID, so a different goroutine may unlock
// it. Code should still keep ownership explicit unless a deliberate handoff is
// part of the design.
func (m *Mutex) Unlock() {
	m.lockQueue()

	if m.state.Load() != mutexLocked {
		m.unlockQueue()
		panic("mutex: unlock of unlocked mutex")
	}

	if m.head == nil {
		// No waiter needs the lock. Publishing state=0 lets a future Lock acquire
		// it through the fast path.
		m.state.Store(0)
		m.unlockQueue()
		return
	}

	// Keep state locked while removing the oldest waiter. A new Lock therefore
	// cannot barge ahead between this Unlock and the waiter's wake-up.
	w := m.head
	m.head = w.next
	if m.head == nil {
		m.tail = nil
	}
	m.unlockQueue()
	close(w.ready)
}

func (m *Mutex) lockQueue() {
	for !m.queueGuard.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
}

func (m *Mutex) unlockQueue() {
	m.queueGuard.Store(0)
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
