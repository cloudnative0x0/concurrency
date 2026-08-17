package semaphore

import (
	"context"
	"errors"
	"sync"
)

var ErrSemaphoreClosed = errors.New("semaphore: closed")

type Semaphore struct {
	noCopy noCopy //nolint:unused

	tokens chan struct{}
	mu     sync.RWMutex
	closed bool
}

func NewSemaphore(n int) *Semaphore {
	if n <= 0 {
		panic("semaphore: capacity must be > 0")
	}

	return &Semaphore{
		tokens: make(chan struct{}, n),
	}
}

func (s *Semaphore) Acquire(ctx context.Context) (release func(), err error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrSemaphoreClosed
	}

	select {
	case s.tokens <- struct{}{}:
		var once sync.Once

		return func() {
			once.Do(func() { <-s.tokens })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Semaphore) TryAcquire() (release func(), ok bool) {
	select {
	case s.tokens <- struct{}{}:
		var once sync.Once

		return func() {
			once.Do(func() { <-s.tokens })
		}, true
	default:
		return nil, false
	}
}

func (s *Semaphore) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

type noCopy struct{}

func (*noCopy) Lock()   {} //nolint:unused
func (*noCopy) UnLock() {} //nolint:unused
