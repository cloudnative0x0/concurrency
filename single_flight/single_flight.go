package singleflight

import (
	"sync"
)

type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error

	panicValue interface{}

	forgotten bool

	shared bool
}

type SingleFlight struct {
	mutex sync.Mutex
	calls map[string]*call
}

func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		calls: make(map[string]*call),
	}
}

func (s *SingleFlight) Do(key string, action func() (interface{}, error)) (v interface{}, err error, shared bool) {
	s.mutex.Lock()
	if c, found := s.calls[key]; found {
		c.shared = true
		s.mutex.Unlock()
		c.wg.Wait()

		if c.panicValue != nil {
			panic(c.panicValue)
		}

		return c.val, c.err, true
	}

	c := new(call)
	c.wg.Add(1)
	s.calls[key] = c
	s.mutex.Unlock()

	s.doCall(c, key, action)

	if c.panicValue != nil {
		panic(c.panicValue)
	}

	return c.val, c.err, c.shared
}

func (s *SingleFlight) doCall(c *call, key string, action func() (interface{}, error)) {
	defer func() {
		s.mutex.Lock()
		if !c.forgotten {
			delete(s.calls, key)
		}
		s.mutex.Unlock()

		if r := recover(); r != nil {
			c.panicValue = r
		}

		c.wg.Done()

		if c.panicValue != nil {
			panic(c.panicValue)
		}
	}()

	c.val, c.err = action()
}

func (s *SingleFlight) Forget(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if c, ok := s.calls[key]; ok {
		c.forgotten = true
	}

	delete(s.calls, key)
}
