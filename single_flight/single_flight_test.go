package singleflight

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStressSingleFlight(t *testing.T) {
	const inFlightRequests = 200
	const key = "same_key"

	sf := NewSingleFlight()

	var actionCalls int32
	var sharedCount int32

	var wg sync.WaitGroup
	wg.Add(inFlightRequests)

	for i := 0; i < inFlightRequests; i++ {
		i := i
		go func() {
			defer wg.Done()

			value, err, shared := sf.Do(key, func() (interface{}, error) {
				atomic.AddInt32(&actionCalls, 1)
				time.Sleep(50 * time.Millisecond)
				return "result", nil
			})

			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, err)
			}
			if value != "result" {
				t.Errorf("goroutine %d: unexpected value: %v", i, value)
			}
			if shared {
				atomic.AddInt32(&sharedCount, 1)
			}
		}()
	}

	wg.Wait()

	if got := atomic.LoadInt32(&actionCalls); got != 1 {
		t.Fatalf("action() should be called exactly once, got %d calls", got)
	}
	if sharedCount == 0 {
		t.Fatalf("expected at least some goroutines to report shared=true")
	}

	t.Logf("action calls: %d, shared: %d/%d", actionCalls, sharedCount, inFlightRequests)
}

func TestStressSingleFlight_MultipleKeys(t *testing.T) {
	const keysCount = 20
	const requestsPerKey = 50

	sf := NewSingleFlight()

	var callsPerKey sync.Map // key -> *int32

	var wg sync.WaitGroup
	wg.Add(keysCount * requestsPerKey)

	for k := 0; k < keysCount; k++ {
		key := fmt.Sprintf("key-%d", k)

		counter := new(int32)
		callsPerKey.Store(key, counter)

		for r := 0; r < requestsPerKey; r++ {
			go func(key string, counter *int32) {
				defer wg.Done()

				value, err, _ := sf.Do(key, func() (interface{}, error) {
					atomic.AddInt32(counter, 1)
					time.Sleep(20 * time.Millisecond)
					return key, nil
				})

				if err != nil {
					t.Errorf("key %s: unexpected error: %v", key, err)
				}
				if value != key {
					t.Errorf("key %s: unexpected value: %v", key, value)
				}
			}(key, counter)
		}
	}

	wg.Wait()

	callsPerKey.Range(func(k, v interface{}) bool {
		counter := v.(*int32)
		if got := atomic.LoadInt32(counter); got != 1 {
			t.Errorf("key %v: action() should be called exactly once, got %d", k, got)
		}
		return true
	})
}

func TestStressSingleFlight_SequentialCalls(t *testing.T) {
	const key = "same_key"
	const iterations = 100

	sf := NewSingleFlight()

	var actionCalls int32

	for i := 0; i < iterations; i++ {
		value, err, shared := sf.Do(key, func() (interface{}, error) {
			atomic.AddInt32(&actionCalls, 1)
			return "result", nil
		})

		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if value != "result" {
			t.Fatalf("iteration %d: unexpected value: %v", i, value)
		}
		if shared {
			t.Fatalf("iteration %d: sequential call should not be shared", i)
		}
	}

	if got := atomic.LoadInt32(&actionCalls); got != iterations {
		t.Fatalf("expected %d action calls for sequential Do, got %d", iterations, got)
	}
}

func TestStressSingleFlight_Error(t *testing.T) {
	const inFlightRequests = 100
	const key = "same_key"

	sf := NewSingleFlight()
	wantErr := errors.New("boom")

	var wg sync.WaitGroup
	wg.Add(inFlightRequests)

	for i := 0; i < inFlightRequests; i++ {
		go func() {
			defer wg.Done()

			value, err, _ := sf.Do(key, func() (interface{}, error) {
				time.Sleep(30 * time.Millisecond)
				return nil, wantErr
			})

			if !errors.Is(err, wantErr) {
				t.Errorf("unexpected error: %v", err)
			}
			if value != nil {
				t.Errorf("unexpected value on error: %v", value)
			}
		}()
	}

	wg.Wait()
}

func TestStressSingleFlight_Panic(t *testing.T) {
	const inFlightRequests = 50
	const key = "same_key"

	sf := NewSingleFlight()

	var wg sync.WaitGroup
	wg.Add(inFlightRequests)

	var recovered int32

	for i := 0; i < inFlightRequests; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&recovered, 1)
				}
			}()

			_, _, _ = sf.Do(key, func() (interface{}, error) {
				time.Sleep(20 * time.Millisecond)
				panic("boom")
			})
		}()
	}

	wg.Wait()

	if got := atomic.LoadInt32(&recovered); got != inFlightRequests {
		t.Fatalf("expected panic to propagate to all %d goroutines, got %d", inFlightRequests, got)
	}
}

func TestStressSingleFlight_Forget(t *testing.T) {
	const key = "same_key"

	sf := NewSingleFlight()

	var actionCalls int32
	started := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_, _, _ = sf.Do(key, func() (interface{}, error) {
			atomic.AddInt32(&actionCalls, 1)
			close(started)
			<-release

			return "first", nil
		})
	}()

	<-started
	sf.Forget(key)

	value, err, shared := sf.Do(key, func() (interface{}, error) {
		atomic.AddInt32(&actionCalls, 1)
		return "second", nil
	})

	close(release)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "second" {
		t.Fatalf("expected 'second', got %v", value)
	}
	if shared {
		t.Fatalf("expected non-shared call after Forget")
	}
	if got := atomic.LoadInt32(&actionCalls); got != 2 {
		t.Fatalf("expected 2 action calls after Forget, got %d", got)
	}
}
