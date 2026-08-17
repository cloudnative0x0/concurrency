package semaphore

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStress_NeverExceedsCapacity(t *testing.T) {
	const (
		capacity   = 8
		goroutines = 500
		iterations = 200
	)

	sem := NewSemaphore(capacity)

	var (
		current int64
		max     int64
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))

			for i := 0; i < iterations; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				release, err := sem.Acquire(ctx)
				cancel()
				if err != nil {
					continue
				}

				n := atomic.AddInt64(&current, 1)
				for {
					old := atomic.LoadInt64(&max)
					if n <= old || atomic.CompareAndSwapInt64(&max, old, n) {
						break
					}
				}

				if rnd.Intn(10) == 0 {
					time.Sleep(time.Duration(rnd.Intn(500)) * time.Microsecond)
				} else {
					runtime.Gosched()
				}

				atomic.AddInt64(&current, -1)
				release()
			}
		}(int64(g))
	}

	wg.Wait()

	if max > capacity {
		t.Fatalf("semaphore invariant violated: max concurrency = %d, capacity = %d", max, capacity)
	}
	if atomic.LoadInt64(&current) != 0 {
		t.Fatalf("token leak: current = %d after all goroutines finished", current)
	}
	t.Logf("ok: max concurrency = %d (capacity = %d)", max, capacity)
}

func TestStress_DoubleReleaseIsSafe(t *testing.T) {
	const capacity = 4
	sem := NewSemaphore(capacity)

	ctx := context.Background()
	release, err := sem.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release()
		}()
	}
	wg.Wait()

	acquired := 0
	for i := 0; i < capacity; i++ {
		if r, ok := sem.TryAcquire(); ok {
			acquired++
			_ = r
		}
	}
	if _, ok := sem.TryAcquire(); ok {
		t.Fatalf("semaphore granted more slots than capacity=%d — double release broke the counter", capacity)
	}
	if acquired != capacity {
		t.Fatalf("expected to acquire %d slots, got %d", capacity, acquired)
	}
}

func TestStress_TryAcquireUnderContention(t *testing.T) {
	const (
		capacity   = 5
		goroutines = 300
	)
	sem := NewSemaphore(capacity)

	var successes int64
	var wg sync.WaitGroup
	wg.Add(goroutines)

	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start

			release, ok := sem.TryAcquire()
			if !ok {
				return
			}
			atomic.AddInt64(&successes, 1)
			runtime.Gosched()
			release()
		}()
	}
	close(start)
	wg.Wait()

	drained := 0
	for {
		r, ok := sem.TryAcquire()
		if !ok {
			break
		}
		drained++
		_ = r
	}
	if drained != capacity {
		t.Fatalf("expected %d free slots after stress, got %d — tokens leaked", capacity, drained)
	}
}

func TestStress_CloseDuringConcurrentAcquire(t *testing.T) {
	const (
		capacity   = 4
		goroutines = 200
	)

	sem := NewSemaphore(capacity)

	var (
		closedErrs int64
		succeeded  int64
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			release, err := sem.Acquire(ctx)
			switch {
			case err == ErrSemaphoreClosed:
				atomic.AddInt64(&closedErrs, 1)
			case err != nil:
			default:
				atomic.AddInt64(&succeeded, 1)
				time.Sleep(time.Millisecond)
				release()
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	sem.Close()

	wg.Wait()

	t.Logf("succeeded=%d, closedErrs=%d (goroutines=%d)", succeeded, closedErrs, goroutines)
	if succeeded == 0 {
		t.Fatalf("no goroutine managed to acquire a slot before Close() — suspicious test outcome")
	}
}

func TestStress_ContextCancellationDoesNotLeakTokens(t *testing.T) {
	const capacity = 2
	sem := NewSemaphore(capacity)

	holders := make([]func(), 0, capacity)
	for i := 0; i < capacity; i++ {
		r, err := sem.Acquire(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		holders = append(holders, r)
	}

	const waiters = 100
	var timeouts int64
	var wg sync.WaitGroup
	wg.Add(waiters)

	for i := 0; i < waiters; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			_, err := sem.Acquire(ctx)
			if err != nil {
				atomic.AddInt64(&timeouts, 1)
				return
			}
			t.Errorf("Acquire unexpectedly succeeded while semaphore was fully occupied")
		}()
	}
	wg.Wait()

	if timeouts != waiters {
		t.Fatalf("expected %d timeouts, got %d", waiters, timeouts)
	}

	for _, release := range holders {
		release()
	}
	for i := 0; i < capacity; i++ {
		if _, ok := sem.TryAcquire(); !ok {
			t.Fatalf("slot %d unavailable after releasing holders — tokens lost", i)
		}
	}
}

func TestStress_LongRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long stress test in -short mode")
	}

	const (
		capacity   = 16
		goroutines = 1000
		duration   = 2 * time.Second
	)

	sem := NewSemaphore(capacity)
	deadline := time.Now().Add(duration)

	var current, max int64
	var ops int64

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				release, ok := sem.TryAcquire()
				if !ok {
					runtime.Gosched()
					continue
				}

				n := atomic.AddInt64(&current, 1)
				for {
					old := atomic.LoadInt64(&max)
					if n <= old || atomic.CompareAndSwapInt64(&max, old, n) {
						break
					}
				}

				atomic.AddInt64(&ops, 1)
				atomic.AddInt64(&current, -1)
				release()
			}
		}()
	}
	wg.Wait()

	t.Logf("total ops = %d, max concurrency = %d / capacity %d", ops, max, capacity)
	if max > capacity {
		t.Fatalf("invariant violated: max concurrency = %d > capacity = %d", max, capacity)
	}
}
