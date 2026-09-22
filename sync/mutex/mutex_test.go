package mutex

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var _ sync.Locker = (*Mutex)(nil)

func TestMutexZeroValue(t *testing.T) {
	var mu Mutex

	mu.Lock()
	mu.Unlock()

	if !mu.TryLock() {
		t.Fatal("TryLock failed for an unlocked zero-value Mutex")
	}
	mu.Unlock()
}

func TestMutexProvidesMutualExclusion(t *testing.T) {
	const (
		goroutines = 64
		iterations = 2_000
	)

	var (
		mu        Mutex
		wg        sync.WaitGroup
		counter   int
		inside    atomic.Int64
		maxInside atomic.Int64
	)
	wg.Add(goroutines)

	for id := 0; id < goroutines; id++ {
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				mu.Lock()

				current := inside.Add(1)
				updateMaximum(&maxInside, current)
				counter++
				inside.Add(-1)

				mu.Unlock()
				if iteration%17 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}

	waitGroupWithTimeout(t, &wg, 10*time.Second)

	if got, want := counter, goroutines*iterations; got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}
	if got := maxInside.Load(); got != 1 {
		t.Fatalf("critical section contained %d goroutines at once, want 1", got)
	}
}

func TestMutexBlocksUntilUnlock(t *testing.T) {
	var mu Mutex
	mu.Lock()

	acquired := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		mu.Lock()
		close(acquired)
		mu.Unlock()
		close(finished)
	}()

	select {
	case <-acquired:
		t.Fatal("second goroutine acquired an already locked Mutex")
	case <-time.After(20 * time.Millisecond):
	}

	mu.Unlock()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("waiting goroutine did not acquire Mutex after Unlock")
	}
}

func TestMutexTryLock(t *testing.T) {
	var mu Mutex

	if !mu.TryLock() {
		t.Fatal("TryLock failed for an unlocked Mutex")
	}
	if mu.TryLock() {
		t.Fatal("TryLock acquired an already locked Mutex")
	}

	mu.Unlock()
	if !mu.TryLock() {
		t.Fatal("TryLock failed after Unlock")
	}
	mu.Unlock()
}

func TestMutexCanBeUnlockedByAnotherGoroutine(t *testing.T) {
	var mu Mutex
	mu.Lock()

	unlocked := make(chan struct{})
	go func() {
		mu.Unlock()
		close(unlocked)
	}()

	select {
	case <-unlocked:
	case <-time.After(time.Second):
		t.Fatal("Mutex was not unlocked by another goroutine")
	}

	if !mu.TryLock() {
		t.Fatal("Mutex remained locked")
	}
	mu.Unlock()
}

func TestMutexWaiterMakesProgressUnderContention(t *testing.T) {
	var mu Mutex
	mu.Lock()

	waiterAcquired := make(chan struct{})
	go func() {
		mu.Lock()
		close(waiterAcquired)
		mu.Unlock()
	}()

	// Let the waiter enter the runtime semaphore before barging begins.
	time.Sleep(2 * time.Millisecond)

	stop := make(chan struct{})
	var bargers sync.WaitGroup
	for i := 0; i < 16; i++ {
		bargers.Add(1)
		go func() {
			defer bargers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					mu.Lock()
					mu.Unlock()
				}
			}
		}()
	}

	mu.Unlock()
	select {
	case <-waiterAcquired:
	case <-time.After(3 * time.Second):
		close(stop)
		bargers.Wait()
		t.Fatal("an existing waiter starved while new goroutines contended for Mutex")
	}

	close(stop)
	bargers.Wait()
}

func TestMutexUnlockOfUnlockedPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Unlock of an unlocked Mutex did not panic")
		}
	}()

	var mu Mutex
	mu.Unlock()
}

func updateMaximum(maximum *atomic.Int64, candidate int64) {
	for {
		current := maximum.Load()
		if candidate <= current || maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func waitGroupWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("concurrent Mutex test timed out")
	}
}

func BenchmarkMutexUncontended(b *testing.B) {
	var mu Mutex
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkMutexContended(b *testing.B) {
	var mu Mutex
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			mu.Unlock()
		}
	})
}
