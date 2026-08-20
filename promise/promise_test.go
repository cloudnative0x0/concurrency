package promise

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if after := runtime.NumGoroutine(); after <= before {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("possible goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}

func TestOriginalScenario(t *testing.T) {
	asyncJob := func(ctx context.Context) (string, error) {
		select {
		case <-time.After(time.Second):
			return "ok", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	ctx := context.Background()
	p := NewPromise(ctx, asyncJob)

	var wg sync.WaitGroup
	wg.Add(2)

	p.Then(
		func(value string) { fmt.Println("subscriber 1:", value); wg.Done() },
		func(err error) { fmt.Println("subscriber 1 error:", err); wg.Done() },
	)
	p.Then(
		func(value string) { fmt.Println("subscriber 2:", value); wg.Done() },
		func(err error) { fmt.Println("subscriber 2 error:", err); wg.Done() },
	)

	val, err := p.Await()
	fmt.Println("await:", val, err)
	if err != nil || val != "ok" {
		t.Fatalf("unexpected result: val=%q err=%v", val, err)
	}

	panicPromise := NewPromise(ctx, func(ctx context.Context) (int, error) {
		panic("boom")
	})
	if _, err := panicPromise.Await(); err != nil {
		fmt.Println("panic recovered as error:", err)
	} else {
		t.Fatal("expected panic to be recovered as error")
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	slow := NewPromise(context.Background(), func(ctx context.Context) (string, error) {
		time.Sleep(time.Second)
		return "too slow", nil
	})
	if _, err := slow.AwaitContext(timeoutCtx); errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("timed out as expected")
	} else {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	wg.Wait()
}

func TestStress_ManySubscribers(t *testing.T) {
	const numPromises = 200
	const subscribersPerPromise = 50

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	var successCount, errorCount int64

	expectedSuccess, expectedFail := 0, 0

	for i := 0; i < numPromises; i++ {
		i := i
		shouldFail := i%3 == 0
		if shouldFail {
			expectedFail++
		} else {
			expectedSuccess++
		}

		p := NewPromise(context.Background(), func(ctx context.Context) (int, error) {
			if shouldFail {
				return 0, fmt.Errorf("intentional failure %d", i)
			}
			return i, nil
		})

		for s := 0; s < subscribersPerPromise; s++ {
			wg.Add(1)
			p.Then(
				func(v int) {
					atomic.AddInt64(&successCount, 1)
					wg.Done()
				},
				func(err error) {
					atomic.AddInt64(&errorCount, 1)
					wg.Done()
				},
			)
		}
	}

	wg.Wait()

	if got := atomic.LoadInt64(&successCount); got != int64(expectedSuccess*subscribersPerPromise) {
		t.Errorf("success callbacks: got %d, want %d", got, expectedSuccess*subscribersPerPromise)
	}
	if got := atomic.LoadInt64(&errorCount); got != int64(expectedFail*subscribersPerPromise) {
		t.Errorf("error callbacks: got %d, want %d", got, expectedFail*subscribersPerPromise)
	}

	assertNoGoroutineLeak(t, before)
}

func TestStress_ThenBeforeAndAfterResolve(t *testing.T) {
	const iterations = 500

	before := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		p := NewPromise(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})

		var wg sync.WaitGroup

		// Подписчики "до" резолва.
		for s := 0; s < 5; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.Then(func(v int) {}, func(err error) {})
			}()
		}

		_, _ = p.Await()

		for s := 0; s < 5; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				val, err := p.Await()
				if err != nil || val != 42 {
					t.Errorf("unexpected: val=%d err=%v", val, err)
				}
			}()
		}

		wg.Wait()
	}

	assertNoGoroutineLeak(t, before)
}

func TestStress_ConcurrentPanics(t *testing.T) {
	const n = 500

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		p := NewPromise(context.Background(), func(ctx context.Context) (int, error) {
			panic(fmt.Sprintf("boom-%d", i))
		})
		go func() {
			defer wg.Done()
			if _, err := p.Await(); err == nil {
				t.Errorf("expected panic recovered as error for promise %d", i)
			}
		}()
	}
	wg.Wait()

	assertNoGoroutineLeak(t, before)
}

func TestStress_NoSubscriberNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 1000; i++ {
		_ = NewPromise(context.Background(), func(ctx context.Context) (int, error) {
			return 1, nil
		})
	}

	assertNoGoroutineLeak(t, before)
}

func TestStress_AwaitContextRace(t *testing.T) {
	const n = 300

	before := runtime.NumGoroutine()

	var timedOut, completed int64
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Intn(10))*time.Millisecond)
			defer cancel()

			p := NewPromise(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				return 1, nil
			})

			_, err := p.AwaitContext(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				atomic.AddInt64(&timedOut, 1)
			} else {
				atomic.AddInt64(&completed, 1)
			}
		}()
	}
	wg.Wait()
	t.Logf("timedOut=%d completed=%d", timedOut, completed)

	assertNoGoroutineLeak(t, before)
}
