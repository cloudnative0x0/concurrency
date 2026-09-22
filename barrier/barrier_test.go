package barrier

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBarrierWaitsForEveryParticipant(t *testing.T) {
	const participants = 3
	b := NewBarrier(participants)

	beforeDone := make(chan struct{}, participants)
	for i := 0; i < participants-1; i++ {
		go func() {
			b.Before()
			beforeDone <- struct{}{}
		}()
	}

	waitForCount(t, b, participants-1)
	assertNoSignal(t, beforeDone, "Before returned before every participant arrived")

	go func() {
		b.Before()
		beforeDone <- struct{}{}
	}()
	awaitSignals(t, beforeDone, participants, "Before did not release every participant")

	afterDone := make(chan struct{}, participants)
	for i := 0; i < participants-1; i++ {
		go func() {
			b.After()
			afterDone <- struct{}{}
		}()
	}

	waitForCount(t, b, 1)
	assertNoSignal(t, afterDone, "After returned before every participant arrived")

	go func() {
		b.After()
		afterDone <- struct{}{}
	}()
	awaitSignals(t, afterDone, participants, "After did not release every participant")
}

func TestBarrierCanBeReused(t *testing.T) {
	const (
		participants = 8
		rounds       = 100
	)

	b := NewBarrier(participants)
	var arrivals atomic.Int64
	var wg sync.WaitGroup
	wg.Add(participants)

	for i := 0; i < participants; i++ {
		go func() {
			defer wg.Done()

			for round := 0; round < rounds; round++ {
				b.Before()
				arrivals.Add(1)
				b.After()

				if got, want := arrivals.Load(), int64((round+1)*participants); got != want {
					t.Errorf("round %d: got %d arrivals after barrier, want %d", round, got, want)
					return
				}
				runtime.Gosched()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reusable barrier deadlocked")
	}

	if got, want := arrivals.Load(), int64(participants*rounds); got != want {
		t.Fatalf("got %d total arrivals, want %d", got, want)
	}
}

func TestBarrierWithOneParticipant(t *testing.T) {
	b := NewBarrier(1)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			b.Before()
			b.After()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("barrier with one participant deadlocked")
	}
}

func waitForCount(t *testing.T, b *Barrier, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mutex.Lock()
		got := b.count
		b.mutex.Unlock()

		if got == want {
			return
		}
		runtime.Gosched()
	}

	t.Fatalf("barrier count did not reach %d", want)
}

func assertNoSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(message)
	default:
	}
}

func awaitSignals(t *testing.T, ch <-chan struct{}, count int, message string) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal(message)
		}
	}
}
