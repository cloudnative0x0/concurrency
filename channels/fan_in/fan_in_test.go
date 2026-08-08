package fan_in

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func drain[T any](t *testing.T, ch <-chan T, timeout time.Duration) []T {
	t.Helper()

	var result []T
	deadline := time.After(timeout)

	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, v)
		case <-deadline:
			t.Fatalf("timeout: merge did not closed exit channel for %s", timeout)
			return nil
		}
	}
}

func toChan[T any](vals ...T) <-chan T {
	ch := make(chan T, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestMerge_NoChannels(t *testing.T) {
	out := mergeChs[int]()

	got := drain(t, out, time.Second)

	if len(got) != 0 {
		t.Fatalf("Empty result expected, got: %v", got)
	}
}

func TestMerge_SingleChannel(t *testing.T) {
	in := toChan(1, 2, 3, 4, 5)

	out := mergeChs(in)
	got := drain(t, out, time.Second)

	sort.Ints(got)
	want := []int{1, 2, 3, 4, 5}

	if len(got) != len(want) {
		t.Fatalf("got %d elements, expected %d: got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}

func TestMerge_MultipleChannels(t *testing.T) {
	ch1 := toChan(1, 2, 3)
	ch2 := toChan(4, 5, 6)
	ch3 := toChan(7, 8, 9, 10)

	out := mergeChs(ch1, ch2, ch3)
	got := drain(t, out, 2*time.Second)

	sort.Ints(got)
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	if len(got) != len(want) {
		t.Fatalf("got %d elements, expected %d: got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}

func TestMerge_ClosesAfterAllInputsClose(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)

	out := mergeChs[int](ch1, ch2)

	close(ch1)

	select {
	case v, ok := <-out:
		if !ok {
			t.Fatal("The output channel was closed early: not all input channels were closed")
		}
		t.Fatalf("Unexpectedly received value %v before data was sent", v)
	case <-time.After(200 * time.Millisecond):
	}

	ch2 <- 42
	close(ch2)

	got := drain(t, out, time.Second)
	if len(got) != 1 || got[0] != 42 {
		t.Fatalf("got=%v, want=[42]", got)
	}
}

func TestMerge_NoDataLoss(t *testing.T) {
	const (
		numChannels = 50
		valsPerCh   = 200
	)

	channels := make([]<-chan int, numChannels)
	for i := 0; i < numChannels; i++ {
		ch := make(chan int, valsPerCh)
		for j := 0; j < valsPerCh; j++ {
			ch <- i*valsPerCh + j
		}
		close(ch)
		channels[i] = ch
	}

	out := mergeChs(channels...)
	got := drain(t, out, 5*time.Second)

	wantCount := numChannels * valsPerCh
	if len(got) != wantCount {
		t.Fatalf("Received %d values, expected %d (data loss or duplication may have occurred)", len(got), wantCount)
	}

	seen := make(map[int]bool, wantCount)
	for _, v := range got {
		if seen[v] {
			t.Fatalf("Value %d received more than once (duplication)", v)
		}
		seen[v] = true
	}
}

func TestMerge_ConcurrentSenders(t *testing.T) {
	const (
		numChannels = 10
		valsPerCh   = 100
	)

	rawChannels := make([]chan int, numChannels)
	inputChannels := make([]<-chan int, numChannels)
	for i := range rawChannels {
		rawChannels[i] = make(chan int)
		inputChannels[i] = rawChannels[i]
	}

	out := mergeChs(inputChannels...)

	var wg sync.WaitGroup
	wg.Add(numChannels)
	for i, ch := range rawChannels {
		go func(idx int, ch chan int) {
			defer wg.Done()
			defer close(ch)
			for j := 0; j < valsPerCh; j++ {
				ch <- idx*valsPerCh + j
			}
		}(i, ch)
	}

	var count int64
	done := make(chan struct{})
	go func() {
		for range out {
			atomic.AddInt64(&count, 1)
		}
		close(done)
	}()

	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: The output channel did not close after all senders finished")
	}

	if got, want := atomic.LoadInt64(&count), int64(numChannels*valsPerCh); got != want {
		t.Fatalf("Received %d values, expected %d", got, want)
	}
}

func TestMerge_EmptyInputChannels(t *testing.T) {
	empty1 := toChan[int]()
	empty2 := toChan[int]()
	withData := toChan(1, 2, 3)

	out := mergeChs(empty1, empty2, withData)
	got := drain(t, out, time.Second)

	sort.Ints(got)
	want := []int{1, 2, 3}

	if len(got) != len(want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v, want=%v", got, want)
		}
	}
}
