package worker_pool

import (
	"sort"
	"sync"
	"testing"
	"time"
)

func collectWithTimeout[T any](t *testing.T, ch <-chan T, timeout time.Duration) []T {
	t.Helper()
	var out []T
	deadline := time.After(timeout)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, v)
		case <-deadline:
			t.Fatalf("timed out after %s waiting for channel to close", timeout)
			return nil
		}
	}
}

func makeChan[T any](values ...T) <-chan T {
	ch := make(chan T)
	go func() {
		defer close(ch)
		for _, v := range values {
			ch <- v
		}
	}()
	return ch
}

func TestParse_PreservesValuesAndOrder(t *testing.T) {
	in := makeChan(1, 2, 3, 4, 5)
	out := parse(in)

	got := collectWithTimeout(t, out, time.Second)
	want := []int{1, 2, 3, 4, 5}

	if len(got) != len(want) {
		t.Fatalf("got %d values, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestParse_EmptyInput(t *testing.T) {
	in := makeChan[int]()
	out := parse(in)

	got := collectWithTimeout(t, out, time.Second)
	if len(got) != 0 {
		t.Fatalf("expected no values, got %v", got)
	}
}

func TestParse_ClosesOutputWhenInputCloses(t *testing.T) {
	in := make(chan string)
	close(in)
	out := parse(in)

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected output channel to be closed with no values")
		}
	case <-time.After(time.Second):
		t.Fatal("output channel was not closed in time")
	}
}

func TestSend_AllValuesDeliveredNoDuplicatesNoLoss(t *testing.T) {
	tests := []struct {
		name    string
		items   int
		workers int
	}{
		{"more workers than items", 3, 10},
		{"fewer workers than items", 100, 4},
		{"single worker", 20, 1},
		{"equal workers and items", 10, 10},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			values := make([]int, tt.items)
			for i := range values {
				values[i] = i
			}
			in := makeChan(values...)
			out := send(in, tt.workers)

			got := collectWithTimeout(t, out, 3*time.Second)
			if len(got) != tt.items {
				t.Fatalf("got %d values, want %d (потеря или дублирование)", len(got), tt.items)
			}

			sort.Ints(got)
			for i, v := range got {
				if v != i {
					t.Fatalf("mismatch at position %d: got %d", i, v)
				}
			}
		})
	}
}

func TestSend_OutputClosesAfterAllWorkersFinish(t *testing.T) {
	in := makeChan(1, 2, 3)
	out := send(in, 5) // воркеров больше, чем элементов

	got := collectWithTimeout(t, out, time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d values, want 3", len(got))
	}
}

func TestSend_ZeroWorkersWithEmptyInputClosesImmediately(t *testing.T) {
	in := makeChan[int]()
	out := send(in, 0)

	got := collectWithTimeout(t, out, time.Second)
	if len(got) != 0 {
		t.Fatalf("expected no values with 0 workers, got %v", got)
	}
}

func TestPipeline_ParseThenSend(t *testing.T) {
	values := []string{"a", "b", "c", "d", "e", "f"}
	in := makeChan(values...)

	out := send(parse(in), 3)
	got := collectWithTimeout(t, out, time.Second)

	if len(got) != len(values) {
		t.Fatalf("got %d values, want %d", len(got), len(values))
	}

	seen := make(map[string]int)
	for _, v := range got {
		seen[v]++
	}
	for _, v := range values {
		if seen[v] != 1 {
			t.Errorf("value %q seen %d times, want exactly once", v, seen[v])
		}
	}
}

func TestSend_NoRaceOnConcurrentWrites(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[int]bool)

	values := make([]int, 200)
	for i := range values {
		values[i] = i
	}
	in := makeChan(values...)
	out := send(in, 8)

	for v := range out {
		mu.Lock()
		seen[v] = true
		mu.Unlock()
	}

	if len(seen) != len(values) {
		t.Fatalf("got %d unique values, want %d", len(seen), len(values))
	}
}
