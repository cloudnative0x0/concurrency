package fan_out

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func collectPerChannel[T any](t *testing.T, chans []<-chan T, timeout time.Duration) [][]T {
	t.Helper()

	results := make([][]T, len(chans))

	var wg sync.WaitGroup
	wg.Add(len(chans))
	for i, ch := range chans {
		go func(i int, ch <-chan T) {
			defer wg.Done()
			for v := range ch {
				results[i] = append(results[i], v)
			}
		}(i, ch)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return results
	case <-time.After(timeout):
		t.Fatalf("timeout: Not all splitCh output channels have closed for %s", timeout)
		return nil
	}
}

func TestSplitCh_ReturnsNChannels(t *testing.T) {
	in := make(chan int)
	close(in)

	chans := splitCh(in, 5)

	if len(chans) != 5 {
		t.Fatalf("%d channels received; 5 expected", len(chans))
	}
}

func TestSplitCh_RoundRobinOrder(t *testing.T) {
	const n = 3

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 0; i < 10; i++ {
			in <- i
		}
	}()

	chans := splitCh(in, n)
	got := collectPerChannel(t, chans, time.Second)

	want := [][]int{
		{0, 3, 6, 9},
		{1, 4, 7},
		{2, 5, 8},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incorrect round-robin distribution:\ngot:  %v\nwant: %v", got, want)
	}
}

func TestSplitCh_NoDataLossOrDuplication(t *testing.T) {
	const (
		n         = 7
		numValues = 1000
	)

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 0; i < numValues; i++ {
			in <- i
		}
	}()

	chans := splitCh(in, n)
	perChannel := collectPerChannel(t, chans, 5*time.Second)

	seen := make(map[int]int, numValues)
	total := 0
	for _, vals := range perChannel {
		total += len(vals)
		for _, v := range vals {
			seen[v]++
		}
	}

	if total != numValues {
		t.Fatalf("A total of %d values were received; %d were expected", total, numValues)
	}
	for v := 0; v < numValues; v++ {
		if seen[v] != 1 {
			t.Fatalf("The value %d occurred %d times; exactly 1 was expected.", v, seen[v])
		}
	}
}

func TestSplitCh_ClosesAllOutputChannels(t *testing.T) {
	const n = 4

	in := make(chan int)
	close(in)

	chans := splitCh(in, n)

	var wg sync.WaitGroup
	wg.Add(n)
	closed := make([]bool, n)
	for i, ch := range chans {
		go func(i int, ch <-chan int) {
			defer wg.Done()
			_, ok := <-ch
			closed[i] = !ok
		}(i, ch)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout: not all outgoing channels were closed.")
	}

	for i, ok := range closed {
		if !ok {
			t.Errorf("channel %d was not closed", i)
		}
	}
}

func TestSplitCh_EmptyInput(t *testing.T) {
	const n = 3

	in := make(chan int)
	close(in)

	chans := splitCh(in, n)
	got := collectPerChannel(t, chans, time.Second)

	for i, vals := range got {
		if len(vals) != 0 {
			t.Errorf("Channel %d received %v; an empty value was expected", i, vals)
		}
	}
}

func TestSplitCh_SingleChannel(t *testing.T) {
	in := make(chan int)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	chans := splitCh(in, 1)
	got := collectPerChannel(t, chans, time.Second)

	want := []int{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("got=%v, want=%v", got[0], want)
	}
}

func TestSplitCh_UnevenConsumerSpeeds(t *testing.T) {
	const (
		n         = 4
		numValues = 200
	)

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 0; i < numValues; i++ {
			in <- i
		}
	}()

	chans := splitCh(in, n)

	var (
		mu    sync.Mutex
		total int
	)

	var wg sync.WaitGroup
	wg.Add(n)
	for i, ch := range chans {
		go func(workerID int, ch <-chan int) {
			defer wg.Done()
			count := 0
			for range ch {
				count++
				if workerID%2 == 0 && count%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
			mu.Lock()
			total += count
			mu.Unlock()
		}(i, ch)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: workers running at different speeds have not finished")
	}

	if total != numValues {
		t.Fatalf("A total of %d values were processed; %d were expected", total, numValues)
	}
}

func ExamplesplitCh_orderProcessing() {
	type processedOrder struct {
		workerID int
		orderID  int
	}

	orders := make(chan int)
	go func() {
		defer close(orders)
		for orderID := 1; orderID <= 9; orderID++ {
			orders <- orderID
		}
	}()

	const workerCount = 3
	workerChs := splitCh(orders, workerCount)

	var (
		mu      sync.Mutex
		results []processedOrder
		wg      sync.WaitGroup
	)

	wg.Add(workerCount)
	for workerID, ch := range workerChs {
		go func(workerID int, ch <-chan int) {
			defer wg.Done()
			for orderID := range ch {
				mu.Lock()
				results = append(results, processedOrder{workerID: workerID, orderID: orderID})
				mu.Unlock()
			}
		}(workerID, ch)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		return results[i].orderID < results[j].orderID
	})

	for _, r := range results {
		fmt.Printf("worker %d processed order %d\n", r.workerID, r.orderID)
	}
}
