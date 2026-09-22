package transformer

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransformAppliesActionToEveryValueInOrder(t *testing.T) {
	inputCh := make(chan int, 5)
	for _, value := range []int{1, 2, 3, 4, 5} {
		inputCh <- value
	}
	close(inputCh)

	var calls atomic.Int64
	outputCh := Transform(inputCh, func(value int) int {
		calls.Add(1)
		return value * value
	})

	var got []int
	for value := range outputCh {
		got = append(got, value)
	}

	want := []int{1, 4, 9, 16, 25}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Transform() returned %v, want %v", got, want)
	}
	if gotCalls := calls.Load(); gotCalls != int64(len(want)) {
		t.Fatalf("action called %d times, want %d", gotCalls, len(want))
	}
}

func TestTransformSupportsStructValues(t *testing.T) {
	type transaction struct {
		amount int64
		fee    int64
		total  int64
	}

	inputCh := make(chan transaction, 2)
	inputCh <- transaction{amount: 100, fee: 2}
	inputCh <- transaction{amount: 250, fee: 5}
	close(inputCh)

	outputCh := Transform(inputCh, func(tx transaction) transaction {
		tx.total = tx.amount + tx.fee
		return tx
	})

	got := collect(outputCh)
	want := []transaction{
		{amount: 100, fee: 2, total: 102},
		{amount: 250, fee: 5, total: 255},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Transform() returned %+v, want %+v", got, want)
	}
}

func TestTransformClosesOutputForEmptyInput(t *testing.T) {
	inputCh := make(chan string)
	close(inputCh)

	outputCh := Transform(inputCh, func(value string) string {
		t.Fatal("action must not be called for an empty input channel")
		return value
	})

	awaitTransformClosed(t, outputCh)
}

func TestTransformAppliesBackpressure(t *testing.T) {
	inputCh := make(chan int, 3)
	inputCh <- 1
	inputCh <- 2
	inputCh <- 3
	close(inputCh)

	actionCalled := make(chan int, 3)
	outputCh := Transform(inputCh, func(value int) int {
		actionCalled <- value
		return value * 10
	})

	if got := receiveTransformValue(t, actionCalled); got != 1 {
		t.Fatalf("first action value = %d, want 1", got)
	}
	assertNoTransformValue(t, actionCalled, "action processed a second value while the first output was blocked")

	if got := receiveTransformValue(t, outputCh); got != 10 {
		t.Fatalf("first output = %d, want 10", got)
	}
	if got := receiveTransformValue(t, actionCalled); got != 2 {
		t.Fatalf("second action value = %d, want 2", got)
	}
	assertNoTransformValue(t, actionCalled, "action processed a third value while the second output was blocked")

	if got := receiveTransformValue(t, outputCh); got != 20 {
		t.Fatalf("second output = %d, want 20", got)
	}
	if got := receiveTransformValue(t, actionCalled); got != 3 {
		t.Fatalf("third action value = %d, want 3", got)
	}
	if got := receiveTransformValue(t, outputCh); got != 30 {
		t.Fatalf("third output = %d, want 30", got)
	}
	awaitTransformClosed(t, outputCh)
}

func TestTransformHandlesManyValues(t *testing.T) {
	const count = 10_000

	inputCh := make(chan int)
	go func() {
		defer close(inputCh)
		for value := 0; value < count; value++ {
			inputCh <- value
		}
	}()

	expected := 1
	seen := 0
	for value := range Transform(inputCh, func(value int) int { return value + 1 }) {
		if value != expected {
			t.Fatalf("output %d = %d, want %d", seen, value, expected)
		}
		expected++
		seen++
	}

	if seen != count {
		t.Fatalf("received %d values, want %d", seen, count)
	}
}

func collect[T any](ch <-chan T) []T {
	var values []T
	for value := range ch {
		values = append(values, value)
	}
	return values
}

func receiveTransformValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before a value was received")
		}
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a value")
		var zero T
		return zero
	}
}

func assertNoTransformValue[T any](t *testing.T, ch <-chan T, message string) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal(message)
	default:
	}
}

func awaitTransformClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()

	select {
	case value, ok := <-ch:
		if ok {
			t.Fatalf("received unexpected value: %v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}
