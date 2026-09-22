package bridge

import (
	"reflect"
	"testing"
	"time"
)

func TestBridgeFlattensChannelsInOrder(t *testing.T) {
	inputChCh := make(chan chan int, 4)

	for _, values := range [][]int{{1, 2}, {}, {3}, {4, 5, 6}} {
		inputCh := make(chan int, len(values))
		for _, value := range values {
			inputCh <- value
		}
		close(inputCh)
		inputChCh <- inputCh
	}
	close(inputChCh)

	var got []int
	for value := range Bridge(inputChCh) {
		got = append(got, value)
	}

	want := []int{1, 2, 3, 4, 5, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Bridge() returned %v, want %v", got, want)
	}
}

func TestBridgeDrainsCurrentChannelBeforeReadingNext(t *testing.T) {
	inputChCh := make(chan chan int, 2)
	first := make(chan int, 1)
	second := make(chan int, 1)

	first <- 1
	second <- 2
	close(second)

	inputChCh <- first
	inputChCh <- second
	close(inputChCh)

	outputCh := Bridge(inputChCh)
	if got := receiveValue(t, outputCh); got != 1 {
		t.Fatalf("first value = %d, want 1", got)
	}

	select {
	case value, ok := <-outputCh:
		if !ok {
			t.Fatal("output channel closed while the first input channel was still open")
		}
		t.Fatalf("received %d from the next channel before the first channel was closed", value)
	default:
	}

	close(first)
	if got := receiveValue(t, outputCh); got != 2 {
		t.Fatalf("second value = %d, want 2", got)
	}
	awaitClosed(t, outputCh)
}

func TestBridgeStaysOpenUntilOuterChannelCloses(t *testing.T) {
	inputChCh := make(chan chan string)
	outputCh := Bridge(inputChCh)

	inputCh := make(chan string, 1)
	inputCh <- "block-1"
	close(inputCh)
	inputChCh <- inputCh

	if got := receiveValue(t, outputCh); got != "block-1" {
		t.Fatalf("value = %q, want %q", got, "block-1")
	}

	select {
	case _, ok := <-outputCh:
		if !ok {
			t.Fatal("output channel closed before the outer channel was closed")
		}
		t.Fatal("received an unexpected value")
	default:
	}

	close(inputChCh)
	awaitClosed(t, outputCh)
}

func TestBridgeWithNoInputChannels(t *testing.T) {
	inputChCh := make(chan chan int)
	close(inputChCh)

	awaitClosed(t, Bridge(inputChCh))
}

func TestBridgeHandlesManyChannels(t *testing.T) {
	const (
		channels         = 100
		valuesPerChannel = 50
	)

	inputChCh := make(chan chan int, channels)
	for channelIndex := 0; channelIndex < channels; channelIndex++ {
		inputCh := make(chan int, valuesPerChannel)
		for valueIndex := 0; valueIndex < valuesPerChannel; valueIndex++ {
			inputCh <- channelIndex*valuesPerChannel + valueIndex
		}
		close(inputCh)
		inputChCh <- inputCh
	}
	close(inputChCh)

	expected := 0
	for value := range Bridge(inputChCh) {
		if value != expected {
			t.Fatalf("value %d: got %d, want %d", expected, value, expected)
		}
		expected++
	}

	if want := channels * valuesPerChannel; expected != want {
		t.Fatalf("received %d values, want %d", expected, want)
	}
}

func receiveValue[T any](t *testing.T, ch <-chan T) T {
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

func awaitClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()

	select {
	case value, ok := <-ch:
		if ok {
			t.Fatalf("received unexpected value after input was exhausted: %v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}
