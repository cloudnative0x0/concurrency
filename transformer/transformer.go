package transformer

import "fmt"

func Transform[T any](inputCh <-chan T, action func(T) T) <-chan T {
	outputCh := make(chan T)

	go func() {
		defer close(outputCh)
		for number := range inputCh {
			outputCh <- action(number)
		}
	}()

	return outputCh
}

// Example of usage:
//
//nolint:unused
func main() {
	channel := make(chan int)

	go func() {
		defer close(channel)
		for i := 0; i < 5; i++ {
			channel <- i
		}
	}()

	mul := func(value int) int {
		return value * value
	}

	for number := range Transform(channel, mul) {
		fmt.Println(number)
	}
}
