package worker_pool

import (
	"sync"
)

// Name: parallel pipeline

func parse[T any](inputCh <-chan T) <-chan T {
	outputCh := make(chan T)

	go func() {
		defer close(outputCh)
		for val := range inputCh {
			outputCh <- val
		}
	}()

	return outputCh
}

func send[T any](inputCh <-chan T, n int) <-chan T {
	var wg sync.WaitGroup
	wg.Add(n)

	outputCh := make(chan T)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			for val := range inputCh {
				outputCh <- val
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outputCh)
	}()

	return outputCh
}

type Transaction struct {
	ID     int
	From   string
	To     string
	Amount float64
}
