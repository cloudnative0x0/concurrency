package fan_in

import (
	"fmt"
	"sync"
)

func mergeChs[T any](channels ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	outputCh := make(chan T)
	for _, ch := range channels {
		go func() {
			defer wg.Done()

			for val := range ch {
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

func ExampleFanIn() {
	// три независимых источника — например, коннекторы к разным биржам,
	// каждый уже существует сам по себе, никто их специально не порождал
	btcFeed := make(chan int)
	ethFeed := make(chan int)
	solFeed := make(chan int)

	go func() {
		defer close(btcFeed)
		for _, price := range []int{42000, 42150, 41980} {
			btcFeed <- price
		}
	}()

	go func() {
		defer close(ethFeed)
		for _, price := range []int{2200, 2215} {
			ethFeed <- price
		}
	}()

	go func() {
		defer close(solFeed)
		for _, price := range []int{95, 97, 96, 94} {
			solFeed <- price
		}
	}()

	var total int
	for price := range mergeChs(btcFeed, ethFeed, solFeed) {
		total += price
	}

	fmt.Println(total)
	// Output: 130927
}
