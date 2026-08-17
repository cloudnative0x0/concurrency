package fan_in

import (
	"sync"
)

func mergeChs[T any](channels ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	outputCh := make(chan T)
	for _, ch := range channels {
		go func(ch <-chan T) {
			defer wg.Done()

			for val := range ch {
				outputCh <- val
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(outputCh)
	}()

	return outputCh
}
