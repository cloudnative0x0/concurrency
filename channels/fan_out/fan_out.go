package fan_out

func splitCh[T any](inputCh <-chan T, n int) []<-chan T {
	outputChs := make([]chan T, n)
	for i := 0; i < n; i++ {
		outputChs[i] = make(chan T)
	}

	go func() {
		idx := 0
		for val := range inputCh {
			outputChs[idx] <- val
			idx = (idx + 1) % n
		}

		for _, ch := range outputChs {
			close(ch)
		}
	}()

	resChs := make([]<-chan T, n)
	for i := 0; i < n; i++ {
		resChs[i] = outputChs[i]
	}

	return resChs
}
