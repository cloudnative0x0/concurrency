package pipeline

func generate[T any](values ...T) <-chan T {
	outputCh := make(chan T)
	go func() {
		defer close(outputCh)
		for _, value := range values {
			outputCh <- value
		}
	}()

	return outputCh
}

func process[T any](inputCh <-chan T, action func(T) T) <-chan T {
	outputCh := make(chan T)
	go func() {
		defer close(outputCh)
		for value := range inputCh {
			outputCh <- action(value)
		}
	}()

	return outputCh
}
