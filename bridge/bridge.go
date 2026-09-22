package bridge

import (
	"fmt"
)

func Bridge[T any](inputChCh chan chan T) <-chan T {
	outputCh := make(chan T)

	go func() {
		defer close(outputCh)
		for inputCh := range inputChCh {
			for value := range inputCh {
				outputCh <- value
			}
		}
	}()

	return outputCh
}

func main() {
	channelChannel := make(chan chan string)

	go func() {
		channel1 := make(chan string, 3)
		for i := 0; i < 3; i++ {
			channel1 <- "channel-1"
		}

		close(channel1)

		channel2 := make(chan string, 3)
		for i := 0; i < 3; i++ {
			channel2 <- "channel-2"
		}

		close(channel2)

		channelChannel <- channel1
		channelChannel <- channel2
		close(channelChannel)
	}()

	for value := range Bridge(channelChannel) {
		fmt.Println(value)
	}
}
