package barrier

import (
	"log"
	"sync"
)

type Barrier struct {
	mutex sync.Mutex
	count int
	size  int

	beforeCh chan struct{}
	afterCh  chan struct{}
}

func NewBarrier(size int) *Barrier {
	return &Barrier{
		size:     size,
		beforeCh: make(chan struct{}, size),
		afterCh:  make(chan struct{}, size),
	}
}

func (b *Barrier) Before() {
	b.mutex.Lock()

	b.count++
	if b.count == b.size {
		for i := 0; i < b.size; i++ {
			b.beforeCh <- struct{}{}
		}
	}

	b.mutex.Unlock()
	<-b.beforeCh
}

func (b *Barrier) After() {
	b.mutex.Lock()

	b.count--
	if b.count == 0 {
		for i := 0; i < b.size; i++ {
			b.afterCh <- struct{}{}
		}
	}

	b.mutex.Unlock()
	<-b.afterCh
}

// Example of usage:
//
//nolint:unused
func main() {
	var wg sync.WaitGroup
	wg.Add(3)

	bootstrap := func() {
		log.Println("bootstrap")
	}

	work := func() {
		log.Println("work")
	}

	count := 3
	barrier := NewBarrier(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				// wait for all workers to finish previous loop
				barrier.Before()
				bootstrap()
				// wait for other workers to bootstrap
				barrier.After()
				work()
			}
		}()
	}

	wg.Wait()
}
