package promise

import (
	"context"
	"fmt"
)

type result[T any] struct {
	val T
	err error
}

type Promise[T any] struct {
	done   chan struct{}
	result result[T]
}

func NewPromise[T any](ctx context.Context, asyncFn func(context.Context) (T, error)) *Promise[T] {
	p := &Promise[T]{done: make(chan struct{})}

	go func() {
		defer close(p.done)

		defer func() {
			if r := recover(); r != nil {
				var zero T
				p.result = result[T]{val: zero, err: fmt.Errorf("promise: panic recovered: %v", r)}
			}
		}()

		val, err := asyncFn(ctx)
		p.result = result[T]{val: val, err: err}
	}()

	return p
}

func (p *Promise[T]) Then(successFn func(T), errorFn func(error)) {
	go func() {
		<-p.done
		if p.result.err == nil {
			if successFn != nil {
				successFn(p.result.val)
			}
		} else {
			if errorFn != nil {
				errorFn(p.result.err)
			}
		}
	}()
}

func (p *Promise[T]) Await() (T, error) {
	<-p.done
	return p.result.val, p.result.err
}

func (p *Promise[T]) AwaitContext(ctx context.Context) (T, error) {
	select {
	case <-p.done:
		return p.result.val, p.result.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
