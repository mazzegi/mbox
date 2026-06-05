package atomix

import (
	"sync/atomic"

	"github.com/mazzegi/mbox/makex"
)

type Value[T any] struct {
	value atomic.Value
}

func NewValue[T any](value T) Value[T] {
	av := Value[T]{}
	av.Set(value)
	return av
}

func (av *Value[T]) Set(value T) {
	av.value.Store(value)
}

func (av *Value[T]) Reset() {
	av.Set(makex.ZeroOf[T]())
}

func (av *Value[T]) Get() T {
	anyV := av.value.Load()
	v, ok := anyV.(T)
	if !ok {
		return makex.ZeroOf[T]()
	}
	return v
}
