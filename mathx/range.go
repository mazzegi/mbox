package mathx

import "golang.org/x/exp/constraints"

func NewRange[T constraints.Ordered](min, max T) Range[T] {
	return Range[T]{
		Min: min,
		Max: max,
	}
}

type Range[T constraints.Ordered] struct {
	Min T
	Max T
}

func (r Range[T]) Contains(t T) bool {
	return t >= r.Min && t <= r.Max
}
