package slicesx

import "golang.org/x/exp/constraints"

type Number interface {
	constraints.Integer | constraints.Float
}

func Sum[T Number](ts []T) T {
	var sum T
	for _, t := range ts {
		sum += t
	}
	return sum
}

func Avg[T Number](ts []T) float64 {
	if len(ts) == 0 {
		return 0
	}
	var sum float64
	for _, t := range ts {
		sum += float64(t)
	}
	return sum / float64(len(ts))
}
