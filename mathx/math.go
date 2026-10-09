package mathx

import (
	"math"

	"github.com/mazzegi/mbox/makex"
	"golang.org/x/exp/constraints"
)

func Abs[T constraints.Integer](t T) T {
	if t < 0 {
		return -t
	}
	return t
}

func Min[T constraints.Ordered](vs ...T) T {
	if len(vs) == 0 {
		return makex.ZeroOf[T]()
	}
	vm := vs[0]
	for _, t := range vs[1:] {
		if t < vm {
			vm = t
		}
	}
	return vm
}

func Max[T constraints.Ordered](vs ...T) T {
	if len(vs) == 0 {
		return makex.ZeroOf[T]()
	}
	vm := vs[0]
	for _, t := range vs[1:] {
		if t > vm {
			vm = t
		}
	}
	return vm
}

func MinMax[T constraints.Ordered](vs ...T) (min, max T) {
	if len(vs) == 0 {
		return makex.ZeroOf[T](), makex.ZeroOf[T]()
	}
	min = vs[0]
	max = vs[0]
	for _, t := range vs[1:] {
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}
	return min, max
}

func RoundPlaces(v float64, places int) float64 {
	if places < 0 {
		return v
	}
	p := math.Pow10(places)
	return math.Round(v*p) / p
}
