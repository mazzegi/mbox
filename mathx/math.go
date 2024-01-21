package mathx

import (
	"math"

	"golang.org/x/exp/constraints"
)

func Abs[T constraints.Integer](t T) T {
	if t < 0 {
		return -t
	}
	return t
}

func Min[T constraints.Ordered](v1 T, v2 T) T {
	if v1 < v2 {
		return v1
	}
	return v2
}

func Max[T constraints.Ordered](v1 T, v2 T) T {
	if v1 > v2 {
		return v1
	}
	return v2
}

func RoundPlaces(v float64, places int) float64 {
	if places < 0 {
		return v
	}
	p := math.Pow10(places)
	return math.Round(v*p) / p
}
