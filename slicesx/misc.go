package slicesx

func Repeat[T any](t T, count int) []T {
	ts := make([]T, count)
	for i := range count {
		ts[i] = t
	}
	return ts
}

func Reversed[T any](ts []T) []T {
	if ts == nil {
		return nil
	}
	size := len(ts)
	rs := make([]T, size)
	for i, t := range ts {
		rs[size-1-i] = t
	}
	return rs
}
