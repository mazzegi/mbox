package slicesx

func Repeat[T any](t T, count int) []T {
	ts := make([]T, count)
	for i := 0; i < count; i++ {
		ts[i] = t
	}
	return ts
}
