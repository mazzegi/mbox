package slicesx

func Map[T any, V any](ts []T, conv func(T) V) []V {
	vs := make([]V, len(ts))
	for i, t := range ts {
		vs[i] = conv(t)
	}
	return vs
}
