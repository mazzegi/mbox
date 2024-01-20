package slicesx

func Anys[S ~[]E, E any](ts []S) []any {
	as := make([]any, len(ts))
	for i, t := range ts {
		as[i] = t
	}
	return as
}
