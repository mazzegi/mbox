package slicesx

func Filter[S ~[]E, E any](ts S, accept func(t E) bool) []E {
	var fts []E
	for _, t := range ts {
		if accept(t) {
			fts = append(fts, t)
		}
	}
	return fts
}
