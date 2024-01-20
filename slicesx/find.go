package slicesx

func FindFnc[S ~[]T, T any](ts S, fnc func(t T) bool) (T, bool) {
	for _, t := range ts {
		if fnc(t) {
			return t, true
		}
	}
	var t T
	return t, false
}

func FindDoFnc[S ~[]T, T any](ts S, fnc func(t T) bool, do func(t *T)) {
	for i, t := range ts {
		if fnc(t) {
			do(&ts[i])
		}
	}
}
