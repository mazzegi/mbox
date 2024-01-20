package slicesx

import "slices"

func Dedup[S ~[]E, E comparable](ts S) []E {
	var dts []E
	for _, t := range ts {
		if !slices.Contains(dts, t) {
			dts = append(dts, t)
		}
	}
	return dts
}

func DedupFnc[S ~[]E, E comparable](ts S, eqFnc func(t1, t2 E) bool) []E {
	var dts []E
	for _, te := range ts {
		if !slices.ContainsFunc(dts, func(t E) bool {
			return eqFnc(te, t)
		}) {
			dts = append(dts, te)
		}
	}
	return dts
}
