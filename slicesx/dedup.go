package slicesx

import "slices"

// Dedup removes duplicate elements from a slice while preserving order.
// Uses a map-based approach (O(n)) for better performance on larger slices.
func Dedup[S ~[]E, E comparable](ts S) []E {
	if len(ts) == 0 {
		return nil
	}
	// For very small slices, the map overhead isn't worth it,
	// but for n > 20, the O(n) map approach is much better than O(n²)
	if len(ts) < 20 {
		var dts []E
		for _, t := range ts {
			if !slices.Contains(dts, t) {
				dts = append(dts, t)
			}
		}
		return dts
	}
	// Map-based deduplication: O(n) time, O(n) space
	seen := make(map[E]struct{}, len(ts))
	dts := make([]E, 0, len(ts))
	for _, t := range ts {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			dts = append(dts, t)
		}
	}
	return dts
}

// DedupFnc removes duplicate elements using a custom equality function.
// Note: Still O(n²) due to custom equality, but with pre-allocation to reduce allocations.
func DedupFnc[S ~[]E, E comparable](ts S, eqFnc func(t1, t2 E) bool) []E {
	if len(ts) == 0 {
		return nil
	}
	dts := make([]E, 0, len(ts))
	for _, te := range ts {
		if !slices.ContainsFunc(dts, func(t E) bool {
			return eqFnc(te, t)
		}) {
			dts = append(dts, te)
		}
	}
	return dts
}
