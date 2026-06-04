package slicesx

import (
	"fmt"
	"slices"
	"testing"
)

// Optimized version using a map for O(n) complexity
func DedupOptimized[S ~[]E, E comparable](ts S) []E {
	if len(ts) == 0 {
		return nil
	}
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

// Optimized version with pre-allocation
func DedupFncOptimized[S ~[]E, E comparable](ts S, eqFnc func(t1, t2 E) bool) []E {
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

func generateIntSlice(size int, duplicationFactor float64) []int {
	uniqueCount := int(float64(size) * duplicationFactor)
	if uniqueCount < 1 {
		uniqueCount = 1
	}
	result := make([]int, size)
	for i := 0; i < size; i++ {
		result[i] = i % uniqueCount
	}
	return result
}

func BenchmarkDedup(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}
	duplications := []float64{0.1, 0.5, 1.0} // 10%, 50%, 100% unique

	for _, size := range sizes {
		for _, dup := range duplications {
			name := fmt.Sprintf("size=%d_unique=%.0f%%", size, dup*100)
			data := generateIntSlice(size, dup)

			b.Run("Original/"+name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = Dedup(data)
				}
			})

			b.Run("Optimized/"+name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = DedupOptimized(data)
				}
			})
		}
	}
}

func BenchmarkDedupFnc(b *testing.B) {
	sizes := []int{10, 100, 1000}
	eqFnc := func(t1, t2 int) bool { return t1 == t2 }

	for _, size := range sizes {
		data := generateIntSlice(size, 0.5)
		name := fmt.Sprintf("size=%d", size)

		b.Run("Original/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = DedupFnc(data, eqFnc)
			}
		})

		b.Run("Optimized/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = DedupFncOptimized(data, eqFnc)
			}
		})
	}
}
