package maps

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func Clone[K comparable, V any](m map[K]V) map[K]V {
	cm := map[K]V{}
	for k, v := range m {
		cm[k] = v
	}
	return cm
}

func OrderedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	var ks []K
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		return ks[i] < ks[j]
	})
	return ks
}

func OrderedKeysFunc[K comparable, V any](m map[K]V, less func(k1, k2 K) bool) []K {
	var ks []K
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		return less(ks[i], ks[j])
	})
	return ks
}

func OrderedValues[K constraints.Ordered, V any](m map[K]V, less func(v1, v2 V) bool) []V {
	var vs []V
	for _, v := range m {
		vs = append(vs, v)
	}
	sort.Slice(vs, func(i, j int) bool {
		return less(vs[i], vs[j])
	})
	return vs
}

func Keys[K comparable, V any](m map[K]V) []K {
	var ks []K
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func Values[K comparable, V any](m map[K]V) []V {
	var vs []V
	for _, v := range m {
		vs = append(vs, v)
	}
	return vs
}
