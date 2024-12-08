package blobix

func MkTuple[K comparable, V any](k K, v V) Tuple[K, V] {
	return Tuple[K, V]{
		Key:   k,
		Value: v,
	}
}

type Tuple[K comparable, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}
