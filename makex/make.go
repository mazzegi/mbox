package makex

func ZeroOf[T any]() T {
	var t T
	return t
}

func PtrOf[T any](t T) *T {
	return &t
}
