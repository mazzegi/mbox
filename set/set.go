package set

func New[T comparable](ts ...T) Set[T] {
	s := Set[T]{}
	for _, t := range ts {
		s[t] = struct{}{}
	}
	return s
}

type Set[T comparable] map[T]struct{}

func (s Set[T]) Insert(t T) {
	s[t] = struct{}{}
}

func (s Set[T]) Delete(t T) {
	delete(s, t)
}

func (s Set[T]) Contains(t T) bool {
	_, ok := s[t]
	return ok
}

func (s Set[T]) Values() []T {
	var ts []T
	for t := range s {
		ts = append(ts, t)
	}
	return ts
}

func (s Set[T]) Len() int {
	return len(s)
}
