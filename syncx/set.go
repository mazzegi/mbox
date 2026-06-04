package syncx

import (
	"sync"

	"github.com/mazzegi/mbox/set"
)

func NewSet[T comparable](ts ...T) *Set[T] {
	return &Set[T]{
		_set: set.New(ts...),
	}
}

type Set[T comparable] struct {
	mx   sync.RWMutex
	_set set.Set[T]
}

func (s *Set[T]) Insert(t T) {
	s.mx.Lock()
	defer s.mx.Unlock()
	s._set.Insert(t)
}

func (s *Set[T]) Delete(t T) {
	s.mx.Lock()
	defer s.mx.Unlock()
	s._set.Delete(t)
}

func (s *Set[T]) Contains(t T) bool {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s._set.Contains(t)
}

func (s *Set[T]) Values() []T {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s._set.Values()
}

func (s *Set[T]) Len() int {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s._set.Len()
}

func (s *Set[T]) Each(do func(t T)) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	for _, t := range s._set.Values() {
		do(t)
	}
}
