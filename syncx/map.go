package syncx

import "sync"

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		_map: make(map[K]V),
	}
}

type Map[K comparable, V any] struct {
	mx   sync.RWMutex
	_map map[K]V
}

func (m *Map[K, V]) Clear() {
	m.mx.Lock()
	defer m.mx.Unlock()
	clear(m._map)
}

func (m *Map[K, V]) Len() int {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return len(m._map)
}

func (m *Map[K, V]) Store(key K, val V) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m._map[key] = val
}

func (m *Map[K, V]) Load(key K) (V, bool) {
	m.mx.RLock()
	defer m.mx.RUnlock()
	v, ok := m._map[key]
	return v, ok
}

func (m *Map[K, V]) Delete(key K) {
	m.mx.Lock()
	defer m.mx.Unlock()
	delete(m._map, key)
}
