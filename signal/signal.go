package signal

import (
	"context"
	"sync"
	"time"
)

type topicSignals map[chan struct{}]struct{}

func New() *Signals {
	return &Signals{
		subs: make(map[string]topicSignals),
	}
}

type Signals struct {
	sync.RWMutex
	subs   map[string]topicSignals
	closed bool
}

func (s *Signals) Close() {
	s.Lock()
	defer s.Unlock()
	s.closed = true
	for _, tss := range s.subs {
		for c := range tss {
			close(c)
		}
	}
}

func (s *Signals) isClosed() bool {
	s.RLock()
	defer s.RUnlock()
	return s.closed
}

func (s *Signals) Emit(topic string) {
	s.Lock()
	defer s.Unlock()
	for c := range s.subs[topic] {
		close(c)
	}
	delete(s.subs, topic)
}

func (s *Signals) subscribe(topic string) chan struct{} {
	s.Lock()
	defer s.Unlock()
	c := make(chan struct{})
	if _, ok := s.subs[topic]; !ok {
		s.subs[topic] = make(topicSignals)
	}
	s.subs[topic][c] = struct{}{}
	return c
}

func (s *Signals) unsubscribe(topic string, c chan struct{}) {
	s.Lock()
	defer s.Unlock()
	cs, ok := s.subs[topic]
	if !ok {
		return
	}
	delete(cs, c)
	close(c)
}

func (s *Signals) WaitContext(ctx context.Context, topic string, timeout time.Duration) bool {
	c := s.subscribe(topic)
	timer := time.NewTimer(timeout)
	select {
	case <-ctx.Done():
		s.unsubscribe(topic, c)
		return false
	case <-timer.C:
		s.unsubscribe(topic, c)
		return false
	case <-c:
		if s.isClosed() {
			return false
		} else {
			return true
		}
	}
}
