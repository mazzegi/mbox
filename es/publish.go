package es

import (
	"sync"
)

type StreamUpdateSubscription struct {
	streamID  StreamID
	C         chan StreamID
	publisher *StreamUpdatePublisher
}

func newStreamUpdateSubscription(publisher *StreamUpdatePublisher, streamID StreamID) *StreamUpdateSubscription {
	return &StreamUpdateSubscription{
		streamID:  streamID,
		C:         make(chan StreamID),
		publisher: publisher,
	}
}

func (sub *StreamUpdateSubscription) Close() {
	sub.publisher.removeAndCloseSubscription(sub)
}

func (sub *StreamUpdateSubscription) PublishStreamUpdate(streamID StreamID) {
	if sub.streamID == StreamIDAll || sub.streamID == streamID {
		sub.C <- streamID
	}
}

//

type StreamUpdatePublisher struct {
	sync.RWMutex
	subscriptions map[*StreamUpdateSubscription]bool
}

func NewStreamUpdatePublisher() *StreamUpdatePublisher {
	return &StreamUpdatePublisher{
		subscriptions: map[*StreamUpdateSubscription]bool{},
	}
}

func (p *StreamUpdatePublisher) Close() {
	p.Lock()
	defer p.Unlock()
	for sub := range p.subscriptions {
		close(sub.C)
	}
	p.subscriptions = map[*StreamUpdateSubscription]bool{}
}

func (p *StreamUpdatePublisher) Subscribe(streamID StreamID) *StreamUpdateSubscription {
	p.Lock()
	defer p.Unlock()
	sub := newStreamUpdateSubscription(p, streamID)
	p.subscriptions[sub] = true
	return sub
}

func (p *StreamUpdatePublisher) removeAndCloseSubscription(sub *StreamUpdateSubscription) {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.subscriptions[sub]; ok {
		delete(p.subscriptions, sub)
		close(sub.C)
	}
}

func (p *StreamUpdatePublisher) PublishStreamUpdate(streamID StreamID) {
	p.RLock()
	defer p.RUnlock()
	for sub := range p.subscriptions {
		sub.PublishStreamUpdate(streamID)
	}
}
