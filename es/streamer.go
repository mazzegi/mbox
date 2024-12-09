package es

import (
	"context"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/syncx"
)

type Streamer struct {
	store    Store
	streamID StreamID
}

func NewStreamer(store Store, streamID StreamID) *Streamer {
	s := &Streamer{
		store:    store,
		streamID: streamID,
	}
	return s
}

func (s *Streamer) LoadFrom(version uint64) RawEventsStream {
	stream := make(RawEventsStream)
	go func() {
		defer close(stream)
		lo := LimitOffset{Offset: version, Limit: 50}
		for {
			evts, err := s.store.LoadSlice(s.streamID, lo)
			if err != nil {
				log.Errorf("load-slice: %v", err)
				return
			}
			if len(evts) == 0 {
				return
			}
			stream <- evts
			lo.Offset += uint64(len(evts))
		}
	}()
	return stream
}

func (s *Streamer) LoadFromVersion(version uint64) RawEventsStream {
	stream := make(RawEventsStream)
	go func() {
		defer close(stream)
		lo := LimitOffset{Offset: 0, Limit: 50}
		for {
			evts, err := s.store.LoadSliceFromVersion(s.streamID, version, lo)
			if err != nil {
				log.Errorf("load-slice: %v", err)
				return
			}
			if len(evts) == 0 {
				return
			}
			stream <- evts
			lo.Offset += uint64(len(evts))
		}
	}()
	return stream
}

func (s *Streamer) LoadFromCtx(ctx context.Context, version uint64) RawEventsStream {
	stream := make(RawEventsStream)
	go func() {
		defer close(stream)
		lo := LimitOffset{Offset: version, Limit: 50}
		for {
			evts, err := s.store.LoadSlice(s.streamID, lo)
			if err != nil {
				log.Errorf("load-slice: %v", err)
				return
			}
			if len(evts) == 0 {
				return
			}
			stream <- evts
			if syncx.IsContextDone(ctx) {
				return
			}
			lo.Offset += uint64(len(evts))
		}
	}()
	return stream
}

func (s *Streamer) LoadFromUntil(version uint64, until time.Time) RawEventsStream {
	stream := make(RawEventsStream)
	go func() {
		defer close(stream)
		lo := LimitOffset{Offset: version, Limit: 50}
		for {
			evts, err := s.store.LoadSliceUntil(s.streamID, lo, until)
			if err != nil {
				log.Errorf("load-slice: %v", err)
				return
			}
			if len(evts) == 0 {
				return
			}
			stream <- evts
			lo.Offset += uint64(len(evts))
		}
	}()
	return stream
}

func (s *Streamer) StreamFromCtx(ctx context.Context, version uint64) RawEventsStream {
	stream := make(RawEventsStream)
	go func() {
		defer close(stream)

		lo := LimitOffset{Offset: version, Limit: 50}
		loadUntilEmpty := func() {
			for {
				evts, err := s.store.LoadSlice(s.streamID, lo)
				if err != nil {
					log.Errorf("load-slice: %v", err)
					return
				}
				if len(evts) == 0 {
					return
				}
				stream <- evts
				lo.Offset += uint64(len(evts))
			}
		}

		loadUntilEmpty()
		sub := s.store.Subscribe(s.streamID)
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				log.Infof("streamer done")
				return
			case _, ok := <-sub.C:
				if !ok {
					//return when subscription channel is closed
					return
				}
				loadUntilEmpty()
			}
		}
	}()
	return stream
}
