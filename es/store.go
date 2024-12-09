package es

import (
	"fmt"
	"time"
)

var DefaultPageSize = 50

type LimitOffset struct {
	Limit  uint64
	Offset uint64
}

type StreamID string

const (
	StreamIDAll StreamID = "$all"
)

type ExpectedVersionError struct {
	exp  uint64
	curr uint64
}

func NewExpectedVersionError(exp, curr uint64) ExpectedVersionError {
	return ExpectedVersionError{
		exp:  exp,
		curr: curr,
	}
}

func (e ExpectedVersionError) Error() string {
	return fmt.Sprintf("expected-version-error: expect %d, current %d", e.exp, e.curr)
}

func (sid StreamID) IsAll() bool {
	return sid == StreamIDAll
}

type QueryParams struct {
	StreamID string
	ToDate   time.Time
	Type     string
	SortASC  bool
}

type Store interface {
	Close()
	Subscribe(streamID StreamID) *StreamUpdateSubscription
	StreamVersion(streamID StreamID) uint64
	StoreVersion() uint64
	Append(streamID StreamID, expectedVersion uint64, events ...RawEvent) error
	Create(events ...RawEvent) error
	LoadSlice(streamID StreamID, lo LimitOffset) (RawEvents, error)
	LoadSliceUntil(streamID StreamID, lo LimitOffset, until time.Time) (RawEvents, error)
	LoadSliceDescending(streamID StreamID, lo LimitOffset) (RawEvents, error)
	LoadSliceFromVersion(streamID StreamID, version uint64, lo LimitOffset) (RawEvents, error)
	Query(params QueryParams, lo LimitOffset) (RawEvents, error)
	QueryWithTypePrefix(prefix string, params QueryParams, lo LimitOffset) (RawEvents, error)
	Find(id ID) (RawEvent, bool)

	PurgeBefore(t time.Time) (numDeleted int, err error)
	AllStreamIDs() ([]StreamID, error)

	LoadLatestFromAll() (RawEvents, error)
	LoadLatestFrom(streamIDs []string) (RawEvents, error)
}
