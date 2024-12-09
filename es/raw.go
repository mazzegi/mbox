package es

import (
	"encoding/json"
	"time"

	"github.com/mazzegi/mbox/uuid"
)

type ID string

func MakeID() ID {
	return ID(uuid.MustMakeV4())
}

type RawEvent struct {
	ID          ID              `json:"id,omitempty"`          // the unique id of the event
	StoreIndex  uint64          `json:"store-index"`           // the index of the event within the whole store
	StreamID    string          `json:"stream-id,omitempty"`   // the id of the current stream
	StreamIndex uint64          `json:"stream-index"`          // the index of the event within the current stream
	RecordedOn  time.Time       `json:"recorded-on,omitempty"` // the time the event was first recorded
	OccurredOn  time.Time       `json:"occurred-on,omitempty"` // the time the event occurred
	Type        string          `json:"type"`                  // the type of the domain event
	Data        json.RawMessage `json:"data,omitempty"`        // the data of the domain event
}

type (
	RawEvents       []RawEvent
	RawEventStream  chan RawEvent
	RawEventsStream chan RawEvents
)
