package es

import (
	"time"

	"github.com/mazzegi/mbox/uuid"
)

type (
	DomainEvent interface {
		ID() ID
		OccurredOn() time.Time
		Meta() *MetaData
	}

	DomainEvents      []DomainEvent
	DomainEventStream chan DomainEvent
)

type Base struct {
	EvtID         ID        `json:"id"`
	EvtOccurredOn time.Time `json:"occurred-on"`
	MetaData      MetaData  `json:"meta"`
}

func MakeBase() Base {
	return Base{
		EvtID:         ID(uuid.MustMakeV4()),
		EvtOccurredOn: time.Now().UTC(),
		MetaData:      make(MetaData),
	}
}

func MakeBaseWithMeta(meta MetaData) Base {
	return Base{
		EvtID:         ID(uuid.MustMakeV4()),
		EvtOccurredOn: time.Now().UTC(),
		MetaData:      meta,
	}
}

func (e Base) ID() ID {
	return e.EvtID
}

func (e Base) OccurredOn() time.Time {
	return e.EvtOccurredOn
}

func (e Base) Meta() *MetaData {
	return &e.MetaData
}

func WithMeta(e DomainEvent, meta MetaData) {
	emeta := e.Meta()
	for k, v := range meta {
		(*emeta)[k] = v
	}
}
