package es

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/maps"
	"github.com/mazzegi/mbox/uuid"
)

type Codec struct {
	registry map[string]DomainEvent
}

func NewCodec() *Codec {
	return &Codec{
		registry: map[string]DomainEvent{},
	}
}

func (codec *Codec) TypeNames() []string {
	return maps.OrderedKeys(codec.registry)
}

func (codec *Codec) Register(typeName string, prototype DomainEvent) {
	codec.registry[typeName] = prototype
}

func (codec *Codec) lookupTypeName(v DomainEvent) (string, bool) {
	for typeName, proto := range codec.registry {
		if reflect.TypeOf(v) == reflect.TypeOf(proto) {
			return typeName, true
		}
	}
	return "", false
}

func (codec *Codec) ContainsEventType(v DomainEvent) bool {
	for _, proto := range codec.registry {
		if reflect.TypeOf(v) == reflect.TypeOf(proto) {
			return true
		}
	}
	return false
}

func (codec *Codec) ContainsTypeName(typeName string) bool {
	_, contains := codec.registry[typeName]
	return contains
}

func (codec *Codec) Encode(v DomainEvent) (RawEvent, error) {
	re := RawEvent{}
	typeName, contains := codec.lookupTypeName(v)
	if !contains {
		return re, fmt.Errorf("codec-encode: (%s) is not registered", reflect.TypeOf(v).Name())
	}
	eventID := v.ID()
	if eventID == "" {
		eventID = ID(uuid.MustMakeV4())
	}
	bData, err := json.Marshal(v)
	if err != nil {
		return re, err
	}
	re.ID = eventID
	re.OccurredOn = v.OccurredOn()
	re.Type = typeName
	re.Data = json.RawMessage(bData)
	return re, nil
}

func (codec *Codec) Decode(re RawEvent) (DomainEvent, error) {
	proto, contains := codec.registry[re.Type]
	if !contains {
		return nil, fmt.Errorf("codec-decode: (%s) is not registered", re.Type)
	}
	pointerToI := reflect.New(reflect.TypeOf(proto))
	err := json.Unmarshal(re.Data, pointerToI.Interface())
	if err != nil {
		return nil, err
	}
	return pointerToI.Elem().Interface().(DomainEvent), nil
}

func (codec *Codec) EncodeEvents(es ...DomainEvent) RawEvents {
	var res RawEvents
	for _, e := range es {
		re, err := codec.Encode(e)
		if err != nil {
			log.Warnf("encode %T: %v", e, err)
			continue
		}
		res = append(res, re)
	}
	return res
}
