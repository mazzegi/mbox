package es

import (
	"fmt"
	"reflect"
)

type CodecUnion struct {
	codecs []*Codec
}

func NewCodecUnion(codecs ...*Codec) *CodecUnion {
	return &CodecUnion{
		codecs: codecs,
	}
}

func (cu *CodecUnion) findCodecForEventType(v DomainEvent) (*Codec, error) {
	for _, codec := range cu.codecs {
		if contains := codec.ContainsEventType(v); contains {
			return codec, nil
		}

	}
	return nil, fmt.Errorf("codec-union: no codec found for type %q", reflect.TypeOf(v).Name())
}

func (cu *CodecUnion) findCodecForTypeName(typeName string) (*Codec, error) {
	for _, codec := range cu.codecs {
		if contains := codec.ContainsTypeName(typeName); contains {
			return codec, nil
		}

	}
	return nil, fmt.Errorf("codec-union: no codec found for type-name %q", typeName)
}

func (cu *CodecUnion) Encode(v DomainEvent) (RawEvent, error) {
	codec, err := cu.findCodecForEventType(v)
	if err != nil {
		return RawEvent{}, err
	}
	return codec.Encode(v)
}

func (cu *CodecUnion) Decode(re RawEvent) (DomainEvent, error) {
	codec, err := cu.findCodecForTypeName(re.Type)
	if err != nil {
		return nil, err
	}
	return codec.Decode(re)
}
