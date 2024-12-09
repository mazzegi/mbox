package es

import (
	"encoding/json"
	"testing"
)

type TestEvent struct {
	Base
	Value string `json:"value"`
}

func makeCodec() *Codec {
	codec := NewCodec()
	codec.Register("test-event", TestEvent{})
	return codec
}

func TestDomainEventMeta(t *testing.T) {
	codec := makeCodec()
	e := TestEvent{
		Base:  MakeBase(),
		Value: "foo",
	}
	WithMeta(e, UserMeta("acme"))

	re, err := codec.Encode(e)
	if err != nil {
		t.Fatalf("encode")
	}
	bs, err := json.MarshalIndent(re, "", " ")
	if err != nil {
		t.Fatalf("marshal raw-event")
	}
	t.Logf("raw-event\n%s", string(bs))

}
