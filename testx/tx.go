package testx

import (
	"reflect"
	"testing"
)

func NewTx(t *testing.T) *Tx {
	return &Tx{t: t}
}

type Tx struct {
	t *testing.T
}

func (tx *Tx) AssertEqual(want, have any) {
	tx.t.Helper()
	if reflect.DeepEqual(want, have) {
		return
	}
	tx.t.Fatalf("want %v, have %v", want, have)
}

func (tx *Tx) AssertNoErr(err error) {
	tx.t.Helper()
	if err == nil {
		return
	}
	tx.t.Fatalf("error is not-nil but: %v", err)
}

func (tx *Tx) AssertErr(err error) {
	tx.t.Helper()
	if err != nil {
		return
	}
	tx.t.Fatalf("expect err; got none")
}
