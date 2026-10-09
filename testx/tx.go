package testx

import (
	"fmt"
	"reflect"
	"testing"
)

func NewTx(t *testing.T) *Tx {
	return &Tx{
		t:    t,
		name: "default",
	}
}

func NewTxWithName(t *testing.T, name string) *Tx {
	return &Tx{
		t:    t,
		name: name,
	}
}

func NewTxWithTestIdx(t *testing.T, idx int) *Tx {
	return &Tx{
		t:    t,
		name: fmt.Sprintf("test_%03d", idx),
	}
}

type Tx struct {
	t    *testing.T
	name string
}

func (tx *Tx) T() *testing.T {
	return tx.t
}

func (tx *Tx) AssertEqual(want, have any) {
	tx.t.Helper()
	if reflect.DeepEqual(want, have) {
		return
	}
	tx.t.Fatalf("%s: want %v, have %v", tx.name, want, have)
}

func (tx *Tx) AssertNoErr(err error) {
	tx.t.Helper()
	if err == nil {
		return
	}
	tx.t.Fatalf("%s: error is not-nil but: %v", tx.name, err)
}

func (tx *Tx) AssertErr(err error) {
	tx.t.Helper()
	if err != nil {
		return
	}
	tx.t.Fatalf("%s: expect err; got none", tx.name)
}
