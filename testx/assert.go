package testx

import (
	"reflect"
	"testing"

	"golang.org/x/exp/constraints"
)

func AssertEqual(t *testing.T, want, have any) {
	t.Helper()
	if reflect.DeepEqual(want, have) {
		return
	}
	t.Fatalf("want %v, have %v", want, have)
}

func AssertInRange[T constraints.Ordered](t *testing.T, val, lower, upper T) {
	t.Helper()
	if val >= lower && val <= upper {
		return
	}
	t.Fatalf("%v not in range [%v, %v]", val, lower, upper)
}

func AssertNoErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Fatalf("error is not-nil but: %v", err)
}

func AssertErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		return
	}
	t.Fatalf("expect err; got none")
}
