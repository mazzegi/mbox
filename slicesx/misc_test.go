package slicesx

import (
	"testing"

	"github.com/mazzegi/mbox/testx"
)

func TestReversed(t *testing.T) {
	tests := []struct {
		in   []string
		want []string
	}{
		{in: []string{}, want: []string{}},
		{in: nil, want: nil},
		{in: []string{"1", "2"}, want: []string{"2", "1"}},
		{in: []string{"1", "2", "foo", "bar", "baz"}, want: []string{"baz", "bar", "foo", "2", "1"}},
	}
	for i, test := range tests {
		tx := testx.NewTxWithTestIdx(t, i)
		res := Reversed(test.in)
		tx.AssertEqual(test.want, res)
	}
}
