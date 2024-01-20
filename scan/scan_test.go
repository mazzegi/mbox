package scan

import (
	"testing"

	"github.com/mazzegi/mbox/testx"
)

func TestSlice(t *testing.T) {
	tx := testx.NewTx(t)

	type test struct {
		Int    int
		String string
		Float  float64
	}

	sl := []any{42, "hans", 1.2314}
	res, err := Slice[test](sl)
	tx.AssertNoErr(err)
	tx.AssertEqual(test{42, "hans", 1.2314}, res)
}
