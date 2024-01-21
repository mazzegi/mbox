package mathx

import (
	"fmt"
	"testing"

	"github.com/mazzegi/mbox/testx"
)

func TestRoundPlaces(t *testing.T) {
	tx := testx.NewTx(t)
	tests := []struct {
		in     float64
		places int
		exp    float64
	}{
		{34.4561, 2, 34.46},
		{5.4999, 3, 5.500},
		{12.12, 0, 12.0},
		{12.512, 2, 12.51},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%02d", i), func(t *testing.T) {
			res := RoundPlaces(test.in, test.places)
			tx.AssertEqual(test.exp, res)
		})
	}
}
