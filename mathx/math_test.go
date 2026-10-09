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

func TestMinMax(t *testing.T) {
	tests := []struct {
		values  []float64
		wantMin float64
		wantMax float64
	}{
		{[]float64{}, 0, 0},
		{nil, 0, 0},
		{[]float64{12.4}, 12.4, 12.4},
		{[]float64{12.4, 3.001}, 3.001, 12.4},
		{[]float64{12.4, 3.001, 12323.01, -56.2}, -56.2, 12323.01},
	}
	for i, test := range tests {
		tx := testx.NewTxWithTestIdx(t, i)
		min := Min(test.values...)
		max := Max(test.values...)
		tx.AssertEqual(test.wantMin, min)
		tx.AssertEqual(test.wantMax, max)
	}
	for i, test := range tests {
		tx := testx.NewTxWithTestIdx(t, i)
		min, max := MinMax(test.values...)
		tx.AssertEqual(test.wantMin, min)
		tx.AssertEqual(test.wantMax, max)
	}
}
