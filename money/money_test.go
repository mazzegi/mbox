package money

import (
	"fmt"
	"testing"

	"github.com/mazzegi/mbox/testx"
)

func TestFloat(t *testing.T) {
	curr := EUR
	tests := []struct {
		in  float64
		exp Money
	}{
		{1.2, Money{Amount: 120, Currency: EUR}},
		{5.48, Money{Amount: 548, Currency: EUR}},
		{6.537, Money{Amount: 654, Currency: EUR}},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("Test #%02d", i), func(t *testing.T) {
			res := Decimal(test.in, curr)
			if res != test.exp {
				t.Fatalf("want %v, have %v", test.exp, res)
			}
		})
	}
}

func TestFormatHR(t *testing.T) {
	tests := []struct {
		in   Money
		curr string
		want string
	}{
		{
			in:   Money{Amount: 12000, Currency: EUR},
			curr: "€",
			want: "120,00 €",
		},
		{
			in:   Money{Amount: 153400, Currency: EUR},
			curr: "€",
			want: "1.534,00 €",
		},
		{
			in:   Money{Amount: 7234578112, Currency: EUR},
			curr: "€",
			want: "72.345.781,12 €",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("Test #%02d", i), func(t *testing.T) {
			res := test.in.FormatHR(test.curr)
			testx.AssertEqual(t, test.want, res)
		})
	}
}
