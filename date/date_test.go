package date

import (
	"testing"
	"time"

	"github.com/mazzegi/mbox/testx"
)

func TestDefaultLocation(t *testing.T) {
	tx := testx.NewTx(t)

	wantDefaultLocation, err := time.LoadLocation("Europe/Berlin")
	tx.AssertNoErr(err)
	tx.AssertEqual(wantDefaultLocation.String(), DefaultLocation().String())
}

func TestParse(t *testing.T) {
	tx := testx.NewTx(t)
	tests := []struct {
		in   string
		want Date
	}{
		{"2024-02-27T23:00:00Z", Make(2024, 02, 28)},
		{"14.02.2004", Make(2004, 02, 14)},
	}
	for i, test := range tests {
		t.Run(testx.Name(i), func(t *testing.T) {
			res, err := Parse(test.in)
			tx.AssertNoErr(err)
			tx.AssertEqual(test.want, res)
		})
	}
}

func TestNumDays(t *testing.T) {
	tx := testx.NewTx(t)
	type test struct {
		inFrom Date
		inTo   Date
		want   int
	}

	tests := []test{
		{
			inFrom: Make(2024, 4, 3),
			inTo:   Make(2024, 4, 3),
			want:   1,
		},
		{
			inFrom: Make(2024, 4, 4),
			inTo:   Make(2024, 4, 3),
			want:   0,
		},
		{
			inFrom: Make(2024, 4, 3),
			inTo:   Make(2024, 4, 4),
			want:   2,
		},
		{
			inFrom: Make(2024, 4, 3),
			inTo:   Make(2024, 4, 5),
			want:   3,
		},
		{
			inFrom: Make(2024, 4, 3),
			inTo:   Make(2024, 5, 3),
			want:   31,
		},
	}

	testx.RunTestsParallel(tx, tests, func(tx *testx.Tx, test test) {
		res := NumDays(test.inFrom, test.inTo)
		tx.AssertEqual(test.want, res)
	})
}

func TestBeterrnInclusive(t *testing.T) {
	tx := testx.NewTx(t)
	type test struct {
		in          Date
		inFrom      Date
		inTo        Date
		betweenIncl bool
	}

	tests := []test{
		{
			in:          Make(2024, 4, 3),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 3),
			betweenIncl: true,
		},
		{
			in:          Make(2024, 4, 3),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 7),
			betweenIncl: true,
		},
		{
			in:          Make(2024, 4, 7),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 7),
			betweenIncl: true,
		},
		{
			in:          Make(2024, 4, 5),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 7),
			betweenIncl: true,
		},
		{
			in:          Make(2024, 4, 2),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 7),
			betweenIncl: false,
		},
		{
			in:          Make(2024, 4, 12),
			inFrom:      Make(2024, 4, 3),
			inTo:        Make(2024, 4, 7),
			betweenIncl: false,
		},
	}

	testx.RunTestsParallel(tx, tests, func(tx *testx.Tx, test test) {
		res := test.in.BetweenInclusive(test.inFrom, test.inTo)
		tx.AssertEqual(test.betweenIncl, res)
	})
}
