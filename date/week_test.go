package date

import (
	"fmt"
	"testing"

	"github.com/mazzegi/mbox/testx"
)

func TestWeekDateOf(t *testing.T) {
	tx := testx.NewTx(t)
	tests := []struct {
		week     ISOWeek
		day      GermanWeekday
		wantDate Date
	}{
		{
			week:     ISOWeek{Year: 2024, Week: 9},
			day:      Monday,
			wantDate: Make(2024, 2, 26),
		},
		{
			week:     ISOWeek{Year: 2024, Week: 1},
			day:      Wednesday,
			wantDate: Make(2024, 1, 3),
		},
		{
			week:     ISOWeek{Year: 2023, Week: 52},
			day:      Tuesday,
			wantDate: Make(2023, 12, 26),
		},
		{
			week:     ISOWeek{Year: 2024, Week: 26},
			day:      Monday,
			wantDate: Make(2024, 6, 24),
		},
		{
			week:     ISOWeek{Year: 2024, Week: 52},
			day:      Monday,
			wantDate: Make(2024, 12, 23),
		},
		{
			week:     ISOWeek{Year: 2025, Week: 1},
			day:      Monday,
			wantDate: Make(2024, 12, 30),
		},
		{
			week:     ISOWeek{Year: 2025, Week: 1},
			day:      Friday,
			wantDate: Make(2025, 1, 3),
		},
		{
			week:     ISOWeek{Year: 2025, Week: 2},
			day:      Wednesday,
			wantDate: Make(2025, 1, 8),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_%03d", i), func(t *testing.T) {
			res := test.week.DateOf(test.day)
			tx.AssertEqual(test.wantDate, res)
		})
	}
}
