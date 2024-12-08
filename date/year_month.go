package date

import (
	"fmt"
	"time"
)

type YearMonth struct {
	d Date
}

func YearMonthNow() YearMonth {
	td := Today()
	d := Make(td.Year(), td.Month(), 1)
	return YearMonth{d: d}
}

func (ym YearMonth) Year() int {
	return ym.d.Year()
}

func (ym YearMonth) Month() time.Month {
	return ym.d.Month()
}

func (ym YearMonth) Previous() YearMonth {
	return YearMonth{d: ym.d.AddMonths(-1)}
}

func (ym YearMonth) Next() YearMonth {
	return YearMonth{d: ym.d.AddMonths(1)}
}

func (ym YearMonth) AddMonths(n int) YearMonth {
	return YearMonth{d: ym.d.AddMonths(n)}
}

func (ym YearMonth) String() string {
	return fmt.Sprintf("%d-%02d", ym.Year(), ym.Month())
}
