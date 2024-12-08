package date

import (
	"fmt"
	"strconv"
	"strings"
)

type ISOWeek struct {
	Year int `json:"year"`
	Week int `json:"week"`
}

func (w ISOWeek) Encode() string {
	return fmt.Sprintf("%d_%d", w.Year, w.Week)
}

func DecodeISOWeek(s string) (ISOWeek, error) {
	ys, ws, ok := strings.Cut(s, "_")
	if !ok {
		return ISOWeek{}, fmt.Errorf("invalid format")
	}
	y, err := strconv.Atoi(ys)
	if err != nil {
		return ISOWeek{}, fmt.Errorf("year atoi: %w", err)
	}
	w, err := strconv.Atoi(ws)
	if err != nil {
		return ISOWeek{}, fmt.Errorf("week atoi: %w", err)
	}
	return ISOWeek{Year: y, Week: w}, nil
}

func (w ISOWeek) Format() string {
	//return fmt.Sprintf("%d-W%d", w.Year, w.Week)
	return fmt.Sprintf("KW%d / %d", w.Week, w.Year)
}

func (w ISOWeek) DateOf(day GermanWeekday) Date {
	// calculate earliest day
	start := Make(w.Year, 1, 1)
	offsetDays := (w.Week-3)*7 - 1
	start = start.AddDays(offsetDays)
	for {
		start = start.AddDays(1)
		if start.GermanWeekDay() != day {
			continue
		}
		if start.ISOWeek() == w {
			return start
		}
	}
}

func (w ISOWeek) Next() ISOWeek {
	d := w.DateOf(Monday)
	return d.AddDays(7).ISOWeek()
}

func (w ISOWeek) AddWeeks(n int) ISOWeek {
	d := w.DateOf(Monday)
	return d.AddDays(n * 7).ISOWeek()
}
