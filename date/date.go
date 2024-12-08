package date

import (
	"encoding/json"
	"fmt"
	"time"
)

type GermanWeekday int

const (
	Monday GermanWeekday = 1 + iota
	Tuesday
	Wednesday
	Thursday
	Friday
	Saturday
	Sunday
)

func AllGermanWeekdays() []GermanWeekday {
	return []GermanWeekday{
		Monday,
		Tuesday,
		Wednesday,
		Thursday,
		Friday,
		Saturday,
		Sunday,
	}
}

func AllGermanWorkingWeekdays() []GermanWeekday {
	return []GermanWeekday{
		Monday,
		Tuesday,
		Wednesday,
		Thursday,
		Friday,
	}
}

func (gwd GermanWeekday) Name() string {
	switch gwd {
	case Monday:
		return "Montag"
	case Tuesday:
		return "Dienstag"
	case Wednesday:
		return "Mittwoch"
	case Thursday:
		return "Donnerstag"
	case Friday:
		return "Freitag"
	case Saturday:
		return "Samstag"
	case Sunday:
		return "Sonntag"
	default:
		return ""
	}
}

func (gwd GermanWeekday) Abbreviate() string {
	switch gwd {
	case Monday:
		return "Mo"
	case Tuesday:
		return "Di"
	case Wednesday:
		return "Mi"
	case Thursday:
		return "Do"
	case Friday:
		return "Fr"
	case Saturday:
		return "Sa"
	case Sunday:
		return "So"
	default:
		return ""
	}
}

var loc = time.FixedZone("default", 0)

func Today() Date {
	return FromTime(time.Now())
}

func Yesterday() Date {
	return FromTime(time.Now()).AddDays(-1)
}

func Tomorrow() Date {
	return FromTime(time.Now()).AddDays(1)
}

func FromTime(t time.Time) Date {
	return Date{
		t: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc),
	}
}

type Date struct {
	t time.Time
}

func Make(year int, month time.Month, day int) Date {
	return Date{
		t: time.Date(year, month, day, 0, 0, 0, 0, loc),
	}
}

const CanonicalDate = "2006-01-02"

var parseLayouts = []string{
	CanonicalDate,
	time.RFC3339Nano,
	"02.01.2006",
}

func Parse(s string) (Date, error) {
	for _, layout := range parseLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			t = t.In(DefaultLocation())
			return FromTime(t), nil
		}
	}
	return Date{}, fmt.Errorf("cannot parse %q in any layout of %v", s, parseLayouts)
}

func (d Date) CanonicalString() string {
	return d.t.Format(CanonicalDate)
}

func (d Date) Format() string {
	return d.t.Format("02.01.2006")
}

func (d Date) FormatInLayout(layout string) string {
	return d.t.Format(layout)
}

func (d Date) String() string {
	return d.Format()
}

func (d Date) RFCString() string {
	return d.t.Format(time.RFC3339Nano)
}

func (d Date) Time() time.Time {
	return d.t
}

func (d Date) Sub(ot Date) time.Duration {
	return d.t.Sub(ot.t)
}

func (d Date) ISOWeek() ISOWeek {
	y, w := d.t.ISOWeek()
	return ISOWeek{Year: y, Week: w}
}

func (d Date) Year() int {
	return d.t.Year()
}

func (d Date) Month() time.Month {
	return d.t.Month()
}

func (d Date) Day() int {
	return d.t.Day()
}

func (d Date) WeekDay() time.Weekday {
	return d.t.Weekday()
}

func (d Date) GermanWeekDay() GermanWeekday {
	wd := d.t.Weekday()
	switch wd {
	case time.Sunday:
		return Sunday
	default:
		return GermanWeekday(wd)
	}
}

func (d Date) IsZero() bool {
	return d.t.IsZero()
}

func (d Date) Before(od Date) bool {
	return d.t.Before(od.t)
}

func (d Date) BeforeOrAt(od Date) bool {
	return d.t.Before(od.t) || d.t == od.t
}

func (d Date) After(od Date) bool {
	return d.t.After(od.t)
}

func (d Date) BetweenInclusive(from, to Date) bool {
	return !d.Before(from) && !d.After(to)
}

func (d Date) AddYears(years int) Date {
	return FromTime(d.t.AddDate(years, 0, 0))
}

func (d Date) AddMonths(months int) Date {
	return FromTime(d.t.AddDate(0, months, 0))
}

func (d Date) AddDays(days int) Date {
	return FromTime(d.t.AddDate(0, 0, days))
}

func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.CanonicalString())
}

func (d *Date) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	dt, err := Parse(s)
	if err != nil {
		return fmt.Errorf("parse-date %q: %w", s, err)
	}
	*d = dt
	return nil
}

func NumDays(from Date, to Date) int {
	if from.After(to) {
		return 0
	}
	return 1 + int(to.Sub(from).Round(24*time.Hour).Hours())/24
}
