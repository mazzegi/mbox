package clock

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mazzegi/mbox/date"
)

var loc = time.FixedZone("default", 0)

func Now() Clock {
	return FromTime(time.Now())
}

func FromTime(t time.Time) Clock {
	return Clock{
		t: time.Date(1970, 01, 01, t.Hour(), t.Minute(), t.Second(), 0, loc),
	}
}

func Make(hour, minute, second int) Clock {
	return Clock{
		t: time.Date(1970, 01, 01, hour, minute, second, 0, loc),
	}
}

var Zero = Make(0, 0, 0)

var parseLayouts = []string{
	"15:04:05",
	"15:04",
	"15",
}

func Parse(s string) (Clock, error) {
	for _, layout := range parseLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return FromTime(t), nil
		}
	}
	return Clock{}, fmt.Errorf("cannot parse %q in any layout of %v", s, parseLayouts)
}

// func Parse(s string) (Clock, error) {
// 	t, err := time.Parse("15:04:05", s)
// 	if err != nil {
// 		return Clock{}, err
// 	}
// 	return FromTime(t), nil
// }

func MustParse(s string) Clock {
	c, err := Parse(s)
	if err != nil {
		panic(fmt.Sprintf("parse %q as clock: %v", s, err))
	}
	return c
}

type Clock struct {
	t time.Time
}

func (c Clock) Time() time.Time {
	return c.t
}

func (c Clock) TimeAt(d date.Date, loc *time.Location) time.Time {
	//return time.Date(date.Year(), date.Month(), date.Day(), c.Hour(), c.Minute(), c.Second(), 0, loc)
	return time.Date(d.Year(), d.Month(), d.Day(), c.Hour(), c.Minute(), c.Second(), 0, loc)
}

func (c Clock) String() string {
	return c.t.Format("15:04:05")
}

func (c Clock) Format() string {
	return c.t.Format("15:04:05")
}

func (c Clock) FormatInLayout(layout string) string {
	return c.t.Format(layout)
}

func (c Clock) IsZero() bool {
	//return c.t.IsZero()
	return c == Zero
}

func (c Clock) Hour() int {
	return c.t.Hour()
}

func (c Clock) Minute() int {
	return c.t.Minute()
}

func (c Clock) Second() int {
	return c.t.Second()
}

func (c Clock) Before(oc Clock) bool {
	return c.t.Before(oc.t)
}

func (c Clock) After(oc Clock) bool {
	return c.t.After(oc.t)
}

func (c Clock) Sub(oc Clock) time.Duration {
	return c.t.Sub(oc.t)
}

func (c Clock) Add(d time.Duration) Clock {
	return FromTime(c.t.Add(d))

}

func (c Clock) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Format())
}

func (c *Clock) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	dc, err := Parse(s)
	if err != nil {
		return fmt.Errorf("parse-date %q: %w", s, err)
	}
	*c = dc
	return nil
}
