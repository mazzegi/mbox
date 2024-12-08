package clock

import (
	"testing"
	"time"

	"github.com/mazzegi/mbox/testx"
)

func TestClock(t *testing.T) {
	//var err error
	var c1, c2 Clock

	c1 = Make(14, 23, 45)
	c2 = Make(6, 7, 2)
	t.Logf("c1: %q", c1)
	t.Logf("c2: %q", c2)
	if !c1.After(c2) {
		t.Fatalf("%q not after %q", c1, c2)
	}
	if !c2.Before(c1) {
		t.Fatalf("%q not before %q", c2, c1)
	}

	c2 = c2.Add(9 * time.Hour)
	t.Logf("c1: %q", c1)
	t.Logf("c2: %q", c2)
	if c1.After(c2) {
		t.Fatalf("%q after %q", c1, c2)
	}
	if c2.Before(c1) {
		t.Fatalf("%q before %q", c2, c1)
	}

	c2 = c2.Add(9 * time.Hour)
	t.Logf("c1: %q", c1)
	t.Logf("c2: %q", c2)
	if !c1.After(c2) {
		t.Fatalf("%q not after %q", c1, c2)
	}
	if !c2.Before(c1) {
		t.Fatalf("%q not before %q", c2, c1)
	}
}

func TestClockMidnight(t *testing.T) {
	c := Make(24, 15, 0)
	testx.AssertEqual(t, "00:15:00", c.Format())
}
