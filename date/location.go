package date

import (
	"sync"
	"time"
)

var defaultLocationOnce sync.Once
var defaultLocation *time.Location

func DefaultLocation() *time.Location {
	defaultLocationOnce.Do(func() {
		loc, err := time.LoadLocation("Europe/Berlin")
		if err != nil {
			// fallback to default local
			loc = time.Now().Location()
		}
		defaultLocation = loc

	})
	return defaultLocation
}
