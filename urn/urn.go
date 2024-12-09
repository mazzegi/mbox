package urn

import (
	"fmt"
	"strings"
)

// URN is a urn due to RFC ...
type URN string

// Make creates a URN from the passed values
func Make(vs ...interface{}) URN {
	sl := make([]string, len(vs))
	for i, v := range vs {
		sl[i] = fmt.Sprintf("%v", v)
	}
	return URN(strings.Join(sl, ":"))
}

func (u URN) String() string {
	return string(u)
}
