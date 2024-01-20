package errorx

import (
	"fmt"
	"strings"
)

type Group struct {
	errs []error
}

func NewGroup(errs ...error) *Group {
	return &Group{errs: errs}
}

func (g *Group) Append(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}
		g.errs = append(g.errs, err)
	}
}

func (g *Group) Error() error {
	var sl []string
	for _, err := range g.errs {
		if err == nil {
			continue
		}
		sl = append(sl, err.Error())
	}
	if len(sl) == 0 {
		return nil
	}
	return fmt.Errorf(strings.Join(sl, " | "))
}

func (g *Group) IsEmpty() bool {
	return len(g.errs) == 0
}

func (g *Group) Do(fn func() error) {
	if len(g.errs) > 0 {
		return
	}
	g.Append(fn())
}
