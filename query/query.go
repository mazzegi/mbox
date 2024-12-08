package query

import "fmt"

type LimitOffset struct {
	Limit  int
	Offset int
}

func (lo LimitOffset) OneMore() LimitOffset {
	return LO(lo.Limit+1, lo.Offset)
}

func LO(limit, offset int) LimitOffset {
	return LimitOffset{
		Limit:  limit,
		Offset: offset,
	}
}

type Comparator string

const (
	ComparatorEqual        Comparator = "eq"
	ComparatorNotEqual     Comparator = "neq"
	ComparatorLess         Comparator = "ls"
	ComparatorGreater      Comparator = "gt"
	ComparatorLessEqual    Comparator = "lseq"
	ComparatorGreaterEqual Comparator = "gteq"
	ComparatorLike         Comparator = "like"
	ComparatorIn           Comparator = "in"
)

type SortOrder string

const (
	SortNone SortOrder = "None"
	SortASC  SortOrder = "ASC"
	SortDESC SortOrder = "DESC"
)

type Condition struct {
	Name  string
	Comp  Comparator
	Value any
}

func (c Condition) ValueString() string {
	return fmt.Sprintf("%v", c.Value)
}

type Sort struct {
	Name  string
	Order SortOrder
}

type Search struct {
	Fields []string
	Value  string
}

func SearchFor(val string, in ...string) Search {
	return Search{
		Fields: in,
		Value:  val,
	}
}

func C(name string, comp Comparator, val any) Condition {
	return Condition{
		Name:  name,
		Comp:  comp,
		Value: val,
	}
}

func S(name string, ord SortOrder) Sort {
	return Sort{
		Name:  name,
		Order: ord,
	}
}

type Query struct {
	LimitOffset LimitOffset
	Conditions  []Condition
	Sorts       []Sort
	Search      Search
}

func (q Query) FindCondition(name string) (Condition, bool) {
	for _, c := range q.Conditions {
		if c.Name == name {
			return c, true
		}
	}
	return Condition{}, false
}
