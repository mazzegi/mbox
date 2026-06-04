package slicesx

import (
	"reflect"
	"strings"
	"testing"
)

func TestDedup(t *testing.T) {
	tests := map[string]struct {
		in  []int
		exp []int
	}{
		"no_duplicates": {
			in:  []int{1, 2, 3, 4, 5},
			exp: []int{1, 2, 3, 4, 5},
		},
		"with_duplicates": {
			in:  []int{1, 2, 2, 3, 4, 3, 5, 1},
			exp: []int{1, 2, 3, 4, 5},
		},
		"all_same": {
			in:  []int{7, 7, 7, 7},
			exp: []int{7},
		},
		"empty": {
			in:  []int{},
			exp: nil,
		},
		"single_element": {
			in:  []int{42},
			exp: []int{42},
		},
		"maintains_order": {
			in:  []int{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5},
			exp: []int{3, 1, 4, 5, 9, 2, 6},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := Dedup(test.in)
			if !reflect.DeepEqual(test.exp, res) {
				t.Fatalf("want: %v, have %v", test.exp, res)
			}
		})
	}
}

func TestDedupStrings(t *testing.T) {
	tests := map[string]struct {
		in  []string
		exp []string
	}{
		"no_duplicates": {
			in:  []string{"a", "b", "c"},
			exp: []string{"a", "b", "c"},
		},
		"with_duplicates": {
			in:  []string{"foo", "bar", "foo", "baz", "bar"},
			exp: []string{"foo", "bar", "baz"},
		},
		"empty": {
			in:  []string{},
			exp: nil,
		},
		"all_same": {
			in:  []string{"test", "test", "test"},
			exp: []string{"test"},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := Dedup(test.in)
			if !reflect.DeepEqual(test.exp, res) {
				t.Fatalf("want: %v, have %v", test.exp, res)
			}
		})
	}
}

func TestDedupFnc(t *testing.T) {
	tests := map[string]struct {
		in    []string
		eqFnc func(t1, t2 string) bool
		exp   []string
	}{
		"case_insensitive": {
			in: []string{"Hello", "world", "HELLO", "World", "test"},
			eqFnc: func(t1, t2 string) bool {
				return strings.EqualFold(t1, t2)
			},
			exp: []string{"Hello", "world", "test"},
		},
		"by_length": {
			in: []string{"a", "bb", "ccc", "d", "ee", "fff"},
			eqFnc: func(t1, t2 string) bool {
				return len(t1) == len(t2)
			},
			exp: []string{"a", "bb", "ccc"},
		},
		"empty": {
			in: []string{},
			eqFnc: func(t1, t2 string) bool {
				return t1 == t2
			},
			exp: nil,
		},
		"no_matches": {
			in: []string{"alpha", "beta", "gamma"},
			eqFnc: func(t1, t2 string) bool {
				return t1 == t2
			},
			exp: []string{"alpha", "beta", "gamma"},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := DedupFnc(test.in, test.eqFnc)
			if !reflect.DeepEqual(test.exp, res) {
				t.Fatalf("want: %v, have %v", test.exp, res)
			}
		})
	}
}

type TestPerson struct {
	Name string
	Age  int
}

func TestDedupFncStruct(t *testing.T) {
	tests := map[string]struct {
		in    []TestPerson
		eqFnc func(t1, t2 TestPerson) bool
		exp   []TestPerson
	}{
		"by_name": {
			in: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
				{Name: "Alice", Age: 35},
				{Name: "Charlie", Age: 40},
			},
			eqFnc: func(t1, t2 TestPerson) bool {
				return t1.Name == t2.Name
			},
			exp: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
				{Name: "Charlie", Age: 40},
			},
		},
		"by_age": {
			in: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
				{Name: "Charlie", Age: 30},
				{Name: "David", Age: 25},
			},
			eqFnc: func(t1, t2 TestPerson) bool {
				return t1.Age == t2.Age
			},
			exp: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
			},
		},
		"exact_match": {
			in: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
				{Name: "Alice", Age: 30},
			},
			eqFnc: func(t1, t2 TestPerson) bool {
				return t1.Name == t2.Name && t1.Age == t2.Age
			},
			exp: []TestPerson{
				{Name: "Alice", Age: 30},
				{Name: "Bob", Age: 25},
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := DedupFnc(test.in, test.eqFnc)
			if !reflect.DeepEqual(test.exp, res) {
				t.Fatalf("want: %v, have %v", test.exp, res)
			}
		})
	}
}
