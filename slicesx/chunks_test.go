package slicesx

import (
	"reflect"
	"testing"
)

func TestChunks(t *testing.T) {
	tests := map[string]struct {
		in        []int
		chunkSize int
		exp       [][]int
	}{
		"simple": {
			in:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			chunkSize: 3,
			exp: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		"with_overlap_1": {
			in:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			chunkSize: 3,
			exp: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{10},
			},
		},
		"with_overlap_2": {
			in:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			chunkSize: 3,
			exp: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{10, 11},
			},
		},
		"first_not_full": {
			in:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			chunkSize: 20,
			exp: [][]int{
				{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
		},
		"empty": {
			in:        []int{},
			chunkSize: 5,
			exp:       [][]int{},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := Chunks(test.in, test.chunkSize)
			if !reflect.DeepEqual(test.exp, res) {
				t.Fatalf("want: %v, have %v", test.exp, res)
			}
		})
	}
}
