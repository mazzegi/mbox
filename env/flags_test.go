package env

import (
	"reflect"
	"testing"
)

func TestFlags(t *testing.T) {
	tests := map[string]struct {
		args     []string
		expFlags map[string]any
	}{
		"test_01": {
			args: []string{
				"-foo=bar",
			},
			expFlags: map[string]any{
				"foo": "bar",
			},
		},
		"test_02": {
			args: []string{
				"--foo=bar",
			},
			expFlags: map[string]any{
				"foo": "bar",
			},
		},
		"test_03": {
			args: []string{
				"-foo", "bar",
			},
			expFlags: map[string]any{
				"foo": "bar",
			},
		},
		"test_04": {
			args: []string{
				"--foo", "bar",
			},
			expFlags: map[string]any{
				"foo": "bar",
			},
		},
		"test_05": {
			args: []string{
				"-bbar",
			},
			expFlags: map[string]any{
				"bbar": true,
			},
		},
		"test_06": {
			args: []string{
				"--bbar",
			},
			expFlags: map[string]any{
				"bbar": true,
			},
		},
		"test_07": {
			args: []string{
				"--bbar", "-foo=baz", "--wop", "22",
			},
			expFlags: map[string]any{
				"bbar": true,
				"foo":  "baz",
				"wop":  "22",
			},
		},
		"test_08": {
			args: []string{
				"--wop", "22", "-foo=baz", "--bbar",
			},
			expFlags: map[string]any{
				"bbar": true,
				"foo":  "baz",
				"wop":  "22",
			},
		},
		"test_09": {
			args: []string{
				"--wop", "22", "-foo=baz", "vamos", "--bbar",
			},
			expFlags: map[string]any{
				"bbar": true,
				"foo":  "baz",
				"wop":  "22",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := ParseFlags(test.args)
			if !reflect.DeepEqual(test.expFlags, res) {
				t.Fatalf("want %v, have %v", test.expFlags, res)
			}
		})
	}
}
