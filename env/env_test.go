package env

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEnvExpand(t *testing.T) {
	tests := []struct {
		in    []Var
		probe map[string]string
	}{
		{
			in: []Var{
				MkVar("s", "b"),
				MkVar("c", "d"),
			},
			probe: map[string]string{
				"s": "b",
				"c": "d",
			},
		},
		{
			in: []Var{
				MkVar("esc", "foo\\bar"),
				MkVar("glob", "glob.attr"),
				MkVar("p1", "{glob}_v1"),
				MkVar("p2", "v2_{glob}"),
				MkVar("p3", "v3_{glob}_v4"),
				MkVar("pesc", "p_{esc}_s"),
			},
			probe: map[string]string{
				"p1":   "glob.attr_v1",
				"p2":   "v2_glob.attr",
				"p3":   "v3_glob.attr_v4",
				"pesc": "p_foo\\bar_s",
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_%02d", i), func(t *testing.T) {
			env := Load(test.in...)
			for k, v := range test.probe {
				res, ok := env.String(k)
				if !ok {
					t.Fatalf("expect key %q present, but it is not", k)
				}
				if res != v {
					t.Fatalf("value for %q: want %q, have %q", k, v, res)
				}
			}
		})
	}
}

func TestEnvFilesExpand(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	wd, err = filepath.Abs(wd)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	defer os.Chdir(wd)
	os.Chdir(filepath.Join(wd, "test_files", "sub_1", "sub_2"))

	tests := []struct {
		in    []Var
		probe map[string]string
	}{
		{
			in: []Var{
				MkVar("s", "b"),
				MkVar("c", "d"),
			},
			probe: map[string]string{
				"s": "b",
				"c": "d",
			},
		},
		{
			in: []Var{
				MkVar("esc", "foo\\bar"),
				MkVar("glob", "glob.attr"),
				MkVar("p1", "{glob}_v1"),
				MkVar("p2", "v2_{glob}"),
				MkVar("p3", "v3_{glob}_v4"),
				MkVar("pesc", "p_{esc}_s"),
			},
			probe: map[string]string{
				"p1":         "glob.attr_v1",
				"p2":         "v2_glob.attr",
				"p3":         "v3_glob.attr_v4",
				"pesc":       "p_foo\\bar_s",
				"global_dsn": "user\\domain@foo.bar",
				"loc_dsn":    "dsn=user\\domain@foo.bar/local",
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_%02d", i), func(t *testing.T) {
			env := Load(test.in...)
			for k, v := range test.probe {
				res, ok := env.String(k)
				if !ok {
					t.Fatalf("expect key %q present, but it is not", k)
				}
				if res != v {
					t.Fatalf("value for %q: want %q, have %q", k, v, res)
				}
			}
		})
	}
}
