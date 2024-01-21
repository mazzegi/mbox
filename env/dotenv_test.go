package env

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFiles(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	wd, err = filepath.Abs(wd)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	defer os.Chdir(wd)

	//
	os.Chdir(filepath.Join(wd, "test_files", "sub_1", "sub_2"))
	e := LoadDotenv()
	exp := map[string]any{
		"foo2":       "bazoo",
		"bar":        int64(40),
		"ixy":        "np",
		"foo1":       "baz",
		"foo":        "bar",
		"acme":       "  inc. unlimited ...  ",
		"dev":        true,
		"global_dsn": "user\\domain@foo.bar",
		"loc_dsn":    "dsn={global_dsn}/local",
	}
	if !reflect.DeepEqual(exp, e) {
		t.Fatalf("want %v, have %v", exp, e)
	}
}
