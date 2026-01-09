package jsonx

import (
	"encoding/json"
	"fmt"
	"io"
)

func DumpIndent(w io.Writer, v any) {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(w, "ERROR: %v", err)
		return
	}
	w.Write(bs)
}

func Dump(w io.Writer, v any) {
	bs, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(w, "ERROR: %v", err)
		return
	}
	w.Write(bs)
}
