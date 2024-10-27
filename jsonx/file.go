package jsonx

import (
	"encoding/json"
	"fmt"
	"os"
)

func DecodeFile[T any](file string) (T, error) {
	f, err := os.Open(file)
	if err != nil {
		var zT T
		return zT, fmt.Errorf("open-file %q: %w", file, err)
	}
	defer f.Close()

	var t T
	err = json.NewDecoder(f).Decode(&t)
	if err != nil {
		var zT T
		return zT, fmt.Errorf("decode-json: %w", err)
	}
	return t, nil
}

func EncodeFile[T any](file string, t T, indent bool) error {
	f, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("create-file %q: %w", file, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if indent {
		enc.SetIndent("", "  ")
	}
	err = enc.Encode(t)
	if err != nil {
		return fmt.Errorf("encode-json: %w", err)
	}
	return nil
}
