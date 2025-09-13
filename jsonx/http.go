package jsonx

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mazzegi/mbox/makex"
)

func WriteHTTP(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func HttpGET[T any](url string) (T, error) {
	resp, err := http.Get(url)
	if err != nil {
		return makex.ZeroOf[T](), fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return makex.ZeroOf[T](), fmt.Errorf("http status code: %d", resp.StatusCode)
	}
	var t T
	err = json.NewDecoder(resp.Body).Decode(&t)
	if err != nil {
		return makex.ZeroOf[T](), fmt.Errorf("json decode: %w", err)
	}
	return t, nil
}
