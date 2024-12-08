package blobix

import "github.com/mazzegi/mbox/query"

func Value[T any](bucket Bucket, key string) (T, error) {
	var t T
	_, err := bucket.JSON(key, &t)
	if err != nil {
		return t, err
	}
	return t, nil
}

func AllValues[T any](bucket Bucket) ([]T, error) {
	keys, err := bucket.Keys()
	if err != nil {
		return nil, err
	}
	var ts []T
	for _, key := range keys {
		var t T
		_, err = bucket.JSON(key, &t)
		if err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}
	return ts, nil
}

func AllPrefixValues[T any](bucket Bucket, prefix string) ([]T, error) {
	keys, err := bucket.KeysWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var ts []T
	for _, key := range keys {
		var t T
		_, err = bucket.JSON(key, &t)
		if err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}
	return ts, nil
}

type KeysPage struct {
	Keys  []string
	Error error
	Idx   int
}

func StreamKeys(bucket Bucket, pageLimit int) <-chan KeysPage {
	c := make(chan KeysPage)
	go func() {
		defer close(c)
		skip := 0
		idx := 0
		for {
			ks, err := bucket.KeysPage(skip, pageLimit, query.SortASC)
			if err != nil {
				c <- KeysPage{Error: err}
				return
			}
			c <- KeysPage{
				Keys: ks,
				Idx:  idx,
			}
			if len(ks) < pageLimit {
				return
			}
			skip += pageLimit
			idx++
		}
	}()
	return c
}
