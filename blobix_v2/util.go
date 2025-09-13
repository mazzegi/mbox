package blobix_v2

import "github.com/mazzegi/mbox/query"

type StringMarshaler interface {
	MarshalString() string
}

type KeysPage struct {
	Keys  []string
	Error error
	Idx   int
}

func StreamKeys(store Store, bucket string, pageLimit int) <-chan KeysPage {
	c := make(chan KeysPage)
	go func() {
		defer close(c)
		skip := 0
		idx := 0
		for {
			ks, err := store.KeysPage(bucket, skip, pageLimit, query.SortASC)
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
