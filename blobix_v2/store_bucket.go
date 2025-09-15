package blobix_v2

import (
	"encoding/json"
	"fmt"
	"iter"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/query"
)

type BucketIndex[T any] struct {
	IndexName string
	Fields    []IndexField[T]
}

// Typed Bucket funcs
func NewBucket[T any](store Store, name string) *Bucket[T] {
	return &Bucket[T]{
		name:    name,
		store:   store,
		indexes: make(map[string]BucketIndex[T]),
	}
}

type Bucket[T any] struct {
	name    string
	store   Store
	indexes map[string]BucketIndex[T]
}

func indexValues[T any](fields []IndexField[T], t T) map[string]any {
	values := map[string]any{}
	for _, field := range fields {
		val := field.ValueFunc(t)
		values[field.Descriptor.Name] = val
	}
	return values
}

func (b *Bucket[T]) Save(key string, t T) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("json.marshal: %w", err)
	}
	tx, err := b.store.BeginTx()
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	err = tx.SaveRaw(b.name, key, raw)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("save-raw: %w", err)
	}
	for _, idx := range b.indexes {
		values := indexValues(idx.Fields, t)
		err := tx.UpdateIndex(b.name, idx.IndexName, key, values)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("update-index: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (b *Bucket[T]) SaveMany(kvs []Tuple[string, T]) error {
	rawKVs := make([]Tuple[string, []byte], len(kvs))
	for i, kvv := range kvs {
		raw, err := json.Marshal(kvv.Value)
		if err != nil {
			return fmt.Errorf("json.marshal: %w", err)
		}
		rawKVs[i] = MkTuple(kvv.Key, raw)
	}

	tx, err := b.store.BeginTx()
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	err = tx.SaveRawMany(b.name, rawKVs)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("save-raw: %w", err)
	}
	// indexes
	for _, kvv := range kvs {
		for _, idx := range b.indexes {
			values := indexValues(idx.Fields, kvv.Value)
			err := tx.UpdateIndex(b.name, idx.IndexName, kvv.Key, values)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("update-index: %w", err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (b *Bucket[T]) Delete(keys ...string) error {
	tx, err := b.store.BeginTx()
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	err = tx.Delete(b.name, keys...)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("delete: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (b *Bucket[T]) Find(key string) (T, query.Found, error) {
	var t T
	raw, found, err := b.store.FindRaw(b.name, key)
	if err != nil {
		return t, false, fmt.Errorf("find-raw: %w", err)
	}
	if !found {
		return t, false, nil
	}
	err = json.Unmarshal(raw, &t)
	if err != nil {
		return t, false, fmt.Errorf("json.unmarshal")
	}
	return t, true, nil
}

func (b *Bucket[T]) Values(keys ...string) ([]T, error) {
	rawValues, err := b.store.FindRawMany(b.name, keys...)
	if err != nil {
		return nil, fmt.Errorf("raw-values: %w", err)
	}
	var ts []T
	for _, key := range keys {
		rv, ok := rawValues[key]
		if !ok {
			continue
		}
		var t T
		err := json.Unmarshal([]byte(rv), &t)
		if err != nil {
			return nil, fmt.Errorf("json.unmarshal: %w", err)
		}
		ts = append(ts, t)
	}
	return ts, nil
}

func (b *Bucket[T]) KeyValues(keys ...string) (map[string]T, error) {
	rawValues, err := b.store.FindRawMany(b.name, keys...)
	if err != nil {
		return nil, fmt.Errorf("raw-values: %w", err)
	}
	kvs := map[string]T{}
	for _, key := range keys {
		rv, ok := rawValues[key]
		if !ok {
			continue
		}
		var t T
		err := json.Unmarshal([]byte(rv), &t)
		if err != nil {
			return nil, fmt.Errorf("json.unmarshal: %w", err)
		}
		kvs[key] = t
	}
	return kvs, nil
}

func (b *Bucket[T]) Query(indexName string, q query.Query) ([]T, error) {
	keys, err := b.store.QueryKeys(b.name, indexName, q)
	if err != nil {
		return nil, fmt.Errorf("store.query-keys: %w", err)
	}
	vs, err := b.Values(keys...)
	if err != nil {
		return nil, fmt.Errorf("values: %w", err)
	}
	return vs, nil
}

// To query for other (alias) types
func QueryTyped[DESTTYPE any](store Store, bucketName string, indexName string, q query.Query) ([]DESTTYPE, error) {
	keys, err := store.QueryKeys(bucketName, indexName, q)
	if err != nil {
		return nil, fmt.Errorf("store.query-keys: %w", err)
	}
	rawValues, err := store.FindRawMany(bucketName, keys...)
	if err != nil {
		return nil, fmt.Errorf("raw-values: %w", err)
	}
	var ts []DESTTYPE
	for _, key := range keys {
		rv, ok := rawValues[key]
		if !ok {
			continue
		}
		var t DESTTYPE
		err := json.Unmarshal([]byte(rv), &t)
		if err != nil {
			return nil, fmt.Errorf("json.unmarshal: %w", err)
		}
		ts = append(ts, t)
	}
	return ts, nil
}

func FindTyped[DESTTYPE any](store Store, bucketName string, key string) (DESTTYPE, query.Found, error) {
	var t DESTTYPE
	raw, found, err := store.FindRaw(bucketName, key)
	if err != nil {
		return t, false, fmt.Errorf("find-raw: %w", err)
	}
	if !found {
		return t, false, nil
	}
	err = json.Unmarshal(raw, &t)
	if err != nil {
		return t, false, fmt.Errorf("json.unmarshal")
	}
	return t, true, nil
}

func ValuesTyped[DESTTYPE any](store Store, bucketName string, keys ...string) ([]DESTTYPE, error) {
	rawValues, err := store.FindRawMany(bucketName, keys...)
	if err != nil {
		return nil, fmt.Errorf("raw-values: %w", err)
	}
	var ts []DESTTYPE
	for _, key := range keys {
		rv, ok := rawValues[key]
		if !ok {
			continue
		}
		var t DESTTYPE
		err := json.Unmarshal([]byte(rv), &t)
		if err != nil {
			return nil, fmt.Errorf("json.unmarshal: %w", err)
		}
		ts = append(ts, t)
	}
	return ts, nil
}

func (b *Bucket[T]) IterKeys() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		skip := 0
		pageLimit := 50
		for {
			ks, err := b.store.KeysPage(b.name, skip, pageLimit, query.SortASC)
			if err != nil {
				yield("", fmt.Errorf("keys-page: %w", err))
				return
			}
			for _, k := range ks {
				ok := yield(k, nil)
				if !ok {
					return
				}
			}
			if len(ks) < pageLimit {
				return
			}
			skip += pageLimit
		}
	}
}

func (b *Bucket[T]) testIterKeysWithErrFunc(errFnc func(int) error) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		defer func() {
			log.Debugf("goodbye!")
		}()
		skip := 0
		idx := 0
		pageLimit := 50
		for {
			ks, err := b.store.KeysPage(b.name, skip, pageLimit, query.SortASC)
			if err != nil {
				yield("", fmt.Errorf("keys-page: %w", err))
				return
			}

			for _, k := range ks {
				if errFnc(idx) != nil {
					yield("", fmt.Errorf("error func striked on idx %d", idx))
					return
				}
				ok := yield(k, nil)
				idx++
				_ = ok
				if !ok {
					return
				}
			}
			if len(ks) < pageLimit {
				return
			}
			skip += pageLimit
		}
	}
}
