package blobix

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mazzegi/mbox/query"
)

type IndexFieldType string

const (
	IndexFieldAny    IndexFieldType = "" // for compatibility with old indexes without type
	IndexFieldString IndexFieldType = "string"
	IndexFieldInt    IndexFieldType = "int"
	IndexFieldFloat  IndexFieldType = "float"
)

func IF(name, path string) IndexField {
	return IndexField{
		Name: name,
		Path: path,
	}
}

func TIF(name, path string, typ IndexFieldType) IndexField {
	return IndexField{
		Name: name,
		Path: path,
		Type: typ,
	}
}

type IndexField struct {
	Name string         `json:"name"`
	Path string         `json:"path"`
	Type IndexFieldType `json:"type"`
}

type QueryResult struct {
	ModifiedOn time.Time
	Meta       json.RawMessage
}

func (qr QueryResult) DecodeMeta(v any) error {
	return json.Unmarshal(qr.Meta, v)
}

type Store interface {
	Close()
	Bucket(name string) Bucket
}

type Bucket interface {
	PutJSONMany(ts ...Tuple[string, any]) error
	PutJSON(key string, value any) error
	PutJSONWithMeta(key string, value any, meta any) error

	// PutJSONIndexValue(key string, value any, indexValue any) error
	// PutJSONIndexValueMeta(key string, value any, indexValue any, meta any) error

	JSON(key string, v any) (QueryResult, error)
	RawValues(keys ...string) (map[string]string, error)
	Delete(keys ...string) error
	Clear() error
	Keys() ([]string, error)

	KeysPage(skip, limit int, sort query.SortOrder) ([]string, error)
	KeysWithPrefixPage(prefix string, skip, limit int, sort query.SortOrder) ([]string, error)

	ExistsKey(key string) bool
	KeysWithPrefix(prefix string) ([]string, error)
	AddIndex(name string, fields ...IndexField) (Index, error)
	AddOrUpdateIndex(name string, fields ...IndexField) (Index, error)
	DeleteIndex(name string) error
	RebuildIndex(name string) error
	Index(name string) (Index, error)
	QueryKeys(indexName string, lo query.LimitOffset, fields []query.Condition, sorts []query.Sort, search query.Search) ([]string, error)
	QueryDistinct(indexName string, field string) ([]string, error)
}

type Index interface {
	Fields() []string
}

func Values[T any](bucket Bucket, keys ...string) ([]T, error) {
	rvs, err := bucket.RawValues(keys...)
	if err != nil {
		return nil, fmt.Errorf("raw-values: %w", err)
	}
	var ts []T
	for _, key := range keys {
		rv, ok := rvs[key]
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

func Query[T any](bucket Bucket, indexName string, q query.Query) ([]T, error) {
	keys, err := bucket.QueryKeys(indexName, q.LimitOffset, q.Conditions, q.Sorts, q.Search)
	if err != nil {
		return nil, fmt.Errorf("query-keys: %w", err)
	}
	return Values[T](bucket, keys...)
}

func QueryKeys(bucket Bucket, indexName string, q query.Query) ([]string, error) {
	keys, err := bucket.QueryKeys(indexName, q.LimitOffset, q.Conditions, q.Sorts, q.Search)
	if err != nil {
		return nil, fmt.Errorf("query-keys: %w", err)
	}
	return keys, nil
}
