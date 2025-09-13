package blobix_v2

import "github.com/mazzegi/mbox/query"

type Tx interface {
	Rollback() error
	Commit() error
	SaveRaw(bucket string, key string, raw []byte) error
	SaveRawMany(bucket string, kvs []Tuple[string, []byte]) error
	Delete(bucket string, keys ...string) error
	UpdateIndex(bucketName string, idxName string, key string, values map[string]any) error
}
type Store interface {
	BeginTx() (Tx, error)
	FindRaw(bucket string, key string) ([]byte, query.Found, error)
	FindRawMany(bucket string, keys ...string) (map[string][]byte, error)
	Keys(bucket string) ([]string, error)
	KeysPage(bucket string, skip, limit int, sort query.SortOrder) ([]string, error)

	// Index stuff
	FindIndexDescriptor(bucketName string, idxName string) (IndexDescriptor, bool)
	CreateIndex(bucketName string, idxName string, fields []IndexFieldDescriptor) error
	DeleteIndex(bucketName string, idxName string) error

	// query
	QueryKeys(bucketName string, indexName string, q query.Query) ([]string, error)
}
