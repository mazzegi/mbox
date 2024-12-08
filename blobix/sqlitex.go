package blobix

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/query"
	"github.com/mazzegi/mbox/slicesx"
	"github.com/mazzegi/mbox/sqlitex"
)

const sqlitex_v1_init = `
CREATE TABLE IF NOT EXISTS data (
	bucket 		TEXT,
	key	   		TEXT,
	modified_on TEXT,
	meta 		TEXT,
	value  		TEXT,
	PRIMARY KEY (bucket, key)
);

CREATE TABLE IF NOT EXISTS index_data (
	bucket 		TEXT,
	name		TEXT,
	meta 		TEXT,	
	PRIMARY KEY (bucket, name)
);
`

var _ Store = (*SqliteXStore)(nil)

func NewSqliteXStore(file string) (*SqliteXStore, error) {
	dbx, err := sqlitex.NewDB(file)
	if err != nil {
		return nil, fmt.Errorf("sqlitex.newdb at %q: %w", file, err)
	}
	_, err = dbx.ExecContext(context.Background(), sqlitex_v1_init)
	if err != nil {
		return nil, fmt.Errorf("exec-init: %w", err)
	}
	im, err := NewSqliteXIndexManager(dbx)
	if err != nil {
		return nil, fmt.Errorf("new-index-manager: %w", err)
	}

	s := &SqliteXStore{
		dbx:          dbx,
		indexManager: im,
	}
	return s, nil
}

type SqliteXStore struct {
	dbx          *sqlitex.DB
	indexManager *SqliteXIndexManager
}

func (s *SqliteXStore) Close() {
	s.dbx.Close()
}

func (s *SqliteXStore) Bucket(name string) Bucket {
	return &SqliteXStoreBucket{
		name:         name,
		dbx:          s.dbx,
		indexManager: s.indexManager,
	}
}

type SqliteXStoreBucket struct {
	name         string
	dbx          *sqlitex.DB
	indexManager *SqliteXIndexManager
}

func formatTime(t time.Time) string {
	return t.UTC().Round(time.Microsecond).Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
	if err != nil {
		return time.Time{}
	}
	return t.Round(time.Microsecond)
}

func (b *SqliteXStoreBucket) PutJSONMany(ts ...Tuple[string, any]) error {
	for _, t := range ts {
		err := b.PutJSON(t.Key, t.Value)
		if err != nil {
			return fmt.Errorf("put-json: %w", err)
		}
	}
	return nil
}

func (b *SqliteXStoreBucket) PutJSON(key string, value any) error {
	return b.PutJSONWithMeta(key, value, nil)
}

func (b *SqliteXStoreBucket) PutJSONWithMeta(key string, value any, meta any) error {
	return b.PutJSONIndexValueMeta(key, value, value, meta)

}

func (b *SqliteXStoreBucket) PutJSONIndexValue(key string, value any, indexValue any) error {
	return b.PutJSONIndexValueMeta(key, value, indexValue, nil)
}

func (b *SqliteXStoreBucket) PutJSONIndexValueMeta(key string, value any, indexValue any, meta any) error {
	bs, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json.marshal value: %w", err)
	}
	var metabs []byte
	if meta != nil {
		metabs, err = json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("json.marshal meta: %w", err)
		}
	}
	tx, err := b.dbx.BeginTx(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}

	_, err = tx.ExecContext(
		context.TODO(),
		"INSERT OR REPLACE INTO data (bucket ,key, modified_on, meta, value) VALUES(?,?,?,?,?);",
		b.name, key, formatTime(time.Now()), string(metabs), string(bs),
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("exec insert: %w", err)
	}

	err = b.indexManager.updateIndexes(tx, b.name, key, indexValue)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("update indexes: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit-tx: %w", err)
	}
	return err
}

func (b *SqliteXStoreBucket) RebuildIndex(name string) error {
	idxMeta, ok := b.indexManager.findIndexMeta(b.name, name)
	if !ok {
		return fmt.Errorf("index not found (%s:%s)", b.name, name)
	}
	tx, err := b.dbx.BeginTx(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	log.Debugf("rebuild-index: %s", idxMeta.Name)

	for kp := range StreamKeys(b, 500) {
		if kp.Error != nil {
			tx.Rollback()
			return fmt.Errorf("stream-keys: %w", kp.Error)
		}
		log.Debugf("rebuild-index: page %d ", kp.Idx+1)
		for _, key := range kp.Keys {
			var val map[string]any
			_, err := b.JSON(key, &val)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("json: %w", err)
			}
			b.indexManager.updateIndex(tx, idxMeta, key, val)
		}
	}
	log.Debugf("rebuild-index: %s ... done", idxMeta.Name)
	return tx.Commit()
}

func (b *SqliteXStoreBucket) Clear() error {
	tx, err := b.dbx.BeginTx(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}

	_, err = tx.ExecContext(
		context.TODO(),
		"DELETE FROM data WHERE bucket = ?;", b.name)

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("exec delete: %w", err)
	}

	err = b.indexManager.onClear(tx, b.name)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("delete from indexes: %w", err)
	}

	//TODO: update indexes
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit-tx: %w", err)
	}
	return err
}

func (b *SqliteXStoreBucket) Delete(keys ...string) error {
	tx, err := b.dbx.BeginTx(context.TODO(), nil)
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}

	whereIn := make([]string, len(keys))
	for i, k := range keys {
		whereIn[i] = fmt.Sprintf("'%s'", k)
	}
	_, err = tx.ExecContext(
		context.TODO(),
		fmt.Sprintf("DELETE FROM data WHERE bucket = ? AND key IN (%s);",
			strings.Join(whereIn, ", ")), b.name)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("exec delete: %w", err)
	}

	err = b.indexManager.onDelete(tx, b.name, keys...)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("delete from indexes: %w", err)
	}

	//TODO: update indexes
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit-tx: %w", err)
	}
	return err
}

func (b *SqliteXStoreBucket) JSON(key string, v any) (QueryResult, error) {
	row := b.dbx.QueryRowContext(
		context.TODO(),
		"SELECT modified_on, meta, value FROM data WHERE bucket = ? AND key = ?;",
		b.name, key)

	var modified sql.NullString
	var meta sql.NullString
	var value sql.NullString

	err := row.Scan(&modified, &meta, &value)
	if err != nil {
		return QueryResult{}, err
	}
	err = json.Unmarshal([]byte(value.String), v)
	if err != nil {
		return QueryResult{}, fmt.Errorf("json.unmarshal value: %w", err)
	}
	res := QueryResult{
		ModifiedOn: parseTime(modified.String),
		Meta:       json.RawMessage(meta.String),
	}
	return res, err
}

// func (s *Store) RawValues(bucket string, allKeys ...string) (map[string]string, error) {
// 	if len(allKeys) == 0 {
// 		return map[string]string{}, nil
// 	}
// 	rvs := map[string]string{}
// 	keyChunks := slices.Chunks(allKeys, 500)
// 	for _, keys := range keyChunks {

// 		args := append([]any{bucket}, slices.Anys(keys)...)
// 		keyPHs := strings.Join(slices.Repeat("?", len(keys)), ",")
// 		rows, err := s.db.Query(
// 			fmt.Sprintf(`SELECT key, value FROM data WHERE bucket = ? AND key IN (%s);`, keyPHs),
// 			args...,
// 		)
// 		if err != nil {
// 			return nil, fmt.Errorf("query: %w", err)
// 		}
// 		defer rows.Close()
// 		for rows.Next() {
// 			var key sql.NullString
// 			var rv sql.NullString
// 			err := rows.Scan(&key, &rv)
// 			if err != nil {
// 				return nil, fmt.Errorf("scan: %w", err)
// 			}
// 			rvs[key.String] = rv.String
// 		}
// 	}

// 	return rvs, nil
// }

func (b *SqliteXStoreBucket) RawValues(allKeys ...string) (map[string]string, error) {
	if len(allKeys) == 0 {
		return map[string]string{}, nil
	}
	rvs := map[string]string{}

	keyChunks := slicesx.Chunks(allKeys, 500)
	for _, keys := range keyChunks {
		args := append([]any{b.name}, slicesx.Anys(keys)...)
		keyPHs := strings.Join(slicesx.Repeat("?", len(keys)), ",")
		rows, err := b.dbx.Query(
			fmt.Sprintf(`SELECT key, value FROM data WHERE bucket = ? AND key IN (%s);`, keyPHs),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var key sql.NullString
			var rv sql.NullString
			err := rows.Scan(&key, &rv)
			if err != nil {
				return nil, fmt.Errorf("scan: %w", err)
			}
			rvs[key.String] = rv.String
		}
	}

	return rvs, nil
}

func (b *SqliteXStoreBucket) Keys() ([]string, error) {
	rows, err := b.dbx.QueryContext(
		context.TODO(),
		"SELECT key FROM data WHERE bucket = ?;",
		b.name)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var keys []string
	var key sql.NullString
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, key.String)
	}
	return keys, nil
}

func (b *SqliteXStoreBucket) KeysPage(skip, limit int, sort query.SortOrder) ([]string, error) {
	rows, err := b.dbx.QueryContext(
		context.TODO(),
		fmt.Sprintf("SELECT key FROM data WHERE bucket = ? ORDER BY key %s LIMIT ? OFFSET ?;", sort),
		b.name, limit, skip)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var keys []string
	var key sql.NullString
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, key.String)
	}
	return keys, nil
}

func (b *SqliteXStoreBucket) KeysWithPrefixPage(prefix string, skip, limit int, sort query.SortOrder) ([]string, error) {
	rows, err := b.dbx.QueryContext(
		context.TODO(),
		fmt.Sprintf("SELECT key FROM data WHERE bucket = ? AND key GLOB ? ORDER BY key %s LIMIT ? OFFSET ?;", sort),
		b.name, prefix+"*", limit, skip)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var keys []string
	var key sql.NullString
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, key.String)
	}
	return keys, nil
}

func (b *SqliteXStoreBucket) KeysWithPrefix(prefix string) ([]string, error) {
	rows, err := b.dbx.QueryContext(
		context.TODO(),
		"SELECT key FROM data WHERE bucket = ? AND key GLOB ?;",
		b.name, prefix+"*")
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	var keys []string
	var key sql.NullString
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, key.String)
	}
	return keys, nil
}

func (b *SqliteXStoreBucket) ExistsKey(key string) bool {
	row := b.dbx.QueryRow("SELECT 1 FROM data WHERE bucket = ? AND key = ?;", b.name, key)
	var one sql.NullInt64
	err := row.Scan(&one)
	return err == nil
}

func (b *SqliteXStoreBucket) AddIndex(name string, fields ...IndexField) (Index, error) {
	return b.indexManager.AddIndex(b.name, name, fields...)
}

func (b *SqliteXStoreBucket) AddOrUpdateIndex(name string, fields ...IndexField) (Index, error) {
	idxMeta, ok := b.indexManager.findIndexMeta(b.name, name)
	if !ok {
		idx, err := b.AddIndex(name, fields...)
		if err != nil {
			return nil, fmt.Errorf("add-index: %w", err)
		}
		err = b.RebuildIndex(name)
		if err != nil {
			return nil, fmt.Errorf("rebuild-index: %w", err)
		}
		return idx, nil
	}
	//index exists - is it the same?
	updateIdxMeta := sqliteXIndexMeta{
		Bucket:    b.name,
		Name:      name,
		TableName: b.indexManager.indexTabName(b.name, name),
		Fields:    fields,
	}
	if reflect.DeepEqual(idxMeta, updateIdxMeta) {
		// same index
		// rebuild?
		return b.indexManager.Index(b.name, name)
	}
	// its different - drop old and create new one
	err := b.DeleteIndex(name)
	if err != nil {
		return nil, fmt.Errorf("delete-index: %w", err)
	}
	idx, err := b.AddIndex(name, fields...)
	if err != nil {
		return nil, fmt.Errorf("add-index: %w", err)
	}
	err = b.RebuildIndex(name)
	if err != nil {
		return nil, fmt.Errorf("rebuild-index: %w", err)
	}
	return idx, nil
}

func (b *SqliteXStoreBucket) DeleteIndex(name string) error {
	return b.indexManager.deleteIndex(b.name, name)
}

func (b *SqliteXStoreBucket) Index(name string) (Index, error) {
	return b.indexManager.Index(b.name, name)
}

func (b *SqliteXStoreBucket) QueryKeys(indexName string, lo query.LimitOffset, fields []query.Condition, sorts []query.Sort, search query.Search) ([]string, error) {
	return b.indexManager.Query(b.name, indexName, lo, fields, sorts, search)
}

func (b *SqliteXStoreBucket) QueryDistinct(indexName string, field string) ([]string, error) {
	return b.indexManager.QueryDistinct(b.name, indexName, field)
}

// compatibility with blob store

func (b *SqliteXStore) PutJSON(bucket string, key string, v interface{}) error {
	return b.Bucket(bucket).PutJSON(key, v)
}

func (b *SqliteXStore) JSON(bucket string, key string, v interface{}) error {
	_, err := b.Bucket(bucket).JSON(key, v)
	return err
}

func (b *SqliteXStore) PutJSONMany(bucket string, ts ...Tuple[string, any]) error {
	return b.Bucket(bucket).PutJSONMany(ts...)
}

func (b *SqliteXStore) RawValues(bucket string, keys ...string) (map[string]string, error) {
	return b.Bucket(bucket).RawValues(keys...)
}

func (b *SqliteXStore) ExistsKey(bucket string, key string) bool {
	return b.Bucket(bucket).ExistsKey(key)
}

func (b *SqliteXStore) Keys(bucket string) ([]string, error) {
	return b.Bucket(bucket).Keys()
}

func (b *SqliteXStore) KeysWithPrefix(bucket string, prefix string) ([]string, error) {
	return b.Bucket(bucket).KeysWithPrefix(prefix)
}

func (b *SqliteXStore) KeysPage(bucket string, skip, limit int) ([]string, error) {
	return b.Bucket(bucket).KeysPage(skip, limit, query.SortASC)
}

func (b *SqliteXStore) Delete(bucket string, keys ...string) error {
	return b.Bucket(bucket).Delete(keys...)
}
