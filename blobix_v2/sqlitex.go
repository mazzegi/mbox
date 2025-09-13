package blobix_v2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
	err = s.prepare()
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	return s, nil
}

type SqliteXStore struct {
	dbx          *sqlitex.DB
	indexManager *SqliteXIndexManager

	stmtInsertData *sql.Stmt
	stmtQueryValue *sql.Stmt
}

func (store *SqliteXStore) Close() {
	store.dbx.Close()
}

func (store *SqliteXStore) prepare() error {
	var err error
	store.stmtInsertData, err = store.dbx.PrepareExec("INSERT OR REPLACE INTO data (bucket ,key, modified_on, meta, value) VALUES(?,?,?,?,?);")
	if err != nil {
		return fmt.Errorf("prepare-insert-data: %w", err)
	}
	store.stmtQueryValue, err = store.dbx.PrepareQuery("SELECT value FROM data WHERE bucket = ? AND key = ?;")
	if err != nil {
		return fmt.Errorf("prepare-query-value: %w", err)
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.UTC().Round(time.Microsecond).Format(time.RFC3339Nano)
}

type sqliteXStoreTx struct {
	store *SqliteXStore
	tx    *sql.Tx
}

func (stx *sqliteXStoreTx) Rollback() error {
	return stx.tx.Rollback()
}

func (stx *sqliteXStoreTx) Commit() error {
	return stx.tx.Commit()
}

func (store *SqliteXStore) BeginTx() (Tx, error) {
	tx, err := store.dbx.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("sqlitex.begin-tx: %w", err)
	}
	return &sqliteXStoreTx{
		store: store,
		tx:    tx,
	}, nil
}

func (stx *sqliteXStoreTx) SaveRaw(bucket string, key string, raw []byte) error {
	stmt := stx.tx.Stmt(stx.store.stmtInsertData)
	err := stx.store.saveRawStmtWithMeta(context.Background(), stmt, bucket, key, raw, nil)
	if err != nil {
		return fmt.Errorf("put-json-with-meta: %w", err)
	}
	return nil
}

func (stx *sqliteXStoreTx) SaveRawMany(bucket string, kvs []Tuple[string, []byte]) error {
	stmt := stx.tx.Stmt(stx.store.stmtInsertData)
	for _, t := range kvs {
		err := stx.store.saveRawStmtWithMeta(context.Background(), stmt, bucket, t.Key, t.Value, nil)
		if err != nil {
			return fmt.Errorf("put-json-with-meta: %w", err)
		}
	}
	return nil
}

func (stx *sqliteXStoreTx) Delete(bucket string, keys ...string) error {
	whereIn := make([]string, len(keys))
	for i, k := range keys {
		whereIn[i] = fmt.Sprintf("'%s'", k)
	}
	_, err := stx.tx.ExecContext(
		context.TODO(),
		fmt.Sprintf("DELETE FROM data WHERE bucket = ? AND key IN (%s);",
			strings.Join(whereIn, ", ")), bucket)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	err = stx.store.indexManager.onDelete(stx.tx, bucket, keys...)
	if err != nil {
		return fmt.Errorf("delete from indexes: %w", err)
	}
	return nil
}

func (stx *sqliteXStoreTx) UpdateIndex(bucketName string, idxName string, key string, values map[string]any) error {
	return stx.store.indexManager.updateIndex(stx.tx, bucketName, idxName, key, values)
}

func (store *SqliteXStore) saveRawStmtWithMeta(ctx context.Context, stmt *sql.Stmt, bucket string, key string, raw []byte, meta any) error {
	var metabs []byte
	var err error
	if meta != nil {
		metabs, err = json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("json.marshal meta: %w", err)
		}
	}
	_, err = stmt.ExecContext(ctx, bucket, key, formatTime(time.Now()), string(metabs), raw)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (store *SqliteXStore) FindRaw(bucket string, key string) ([]byte, query.Found, error) {
	row := store.stmtQueryValue.QueryRowContext(context.TODO(), bucket, key)
	var value sql.NullString
	err := row.Scan(&value)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("scan: %w", err)
	}
	return []byte(value.String), true, nil
}

func (store *SqliteXStore) FindRawMany(bucket string, keys ...string) (map[string][]byte, error) {
	rvs := map[string][]byte{}
	keyChunks := slicesx.Chunks(keys, 500)
	for _, keys := range keyChunks {
		args := append([]any{bucket}, slicesx.Anys(keys)...)
		keyPHs := strings.Join(slicesx.Repeat("?", len(keys)), ",")
		rows, err := store.dbx.Query(
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
			rvs[key.String] = []byte(rv.String)
		}
	}
	return rvs, nil
}

func (store *SqliteXStore) Keys(bucket string) ([]string, error) {
	rows, err := store.dbx.QueryContext(
		context.TODO(),
		"SELECT key FROM data WHERE bucket = ?;",
		bucket)
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

func (store *SqliteXStore) KeysPage(bucket string, skip, limit int, sort query.SortOrder) ([]string, error) {
	rows, err := store.dbx.QueryContext(
		context.TODO(),
		fmt.Sprintf("SELECT key FROM data WHERE bucket = ? ORDER BY key %s LIMIT ? OFFSET ?;", sort),
		bucket, limit, skip)
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

// Indexes
func (store *SqliteXStore) FindIndexDescriptor(bucketName string, idxName string) (IndexDescriptor, bool) {
	return store.indexManager.findIndexDescriptor(bucketName, idxName)
}

func (store *SqliteXStore) CreateIndex(bucketName string, idxName string, fields []IndexFieldDescriptor) error {
	return store.indexManager.createIndex(bucketName, idxName, fields...)
}

func (store *SqliteXStore) DeleteIndex(bucketName string, idxName string) error {
	return store.indexManager.deleteIndex(bucketName, idxName)
}

func (store *SqliteXStore) QueryKeys(bucketName string, indexName string, q query.Query) ([]string, error) {
	return store.indexManager.QueryKeys(bucketName, indexName, q)
}
