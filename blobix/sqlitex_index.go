package blobix

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/mazzegi/mbox/query"
	"github.com/mazzegi/mbox/slicesx"
	"github.com/mazzegi/mbox/sqlitex"
)

type sqliteXIndexKey struct {
	bucketName string
	indexName  string
}

func (k sqliteXIndexKey) String() string {
	return fmt.Sprintf("bucket:%s,name:%s", k.bucketName, k.indexName)
}

type sqliteXIndexMeta struct {
	Bucket    string       `json:"bucket"`
	Name      string       `json:"name"`
	TableName string       `json:"table_name"`
	Fields    []IndexField `json:"fields"`
}

func (m sqliteXIndexMeta) key() sqliteXIndexKey {
	return sqliteXIndexKey{bucketName: m.Bucket, indexName: m.Name}
}

func (m sqliteXIndexMeta) containsField(name string) bool {
	for _, f := range m.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

type SqliteXIndex struct {
	meta sqliteXIndexMeta
}

func (ix SqliteXIndex) Fields() []string {
	return slicesx.Map(ix.meta.Fields, func(f IndexField) string {
		return f.Name
	})
}

//

func NewSqliteXIndexManager(dbx *sqlitex.DB) (*SqliteXIndexManager, error) {
	im := &SqliteXIndexManager{
		dbx:     dbx,
		indexes: make(map[sqliteXIndexKey]sqliteXIndexMeta),
	}

	// load indexes
	rows, err := dbx.QueryContext(
		context.TODO(),
		"SELECT bucket, name, meta FROM index_data;",
	)
	if err != nil {
		return nil, fmt.Errorf("query-index-data: %w", err)
	}
	defer rows.Close()
	var (
		bucket string
		name   string
		meta   string
	)
	for rows.Next() {
		err = rows.Scan(&bucket, &name, &meta)
		if err != nil {
			return nil, fmt.Errorf("scan-index-data: %w", err)
		}
		var ixMeta sqliteXIndexMeta
		err = json.Unmarshal([]byte(meta), &ixMeta)
		if err != nil {
			return nil, fmt.Errorf("unmarshal index-meta: %w", err)
		}
		im.indexes[ixMeta.key()] = ixMeta
	}

	return im, nil
}

type SqliteXIndexManager struct {
	dbx     *sqlitex.DB
	indexes map[sqliteXIndexKey]sqliteXIndexMeta
}

func (im *SqliteXIndexManager) findIndexMeta(bucketName string, name string) (sqliteXIndexMeta, bool) {
	key := sqliteXIndexKey{bucketName: bucketName, indexName: name}
	idxMeta, ok := im.indexes[key]
	return idxMeta, ok
}

func (im *SqliteXIndexManager) AddIndex(bucketName string, name string, fields ...IndexField) (Index, error) {
	key := sqliteXIndexKey{bucketName: bucketName, indexName: name}
	idxMeta, ok := im.indexes[key]
	if !ok {
		// not found - create new one
		return im.createIndex(bucketName, name, fields...)
	}
	reqIdxMeta := sqliteXIndexMeta{
		Bucket:    bucketName,
		Name:      name,
		TableName: im.indexTabName(bucketName, name),
		Fields:    fields,
	}
	if !reflect.DeepEqual(idxMeta, reqIdxMeta) {
		return nil, fmt.Errorf("index already exists - but has different meta data")
	}
	ix := SqliteXIndex{meta: idxMeta}
	return ix, nil
}

func (im *SqliteXIndexManager) Index(bucketName string, name string) (Index, error) {
	key := sqliteXIndexKey{bucketName: bucketName, indexName: name}
	idxMeta, ok := im.indexes[key]
	if !ok {
		return nil, fmt.Errorf("no such index: %s", key)
	}
	ix := SqliteXIndex{meta: idxMeta}
	return ix, nil
}

func (im *SqliteXIndexManager) indexTabName(bucketName string, idxName string) string {
	return fmt.Sprintf("_index_%s_%s", bucketName, idxName)
}

func (im *SqliteXIndexManager) createIndex(bucketName string, name string, fields ...IndexField) (Index, error) {
	idxMeta := sqliteXIndexMeta{
		Bucket:    bucketName,
		Name:      name,
		TableName: im.indexTabName(bucketName, name),
		Fields:    fields,
	}
	bs, err := json.Marshal(idxMeta)
	if err != nil {
		//örks!
		return nil, fmt.Errorf("json.marshal index meta")
	}

	tx, err := im.dbx.BeginTx(context.TODO(), nil)
	if err != nil {
		return nil, fmt.Errorf("begin-tx")
	}

	sqliteDataTypeFromIndexFieldType := func(ft IndexFieldType) string {
		switch ft {
		case IndexFieldString:
			return "TEXT"
		case IndexFieldInt:
			return "INTEGER"
		case IndexFieldFloat:
			return "REAL"
		default:
			return "TEXT"
		}
	}

	// create index table
	var colList []string
	for _, field := range fields {
		typ := sqliteDataTypeFromIndexFieldType(field.Type)
		colList = append(colList, fmt.Sprintf("%s %s", field.Name, typ))
	}

	tabName := im.indexTabName(bucketName, name)
	createTabStmt := fmt.Sprintf(`
		CREATE TABLE %s (
			key 	TEXT,
			%s,
			PRIMARY KEY (key)
		)
	`, tabName, strings.Join(colList, ",\n"))
	_, err = tx.ExecContext(context.TODO(), createTabStmt)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("create index table: %w", err)
	}

	// create indexes
	for _, field := range fields {
		idxName := fmt.Sprintf("ix_index_%s_%s_%s", bucketName, name, field.Name)
		createIdxStmt := fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS %s ON %s (%s);
		`, idxName, tabName, field.Name)
		_, err = tx.ExecContext(context.TODO(), createIdxStmt)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("create index: %w", err)
		}
	}

	// write index metadata
	_, err = tx.ExecContext(
		context.TODO(),
		"INSERT INTO index_data (bucket, name, meta) VALUES(?,?,?);",
		bucketName, name, string(bs),
	)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("exec-create-index: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit-tx: %w", err)
	}
	im.indexes[idxMeta.key()] = idxMeta
	ix := SqliteXIndex{meta: idxMeta}
	return ix, nil
}

// func (im *SqliteXIndexManager) rebuildIndex(bucketName string, name string) error {
// 	key := sqliteXIndexKey{bucketName: bucketName, indexName: name}
// 	idxMeta, ok := im.indexes[key]
// 	if !ok {
// 		return fmt.Errorf("no such index: %s", key)
// 	}
// 	// purge table
// 	_, err := im.dbx.Exec(`DELETE FROM %s;`, idxMeta.TableName)
// 	if err != nil {
// 		return fmt.Errorf("delete index table: %w", err)
// 	}
// 	//

// }

func (im *SqliteXIndexManager) deleteIndex(bucketName string, name string) error {
	tabName := im.indexTabName(bucketName, name)
	dropTabStmt := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
	`, tabName)
	_, err := im.dbx.Exec(dropTabStmt)
	if err != nil {
		return fmt.Errorf("drop index table: %w", err)
	}

	_, err = im.dbx.ExecContext(
		context.TODO(),
		"DELETE FROM index_data WHERE bucket=? AND name=?;",
		bucketName, name,
	)
	if err != nil {
		return fmt.Errorf("delete index data: %w", err)
	}

	key := sqliteXIndexKey{bucketName: bucketName, indexName: name}
	delete(im.indexes, key)

	return nil
}

func (im *SqliteXIndexManager) updateIndexes(tx *sql.Tx, bucketName string, key string, value any) error {
	for idxKey, idxMeta := range im.indexes {
		if idxKey.bucketName != bucketName {
			continue
		}
		err := im.updateIndex(tx, idxMeta, key, value)
		if err != nil {
			return fmt.Errorf("update-index %s: %w", idxKey, err)
		}
	}
	return nil
}

func (im *SqliteXIndexManager) onClear(tx *sql.Tx, bucketName string) error {
	for idxKey, idxMeta := range im.indexes {
		if idxKey.bucketName != bucketName {
			continue
		}
		err := im.onIndexClear(tx, idxMeta)
		if err != nil {
			return fmt.Errorf("on-index-delete %s: %w", idxKey, err)
		}
	}
	return nil
}

func (im *SqliteXIndexManager) onDelete(tx *sql.Tx, bucketName string, keys ...string) error {
	for idxKey, idxMeta := range im.indexes {
		if idxKey.bucketName != bucketName {
			continue
		}
		err := im.onIndexDelete(tx, idxMeta, keys...)
		if err != nil {
			return fmt.Errorf("on-index-delete %s: %w", idxKey, err)
		}
	}
	return nil
}

func (im *SqliteXIndexManager) onIndexClear(tx *sql.Tx, idxMeta sqliteXIndexMeta) error {
	_, err := tx.ExecContext(
		context.TODO(),
		fmt.Sprintf("DELETE FROM %s;", idxMeta.TableName),
	)
	if err != nil {
		return fmt.Errorf("exec insert-into-index %q: %w", idxMeta.key(), err)
	}
	return nil
}

func (im *SqliteXIndexManager) onIndexDelete(tx *sql.Tx, idxMeta sqliteXIndexMeta, keys ...string) error {
	inList := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, key := range keys {
		param := fmt.Sprintf("key%d", i+1)
		inList[i] = ":" + param
		args[i] = sql.Named(param, key)
	}

	_, err := tx.ExecContext(
		context.TODO(),
		fmt.Sprintf("DELETE FROM %s WHERE key IN (%s);", idxMeta.TableName, strings.Join(inList, ", ")),
		args...,
	)
	if err != nil {
		return fmt.Errorf("exec insert-into-index %q: %w", idxMeta.key(), err)
	}
	return nil
}

func (im *SqliteXIndexManager) updateIndex(tx *sql.Tx, idxMeta sqliteXIndexMeta, key string, value any) error {
	colList := []string{"key"}
	placeholderList := []string{":key"}
	for _, field := range idxMeta.Fields {
		colList = append(colList, field.Name)
		placeholderList = append(placeholderList, ":"+field.Name)
	}
	args := []any{
		sql.Named("key", key),
	}
	for _, field := range idxMeta.Fields {
		val, err := JSONQuery(value, field.Path)
		if err != nil {
			//log.Warnf("json-query %q: %v", field.Path, err)
			args = append(args, sql.Named(field.Name, nil))
			continue
		}

		if tim, ok := val.(time.Time); ok {
			// if val is time ...
			val = tim.Format(time.RFC3339Nano)
		} else if reflect.TypeOf(val).Kind() == reflect.Struct ||
			// check if val is struct
			reflect.TypeOf(val).Kind() == reflect.Slice {
			stringer, ok := val.(fmt.Stringer)
			if !ok {
				return fmt.Errorf("cannot index struct %T which is not a stringer", val)
			}
			val = stringer.String()
		}

		args = append(args, sql.Named(field.Name, val))
	}

	//
	_, err := tx.ExecContext(
		context.TODO(),
		fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s);", idxMeta.TableName, strings.Join(colList, ", "), strings.Join(placeholderList, ", ")),
		args...,
	)
	if err != nil {
		return fmt.Errorf("exec insert-into-index %q: %w", idxMeta.key(), err)
	}
	return nil
}

func sqlComparator(qc query.Comparator) string {
	switch qc {
	case query.ComparatorEqual:
		return "="
	case query.ComparatorNotEqual:
		return "!="
	case query.ComparatorGreater:
		return ">"
	case query.ComparatorGreaterEqual:
		return ">="
	case query.ComparatorLess:
		return "<"
	case query.ComparatorLessEqual:
		return "<="
	case query.ComparatorLike:
		return "like"
	default:
		return "="
	}
}

func (im *SqliteXIndexManager) QueryDistinct(bucketName string, indexName string, field string) ([]string, error) {
	ixkey := sqliteXIndexKey{bucketName: bucketName, indexName: indexName}
	idxMeta, ok := im.indexes[ixkey]
	if !ok {
		return nil, fmt.Errorf("no such index: %s", ixkey)
	}

	q := fmt.Sprintf("SELECT DISTINCT %s FROM %s ORDER BY %s ASC;", field, idxMeta.TableName, field)
	rows, err := im.dbx.QueryContext(context.TODO(), q)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", q, err)
	}
	defer rows.Close()

	vals := []string{}
	var val sql.NullString
	for rows.Next() {
		err = rows.Scan(&val)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		vals = append(vals, val.String)
	}
	return vals, nil
}

func (im *SqliteXIndexManager) Query(bucketName string, indexName string, lo query.LimitOffset, fields []query.Condition, sorts []query.Sort, search query.Search) ([]string, error) {
	ixkey := sqliteXIndexKey{bucketName: bucketName, indexName: indexName}
	idxMeta, ok := im.indexes[ixkey]
	if !ok {
		return nil, fmt.Errorf("no such index: %s", ixkey)
	}

	// whereRValue := func(val any, paramName string) string {
	// 	switch val.(type) {
	// 	case int:
	// 		return fmt.Sprintf("CAST(:%s AS INTEGER)", paramName)
	// 	default:
	// 		return fmt.Sprintf(":%s", paramName)
	// 	}
	// }

	wheres := []string{}
	args := []any{}
	paramIdx := 0
	for _, fq := range fields {
		if !idxMeta.containsField(fq.Name) {
			return nil, fmt.Errorf("index %s contains no field with name %q", indexName, fq.Name)
		}
		if fq.Comp == query.ComparatorIn {
			// this one is special - for the moment only allow string slices
			vals, ok := fq.Value.([]string)
			if !ok {
				return nil, fmt.Errorf("only string slices are allowed for IN queries")
			}
			inParamNames := []string{}
			for _, val := range vals {
				paramName := fmt.Sprintf("p%03d", paramIdx)
				paramIdx++
				inParamNames = append(inParamNames, ":"+paramName)
				args = append(args, sql.Named(paramName, val))
			}
			wheres = append(wheres, fmt.Sprintf("%s IN (%s)", fq.Name, strings.Join(inParamNames, ",")))

			continue
		}

		//-- check if val is struct
		paramName := fmt.Sprintf("p%03d", paramIdx)
		paramIdx++

		//wheres = append(wheres, fmt.Sprintf("%s %s %s", fq.Name, sqlComparator(fq.Comp), whereRValue(fq.Value, paramName)))
		wheres = append(wheres, fmt.Sprintf("%s %s :%s", fq.Name, sqlComparator(fq.Comp), paramName))
		var val any
		if fq.Comp == query.ComparatorLike {
			val = fmt.Sprintf("%%%v%%", fq.Value)
		} else {
			val = fq.Value
		}

		// check if val is struct
		if reflect.TypeOf(val).Kind() == reflect.Struct {
			stringer, ok := val.(fmt.Stringer)
			if !ok {
				return nil, fmt.Errorf("cannot filter for struct %T which is not a stringer", val)
			}
			val = stringer.String()
		}

		args = append(args, sql.Named(paramName, val))
	}
	if len(search.Fields) > 0 && search.Value != "" {
		//var searchs []string
		searchWords := strings.Split(search.Value, " ")
		searchWords = slicesx.Map(searchWords, strings.TrimSpace)
		searchWords = slices.DeleteFunc(searchWords, func(s string) bool { return s == "" })
		searchWords = slicesx.Dedup(searchWords)

		paramIdx := 0
		var wordsSearchs []string
		for _, word := range searchWords {
			var fieldsSearchs []string
			for _, sf := range search.Fields {
				paramName := fmt.Sprintf("search%03d", paramIdx)
				fieldsSearchs = append(fieldsSearchs, fmt.Sprintf("(%s like :%s)", sf, paramName))
				//args = append(args, sql.Named(paramName, fmt.Sprintf("%%%v%%", search.Value)))
				args = append(args, sql.Named(paramName, fmt.Sprintf("%%%v%%", word)))
				paramIdx++
			}
			wordsSearchs = append(wordsSearchs, "("+strings.Join(fieldsSearchs, " OR ")+")")
		}
		wheres = append(wheres, "("+strings.Join(wordsSearchs, " AND ")+")")
	}

	orderBys := []string{}
	for _, fs := range sorts {
		orderBys = append(orderBys, fmt.Sprintf("%s %s", fs.Name, fs.Order))
	}

	args = append(args,
		sql.Named("limit", lo.Limit),
		sql.Named("offset", lo.Offset),
	)
	var where string
	if len(wheres) > 0 {
		where = " WHERE " + strings.Join(wheres, " AND ")
	}
	var orderBy string
	if len(orderBys) > 0 {
		orderBy = " ORDER BY " + strings.Join(orderBys, ", ")
	}

	q := fmt.Sprintf("SELECT key FROM %s %s %s LIMIT :limit OFFSET :offset;", idxMeta.TableName, where, orderBy)
	rows, err := im.dbx.QueryContext(
		context.TODO(),
		q,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", q, err)
	}
	defer rows.Close()

	keys := []string{}
	var key string
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, key)
	}

	// log.Debugf("query: %s", q)
	// log.Debugf("lo: %v", lo)
	// log.Debugf("fields: %v", fields)
	// log.Debugf("result: %d keys", len(keys))

	return keys, nil
}
