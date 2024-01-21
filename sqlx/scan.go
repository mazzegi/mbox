package sqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type Rows interface {
	Next() bool
	ColumnTypes() ([]*sql.ColumnType, error)
	Scan(...any) error
}

type ScanOptions struct {
	CaseInsensitive       bool
	DisallowUnknownFields bool
}

func NewScanner(rows Rows, options *ScanOptions) (*Scanner, error) {
	opts := ScanOptions{}
	if options != nil {
		opts = *options
	}
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("rows.column-types: %w", err)
	}
	sc := &Scanner{
		rows:    rows,
		columns: columns,
		options: opts,
	}
	// prebuild values
	sc.values = make([]any, len(columns))
	for i, colType := range sc.columns {
		sc.values[i] = nullType(colType)
	}
	return sc, nil
}

type Scanner struct {
	options ScanOptions
	rows    Rows
	columns []*sql.ColumnType
	values  []any
}

func (sc *Scanner) Next() bool {
	return sc.rows.Next()
}

func Scan[T any](sc *Scanner, rows *sql.Rows) (T, error) {
	var t T
	if reflect.TypeOf(t).Kind() != reflect.Struct {
		return t, fmt.Errorf("cannot scan into non-struct type %T", t)
	}
	// scan row into values
	err := sc.rows.Scan(sc.values...)
	if err != nil {
		return t, fmt.Errorf("rows.scan: %w", err)
	}

	rv := reflect.ValueOf(&t).Elem()
	for i, col := range sc.columns {
		// find related struct field
		toElem, ok := findStructFieldByName(rv, col.Name(), sc.options)
		if !ok {
			if sc.options.DisallowUnknownFields {
				return t, fmt.Errorf("no matching field in target for column %q", col.Name())
			}
			// otherwise
			continue
		}
		if !toElem.CanSet() {
			return t, fmt.Errorf("cannot set %s", toElem.Type().String())
		}
		conv, ok := sc.values[i].(converter)
		if !ok {
			return t, fmt.Errorf("source is not a converter but %T", sc.values[i])
		}
		err := conv.convert(toElem)
		if err != nil {
			return t, fmt.Errorf("convert: %w", err)
		}
	}

	return t, nil
}

func findStructFieldByName(sv reflect.Value, name string, options ScanOptions) (reflect.Value, bool) {
	ty := sv.Type()
	for i := 0; i < ty.NumField(); i++ {
		sf := ty.Field(i)
		if namesMatch(sf.Name, name, options) {
			return sv.Field(i), true
		}
		if sqlTag := sf.Tag.Get("sql"); sqlTag != "" {
			sqlName, _, _ := strings.Cut(sqlTag, ",")
			if namesMatch(sqlName, name, options) {
				return sv.Field(i), true
			}
		}
	}
	return reflect.Value{}, false
}

func namesMatch(s1, s2 string, options ScanOptions) bool {
	if options.CaseInsensitive {
		return strings.EqualFold(s1, s2)
	}
	return s1 == s2
}
