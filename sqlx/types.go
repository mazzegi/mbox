package sqlx

import (
	"database/sql"
	"fmt"
	"reflect"
)

func nullType(colType *sql.ColumnType) any {
	sct := colType.ScanType()
	if sct == nil {
		return &nullString{}
	}

	switch colType.ScanType().Kind() {
	case reflect.Bool:
		return &nullBool{}
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		return &nullInt{}
	case reflect.Float32,
		reflect.Float64:
		return &nullFloat{}
	default:
		return &nullString{}
	}
}

type converter interface {
	convert(reflect.Value) error
}

type nullString struct {
	sql.NullString
}

type nullInt struct {
	sql.NullInt64
}

type nullFloat struct {
	sql.NullFloat64
}

type nullBool struct {
	sql.NullBool
}

func (n *nullString) convert(toElem reflect.Value) error {
	if !n.Valid {
		return nil
	}
	return convert(n.String, toElem)
}

func (n *nullInt) convert(toElem reflect.Value) error {
	if !n.Valid {
		return nil
	}
	return convert(n.Int64, toElem)
}

func (n *nullFloat) convert(toElem reflect.Value) error {
	if !n.Valid {
		return nil
	}
	return convert(n.Float64, toElem)
}

func (n *nullBool) convert(toElem reflect.Value) error {
	if !n.Valid {
		return nil
	}
	return convert(n.Bool, toElem)
}

func convert(src any, toElem reflect.Value) error {
	rvSrc := reflect.ValueOf(src)
	if !rvSrc.CanConvert(toElem.Type()) {
		return fmt.Errorf("cannot convert %T to %s", src, toElem.Type().String())
	}
	crv := rvSrc.Convert(toElem.Type())
	toElem.Set(crv)
	return nil
}
