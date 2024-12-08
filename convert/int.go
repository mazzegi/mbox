// Package convert contains functions to convert interface{} values into various other types
package convert

import (
	"database/sql"
	"strconv"
)

// ToInt tries to convert v into an int
func ToInt(v interface{}) (int, bool) {
	switch v := v.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	case bool:
		switch v {
		case true:
			return 1, true
		default:
			return 0, true
		}
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return int(n), true

	case sql.NullString:
		return ToInt(v.String)
	case sql.NullInt64:
		return ToInt(v.Int64)
	case sql.NullInt32:
		return ToInt(v.Int32)
	case sql.NullInt16:
		return ToInt(v.Int16)
	case sql.NullFloat64:
		return ToInt(v.Float64)
	case sql.NullBool:
		return ToInt(v.Bool)
	case sql.NullTime:
		return ToInt(v.Time)
	case *sql.NullString:
		return ToInt(v.String)
	case *sql.NullInt64:
		return ToInt(v.Int64)
	case *sql.NullInt32:
		return ToInt(v.Int32)
	case *sql.NullInt16:
		return ToInt(v.Int16)
	case *sql.NullFloat64:
		return ToInt(v.Float64)
	case *sql.NullBool:
		return ToInt(v.Bool)
	case *sql.NullTime:
		return ToInt(v.Time)

	default:
		return 0, false
	}
}
