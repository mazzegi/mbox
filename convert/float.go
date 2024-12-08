package convert

import (
	"database/sql"
	"strconv"
	"strings"
)

// ToFloat tries to convert v into a float
func ToFloat(v interface{}) (float64, bool) {
	switch v := v.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return float64(v), true
	case bool:
		switch v {
		case true:
			return 1, true
		default:
			return 0, true
		}
	case string:
		// in case it comes with german float notation
		v = strings.ReplaceAll(v, ",", ".")
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true

	case sql.NullString:
		return ToFloat(v.String)
	case sql.NullInt64:
		return ToFloat(v.Int64)
	case sql.NullInt32:
		return ToFloat(v.Int32)
	case sql.NullInt16:
		return ToFloat(v.Int16)
	case sql.NullFloat64:
		return ToFloat(v.Float64)
	case sql.NullBool:
		return ToFloat(v.Bool)
	case sql.NullTime:
		return ToFloat(v.Time)
	case *sql.NullString:
		return ToFloat(v.String)
	case *sql.NullInt64:
		return ToFloat(v.Int64)
	case *sql.NullInt32:
		return ToFloat(v.Int32)
	case *sql.NullInt16:
		return ToFloat(v.Int16)
	case *sql.NullFloat64:
		return ToFloat(v.Float64)
	case *sql.NullBool:
		return ToFloat(v.Bool)
	case *sql.NullTime:
		return ToFloat(v.Time)

	default:
		return 0, false
	}
}
