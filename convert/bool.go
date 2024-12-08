package convert

import "database/sql"

// ToBool tries to convert v into an int
func ToBool(v interface{}) bool {
	switch v := v.(type) {
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	case bool:
		return v
	case string:
		switch v {
		case "true", "1", "True", "t", "T", "on", "On", "yes", "Yes":
			return true
		default:
			return false
		}

	case sql.NullString:
		return ToBool(v.String)
	case sql.NullInt64:
		return ToBool(v.Int64)
	case sql.NullInt32:
		return ToBool(v.Int32)
	case sql.NullInt16:
		return ToBool(v.Int16)
	case sql.NullFloat64:
		return ToBool(v.Float64)
	case sql.NullBool:
		return ToBool(v.Bool)
	case sql.NullTime:
		return ToBool(v.Time)
	case *sql.NullString:
		return ToBool(v.String)
	case *sql.NullInt64:
		return ToBool(v.Int64)
	case *sql.NullInt32:
		return ToBool(v.Int32)
	case *sql.NullInt16:
		return ToBool(v.Int16)
	case *sql.NullFloat64:
		return ToBool(v.Float64)
	case *sql.NullBool:
		return ToBool(v.Bool)
	case *sql.NullTime:
		return ToBool(v.Time)

	default:
		return false
	}
}
