package convert

import (
	"database/sql"
	"fmt"
)

func ToString(v any) string {
	if str, ok := v.(fmt.Stringer); ok {
		return str.String()
	}

	switch v := v.(type) {
	case sql.NullString:
		return v.String
	case sql.NullInt64:
		return ToString(v.Int64)
	case sql.NullInt32:
		return ToString(v.Int32)
	case sql.NullInt16:
		return ToString(v.Int16)
	case sql.NullFloat64:
		return ToString(v.Float64)
	case sql.NullBool:
		return ToString(v.Bool)
	case sql.NullTime:
		return ToString(v.Time)
	case *sql.NullString:
		return v.String
	case *sql.NullInt64:
		return ToString(v.Int64)
	case *sql.NullInt32:
		return ToString(v.Int32)
	case *sql.NullInt16:
		return ToString(v.Int16)
	case *sql.NullFloat64:
		return ToString(v.Float64)
	case *sql.NullBool:
		return ToString(v.Bool)
	case *sql.NullTime:
		return ToString(v.Time)
	default:
		return fmt.Sprintf("%v", v)
	}
}
