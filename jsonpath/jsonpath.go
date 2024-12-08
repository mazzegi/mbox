package jsonpath

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mazzegi/mbox/clock"
	"github.com/mazzegi/mbox/convert"
	"github.com/mazzegi/mbox/date"
)

var (
	ErrNotFound = fmt.Errorf("not-found")
	ErrBadArgs  = fmt.Errorf("bad-args")
)

func isRVZero(rv reflect.Value) bool {
	zv := reflect.Value{}
	return rv == zv
}

// match on either field-name or json-name
func structFieldByName(sv reflect.Value, name string) reflect.Value {
	ty := sv.Type()
	for i := 0; i < ty.NumField(); i++ {
		sf := ty.Field(i)
		if sf.Name == name {
			return sv.Field(i)
		}
		if js := sf.Tag.Get("json"); js != "" {
			jname, _, _ := strings.Cut(js, ",")
			if jname == name {
				return sv.Field(i)
			}
		}
	}
	return reflect.Value{}
}

func queryValue(in any, spath string) (reflect.Value, error) {
	path := strings.Split(spath, "/")
	crv := reflect.ValueOf(in)
	for _, elt := range path {
		if elt == "" {
			//skip empty
			continue
		}
		rty := crv.Type()
		kind := rty.Kind()
		if kind == reflect.Pointer {
			crv = crv.Elem()
			kind = crv.Kind()
		}

		switch kind {
		case reflect.Struct:
			crv = structFieldByName(crv, elt)
			if isRVZero(crv) {
				return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such struct field %q", elt))
			}
		case reflect.Slice:
			ix, err := strconv.ParseInt(elt, 10, 64)
			if err != nil {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("cannot parse %q as int for slice index: %w", elt, err))
			}

			if ix < 0 {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d", ix))
			}
			if ix >= int64(crv.Len()) {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d ( >= len=%d)", ix, crv.Len()))
			}
			crv = crv.Index(int(ix))
		case reflect.Map:
			if crv.Type().Key().Kind() != reflect.String {
				return reflect.Value{}, fmt.Errorf("cannot query non-string map keys. map keys are %s", crv.Type().Kind().String())
			}
			crv = crv.MapIndex(reflect.ValueOf(elt))
			if isRVZero(crv) {
				return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such map key %q", elt))
			}

		default:
			return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("cannot query reflect-kind %T", kind))
		}
	}
	return crv, nil
}

func Query(in any, spath string) (any, error) {
	rv, err := queryValue(in, spath)
	if err != nil {
		return nil, fmt.Errorf("query-value: %w", err)
	}
	return rv.Interface(), nil
}

func Set(in any, spath string, value any) error {
	path := strings.Split(spath, "/")
	crv := reflect.ValueOf(in)
	return set(crv, path, value)
}

func set(crv reflect.Value, path []string, value any) error {
	// if len(path) == 0 {
	// 	return fmt.Errorf("path is empty")
	// }

	//rty := crv.Type()
	kind := crv.Type().Kind()
	if kind == reflect.Pointer {
		crv = crv.Elem()
		kind = crv.Kind()
	}

	if len(path) > 0 {
		elt := path[0]
		switch kind {
		case reflect.Struct:
			crv = structFieldByName(crv, elt)
			if isRVZero(crv) {
				return errors.Join(ErrNotFound, fmt.Errorf("no such struct field %q", elt))
			}
		case reflect.Slice:
			ix, err := strconv.ParseInt(elt, 10, 64)
			if err != nil {
				return errors.Join(ErrBadArgs, fmt.Errorf("cannot parse %q as int for slice index: %w", elt, err))
			}

			if ix < 0 {
				return errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d", ix))
			}
			if ix >= int64(crv.Len()) {
				return errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d ( >= len=%d)", ix, crv.Len()))
			}
			crv = crv.Index(int(ix))
		case reflect.Map:
			if crv.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("cannot query non-string map keys. map keys are %s", crv.Type().Kind().String())
			}
			mix := reflect.ValueOf(elt)
			mapcrv := crv.MapIndex(mix)
			if isRVZero(crv) {
				return errors.Join(ErrNotFound, fmt.Errorf("no such map key %q", elt))
			}
			//
			path = path[1:]

			// create pointer type of target type, to set it
			pcrv := reflect.New(mapcrv.Type())
			pcrv.Elem().Set(mapcrv)
			err := set(pcrv, path, value)
			if err != nil {
				return fmt.Errorf("set map value: %w", err)
			}
			crv.SetMapIndex(mix, pcrv.Elem())
			return nil

		default:
			return errors.Join(ErrNotFound, fmt.Errorf("cannot query reflect-kind %T", kind.String()))
		}
	}

	if len(path) > 0 {
		path = path[1:]
		return set(crv, path, value)
	}

	if !crv.CanSet() {
		return fmt.Errorf("cannot set %v (%s)", crv.String(), crv.Type())
	}

	err := setValue(value, crv)
	if err != nil {
		return fmt.Errorf("set-value: %w", err)
	}

	return nil
}

func trySetValueReflect(value any, toRV reflect.Value) error {
	setVal := reflect.ValueOf(value)
	if !setVal.CanConvert(toRV.Type()) {
		return fmt.Errorf("cannot convert value of type %s to %s", setVal.Type().String(), toRV.Type().String())
	}
	setValConv := setVal.Convert(toRV.Type())
	toRV.Set(setValConv)
	return nil
}

func setValue(val any, toRV reflect.Value) error {
	switch toRV.Kind() {
	case reflect.Bool:
		convVal := convert.ToBool(val)
		return trySetValueReflect(convVal, toRV)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		iv, ok := convert.ToInt(val)
		if !ok {
			return fmt.Errorf("cannot convert %v to %q", val, toRV.Type().String())
		}
		return trySetValueReflect(iv, toRV)
	case reflect.Float32, reflect.Float64:
		iv, ok := convert.ToFloat(val)
		if !ok {
			return fmt.Errorf("cannot convert %v to %q", val, toRV.Type().String())
		}
		return trySetValueReflect(iv, toRV)
	case reflect.String:
		err := trySetValueReflect(fmt.Sprintf("%v", val), toRV)
		return err
	default:
		switch {
		case toRV.Type() == reflect.TypeOf(time.Time{}):
			t, err := time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", val))
			if err != nil {
				return fmt.Errorf("cannot convert string %T to time: %w", val, err)
			}
			return trySetValueReflect(t, toRV)
		case toRV.Type() == reflect.TypeOf(date.Date{}):
			d, err := date.Parse(fmt.Sprintf("%v", val))
			if err != nil {
				return fmt.Errorf("cannot convert string %T to date: %w", val, err)
			}
			return trySetValueReflect(d, toRV)
		case toRV.Type() == reflect.TypeOf(clock.Clock{}):
			cl, err := clock.Parse(fmt.Sprintf("%v", val))
			if err != nil {
				return fmt.Errorf("cannot convert string %T to clock: %w", val, err)
			}
			return trySetValueReflect(cl, toRV)
		}
	}
	return trySetValueReflect(val, toRV)
}
